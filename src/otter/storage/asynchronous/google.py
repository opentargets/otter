"""Google Cloud Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from aiohttp import ServerTimeoutError
from gcloud.aio.storage import Storage as GCSClient
from loguru import logger

from otter.storage.asynchronous.model import AsyncStorage
from otter.storage.model import Revision, StatResult
from otter.storage.settings import GoogleStorageSettings
from otter.storage.storage_context import get_storage_context
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError

REQUEST_TIMEOUT = 300


class AsyncGoogleStorage(AsyncStorage):
    """Google Cloud Storage class using gcloud-aio-storage for async operations.

    This storage backend supports the following context settings (via storage_context):
        - billing_project: Project ID for requester-pays bucket access

    See :class:`otter.storage.settings.GoogleStorageSettings` for detailed documentation of available settings.
    """

    def __init__(self) -> None:
        self._client: GCSClient | None = None

    @classmethod
    def get_context_settings_model(cls) -> type[GoogleStorageSettings]:
        """Get the settings model for Google Cloud Storage context validation.

        :return: GoogleStorageSettings model class.
        :rtype: type[GoogleStorageSettings]
        """
        return GoogleStorageSettings

    def _get_client(self) -> GCSClient:
        if self._client is None:
            self._client = GCSClient()
        return self._client

    def _request_headers(self, headers: dict[str, str] | None = None) -> dict[str, str] | None:
        ctx = get_storage_context()
        if not ctx or not (billing_project := ctx.get('billing_project')):
            return headers

        merged = dict(headers or {})
        merged['x-goog-user-project'] = billing_project
        return merged

    def _request_params(self, params: dict[str, str] | None = None) -> dict[str, str] | None:
        ctx = get_storage_context()
        if not ctx or not (billing_project := ctx.get('billing_project')):
            return params

        merged = dict(params or {})
        merged['userProject'] = billing_project
        return merged

    async def _list_blob_names(self, bucket_name: str, prefix: str, match_glob: str = '') -> list[str]:
        client = self._get_client()
        params = self._request_params({
            'delimiter': '',
            'matchGlob': match_glob,
            'pageToken': '',
            'prefix': prefix,
        })
        items: list[str] = []
        while True:
            content = await client.list_objects(
                bucket_name,
                params=params,
                headers=self._request_headers(),
            )
            items.extend([obj['name'] for obj in content.get('items', [])])
            assert params is not None
            params['pageToken'] = content.get('nextPageToken', '')
            if not params['pageToken']:
                break
        return items

    @property
    def name(self) -> str:
        return 'Google Cloud Storage'

    @classmethod
    def _parse_uri(cls, uri: str) -> tuple[str, str]:
        uri_parts = uri.replace('gs://', '').split('/', 1)
        bucket_name = uri_parts[0]
        prefix = uri_parts[1] if len(uri_parts) > 1 else ''
        return bucket_name, prefix

    async def stat(self, location: str) -> StatResult:
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()

        # root of the bucket
        if not blob_name:
            return StatResult(
                is_dir=True,
                is_reg=False,
                size=0,
            )
        # regular blob
        try:
            metadata = await client.download_metadata(
                bucket_name,
                blob_name,
                headers=self._request_headers(),
            )
            logger.trace(f'got metadata for blob {location}')
            return StatResult(
                is_dir=False,
                is_reg=True,
                size=int(metadata.get('size', 0)),
                revision=metadata.get('generation'),
            )
        # maybe a prefix if blobs exist underneath
        except Exception:
            try:
                prefix = blob_name if blob_name.endswith('/') else f'{blob_name}/'
                blobs = await self._list_blob_names(bucket_name, prefix=prefix)
                if blobs:
                    logger.trace(f'got metadata for prefix {location}')
                    return StatResult(
                        is_dir=True,
                        is_reg=False,
                        size=0,
                    )
            except Exception as e:
                raise StorageError(f'error getting metadata for {location}: {e}')
        # not found
        raise NotFoundError(thing=location)

    async def glob(self, location: str, pattern: str = '*') -> list[str]:
        bucket_name, prefix = self._parse_uri(location)
        if prefix.endswith('/'):
            search_prefix = f'{prefix}{pattern}'
        elif prefix:
            search_prefix = f'{prefix}/{pattern}'
        full_pattern = f'gs://{bucket_name}/{search_prefix}'

        try:
            blobs = await self._list_blob_names(bucket_name, prefix=search_prefix, match_glob=pattern)
        except Exception as e:
            raise StorageError(f'error listing blobs in {location}: {e}')

        if not blobs:
            logger.warning(f'no files found matching glob {full_pattern}')

        return [f'gs://{bucket_name}/{name}' for name in blobs]

    async def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()

        try:
            while True:
                metadata = await client.download_metadata(
                    bucket_name,
                    blob_name,
                    headers=self._request_headers(),
                )
                revision = metadata.get('generation')
                data = await client.download(
                    bucket_name,
                    blob_name,
                    headers=self._request_headers(),
                    timeout=REQUEST_TIMEOUT,
                )
                new_metadata = await client.download_metadata(
                    bucket_name,
                    blob_name,
                    headers=self._request_headers(),
                )
                new_revision = new_metadata.get('generation')
                if revision is None or revision == new_revision:
                    logger.debug(f'downloaded {location}')
                    return data, revision
                logger.info(f'{location} modified during read, retrying')
        except (TimeoutError, ServerTimeoutError) as e:
            raise TimeoutError(f'timeout downloading {location}: {e}')
        except Exception as e:
            if 'Not Found' in str(e) or '404' in str(e):
                raise NotFoundError(thing=location)
            raise StorageError(f'error downloading {location}: {e}')

    async def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        data, revision = await self.read(location)
        try:
            return data.decode(encoding), revision
        except UnicodeDecodeError as e:
            raise StorageError(f'error decoding {location}: {e}')

    async def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision | None = None,
    ) -> Revision:
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()
        headers = None
        if expected_revision is not None:
            headers = {'x-goog-if-generation-match': str(expected_revision)}

        try:
            metadata = await client.upload(
                bucket_name,
                blob_name,
                data,
                parameters=self._request_params(),
                headers=self._request_headers(headers),
            )
            logger.debug(f'uploaded to {location}')
            return metadata.get('generation')
        except Exception as e:
            if hasattr(e, 'status') and e.status == 412:
                raise PreconditionFailedError(f'generation mismatch at {location}')
            raise StorageError(f'error uploading to {location}: {e}')

    async def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision | None = None,
    ) -> Revision:
        return await self.write(
            location,
            data.encode(encoding),
            expected_revision=expected_revision,
        )

    async def copy_within(self, src: str, dst: str) -> Revision:
        src_bucket, src_blob = self._parse_uri(src)
        dst_bucket, dst_blob = self._parse_uri(dst)
        client = self._get_client()

        try:
            await client.copy(
                src_bucket,
                src_blob,
                dst_bucket,
                new_name=dst_blob,
                params=self._request_params(),
                headers=self._request_headers(),
            )
            logger.debug(f'copied {src} to {dst}')
            # Get generation of new blob
            metadata = await client.download_metadata(
                dst_bucket,
                dst_blob,
                headers=self._request_headers(),
            )
            return metadata.get('generation')
        except Exception as e:
            if 'Not Found' in str(e) or '404' in str(e):
                raise NotFoundError(thing=src)
            raise StorageError(f'error copying {src} to {dst}: {e}')

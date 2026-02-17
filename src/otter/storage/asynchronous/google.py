"""Google Cloud Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from aiohttp import ServerTimeoutError
from gcloud.aio.storage import Bucket
from gcloud.aio.storage import Storage as GCSClient
from loguru import logger

from otter.storage.asynchronous.model import AsyncStorage
from otter.storage.model import Revision, StatResult
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError

REQUEST_TIMEOUT = 300


class AsyncGoogleStorage(AsyncStorage):
    """Google Cloud Storage class using gcloud-aio-storage for async operations."""

    def __init__(self) -> None:
        self._client: GCSClient | None = None

    def _get_client(self) -> GCSClient:
        if self._client is None:
            self._client = GCSClient()
        return self._client

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
            metadata = await client.download_metadata(bucket_name, blob_name)
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
                bucket = Bucket(client, bucket_name)
                prefix = blob_name if blob_name.endswith('/') else f'{blob_name}/'
                blobs = await bucket.list_blobs(prefix=prefix)
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
        client = self._get_client()
        bucket = Bucket(client, bucket_name)
        if prefix.endswith('/'):
            search_prefix = f'{prefix}{pattern}'
        elif prefix:
            search_prefix = f'{prefix}/{pattern}'
        full_pattern = f'gs://{bucket_name}/{search_prefix}'

        try:
            blobs = await bucket.list_blobs(prefix=search_prefix, match_glob=pattern)
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
                metadata = await client.download_metadata(bucket_name, blob_name)
                revision = metadata.get('generation')
                data = await client.download(bucket_name, blob_name, timeout=REQUEST_TIMEOUT)
                new_metadata = await client.download_metadata(bucket_name, blob_name)
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
                headers=headers,
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
            await client.copy(src_bucket, src_blob, dst_bucket, new_name=dst_blob)
            logger.debug(f'copied {src} to {dst}')
            # Get generation of new blob
            metadata = await client.download_metadata(dst_bucket, dst_blob)
            return metadata.get('generation')
        except Exception as e:
            if 'Not Found' in str(e) or '404' in str(e):
                raise NotFoundError(thing=src)
            raise StorageError(f'error copying {src} to {dst}: {e}')

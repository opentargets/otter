"""Google Cloud Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from loguru import logger

from otter.storage.synchronous.model import Revision, StatResult, Storage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError


class GoogleStorage(Storage):
    """Google Cloud Storage class using google-cloud-storage for operations."""

    def __init__(self) -> None:
        self._client: storage.Client | None = None

    def _get_client(self) -> storage.Client:
        if self._client is None:
            self._client = storage.Client()
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

    def stat(self, location: str) -> StatResult:
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
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.reload()
            logger.trace(f'got metadata for blob {location}')
            return StatResult(
                is_dir=False,
                is_reg=True,
                size=blob.size or 0,
                revision=str(blob.generation) if blob.generation else None,
            )
        # maybe a prefix if blobs exist underneath
        except NotFound:
            try:
                bucket = client.bucket(bucket_name)
                prefix = blob_name if blob_name.endswith('/') else f'{blob_name}/'
                blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
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

    def glob(self, location: str, pattern: str = '*') -> list[str]:
        bucket_name, prefix = self._parse_uri(location)
        client = self._get_client()
        bucket = client.bucket(bucket_name)

        if prefix.endswith('/'):
            search_prefix = prefix
        elif prefix:
            search_prefix = f'{prefix}/'
        else:
            search_prefix = ''

        full_pattern = f'gs://{bucket_name}/{search_prefix}{pattern}'

        try:
            blobs = bucket.list_blobs(prefix=search_prefix, match_glob=pattern)
            blob_names = [blob.name for blob in blobs]
        except Exception as e:
            raise StorageError(f'error listing blobs in {location}: {e}')

        if not blob_names:
            logger.warning(f'no files found matching glob {full_pattern}')

        return [f'gs://{bucket_name}/{name}' for name in blob_names]

    def open(
        self,
        location: str,
        mode: str = 'r',
    ):
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            return blob.open(mode)
        except NotFound:
            raise NotFoundError(thing=location)
        except Exception as e:
            raise StorageError(f'error opening {location}: {e}')

    def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()
        bucket = client.bucket(bucket_name)

        try:
            while True:
                blob = bucket.blob(blob_name)
                blob.reload()
                revision = str(blob.generation) if blob.generation else None
                data = blob.download_as_bytes()
                blob.reload()
                new_revision = str(blob.generation) if blob.generation else None
                if revision is None or revision == new_revision:
                    logger.debug(f'downloaded {location}')
                    return data, revision
                logger.info(f'{location} modified during read, retrying')
        except NotFound:
            raise NotFoundError(thing=location)
        except Exception as e:
            if 'timeout' in str(e).lower():
                raise TimeoutError(f'timeout downloading {location}: {e}')
            raise StorageError(f'error downloading {location}: {e}')

    def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        data, revision = self.read(location)
        try:
            return data.decode(encoding), revision
        except UnicodeDecodeError as e:
            raise StorageError(f'error decoding {location}: {e}')

    def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision | None = None,
    ) -> Revision:
        bucket_name, blob_name = self._parse_uri(location)
        client = self._get_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            if expected_revision is not None:
                blob.upload_from_string(
                    data,
                    if_generation_match=int(expected_revision),
                )
            else:
                blob.upload_from_string(data)
            logger.debug(f'uploaded to {location}')
            return str(blob.generation) if blob.generation else None
        except PreconditionFailed:
            raise PreconditionFailedError(f'generation mismatch at {location}')
        except Exception as e:
            raise StorageError(f'error uploading to {location}: {e}')

    def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision | None = None,
    ) -> Revision:
        return self.write(
            location,
            data.encode(encoding),
            expected_revision=expected_revision,
        )

    def copy_within(self, src: str, dst: str) -> Revision:
        src_bucket_name, src_blob_name = self._parse_uri(src)
        dst_bucket_name, dst_blob_name = self._parse_uri(dst)
        client = self._get_client()

        try:
            src_bucket = client.bucket(src_bucket_name)
            src_blob = src_bucket.blob(src_blob_name)
            dst_bucket = client.bucket(dst_bucket_name)

            dst_blob = src_bucket.copy_blob(src_blob, dst_bucket, dst_blob_name)
            logger.debug(f'copied {src} to {dst}')
            return str(dst_blob.generation) if dst_blob.generation else None
        except NotFound:
            raise NotFoundError(thing=src)
        except Exception as e:
            raise StorageError(f'error copying {src} to {dst}: {e}')

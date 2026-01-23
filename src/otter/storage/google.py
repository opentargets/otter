"""Google Cloud Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from google import auth
from google.api_core.exceptions import GoogleAPICallError, PreconditionFailed
from google.auth import exceptions as auth_exceptions
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.cloud.exceptions import NotFound
from loguru import logger

from otter.storage.model import Revision, StatResult, Storage
from otter.util import fs
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError

if TYPE_CHECKING:
    from typing import IO

    from google.auth.credentials import Credentials

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/spreadsheets',
]

_cached_google_state: dict | None = None


class GoogleStorage(Storage):
    """Google Cloud Storage class.

    This class implements the Storage interface for Google Cloud Storage.

    :ivar credentials: The Google Cloud Storage credentials.
    :vartype credentials: google.auth.credentials.Credentials
    :ivar client: The Google Cloud Storage client.
    :vartype client: google.cloud.storage.client.Client
    """

    @property
    def name(self) -> str:
        return 'Google Cloud Storage'

    def __init__(self) -> None:
        global _cached_google_state  # noqa: PLW0603
        if _cached_google_state is not None:
            self.client = _cached_google_state['client']
            self.credentials = _cached_google_state['credentials']
            self.project_id = _cached_google_state['project_id']
            logger.trace('gcp storage client is cached')
            return
        try:
            credentials: Credentials
            project_id: str | None
            credentials, project_id = auth.default(scopes=GOOGLE_SCOPES)
            logger.trace(f'gcp authenticated on project {project_id}')
        except auth_exceptions.DefaultCredentialsError as e:
            logger.critical(f'error authenticating on gcp: {e}')
            sys.exit(1)

        self.credentials = credentials
        self.project_id = project_id
        self.client = storage.Client(credentials=credentials)
        _cached_google_state = {
            'client': self.client,
            'credentials': self.credentials,
            'project_id': self.project_id,
        }

    @classmethod
    def _parse_uri(cls, uri: str) -> tuple[str, str | None]:
        uri_parts = uri.replace('gs://', '').split('/', 1)
        bucket_name = uri_parts[0]
        bucket_re = r'^[a-z0-9][a-z0-9-_.]{2,221}[a-z0-9]$'
        if re.match(bucket_re, bucket_name) is None:
            raise StorageError(f'invalid bucket name: {bucket_name}')

        file_path = uri_parts[1] if len(uri_parts) > 1 else None
        return bucket_name, file_path

    def _prepare_blob(
        self,
        bucket: storage.Bucket,
        prefix: str | None,
        generation: int | None = None,
    ) -> storage.Blob:
        if prefix is None:
            raise StorageError(f'invalid prefix: {prefix}')
        try:
            blob = bucket.blob(prefix, generation=generation)
        except GoogleAPICallError as e:
            raise StorageError(f'error preparing blob: {e}')
        return blob

    def stat(self, location: str) -> StatResult:
        bucket_name, prefix = self._parse_uri(location)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)
        assert isinstance(bucket, storage.Bucket)
        blob = bucket.get_blob(prefix)
        assert isinstance(blob, storage.Blob) or blob is None

        if blob:
            try:
                blob.reload()
                logger.trace(f'got metadata for blob {location}')
                return StatResult(
                    is_dir=False,
                    is_reg=True,
                    size=blob.size or 0,
                    revision=blob.generation,
                    mtime=blob.updated.timestamp() if blob.updated else None,
                )
            except GoogleAPICallError as e:
                raise StorageError(f'error getting metadata for {location}: {e}')
        else:
            # could be a prefix
            try:
                is_prefix = len(list(bucket.list_blobs(prefix=prefix, max_results=1))) > 0
                if is_prefix:
                    logger.trace(f'got metadata for prefix {location}')
                    return StatResult(
                        is_dir=True,
                        is_reg=False,
                        size=0,
                        revision=None,
                        mtime=None,
                    )
            except GoogleAPICallError as e:
                raise StorageError(f'error getting metadata for {location}: {e}')
        raise NotFoundError(thing=location)

    def open(self, location: str, mode: str = 'r', revision: Revision = None) -> IO:
        bucket_name, prefix = self._parse_uri(location)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)
        blob = self._prepare_blob(bucket, prefix)

        try:
            # If reading and no generation specified, get the latest generation
            if mode == 'r' and revision is None:
                blob.reload()
                revision = blob.generation
            blob_file = blob.open(mode=mode, generation=revision)
            logger.debug(f'opened blob {location} in mode {mode}')
            return blob_file
        except NotFound:
            raise NotFoundError(thing=location)
        except GoogleAPICallError as e:
            raise StorageError(f'error opening {location}: {e}')

    def glob(self, location: str, pattern: str) -> list[str]:
        bucket_name, prefix = self._parse_uri(location)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)

        if prefix:
            if prefix.endswith('/'):
                prefix_with_glob = f'{prefix}{pattern}'
            else:
                prefix_with_glob = f'{prefix}/{pattern}'
        else:
            prefix_with_glob = pattern

        entries: list[str] = [n.name for n in list(bucket.list_blobs(match_glob=prefix_with_glob))]

        if len(entries) == 0:
            logger.warning(f'no files found matching glob {prefix_with_glob}')

        return [f'gs://{bucket_name}/{entry}' for entry in entries]

    def download_to_file(self, src: str, dst: Path) -> int:
        bucket_name, prefix = self._parse_uri(src)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)
        blob = self._prepare_blob(bucket, prefix)

        fs.check_destination(dst, delete=True)

        try:
            blob.open()
            blob.download_to_filename(dst)
            logger.debug(f'downloaded {src} to {dst}')
        except NotFound:
            raise NotFoundError(thing=src)
        except (GoogleAPICallError, OSError) as e:
            raise StorageError(f'error downloading {src}: {e}')
        return blob.generation or 0

    def download_to_string(self, src: str) -> tuple[str, int]:
        bucket_name, prefix = self._parse_uri(src)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)
        blob = self._prepare_blob(bucket, prefix)

        try:
            blob_str = blob.download_as_string()
        except NotFound:
            raise NotFoundError(thing=src)

        decoded_blob = None
        try:
            decoded_blob = blob_str.decode('utf-8')
            logger.debug(f'downloaded {src} to string')
        except UnicodeDecodeError as e:
            raise StorageError(f'error decoding file {src}: {e}')
        assert blob.generation is not None
        return (decoded_blob, blob.generation)

    def upload(self, src: Path, dst: str, revision: Revision = None) -> int:
        bucket_name, prefix = self._parse_uri(dst)
        bucket = self.client.bucket(bucket_name, user_project=self.project_id)
        blob = self._prepare_blob(bucket, prefix)

        try:
            if revision is not None:
                blob.upload_from_filename(src, if_generation_match=revision)
            else:
                blob.upload_from_filename(src)
            logger.debug(f'uploaded {src} to {dst}')
        except PreconditionFailed:
            raise PreconditionFailedError(f'upload of {src} failed due to generation mismatch')
        except (GoogleAPICallError, OSError) as e:
            raise StorageError(f'error uploading {src}: {e}')
        blob.reload()
        return blob.generation or 0

    def copy_within(self, src: str, dst: str) -> int:
        src_bucket_name, src_prefix = self._parse_uri(src)
        src_bucket = self.client.bucket(src_bucket_name, user_project=self.project_id)
        src_blob = self._prepare_blob(src_bucket, src_prefix)

        dst_bucket_name, dst_prefix = self._parse_uri(dst)
        dst_bucket = self.client.bucket(dst_bucket_name, user_project=self.project_id)
        dst_blob = self._prepare_blob(dst_bucket, dst_prefix)

        try:
            new_blob = src_bucket.copy_blob(
                src_blob,
                dst_bucket,
                dst_blob.name,
            )
            logger.debug(f'copied {src} to {dst}')
        except NotFound:
            raise NotFoundError(thing=src)
        except GoogleAPICallError as e:
            raise StorageError(f'error copying {src} to {dst}: {e}')
        return new_blob.generation or 0

    def get_session(self) -> AuthorizedSession:
        """Get the current authenticated session.

        :return: An authorized session.
        :rtype: AuthorizedSession
        """
        return AuthorizedSession(self.credentials)

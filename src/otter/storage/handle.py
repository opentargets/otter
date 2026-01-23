"""Storage Handle class."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from otter.config.model import Config
from otter.storage.filesystem import FilesystemStorage
from otter.storage.google import GoogleStorage
from otter.storage.http import HTTPStorage
from otter.storage.model import Revision, StatResult, Storage
from otter.storage.noop import NoopStorage
from otter.util.errors import NotFoundError

if TYPE_CHECKING:
    from typing import IO

storage_registry: dict[str | None, type[Storage]] = {
    'gs': GoogleStorage,
    'http': HTTPStorage,
    'https': HTTPStorage,
    None: FilesystemStorage,
}


class StorageHandle:
    """Storage handle class.

    A high-level interface to operate with storage resources (folders or prefixes,
    files and globs) in tasks. It abstracts the underlying storage and provides a
    unified API.

    The ``StorageHandle`` uses the internal method ``_resolve`` in its initialization
    to resolve the absolute location of the resource.

    The resolution chain is:

    1. If the location is absolute (remote url or uri), use it as is.
    2. If ``force_local`` is False and there is a ``remote_uri``, prepend it.
    3. Otherwise prepend ``work_path``.

    Absolute local paths are disallowed, as everything local must be relative
    to the work path.

    When a new cloud storage interface is written, it must be registered in the
    ``storage_registry`` dictionary to be usable by the StorageHandle class. The
    key is the protocol prefix (e.g., 'gs' for Google Storage).
    """

    def __init__(
        self,
        location: str,
        config: Config | None = None,
        force_local: bool = False,
    ) -> None:
        self.location = location
        self.config = config
        self.force_local = force_local
        self._resolved = self._resolve(location)
        self._storage = self._get_storage()

    def _resolve(self, location: str):
        if location.startswith('/'):
            raise ValueError('absolute local paths are not allowed')

        if '://' in location:
            logger.debug(f'location {location} is absolute, using as is')
            return location

        if not self.config:
            raise ValueError('config must be provided for relative locations')

        if not self.force_local and self.config.release_uri:
            resolved = f'{self.config.release_uri}/{location}'
            logger.debug(f'location {location} resolved to remote {resolved}')
            return resolved

        resolved = f'{self.config.work_path}/{location}'
        logger.debug(f'location {location} resolved to local {resolved}')
        return resolved

    def _get_storage(self) -> Storage:
        proto = None
        if '://' in self._resolved:
            proto = self._resolved.split('://', 1)[0]
        storage_class = storage_registry.get(proto, NoopStorage)
        return storage_class()

    @property
    def storage(self) -> Storage:
        """Get the storage backend for this handle.

        :return: The storage backend instance.
        :rtype: Storage
        """
        return self._storage

    @property
    def absolute(self) -> str:
        """Get the absolute location of this storage handle.

        :return: The absolute location.
        :rtype: str
        """
        return self._resolved

    @property
    def is_absolute(self) -> bool:
        """Check if the location is absolute.

        :return: True if the location is absolute, False otherwise.
        :rtype: bool
        """
        return self.location == self._resolved

    def stat(self) -> StatResult:
        """Get metadata for this resource.

        :return: A :class:`StatResult` object containing the resource metadata.
        :rtype: :class:`StatResult`
        :raises NotFoundError: If the resource does not exist.
        """
        return self._storage.stat(self._resolved)

    def open(self, mode: str = 'r', revision: Revision = None) -> IO:
        """Open this resource for reading or writing.

        This method returns a file-like object that can be used to read
        from or write to the resource at the given location. A revision can be
        optionally provided to open a specific version of the resource. If ``None``,
        the latest version should be opened.

        :param mode: The mode to open the resource in. Defaults to ``r`` for read.
        :type mode: str
        :return: A file-like object for the resource.
        """
        return self._storage.open(self._resolved, mode=mode, revision=revision)

    def glob(self, pattern: str) -> list[str]:
        """List resources matching a glob under this storage handle's location.

        :param pattern: The pattern to match files against.
        :type pattern: str
        :return: A list of absolute locations for the matched resources.
        :rtype: list[str]
        """
        return self._storage.glob(location=self._resolved, pattern=pattern)

    def download_to_file(self, dst: Path) -> int:
        """Download this resource to a local file.

        :param dst: The local destination file path.
        :type dst: Path
        :return: The revision of the downloaded resource.
        :rtype: int
        :raises NotFoundError: If the resource does not exist.
        :raises DownloadError: If an error occurs during download.
        """
        if not self.stat().is_reg:
            raise ValueError(f'{self._resolved} is not a regular file')

        dst.parent.mkdir(parents=True, exist_ok=True)
        return self._storage.download_to_file(self._resolved, dst)

    def download_to_string(self) -> tuple[str, int]:
        """Download this resource and return its contents as a string.

        :return: A tuple containing the file contents and the revision.
        :rtype: tuple[str, int]
        :raises ValueError: If the resource is not a regular file.
        :raises DownloadError: If an error occurs during download.
        """
        if not self.stat().is_reg:
            raise ValueError(f'{self._resolved} is not a regular file')

        return self._storage.download_to_string(self._resolved)

    def upload(self, src: Path, revision: Revision = None) -> Revision:
        """Upload a local file to this storage handle.

        :param src: The local file path to upload.
        :type src: Path
        :param revision: The expected revision for precondition checks.
        :type revision: Revision
        :return: The revision of the uploaded resource.
        :rtype: Revision
        :raises UploadError: If an error occurs during upload.
        :raises PreconditionFailedError: If the revision does not match.
        """
        if not src.is_file():
            raise ValueError(f'{self._resolved} is not a regular file')

        return self._storage.upload(src, self._resolved, revision)

    def copy_to(self, dest: StorageHandle) -> Revision:
        """Copy this resource to the destination handle.

        This method copies the resource represented by this storage handle
        to the destination storage handle ``dest``. It follows a multi-strategy
        approach to optimize the copy operation:

        1. If both source and destination are in the same storage backend, it
           attempts to use the backend's native copy method (``copy_within``) for
           efficiency.
        2. If the native copy method is not implemented or if the source and
           destination are in different backends, it falls back to downloading
           the resource to a temporary local file and then uploading it to the
           destination.
        3. If any of those operations are not implemented, it tries using ``open``
            to read and write the resource in chunks.

        :param dest: The destination storage handle.
        :type dest: StorageHandle
        :return: The revision of the copied resource at the destination.
        :rtype: Revision
        """
        try:
            if not self.stat().is_reg:
                raise ValueError('only regular files can be copied')
        except FileNotFoundError:
            raise NotFoundError(f'source file {self._resolved} does not exist')

        if type(self.storage) is type(dest.storage):
            try:
                logger.debug(f'attempting optimized copy_within: {self._resolved} to {dest._resolved}')
                revision = self.storage.copy_within(self._resolved, dest._resolved)
            except NotImplementedError:
                logger.debug('copy_within not implemented, falling back to download/upload')
            else:
                return revision
        # Fallback to download and upload
        try:
            if type(dest.storage) is FilesystemStorage:
                # If destination is filesystem, download directly there
                logger.debug(f'downloading directly to filesystem at {dest._resolved}')
                revision = self.storage.download_to_file(self.absolute, Path(dest.absolute))
            else:
                import tempfile

                with tempfile.NamedTemporaryFile() as tmp_file:
                    self.storage.download_to_file(self._resolved, Path(tmp_file.name))
                    revision = dest.storage.upload(Path(tmp_file.name), dest._resolved)
        # Fallback to open read/write
        except NotImplementedError:
            logger.debug('download/upload not implemented, falling back to open read/write')
            revision = 0
            with self.storage.open(self._resolved, 'rb') as src_file:
                with dest.storage.open(dest._resolved, 'wb') as dest_file:
                    while chunk := src_file.read(8192):
                        dest_file.write(chunk)
        return revision

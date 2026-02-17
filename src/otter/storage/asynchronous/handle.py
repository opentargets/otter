"""Asynchronous Storage Handle class."""

from __future__ import annotations

from loguru import logger

from otter.config.model import Config
from otter.storage.asynchronous.model import AsyncStorage
from otter.storage.model import Revision, StatResult
from otter.storage.registry import async_storage_registry


class AsyncStorageHandle:
    """Asynchronous Storage handle class.

    A high-level interface to operate with storage resources (folders or prefixes,
    files and globs) in tasks. It abstracts the underlying storage and provides a
    unified API.

    The ``AsyncStorageHandle`` uses the internal method ``_resolve`` in its initialization
    to resolve the absolute location of the resource.

    The resolution chain is:

    1. If the location is absolute (remote url or uri), use it as is.
    2. If ``force_local`` is False and there is a ``remote_uri``, prepend it.
    3. Otherwise prepend ``work_path``.

    When a new cloud storage interface is written, it must be registered in the
    ``storage_registry`` dictionary to be usable by the AsyncStorageHandle class. The
    key is the protocol prefix (e.g., 'gs' for Google Storage).
    """

    def __init__(
        self,
        location: str,
        config: Config | None = None,
        force_local: bool = False,
    ) -> None:
        if config and location.startswith(str(config.work_path)):
            location = location[len(str(config.work_path)) :].lstrip('/')
        self.location = location
        self.config = config
        self.force_local = force_local
        self._resolved = self._resolve(location)
        self._storage: AsyncStorage = async_storage_registry.get_storage(self._resolved)

    def _resolve(self, location: str):
        if location.startswith('/'):
            return location

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

    @property
    def storage(self) -> AsyncStorage:
        """Get the storage backend for this handle.

        :return: The storage backend instance.
        :rtype: AsyncStorage
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

        :return: ``True`` if the location is absolute, ``False`` otherwise.
        :rtype: bool
        """
        return self.location == self._resolved

    async def stat(self) -> StatResult:
        """Get metadata for this resource.

        :return: A :class:`StatResult` object containing the resource metadata.
        :rtype: :class:`StatResult`
        :raises NotFoundError: If the resource does not exist.
        """
        return await self._storage.stat(self._resolved)

    async def glob(self, pattern: str) -> list[str]:
        """List resources matching a glob under this storage handle's location.

        :param pattern: The pattern to match files against.
        :type pattern: str
        :return: A list of absolute locations for the matched resources.
        :rtype: list[str]
        """
        return await self._storage.glob(location=self._resolved, pattern=pattern)

    async def read(self) -> tuple[bytes, Revision]:
        """Read the contents of this resource.

        :return: The file contents as bytes.
        :rtype: bytes
        :raises NotFoundError: If the resource does not exist.
        :raises TimeoutError: If the read operation times out.
        """
        return await self._storage.read(self._resolved)

    async def read_text(self, encoding: str = 'utf-8') -> tuple[str, Revision]:
        """Read the contents of this resource as text.

        :param encoding: The text encoding. Defaults to 'utf-8'.
        :type encoding: str
        :return: The file contents as a string.
        :rtype: str
        :raises NotFoundError: If the resource does not exist.
        """
        return await self._storage.read_text(self._resolved, encoding=encoding)

    async def write(
        self,
        data: bytes,
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        """Write data to this resource.

        Optionally, an expected revision can be provided to fail the write if
        the current revision does not match.

        :param data: The data to write.
        :type data: bytes
        :return: The revision of the written resource.
        :rtype: Revision
        """
        return await self._storage.write(self._resolved, data)

    async def write_text(
        self,
        data: str,
        encoding: str = 'utf-8',
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        """Write text to this resource.

        Optionally, an expected revision can be provided to fail the write if
        the current revision does not match.

        :param data: The text to write.
        :type data: str
        :param encoding: The text encoding. Defaults to 'utf-8'.
        :type encoding: str
        :return: The revision of the written resource.
        :rtype: Revision
        """
        return await self._storage.write_text(self._resolved, data, encoding=encoding)

    async def copy_to(self, dest: AsyncStorageHandle) -> Revision:
        """Copy this resource to the destination handle.

        If both source and destination are in the same storage backend, it
        attempts to use the backend's native copy method (``copy_within``) for
        efficiency. Otherwise, it reads from source and writes to destination.

        :param dest: The destination storage handle.
        :type dest: AsyncStorageHandle
        :return: The revision of the copied resource at the destination.
        :rtype: Revision
        :raises NotFoundError: If the source does not exist.
        """
        # try optimized copy_within if same storage type
        if type(self.storage) is type(dest.storage):
            try:
                logger.debug(f'attempting optimized copy_within: {self._resolved} to {dest._resolved}')
                return await self.storage.copy_within(self._resolved, dest._resolved)
            except NotImplementedError:
                logger.debug('copy_within not implemented, falling back to read/write')

        # fallback to read and write
        data, _ = await self.read()
        return await dest.write(data)

"""Registry for storage backends."""

from typing import Generic, TypeVar

from otter.storage.asynchronous.filesystem import AsyncFilesystemStorage
from otter.storage.asynchronous.google import AsyncGoogleStorage
from otter.storage.asynchronous.http import AsyncHTTPStorage
from otter.storage.asynchronous.model import AsyncStorage
from otter.storage.synchronous.filesystem import FilesystemStorage
from otter.storage.synchronous.google import GoogleStorage
from otter.storage.synchronous.http import HTTPStorage
from otter.storage.synchronous.model import Storage
from otter.util.errors import StorageError

S = TypeVar('S', Storage, AsyncStorage)


class StorageRegistry(Generic[S]):
    """Registry that maps protocols to storage backend classes."""

    def __init__(self, mappings: dict[str, type[S]]) -> None:
        self._mappings = mappings
        self._backends: dict[str, S] = {}

    def get_storage(self, location: str) -> S:
        """Get the regular storage for a location."""
        for proto in self._mappings:
            if location.startswith(proto):
                if proto not in self._backends:
                    self._backends[proto] = self._mappings[proto]()
                return self._backends[proto]
        raise StorageError(f'no storage backend found for location: {location}')


async_storage_registry = StorageRegistry[AsyncStorage]({
    'gs://': AsyncGoogleStorage,
    'http://': AsyncHTTPStorage,
    'https://': AsyncHTTPStorage,
    '/': AsyncFilesystemStorage,
})

storage_registry = StorageRegistry[Storage]({
    'gs://': GoogleStorage,
    'http://': HTTPStorage,
    'https://': HTTPStorage,
    '/': FilesystemStorage,
})

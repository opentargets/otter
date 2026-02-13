"""Registry for storage backends."""

from otter.storage.asynchronous.filesystem import AsyncFilesystemStorage
from otter.storage.asynchronous.google import AsyncGoogleStorage
from otter.storage.asynchronous.http import AsyncHTTPStorage
from otter.storage.asynchronous.model import AsyncStorage
from otter.storage.synchronous.filesystem import FilesystemStorage
from otter.storage.synchronous.google import GoogleStorage
from otter.storage.synchronous.http import HTTPStorage
from otter.storage.synchronous.model import Storage
from otter.storage.synchronous.noop import NoopStorage


class StorageRegistry:
    """Registry that maps protocols to storage backend classes."""

    def __init__(self, mappings: dict[str, type[Storage | AsyncStorage]]) -> None:
        self._mappings = mappings
        self._backends: dict[str, Storage | AsyncStorage] = {}

    def get_storage(self, location: str) -> Storage | AsyncStorage:
        """Get the regular storage for a location."""
        for proto in self._mappings:
            if location.startswith(proto):
                if proto not in self._backends:
                    self._backends[proto] = self._mappings[proto]()
                return self._backends[proto]
        return NoopStorage()


async_storage_registry = StorageRegistry({
    'gs://': AsyncGoogleStorage,
    'http://': AsyncHTTPStorage,
    'https://': AsyncHTTPStorage,
    '/': AsyncFilesystemStorage,
})

storage_registry = StorageRegistry({
    'gs://': GoogleStorage,
    'http://': HTTPStorage,
    'https://': HTTPStorage,
    '/': FilesystemStorage,
})

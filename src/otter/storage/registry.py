"""Registry for storage backends."""

from otter.storage.filesystem import FilesystemStorage
from otter.storage.google import GoogleStorage
from otter.storage.http import HTTPStorage
from otter.storage.model import Storage
from otter.storage.noop import NoopStorage


class StorageRegistry:
    """Registry that maps protocols to storage backend classes."""

    def __init__(self, mappings: dict[str, type[Storage]]) -> None:
        self._mappings = mappings
        self._backends: dict[str, Storage] = {}

    def get_storage(self, location: str) -> Storage:
        """Get the storage for a location."""
        for proto in self._mappings:
            if location.startswith(proto):
                if proto not in self._backends:
                    self._backends[proto] = self._mappings[proto]()
                return self._backends[proto]
        return NoopStorage()


storage_registry = StorageRegistry({
    'gs://': GoogleStorage,
    'http://': HTTPStorage,
    'https://': HTTPStorage,
    '/': FilesystemStorage,
})

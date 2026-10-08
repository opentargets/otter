"""Copy multiple files."""

import asyncio
from pathlib import Path
from typing import Any, Self

import yaml
from httpx import ReadTimeout
from loguru import logger
from pydantic import model_validator

from otter.manifest.model import Artifact
from otter.storage.asynchronous.handle import AsyncStorageHandle
from otter.storage.storage_context import storage_context
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.util import split_glob

MAX_RETRIES = 3
RETRY_DELAY = 1.0


class CopyManySpec(Spec):
    """Configuration fields for the copy_many task."""

    sources: list[str] | str | None = None
    """A list of sources or a single entry with a glob or a prefix (when sources
        are from a cloud storage provider). Must be absolute. Exactly one of
        ``sources``, ``source_list_file`` or ``source_mapping_file`` must be
        provided."""
    source_list_file: str | None = None
    """Path (relative to release root) to a file containing a list of source URIs,
        one per line. Optional. Mutually exclusive with ``sources`` and
        ``source_mapping_file``."""
    source_mapping_file: str | None = None
    """Path (relative to release root) to a YAML file that maps each source URI to
        the new file name it will have in ``destination``. Optional. Mutually
        exclusive with ``sources`` and ``source_list_file``. Example:

        .. code-block:: yaml

            https://example.com/a/long_name.tsv.gz: a.tsv.gz
            https://example.com/b/long_name.tsv.gz: b.tsv.gz

        Useful when source file names have clashing basenames.
        New file names from ``source_mapping_file`` are saved relative to ``destination``.
        """
    destination: str
    """The destination directory, relative to the release root."""
    max_concurrency: int = 10
    """Maximum number of concurrent copy operations. Defaults to 10."""
    settings: dict[str, Any] | None = None
    """Optional storage context settings for backend-specific configuration.

    The allowed settings depend on the storage backend being used:
        - For Google Cloud Storage (gs://): See :class:`otter.storage.settings.GoogleStorageSettings`
        - For other backends: Check the backend's documentation for supported settings

    Example:
        settings={'billing_project': 'my-billing-project'}  # For GCS requester-pays buckets
    """

    @model_validator(mode='after')
    def _sources_are_unique(self) -> Self:
        if sum(bool(m) for m in (self.sources, self.source_list_file, self.source_mapping_file)) != 1:
            raise ValueError('exactly one of `sources`, `source_list_file` or `source_mapping_file` must be provided')
        return self


class CopyMany(Task):
    """Copy multiple files.

    Copies multiple files from external sources to a destination directory inside
    the release. Each source file will be copied with its original filename to
    the destination directory, unless a ``source_mapping_file`` is provided, in
    which case each file is renamed to the name given in the mapping.
    """

    def __init__(self, spec: CopyManySpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CopyManySpec

    async def _copy_single_file(self, source: str, semaphore: asyncio.Semaphore, name: str | None = None) -> Artifact:
        async with semaphore:
            filename = name or Path(source).name
            dest_path = f'{self.spec.destination.rstrip("/")}/{filename.lstrip("/")}'

            for attempt in range(MAX_RETRIES + 1):
                try:
                    src = AsyncStorageHandle(source)
                    dst = AsyncStorageHandle(dest_path, config=self.context.config)
                    await src.copy_to(dst)
                    logger.info(f'copied {source} to {dest_path}')
                    return Artifact(source=src.absolute, destination=dst.absolute)
                except (ReadTimeout, TimeoutError):
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAY * (2**attempt)
                        logger.warning(f'timeout copying {source}, retrying')
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f'failed to copy {source}')
                        raise
            raise RuntimeError(f'unexpected error copying {source}')

    def _read_mapping(self, path: str) -> dict[str, str]:
        content, _ = StorageHandle(path, config=self.context.config).read_text()
        mapping = yaml.safe_load(content)
        if not isinstance(mapping, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()
        ):
            raise ValueError(f'{path} must be a YAML mapping of source to new file name')
        names = list(mapping.values())
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f'duplicate destination names in {path}: {", ".join(duplicates)}')
        return mapping

    @report
    async def run(self) -> Self:
        with storage_context(**(self.spec.settings or {})):
            names: dict[str, str] = {}
            sources = self.spec.sources or []
            if self.spec.source_mapping_file:
                logger.info(f'reading source mapping from {self.spec.source_mapping_file}')
                names = self._read_mapping(self.spec.source_mapping_file)
                sources = list(names)
            if self.spec.source_list_file:
                logger.info(f'reading source list from {self.spec.source_list_file}')
                source_list = StorageHandle(self.spec.source_list_file, config=self.context.config)
                content, _ = source_list.read_text()
                sources = content.splitlines()
            if isinstance(sources, str):
                logger.info(f'resolving sources from glob {sources}')
                prefix, glob = split_glob(sources)
                h = StorageHandle(prefix, config=self.context.config)
                sources = h.glob(glob)

            logger.info(f'copying {len(sources)} files to {self.spec.destination}')

            semaphore = asyncio.Semaphore(self.spec.max_concurrency)
            tasks = [self._copy_single_file(source, semaphore, name=names.get(source)) for source in sources]
            self.artifacts = await asyncio.gather(*tasks)

        logger.info(f'successfully copied {len(self.artifacts or [])} files')
        return self

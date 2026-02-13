"""Copy multiple files."""

import asyncio
from pathlib import Path
from typing import Self

from httpx import ReadTimeout
from loguru import logger

from otter.manifest.model import Artifact
from otter.storage.asynchronous.handle import AsyncStorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report

MAX_RETRIES = 3
RETRY_DELAY = 1.0


class CopyManySpec(Spec):
    """Configuration fields for the copy_many task."""

    sources: list[str]
    """The list of source URIs of files to copy. Must be absolute."""
    destination: str
    """The destination directory, relative to the release root."""
    max_concurrency: int = 10
    """Maximum number of concurrent copy operations. Defaults to 5."""


class CopyMany(Task):
    """Copy multiple files.

    Copies multiple files from external sources to a destination directory inside
    the release. Each source file will be copied with its original filename to
    the destination directory.

    .. note:: `sources` must be absolute. This task is intended for external
        resources.
    """

    def __init__(self, spec: CopyManySpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CopyManySpec

    async def _copy_single_file(self, source: str, semaphore: asyncio.Semaphore) -> Artifact:
        async with semaphore:
            filename = Path(source).name
            dest_path = f'{self.spec.destination}/{filename}'

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

    @report
    async def run(self) -> Self:
        logger.info(f'copying {len(self.spec.sources)} files to {self.spec.destination}')

        semaphore = asyncio.Semaphore(self.spec.max_concurrency)
        tasks = [self._copy_single_file(source, semaphore) for source in self.spec.sources]
        self.artifacts = await asyncio.gather(*tasks)

        logger.info(f'successfully copied {len(self.artifacts or [])} files')
        return self

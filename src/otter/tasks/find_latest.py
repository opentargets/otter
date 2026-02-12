"""Find the last-modified file among those in a prefix URI."""

from typing import Self

from loguru import logger

from otter.storage.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.util import split_glob


class FindLatestSpec(Spec):
    """Configuration fields for the find_latest task."""

    source: str
    """The prefix from where the file with the latest modification date will be
        found. It can include glob patterns."""
    scratchpad_key: str | None = None
    """The scratchpad key where the path of the latest file will be stored.
        Defaults to the task name."""


class FindLatest(Task):
    """Find the last-modified file among those in a prefix URI."""

    def __init__(self, spec: FindLatestSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: FindLatestSpec

    @report
    async def run(self) -> Self:
        prefix, glob = split_glob(self.spec.source)
        h = StorageHandle(prefix)
        file_paths = await h.glob(glob)

        latest, latest_mtime = None, None
        for p in file_paths:
            f = StorageHandle(p)
            s = await f.stat()
            if latest_mtime is None or s.mtime > latest_mtime:
                latest, latest_mtime = f, s.mtime

        if latest is None:
            raise FileNotFoundError(f'no files found matching {self.spec.source}')
        else:
            logger.info(f'latest file is {latest.absolute}')
            self.context.scratchpad.store(self.spec.scratchpad_key or self.spec.name, latest.absolute)
        return self

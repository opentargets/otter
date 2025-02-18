"""Find out the file with the latest modification date among those in a prefix URI."""

from typing import Self

from loguru import logger

from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class FindLatestSpec(Spec):
    """Configuration fields for the find_latest task.

    This task has the following custom configuration fields:
        - source str: The prefix from where the file with the latest modification date will
            be found.
        - pattern str: Optional. The pattern to match files against. The pattern should be
            a simple string match, preceded by an exclamation mark to exclude files. For
            example, 'foo' will match all files containing 'foo', while '!foo' will exclude
            all files containing 'foo'.
        - scratchpad_key str: The scratchpad key where the path of the latest file will be
            stored. Defaults to the task name.
    """

    source: str
    pattern: str | None = None
    scratchpad_key: str | None = None


class FindLatest(Task):
    """Find out the file with the latest modification date among those in a prefix URI."""

    def __init__(self, spec: FindLatestSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: FindLatestSpec

    @report
    def run(self) -> Self:
        remote_storage = get_remote_storage(self.spec.source)
        files = remote_storage.list(self.spec.source, self.spec.pattern)
        if not files:
            raise ValueError(f'no files found in {self.spec.source} with pattern {self.spec.pattern}')

        newest_file = files.pop(0)

        if len(files):
            mtime = remote_storage.stat(newest_file)['mtime']

            for file in files:
                new_mtime = remote_storage.stat(file).get('mtime', 0)
                if remote_storage.stat(file).get('mtime', 0) > mtime:
                    newest_file = file
                    mtime = new_mtime

        logger.info(f'latest file is {newest_file}')
        self.context.scratchpad.store(self.spec.scratchpad_key or self.spec.name, newest_file)
        return self

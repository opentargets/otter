"""Create an empty file."""

from typing import Self

from loguru import logger

from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.validators.file import exists


class TouchSpec(Spec):
    """Configuration fields for the touch task."""

    file: str
    """The location to create, relative to the release root."""
    content: str = ''
    """What to write into it. Defaults to an empty string."""


class Touch(Task):
    """Create a file, making its parent directories as needed.

    Intended for setting up a location that a later task will act on.
    """

    def __init__(self, spec: TouchSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: TouchSpec

    @report
    def run(self) -> Self:
        h = StorageHandle(self.spec.file, config=self.context.config)
        logger.info(f'creating {h.absolute}')

        h.write_text(self.spec.content)

        logger.success(f'created {h.absolute}')
        return self

    @report
    def validate(self) -> Self:
        """Check that the file is there."""
        exists(self.spec.file, config=self.context.config)
        return self

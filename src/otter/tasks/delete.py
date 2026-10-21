"""Delete a file or directory."""

from typing import Self

from loguru import logger

from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import NotFoundError, TaskValidationError


class DeleteSpec(Spec):
    """Configuration fields for the delete task."""

    file: str
    """The location to delete, relative to the release root."""
    is_recursive: bool = False
    """Whether to delete a directory, or a prefix and everything under it.
        Defaults to ``False``, which refuses to delete a directory."""


class Delete(Task):
    """Delete a file or directory.

    Deleting something that is not there is not an error, the task succeeds and
    reports zero files deleted.
    """

    def __init__(self, spec: DeleteSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: DeleteSpec

    @report
    def run(self) -> Self:
        h = StorageHandle(self.spec.file, config=self.context.config)
        logger.info(f'deleting {h.absolute}')

        count = h.delete(is_recursive=self.spec.is_recursive)

        logger.success(f'deleted {count} files at {h.absolute}')
        return self

    @report
    def validate(self) -> Self:
        """Check that the location is gone."""
        h = StorageHandle(self.spec.file, config=self.context.config)

        try:
            h.stat()
        except NotFoundError:
            logger.success(f'{h.absolute} is gone')
            return self

        raise TaskValidationError(f'{h.absolute} still exists after delete')

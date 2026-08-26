"""Simple delete example."""

import asyncio
from typing import Self

from loguru import logger

from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class DeleteSpec(Spec):
    """Configuration fields for the delete task."""

    file: str
    """The file path to delete."""


class Delete(Task):
    """Simple delete example."""

    def __init__(self, spec: DeleteSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: DeleteSpec

    @report
    async def run(self) -> Self:
        """Delete a file, then create an artifact about it."""
        logger.info('deleting file...')
        await asyncio.sleep(0.1)
        logger.success(f'file deleted {self.spec.file}')
        return self

    @report
    async def validate(self) -> Self:
        """Dummy validation step."""
        logger.info('did we delete the file properly?')
        await asyncio.sleep(0.1)
        logger.success('yes we did!')
        return self

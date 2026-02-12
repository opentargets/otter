"""Simple hello world example."""

import asyncio
from typing import Self

from loguru import logger

from otter.manifest.model import Artifact
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class HelloWorldSpec(Spec):
    """Configuration fields for the hello_world task."""

    who: str | None = 'world'
    """The person to greet."""


class HelloWorld(Task):
    """Simple hello world example."""

    def __init__(self, spec: HelloWorldSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: HelloWorldSpec

    @report
    async def run(self) -> Self:
        """Say hello, then create an artifact about it."""
        logger.info('saying hello...')
        await asyncio.sleep(0.1)
        logger.success(f'hello {self.spec.who}')
        self.artifact = Artifact(source='me', destination=self.spec.who or 'world')
        return self

    @report
    async def validate(self) -> Self:
        """Dummy validation step."""
        logger.info('did we say hello properly?')
        await asyncio.sleep(0.1)
        logger.success('yes we did!')
        return self

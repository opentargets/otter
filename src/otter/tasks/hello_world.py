"""Simple hello world example."""

from typing import Self

from loguru import logger

from otter.manifest.model import Artifact
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class HelloWorldSpec(Spec):
    """Configuration fields for the hello_world task.

    This task has the following custom configuration fields:
        - who (str): Optional. The person to greet in the output file. Defaults to 'world'.
    """

    who: str | None = 'world'


class HelloWorld(Task):
    """Simple hello world example."""

    def __init__(self, spec: HelloWorldSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: HelloWorldSpec

    @report
    def run(self) -> Self:
        # configure
        who = self.spec.who

        # say hello
        logger.success(f'hello {who}')

        # create an artifact
        self.artifact = Artifact(source='me', destination=self.spec.who or 'world')

        return self

    @report
    def validate(self) -> Self:
        # always pass validation, as an example
        # if you do not validate a task, there is no need to implement validate
        return self

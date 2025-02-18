"""TaskReporter class and report decorator for logging and updating tasks in the manifest."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from functools import wraps
from typing import TYPE_CHECKING

from loguru import logger

from otter.manifest.model import Artifact, Result, TaskManifest
from otter.util.errors import TaskAbortedError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from otter.task.model import Task


class TaskReporter:
    """Class for logging and updating tasks in the manifest."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.manifest: TaskManifest = TaskManifest(name=name)

    @property
    def artifacts(self) -> list[Artifact] | None:
        """Return the `Artifacts` associated with the `Task`."""
        return self.manifest.artifacts

    @artifacts.setter
    def artifacts(self, artifacts: list[Artifact]) -> None:
        """Set the `Artifact` associated with the `Task`."""
        self.manifest.artifacts = artifacts

    def start_run(self) -> None:
        """Update a task that has started running."""
        self.manifest.started_run_at = datetime.now(UTC)
        logger.info(f'task {self.name} started running')

    def finish_run(self) -> None:
        """Update a task that has finished running."""
        self.manifest.finished_run_at = datetime.now(UTC)
        logger.success(f'task {self.name} finished running: took {self.manifest.run_elapsed:.3f}s')

    def start_validation(self) -> None:
        """Update a task that has started validation."""
        self.manifest.started_validation_at = datetime.now(UTC)
        logger.info(f'task {self.name} started validation')

    def finish_validation(self) -> None:
        """Update a task that has finished validation."""
        self.manifest.finished_validation_at = datetime.now(UTC)
        self.manifest.result = Result.SUCCESS
        logger.success(f'task {self.name} finished validation: took {self.manifest.validation_elapsed:.3f}s')
        logger.success(f'task {self.name} completed: took {self.manifest.elapsed:.3f}s')

    def abort(self) -> None:
        """Update a task that has been aborted."""
        self.manifest.result = Result.ABORTED
        logger.warning(f'task {self.name} aborted')

    def fail(self, error: Exception, where: str) -> None:
        """Update a task that has failed running or validation."""
        self.manifest.result = Result.FAILURE
        logger.opt(exception=sys.exc_info()).error(f'task {where} failed: {error}')


def report(func: Callable[..., Task]) -> Callable[..., Task]:
    """Decorator for logging and updating tasks in the manifest."""

    @wraps(func)
    def wrapper(self: Task, *args: Any, **kwargs: Any) -> Task:
        try:
            # perform these before the wrapped method runs
            if func.__name__ == 'run':
                self.start_run()
            elif func.__name__ == 'validate':
                self.start_validation()

            # call the wrapped method
            result: TaskReporter = func(self, *args, **kwargs)

            # perform these after the wrapped method runs
            if func.__name__ == 'run':
                self.finish_run()
            elif func.__name__ == 'validate':
                self.finish_validation()
            return result

        # handle exceptions that happen during the call to the wrapped method
        except Exception as e:
            self.context.abort.set()
            if isinstance(e, TaskAbortedError):
                self.abort()
            else:
                self.fail(e, func.__name__)
            return self

    return wrapper

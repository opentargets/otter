"""Step model."""

import sys
from datetime import UTC, datetime

from loguru import logger

from otter.manifest.model import Result, StepManifest
from otter.task.model import Spec, Task
from otter.task.task_reporter import TaskReporter


class Step:
    """Step class.

    This class represents a step in the pipeline.
    """

    def __init__(
        self,
        name: str,
        specs: list[Spec],
    ) -> None:
        self.name = name
        """The name of the step."""
        self.specs: list[Spec] = specs
        """The list of specs to be processed in this step."""
        self.manifest: StepManifest = StepManifest(name=name)
        """The manifest for the step."""
        self.tasks: dict[str, Task] = {}
        """The dict of tasks that ran for this step."""

    def start(self) -> None:
        """Update a step that has started running."""
        self.manifest.started_run_at = datetime.now(UTC)
        logger.info(f'step {self.name} started running')

    def finish(self, tasks: dict[str, Task]) -> None:
        """Update a step that has finished running."""
        self.manifest.finished_run_at = datetime.now(UTC)

        if all(t.result == Result.SUCCESS for t in self.manifest.tasks):
            self.manifest.result = Result.SUCCESS
            logger.success(f'step {self.name} completed: took {self.manifest.elapsed:.3f}s')
        else:
            self.manifest.result = Result.FAILURE
            logger.opt(exception=sys.exc_info()).error(f'step {self.name} failed')

    def upsert_task_manifest(self, task: TaskReporter) -> None:
        """Update the step manifest with new task manifests."""
        inserted = False
        # first look for an already existing task and update it
        for i, t in enumerate(self.manifest.tasks):
            if t.name == task.name:
                self.manifest.tasks[i] = task.manifest
                # if the task has a resource, add it to the step manifest
                if task.artifacts:
                    self.manifest.artifacts.extend(task.artifacts)
                inserted = True
                break
        # if the task is new, insert it
        if not inserted:
            self.manifest.tasks.append(task.manifest)

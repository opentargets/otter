"""StepReporter class and report decorator for logging and updating steps in the manifest."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from loguru import logger

from otter.manifest.model import Result, StepManifest

if TYPE_CHECKING:
    from collections.abc import Sequence

    from otter.task.model import Spec, Task
    from otter.task.task_reporter import TaskReporter


class StepReporter:
    """Class for logging and updating steps in the manifest."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.manifest: StepManifest = StepManifest(name=name)

    def start(self) -> None:
        """Update a step that has started running."""
        self.manifest.started_run_at = datetime.now(UTC)
        logger.info(f'step {self.name} started running')

    def finish(self, specs: list[Spec], tasks: dict[str, Task]) -> None:
        """Update a step that has finished running."""
        self.manifest.finished_run_at = datetime.now(UTC)

        if len(tasks) == len(specs) and all(t.result == Result.SUCCESS for t in self.manifest.tasks):
            self.manifest.result = Result.SUCCESS
            logger.success(f'step {self.name} completed: took {self.manifest.elapsed:.3f}s')
        else:
            self.manifest.result = Result.FAILURE
            logger.opt(exception=sys.exc_info()).error(f'step {self.name} failed')

    def upsert_task_manifests(self, result: Sequence[TaskReporter]) -> None:
        """Update the step manifest with new task manifests."""
        for task in result:
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

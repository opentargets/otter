"""Step module."""

from __future__ import annotations

import errno
import os
import signal
from concurrent.futures import Future, ProcessPoolExecutor, wait
from multiprocessing import Manager
from threading import Event
from typing import TYPE_CHECKING, Any

from loguru import logger

from otter.config.model import Config
from otter.step.step_reporter import StepReporter
from otter.task.model import DEP_READY_STATES, READY_STATES, State
from otter.task.task_registry import TaskRegistry
from otter.util.errors import StepFailedError
from otter.util.logger import task_logging

if TYPE_CHECKING:
    from collections.abc import Sequence

    from otter.task.model import Spec, Task


MANAGER_POLLING_RATE = 1


class Step(StepReporter):
    """Step class.

    This class represents a step in the pipeline.
    """

    def __init__(
        self,
        name: str,
        specs: list[Spec],
        task_registry: TaskRegistry,
        config: Config,
    ) -> None:
        self.name = name
        self.task_registry = task_registry
        self.specs = specs
        self.config = config
        super().__init__(name)

        self.tasks: dict[str, Task] = {}

    def _instantiate_tasks(self, specs: Sequence[Spec]) -> dict[str, Task]:
        new_tasks = {spec.name: self.task_registry.instantiate(spec) for spec in specs}
        for t in new_tasks:
            if self.tasks.get(t):
                raise StepFailedError(f'duplicate task: {t}')
        return new_tasks

    def _is_task_ready(self, task: Task | None) -> bool:
        """Determine if a task is ready to run."""
        if task is None:
            return False

        if task.context.state not in READY_STATES:
            return False

        for r in task.spec.requires:
            rt = self.tasks.get(r)
            if rt is None or rt.context.state not in DEP_READY_STATES:
                return False

        return True

    def _get_ready_tasks(self, already_running: dict[str, Future[Task]]) -> list[Task]:
        tasks_already_running: list[str] = list(already_running.keys())
        return [t for t in self.tasks.values() if t.spec.name not in tasks_already_running and self._is_task_ready(t)]

    def _get_ready_specs(self) -> list[Spec]:
        """Determine if a spec is ready to be instantiated into a task."""
        ready_specs: list[Spec] = []
        for s in self.specs:
            if s.name in self.tasks:
                continue
            ready = True
            for r in s.requires:
                rt = self.tasks.get(r)
                if rt is None or rt.context.state not in DEP_READY_STATES:
                    ready = False
            if ready:
                ready_specs.append(s)
        return ready_specs

    def _get_task_cycles(self) -> list[str]:
        """Get the cycles in the task dependencies."""
        # TODO implement cycle detection
        return []

    def _is_step_done(self) -> bool:
        all_specs_are_tasks = all(s.name in self.tasks for s in self.specs)
        all_tasks_are_done = all(t.context.state is State.DONE for t in self.tasks.values())
        return all_specs_are_tasks and all_tasks_are_done

    def _are_all_created_tasks_done(self) -> bool:
        """Check if all tasks created from specs are done."""
        return all(t.context.state is State.DONE for t in self.tasks.values())

    def _process_results(self, results: list[Task]) -> None:
        for result in results:
            if result.context.state is State.RUNNING:
                # add new tasks to the queue
                new_specs = result.context.specs
                if new_specs:
                    logger.debug(f'task {result.spec.name} will add {len(new_specs)} new tasks to the queue')
                    self.specs.extend(new_specs)
                # add new keys to the scratchpad
                self.task_registry.scratchpad.merge(result.context.scratchpad)
            # update the task's state once done running/validating
            result.context.state = result.get_next_state()
            self.tasks[result.spec.name] = result

    @staticmethod
    def _run_task(task: Task, abort: Event) -> Task:
        # set the process role in the environment for logging purposes
        os.environ['OTTER_PROCESS_ROLE'] = 'W'
        # update the task's state to running/validating
        task.context.state = task.get_next_state()
        task.context.abort = abort
        with task_logging(task):
            if not abort.is_set():
                func = task.get_state_execution_method()
                func()
            else:
                task.abort()
            return task

    def run(self) -> Step:
        """Run the step."""
        self.start()

        with Manager() as manager, ProcessPoolExecutor(max_workers=self.config.pool_size) as executor:
            abort = manager.Event()
            futures: dict[str, Future[Task]] = {}

            def handle_sigint(*args: Any) -> None:
                logger.error('caught sigint, aborting')
                abort.set()
                manager.shutdown()
                raise SystemExit(errno.ECANCELED)

            signal.signal(signal.SIGINT, handle_sigint)

            try:
                while not self._is_step_done():
                    if abort.is_set():
                        raise StepFailedError('step aborted')

                    # instantiate new tasks from specs
                    ready_specs = self._get_ready_specs()
                    if ready_specs:
                        logger.debug(f'adding {len(ready_specs)} tasks to the queue')
                        self.tasks.update(self._instantiate_tasks(ready_specs))
                    elif self._are_all_created_tasks_done():
                        logger.error(
                            'all tasks instantiated so far have been completed, but remaining specs '
                            'are not ready, there is a cycle in the dependencies'
                        )
                        raise StepFailedError('cycle detected in task dependencies')

                    # add new tasks to the queue
                    ready_tasks = self._get_ready_tasks(futures)
                    for task in ready_tasks:
                        future = executor.submit(self._run_task, task, abort)
                        futures[task.spec.name] = future

                    # process completed tasks
                    if futures:
                        logger.debug(f'waiting for {len(futures)} task(s) to complete')
                        done, _ = wait(futures.values(), timeout=MANAGER_POLLING_RATE, return_when='FIRST_COMPLETED')

                        for future in done:
                            completed_task = future.result()
                            futures.pop(completed_task.spec.name)
                            self._process_results([completed_task])
                            self.upsert_task_manifests([completed_task])

            except Exception:
                abort.set()

            self.finish(self.specs, self.tasks)

        return self

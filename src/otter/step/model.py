"""Step module."""

from __future__ import annotations

import errno
import os
import signal
from concurrent.futures import Future, ProcessPoolExecutor, wait
from graphlib import CycleError, TopologicalSorter
from multiprocessing import Manager
from multiprocessing.managers import SyncManager
from queue import Queue
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

    def _instantiate_tasks(self, specs: Sequence[Spec], manager: SyncManager) -> dict[str, Task]:
        try:
            new_tasks = {spec.name: self.task_registry.instantiate(spec, manager) for spec in specs}
        except Exception as e:
            raise StepFailedError(f'error instantiating tasks: {e}') from e

        logger.debug(f'instantiated {len(new_tasks)} new tasks')
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

    def check_cycles(self):
        """Check if there are cycles in the task dependencies."""
        graph: dict[str, set[str]] = {}
        for s in self.specs:
            if s.name not in graph:
                graph[s.name] = set()
            for r in s.requires:
                if r not in graph:
                    graph[r] = set()
                graph[s.name].add(r)

        ts = TopologicalSorter(graph)
        try:
            ts.prepare()
        except CycleError as e:
            logger.critical(f'task cycle detected: {e.args[1]}')
            raise SystemExit(errno.EINVAL)

    def _is_step_done(self) -> bool:
        all_specs_are_tasks = all(s.name in self.tasks for s in self.specs)
        all_tasks_are_done = all(t.context.state is State.DONE for t in self.tasks.values())
        return all_specs_are_tasks and all_tasks_are_done

    def _are_all_created_tasks_done(self) -> bool:
        """Check if all tasks created from specs are done."""
        return all(t.context.state is State.DONE for t in self.tasks.values())

    def _process_results(self, results: list[Task]) -> None:
        for result in results:
            result.context.state = result.get_next_state()
            if result.context.state is State.DONE:
                result.context.task_queue.task_done()
            self.tasks[result.spec.name] = result

    def _get_new_specs_from_sub_task_queues(self, queues: list[Queue[Spec]]) -> list[Spec]:
        new_specs: list[Spec] = []
        for q in queues:
            while not q.empty():
                new_spec = q.get()
                new_specs.append(new_spec)
        return new_specs

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
            queues: list[Queue[Spec]] = []
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
                        self.tasks.update(self._instantiate_tasks(ready_specs, manager))
                    elif self._are_all_created_tasks_done():
                        logger.error(
                            'all tasks instantiated so far have been completed, but remaining specs '
                            'are not ready, there is a cycle in the dependencies'
                        )
                        raise StepFailedError('cycle detected in task dependencies')

                    # add new tasks to the queue
                    ready_tasks = self._get_ready_tasks(futures)
                    for task in ready_tasks:
                        queues.append(task.context.sub_queue)
                        future = executor.submit(self._run_task, task, abort)
                        futures[task.spec.name] = future

                    # process completed tasks
                    if futures:
                        logger.trace(f'waiting for {len(futures)} task(s) to complete')
                        done, _ = wait(futures.values(), timeout=MANAGER_POLLING_RATE, return_when='FIRST_COMPLETED')
                        for future in done:
                            completed_task = future.result()
                            futures.pop(completed_task.spec.name)
                            self._process_results([completed_task])
                            self.upsert_task_manifests([completed_task])

                    # collect new specs from task queues
                    self.specs.extend(self._get_new_specs_from_sub_task_queues(queues))

            except Exception as e:
                logger.critical(f'error running step {self.name}: {e}')
                abort.set()

            self.finish(self.specs, self.tasks)

        return self

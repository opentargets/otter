"""Coordinator is the class that manages the execution of a step."""

from __future__ import annotations

import asyncio
import queue
from collections import deque
from multiprocessing import Manager, Process
from threading import Event
from typing import TYPE_CHECKING

from loguru import logger

from otter.manifest.model import Result
from otter.step.model import Step
from otter.step.worker import worker_process
from otter.task.model import State
from otter.util.errors import StepFailedError, TaskBuildError, TaskDuplicateError, TaskRunError, TaskValidationError

if TYPE_CHECKING:
    from multiprocessing import Queue

    from otter.config.model import Config
    from otter.task.model import Spec, Task
    from otter.task.task_registry import TaskRegistry

COORDINATOR_POLLING_INTERVAL = 0.5
"""Default polling interval for the coordinator loop, in seconds."""


class Coordinator:
    """Coordinates the execution of a step's tasks."""

    def __init__(
        self,
        step: Step,
        task_registry: TaskRegistry,
        config: Config,
    ) -> None:
        self.step = step
        """The :py:class:`otter.step.model.Step` to run."""
        self.task_registry = task_registry
        """The task registry to build tasks from specs."""
        self.config = config
        """The configuration object."""
        self._manager = Manager()
        self._spec_count: int = len(step.specs)  # total number of specs to be processed
        self._remaining_specs: deque[Spec] = deque(step.specs)  # specs yet to be built into tasks
        self._task_queue: Queue[Task] = self._manager.Queue()  # queue of ready tasks
        self._result_queue: Queue[Task] = self._manager.Queue()  # queue of done task
        self._task_subtasks: dict[str, list[str]] = {}  # used to complete tasks that are waiting for subtasks
        self._shutdown_event: Event = self._manager.Event()
        self._workers: list[Process] = []

    def _is_spec_ready(self, spec: Spec) -> bool:
        """Determine if a spec is ready to build a task with."""
        logger.trace(f'checking if spec {spec.name} is ready')
        for task_name in spec.requires:
            task = self.step.tasks.get(task_name)
            if task is None:
                logger.trace(f'checking spec {spec.name}: task {task_name} is not built yet')
                return False
            else:
                logger.trace(f'checking spec {spec.name}: task {task_name} is {task.context.state}')
            if task is not None and task.context.state != State.DONE:
                logger.trace(f'checking spec {spec.name}: spec not ready, waiting on task {task_name}')
                return False
        logger.trace(f'spec {spec.name} is ready')
        return True

    def _get_ready_specs(self) -> list[Spec]:
        """Find all specs ready to be built into tasks."""
        logger.trace(f'scanning {len(self._remaining_specs)} new specs')
        ready = []
        new_remaining_specs = deque()
        while self._remaining_specs:
            spec = self._remaining_specs.popleft()
            if self._is_spec_ready(spec):
                ready.append(spec)
            else:
                new_remaining_specs.append(spec)
        self._remaining_specs = new_remaining_specs
        logger.trace(f'found {len(ready)} ready specs')
        return ready

    def _build_spec_into_task(self, s: Spec) -> Task:
        """Build a spec into a task, adds it to tasks dict and returns it."""
        logger.debug(f'building task for spec {s.name}')
        try:
            t = self.task_registry.build(s)
            if self.step.tasks.get(s.name):
                raise TaskDuplicateError(s.name)
            self.step.tasks[s.name] = t
            self.step.upsert_task_manifest(t)
            return t
        except Exception as e:
            if isinstance(e, TaskDuplicateError):
                raise
            logger.error(f'error building task for spec {s.name}: {e}')
            raise TaskBuildError(s.name)

    def _enqueue_tasks(self, tasks: list[Task]) -> None:
        """Enqueue a task to be run."""
        for task in tasks:
            logger.debug(f'enqueuing task {task.spec.name}')
            self._task_queue.put_nowait(task)

    def _is_step_complete(self) -> bool:
        """Determine if the step is complete."""
        spec_count_and_task_count_match = len(self.step.tasks) == self._spec_count
        all_tasks_done = all(t.context.state == State.DONE for t in self.step.tasks.values())
        logger.trace(
            f'step complete check: '
            f'spec_count={self._spec_count}, '
            f'task_count={len(self.step.tasks)}, '
            f'all_tasks_done={all_tasks_done}'
        )
        return spec_count_and_task_count_match and all_tasks_done

    def _step_result(self) -> Result:
        """Determine the result of the step."""
        if self._is_step_complete():
            if any(t.manifest.result == Result.FAILURE for t in self.step.tasks.values()):
                return Result.FAILURE
            else:
                return Result.SUCCESS
        else:
            return Result.PENDING

    def _get_task_results(self) -> list[Task]:
        """Get all done tasks from the task queue."""
        done_tasks = []
        while True:
            try:
                done_tasks.append(self._result_queue.get_nowait())
            except queue.Empty:
                break
        logger.trace(f'got {len(done_tasks)} done tasks')
        return done_tasks

    def _add_new_specs_from_task(self, task: Task) -> None:
        """Add new specs generated by a task to the remaining specs."""
        new_specs = task.context.specs
        if new_specs:
            self._remaining_specs.extend(new_specs)
            self._spec_count += len(new_specs)
            # track parent task for each new spec
            self._task_subtasks.setdefault(task.spec.name, []).extend(spec.name for spec in new_specs)
            logger.info(f'task {task.spec.name} generated {len(new_specs)} new specs')

    def _add_sentinels_to_global_scratchpad(self, task: Task) -> None:
        """Add any sentinels from the task context to the global scratchpad."""
        self.task_registry.scratchpad.merge(task.context.scratchpad)

    def _process_done_tasks(self) -> None:
        """Process done tasks."""
        for task in self._get_task_results():
            # if the task failed, update step manifest and raise to stop run
            if task.manifest.result == Result.FAILURE:
                self.step.upsert_task_manifest(task)
                if task.context.state == State.RUNNING:
                    raise TaskRunError(task.manifest.failure_reason)
                if task.context.state == State.VALIDATING:
                    raise TaskValidationError(task.manifest.failure_reason)
                else:
                    raise StepFailedError(f'task {task.spec.name} failed: {task.manifest.failure_reason}')
            task.context.state = task.get_next_state()
            # if the task just finished running, add any new specs it generated and enqueue it for validation
            if task.context.state == State.PENDING_VALIDATION:
                logger.trace(f'task {task.spec.name} completed running, adding new specs from it')
                self._add_new_specs_from_task(task)
                self._add_sentinels_to_global_scratchpad(task)
                self._enqueue_tasks([task])
            # if the task just finished validating, update the manifest
            if task.context.state == State.DONE:
                self.step.upsert_task_manifest(task)
            # update the tasks dict with the updated task coming from the worker
            self.step.tasks[task.spec.name] = task

    def _process_ready_specs(self) -> None:
        """Process all ready specs, building them into tasks and enqueueing them."""
        ready_specs = self._get_ready_specs()
        for spec in ready_specs:
            task = self._build_spec_into_task(spec)
            self._enqueue_tasks([task])

    def _is_task_finished_waiting_for_subtasks(self, task_name: str, subtask_names: list[str]) -> bool:
        """Check if a task waiting for subtasks can be marked as done."""
        task = self.step.tasks.get(task_name)
        if not task or task.context.state != State.WAITING_FOR_SUBTASKS:
            return False
        for subtask_name in subtask_names:
            subtask = self.step.tasks.get(subtask_name)
            if not subtask or subtask.context.state != State.DONE:
                return False
        return True

    def _complete_tasks_waiting_for_subtasks(self) -> None:
        """Complete tasks that were waiting if all their subtasks are done."""
        completed = []
        for task_name, subtask_names in self._task_subtasks.items():
            if self._is_task_finished_waiting_for_subtasks(task_name, subtask_names):
                logger.info(f'completing task {task_name} waiting for subtasks')
                task = self.step.tasks.get(task_name)
                if task:
                    task.context.state = task.get_next_state()
                    self.step.upsert_task_manifest(task)
                completed.append(task_name)
        # remove completed tasks from tracking
        for task_name in completed:
            self._task_subtasks.pop(task_name)
        # recursively check parents
        if completed:
            self._complete_tasks_waiting_for_subtasks()

    def _start_workers(self) -> None:
        """Start worker processes."""
        num_workers = self.config.pool_size
        logger.info(f'starting {num_workers} worker processes')
        for worker_id in range(num_workers):
            worker = Process(
                target=worker_process,
                args=(
                    worker_id,
                    self._task_queue,
                    self._result_queue,
                    self._shutdown_event,
                ),
            )
            worker.start()
            self._workers.append(worker)
            logger.debug(f'started worker {worker_id} (pid={worker.pid})')

    def _stop_workers(self) -> None:
        """Stop all worker processes."""
        logger.info('stopping worker processes')
        self._shutdown_event.set()
        for worker in self._workers:
            worker.join(timeout=5)
            if worker.is_alive():
                logger.warning(f'terminating worker {worker.pid}')
                worker.terminate()
                worker.join()

    def _kill_workers(self) -> None:
        """Kill all worker processes immediately."""
        logger.warning('killing worker processes')
        for worker in self._workers:
            if worker.is_alive():
                logger.warning(f'killing worker {worker.pid}')
                worker.kill()
                worker.join()

    async def run(self) -> None:
        """Run the coordinator loop."""
        logger.info(f'starting coordinator for step: {self.step.name}')
        self.step.start()

        try:
            self._start_workers()

            while not self._is_step_complete():
                self._process_done_tasks()
                self._complete_tasks_waiting_for_subtasks()
                self._process_ready_specs()
                await asyncio.sleep(COORDINATOR_POLLING_INTERVAL)
        except Exception as e:
            failure_reason = str(e)
            logger.error(f'stopping run: {type(e).__name__}: {e}')
            self._kill_workers()
        else:
            failure_reason = None
            self._stop_workers()
        finally:
            self._manager.shutdown()
            self.step.finish(result=self._step_result(), failure_reason=failure_reason)

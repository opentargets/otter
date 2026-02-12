"""Worker process requests tasks from the coordinator and runs them."""

from __future__ import annotations

import asyncio
import os
from multiprocessing import Queue
from threading import Event
from typing import TYPE_CHECKING

from loguru import logger

from otter.util.logger import task_logging

if TYPE_CHECKING:
    from otter.task.model import Task

WORKER_POLLING_INTERVAL = 0.5
"""Default polling interval for the worker loop, in seconds."""


class Worker:
    """Worker that executes tasks with a persistent event loop."""

    def __init__(self, worker_id: int) -> None:
        """Initialize worker with event loop.

        :param worker_id: Unique identifier for this worker
        :type worker_id: int
        """
        self.worker_id = worker_id
        self.loop = asyncio.new_event_loop()

    def run(
        self,
        task_queue: Queue,
        result_queue: Queue,
        shutdown_event: Event,
    ) -> None:
        """Run the worker loop.

        Executes tasks from ``task_queue``, and puts the results in ``result_queue``.
        The worker will stop when ``shutdown_event`` is set.

        :param task_queue: Queue to get tasks from
        :type task_queue: Queue
        :param result_queue: Queue to put results in
        :type result_queue: Queue
        :param shutdown_event: Event to signal shutdown
        :type shutdown_event: Event
        """
        # set process role for logging
        os.environ['OTTER_PROCESS_ROLE'] = 'W'
        asyncio.set_event_loop(self.loop)
        logger.info(f'worker {self.worker_id} started')

        try:
            while not shutdown_event.is_set():
                try:
                    task = task_queue.get(timeout=WORKER_POLLING_INTERVAL)
                except Exception:  # noqa: S112
                    continue

                logger.info(f'worker {self.worker_id} executing task {task.spec.name}')

                task = self.execute_task(task, shutdown_event)

                result_queue.put(task)
                logger.info(f'worker {self.worker_id} completed task {task.spec.name}')
        finally:
            self.loop.close()
            logger.info(f'worker {self.worker_id} shutting down')

    def execute_task(self, task: Task, abort_event: Event) -> Task:
        """Execute a single task.

        :param task: Task to execute
        :type task: Task
        :param abort_event: Event to signal abort
        :type abort_event: Event
        :return: The executed task
        :rtype: Task
        """
        # update task state to running/validating
        task.context.state = task.get_next_state()
        task.context.abort = abort_event

        with task_logging(task):
            if not abort_event.is_set():
                func = task.get_execution_method()

                if asyncio.iscoroutinefunction(func):
                    self.loop.run_until_complete(func())
                else:
                    func()
            else:
                task.abort()

        return task


def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    shutdown_event: Event,
) -> None:
    """Worker process entry point.

    Creates a Worker instance and runs it.

    :param worker_id: Unique identifier for this worker
    :type worker_id: int
    :param task_queue: Queue to get tasks from
    :type task_queue: Queue
    :param result_queue: Queue to put results in
    :type result_queue: Queue
    :param shutdown_event: Event to signal shutdown
    :type shutdown_event: Event
    """
    worker = Worker(worker_id)
    worker.run(task_queue, result_queue, shutdown_event)

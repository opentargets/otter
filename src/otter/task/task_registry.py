"""Registry of task classes, used to instantiate tasks from their spec."""

from __future__ import annotations

import errno
import importlib
import pkgutil
from importlib import resources
from multiprocessing.managers import SyncManager
from pathlib import Path
from queue import Queue
from typing import TYPE_CHECKING

from loguru import logger
from pydantic import ValidationError

from otter.task.model import Spec, TaskContext
from otter.util.errors import log_pydantic

if TYPE_CHECKING:
    from otter.config.model import Config
    from otter.scratchpad.model import Scratchpad
    from otter.task.model import Task

BUILTIN_TASKS_PATH = Path(__file__).parent.parent / 'tasks'
BUILTIN_TASKS_MODULE = 'otter.tasks'


class TaskRegistry:
    """Task types are registered here.

    The registry is where a `Task` will be instantiated from when the `Step` is
    run. It contains the mapping of a `task_type` to its `Task` and `TaskSpec`.

    .. note:: The :py:class:`otter.scratchpad.model.Scratchpad` placeholders are
        replaced into the `Spec` here, right before the `Task` is instantiated.
    """

    def __init__(self, config: Config, scratchpad: Scratchpad) -> None:
        self.config = config
        self.scratchpad = scratchpad
        self._tasks: dict[str, type[Task]] = {}
        self._specs: dict[str, type[Spec]] = {}

    def register(self, package_name: str) -> None:
        """Register tasks in a package into the registry.

        :param package_name: The name of the package to register tasks from.
        :type package_name: str
        :raises SystemExit: If the package is not found, modules are missing the
            expected class, or the class is not found in the module.
        """
        # determine list of files in the package
        try:
            files = str(resources.files(package_name))
        except ModuleNotFoundError:
            logger.critical(f'package {package_name} not found')
            raise SystemExit(errno.ENOENT)

        for _, module_name, ispkg in pkgutil.iter_modules([files], package_name + '.'):
            if ispkg:
                continue
            task_module = importlib.import_module(module_name)
            task_type = module_name.split('.')[-1]
            task_class_name = task_type.replace('_', ' ').title().replace(' ', '')

            try:
                task_class = getattr(task_module, task_class_name)
            except AttributeError:
                logger.critical(f'module {task_module.__name__} does not contain a class {task_class_name}')
                raise SystemExit(errno.ENOENT)

            task_spec_class = getattr(task_module, f'{task_class_name}Spec', Spec)

            # report if a previous task is being overridden
            if p := self._tasks.get(task_type):
                logger.warning(f'task type {task_module.__name__} will override {p.__module__}')

            # register the task
            self._tasks[task_type] = task_class
            self._specs[task_type] = task_spec_class

            logger.debug(f'registered task type {task_type}')

    def instantiate(self, spec: Spec, manager: SyncManager) -> Task:
        """Instantiate a Task.

        Template replacement is performed here, right before initializing the Task.

        :param spec: The spec to instantiate the Task from.
        :type spec: Spec

        :param manager: The SyncManager to use for creating shared queues.
        :type manager: SyncManager
        :raises SystemExit: If the task type is invalid or the spec is invalid.
        :return: The instantiated Task.
        :rtype: Task
        """
        task_type = spec.task_type
        try:
            task_class = self._tasks[task_type]
            spec_class = self._specs[task_type]
        except KeyError:
            logger.critical(f'invalid task type: {task_type}')
            raise SystemExit(errno.EINVAL)

        try:
            spec = spec_class(**spec.model_dump())
            replaced_spec = spec_class(
                **self.scratchpad.replace_dict(
                    spec.model_dump(),
                    ignore_missing=spec.scratchpad_ignore_missing,
                )
            )
        except ValidationError as e:
            logger.critical(f'invalid spec for task {spec.name}')
            logger.error(log_pydantic(e))
            raise SystemExit(errno.EINVAL)

        # create task and attach manifest
        if spec.task_queue:
            context = TaskContext(self.config, self.scratchpad, task_queue=spec.task_queue, sub_queue=manager.Queue())
        else:
            task_queue = self._init_queue(manager, replaced_spec)
            context = TaskContext(self.config, self.scratchpad, task_queue=task_queue, sub_queue=manager.Queue())
        context.sub_queue = manager.Queue()
        return task_class(replaced_spec, context)

    @staticmethod
    def _init_queue(manager: SyncManager, spec: Spec) -> Queue[Spec]:
        """Manage a multiprocessing queue.

        This function is used to initialize a queue for a specific task.
        It puts and gets an item to ensure the queue is set up such that
        task_done() can be called without discriminating between these tasks
        and those that were initialised from a queue.

        :param manager: The SyncManager to use for creating the queue.
        :type manager: SyncManager
        :param spec: The spec to initialize the queue for.
        :type spec: Spec
        :return: The initialized queue.
        :rtype: Queue[Spec]
        """
        task_queue = manager.Queue()
        task_queue.put(spec)
        task_queue.get()
        # disabled for now to allow python versions < 3.13
        # subtask_queue.shutdown()
        return task_queue

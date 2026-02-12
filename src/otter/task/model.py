"""Models for tasks."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from enum import Enum
from threading import Event
from typing import Self, final

from loguru import logger
from pydantic import BaseModel, field_validator

from otter.config.model import Config
from otter.scratchpad.model import Scratchpad
from otter.task.task_reporter import TaskReporter, report


class Spec(BaseModel, extra='allow', arbitrary_types_allowed=True):
    """Task Spec model.

    A `Spec` describes the properties and types for the config of a :py:class:`Task`.
    `Specs` are generated from the config file in :py:meth:`otter.task.load_specs`.

    This is the base on which task `Specs` are built. Specific `Tasks` extend this
    class to add custom attributes.

    The first word in :py:attr:`name` determines the :py:attr:`task_type`. This is
    used to identify the :py:class:`Task` in the :py:class:`otter.task.TaskRegistry`
    and in the config file.

    For example, for a ``DoSomething`` class defining a `Task`, the `task_type`
    will be ``do_something``, and in the configuration file, it could be used
    inside a `Step` like this:

    .. code-block:: yaml

        steps:
            - do_something to create an example resource:
                some_field: some_value
                another_field: another_value
    """

    @field_validator('name')
    @classmethod
    def _name_has_description(cls, value: str) -> str:
        if len(value.split(' ', 1)) < 2 or not value.split(' ', 1)[1]:
            raise ValueError(f'incorrect name {value}: must be task_type followed by a description')
        return value

    name: str
    """The name of the task. It is used to identify the task in the manifest and
        in the configuration file."""
    requires: list[str] = []
    """A list of tasks that this one depends on. The task will only run when all
        the prerequisites are completed."""
    scratchpad_ignore_missing: bool = False
    """Whether to ignore missing sentinels from the scratchpad when building the
        task. This is useful for tasks that use their own placeholders and internal
        scratchpads when running, e.g.: explode task.

        Defaults to ``False``."""

    @property
    def task_type(self) -> str:
        """The task type, used to identify it in the task registry.

        Determined by the first word in the task name.
        """
        return self.name.split(' ')[0]

    @task_type.setter
    def task_type(self, value: str) -> None:
        self.name = f'{value} {self.name.split(" ", 1)[1]}'


class State(Enum):
    """Enumeration of possible states for a :py:class:`otter.task.model.Task`."""

    PENDING_RUN = 0
    RUNNING = 1
    PENDING_VALIDATION = 2
    VALIDATING = 3
    WAITING_FOR_SUBTASKS = 4
    DONE = 5


READY_STATES = [State.PENDING_RUN, State.PENDING_VALIDATION]
"""States on which a task can be considered as ready to be sent to a worker."""


class TaskContext:
    """Task context."""

    def __init__(
        self,
        config: Config,
        scratchpad: Scratchpad,
    ) -> None:
        self.state: State = State.PENDING_RUN
        """The state of the task. See :class:`otter.task.model.State`."""

        self.abort: Event
        """An event that will trigger if another task fails. The `abort` event
            is assigned to the `task context` when the task is sent to run."""

        self.config: Config = config
        """The configuration object. See :class:`otter.config.model.Config`."""

        self.scratchpad: Scratchpad = scratchpad
        """The scratchpad object. See :class:`otter.scratchpad.model.Scratchpad`."""

        self.specs: list[Spec] = []
        """The list of new specs to be added to the step."""

    def add_specs(self, specs: Spec | Sequence[Spec]) -> None:
        """Add specs to the context.

        The coordinator will take these new specs, build tasks from them and add
        them to the step's queue.

        This enables tasks to dynamically generate new tasks.

        .. warning:: Adding requirements to these specs can cause cycles in the
            graph. This can only be checked at runtime, and can cause long running
            steps to fail halfway through.

        :param specs: The list of specs to add.
        :type specs: Sequence[Spec]
        """
        if isinstance(specs, Spec):
            self.specs.append(specs)
        else:
            self.specs.extend(specs)


class Task(TaskReporter):
    """Base class for tasks.

    ``Task`` is the main building block for a ``Step``. They are the main unit of work
    in Otter.

    The config for a ``Task`` is contained in a :py:class:`otter.task.model.Spec`
    object.

    A ``Task`` can optionally have a list of :py:class:`otter.manifest.model.Artifact`,
    which will contain metadata related to its input input and output and will be
    added to the step manifest.

    Tasks subclass :py:class:`otter.task.model.TaskReporter` to provide automatic
    logging, tracking and error handling.

    | To implement a new ``Task``:
    | 1. Create a new class that extends ``Spec`` with the required config fields.
    | 2. Create a subclass of ``Task`` and implement the :py:meth:`run` method.
         Optionally, implement the :py:meth:`validate` method.
    """

    def __init__(self, spec: Spec, context: TaskContext) -> None:
        self.spec = spec
        self.context = context
        super().__init__(spec.name)
        logger.debug(f'initialized task {self.spec.name}')

    @final
    def has_validation(self) -> bool:
        """Determine if the task has validation.

        :return: True if the task has validation, False otherwise.
        :rtype: bool
        """
        return self.__class__.__dict__.get('validate', None) is not Task.validate

    @final
    def has_subtasks(self) -> bool:
        """Determine if the task has generated subtasks.

        :return: True if the task has generated subtasks, False otherwise.
        :rtype: bool
        """
        return len(self.context.specs) > 0

    @final
    def get_execution_method(self) -> Callable[..., Self] | Callable[..., Awaitable[Self]]:
        """Get the method to execute based on the task state.

        :return: The method to execute.
        :rtype: Callable[..., Self] | Callable[..., Awaitable[Self]]
        """
        match self.context.state:
            case State.RUNNING:
                return self.run
            case State.VALIDATING:
                return self.validate
            case _:
                raise ValueError(f'task {self.name} has invalid state {self.context.state}')

    @final
    def get_next_state(self) -> State:
        """Get the next state.

        Returns the immediately next state, except in these cases:
        - If the current state is ``RUNNING`` and the task does not implement
          validation, check for subtasks. If there are subtasks, return
          ``WAITING_FOR_SUBTASKS``, else return ``DONE``.
        - If the current state is ``VALIDATING``, check for subtasks. If there
          are subtasks, return ``WAITING_FOR_SUBTASKS``, else return ``DONE``.

        :return: The next state.
        :rtype: State
        """
        if self.context.state is State.RUNNING:
            if not self.has_validation():
                logger.warning(f'task {self.name} does not implement validation')
                if self.has_subtasks():
                    return State.WAITING_FOR_SUBTASKS
                return State.DONE
        if self.context.state is State.VALIDATING:
            if self.has_subtasks():
                return State.WAITING_FOR_SUBTASKS
            return State.DONE

        return State(self.context.state.value + 1)

    @abstractmethod
    @report
    async def run(self) -> Self:
        """Run the task.

        This method contains the actual work of a ``Task``. Tasks must implement
        ``run``.

        Optionally, a list of :class:`otter.manifest.models.Artifact` object can
        be assigned to ``self.artifacts`` in the body of the method. These will
        be added to the step manifest.

        Optionally, an ``abort`` event can be watched to stop the task if another
        fails. This is useful for long running work that can be stopped midway
        once the run is deemed to be a failure.

        :return: The ``Task`` instance itself must be returned.
        :rtype: Self
        """
        return self

    @report
    async def validate(self) -> Self:
        """Validate the task result.

        This method should be implemented if the task requires validation. If not
        implemented, the task will always be considered valid.

        The validate method should call validators from the :py:mod:`otter.validators`
        module to perform validation. The validators must always return ``None``
        and raise :py:class:`otter.util.errors.TaskValidationError` if the validation
        fails.

        .. seealso:: :py:mod:`otter.validators` for some built-in validators that
            can be used as examples to implement your own.

        :return: The ``Task`` instance itself must be returned.
        :rtype: Self
        """
        return self

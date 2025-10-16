"""Models for tasks."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Sequence
from enum import Enum
from queue import Queue
from threading import Event
from typing import Self, final

from loguru import logger
from pydantic import BaseModel, SkipValidation, field_validator

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
    """A list of task names that this task depends on. The task will only run when
        all the prerequisites are completed."""
    scratchpad_ignore_missing: bool = False
    """Whether to ignore missing keys in the scratchpad when replacing placeholders.

        This is useful for tasks that use their own placeholders, which will happen
        after the spec is instantiated and the placeholders contained in the global
        scratchpad are replaced.

        Defaults to ``False``."""
    task_queue: SkipValidation[Queue[Spec]] | None = None

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
    DONE = 4


DEP_READY_STATES = [State.PENDING_VALIDATION, State.VALIDATING, State.DONE]
"""States on which a task can be considered as ready for others depending on it."""

READY_STATES = [State.PENDING_RUN, State.PENDING_VALIDATION]
"""States on which a task can be considered as ready to be sent to a worker."""


class TaskContext:
    """Task context."""

    def __init__(
        self,
        config: Config,
        scratchpad: Scratchpad,
        task_queue: Queue[Spec],
        sub_queue: Queue[Spec],
    ) -> None:
        self.state: State = State.PENDING_RUN
        """The state of the task. See :class:`otter.task.model.State`."""

        self.abort: Event
        """An event that will trigger if another task fails. The `abort` event
            is assigned to the `task context` when the task is sent to run."""

        self.task_queue: Queue[Spec] = task_queue
        """A queue where the task itself belongs."""

        self.sub_queue: Queue[Spec] = sub_queue
        """A queue where new specs can be added to be instantiated into
            new tasks, i.e. subtasks."""

        self.config: Config = config
        """The configuration object. See :class:`otter.config.model.Config`."""

        self.scratchpad: Scratchpad = scratchpad
        """The scratchpad object. See :class:`otter.scratchpad.model.Scratchpad`."""

        self._specs: list[Spec] = []
        """The list of generated specs."""

    @property
    def specs(self) -> list[Spec]:
        """The list of generated specs."""
        return self._specs

    def add_specs(self, specs: Sequence[Spec]) -> None:
        """Add specs to the context.

        This method can be called from inside a task and passed a list of specs.
        As soon as the task is finished, the specs will be instantiated into new
        tasks and added to the queue.

        This enables tasks to dynamically generate new tasks based on the result
        of the current task.

        .. warning:: Adding requirements to these specs can cause cycles in the
            graph. This can only be checked at runtime, and can cause long running
            steps to fail halfway through.

        :param specs: The list of specs to add.
        :type specs: Sequence[Spec]
        """
        self._specs.extend(specs)


class Task(TaskReporter):
    """Base class for tasks.

    `Task` is the main building block for a `Step`. They are the main unit of work
    in Otter.

    The config for a `Task` is contained in a :py:class:`otter.task.model.Spec`
    object.

    A `Task` can optionally have a list of :py:class:`otter.manifest.model.Artifact`,
    which will contain metadata related to its input input and output and will be
    added to the step manifest.

    Tasks subclass :py:class:`otter.task.model.TaskReporter` to provide automatic
    logging, tracking and error handling.

    | To implement a new `Task`:
    | 1. Create a new class that extends `Spec` with the required config fields.
    | 2. Create a subclass of `Task` and implement the `run` and, optionally,
        `validate` methods.
    """

    def __init__(self, spec: Spec, context: TaskContext) -> None:
        self.spec = spec
        self.context = context
        super().__init__(spec.name)
        logger.debug(f'initialized task {self.spec.name}')

    @final
    def get_state_execution_method(self) -> Callable[..., Task]:
        """Get the method to execute based on the task state.

        :return: The method to execute.
        :rtype: Callable[..., Task]
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

        Checks if the task has a validation method to determine if the next state
        should be `PENDING_VALIDATION` or `DONE`.

        :return: The next state.
        :rtype: State
        """
        if self.context.state is State.RUNNING:
            task_validate = self.__class__.__dict__.get('validate', None)
            if not task_validate:
                logger.warning(f'task {self.name} does not implement validation')
                return State.DONE
        return State(self.context.state.value + 1)

    @final
    def is_next_state_done(self) -> bool:
        """Check if the next state is DONE.

        This is used to determine if the task has finished running and has no
        validation phase, or if the task has finished validating.

        :return: True if the next state is DONE, False otherwise.
        :rtype: bool
        """
        return self.get_next_state() is State.DONE

    @abstractmethod
    @report
    def run(self) -> Self:
        """Run the task.

        This method contains the actual work of a `Task`. All tasks must implement
        `run`.

        Optionally, a list of :class:`otter.manifest.models.Artifact` object can be
        assigned to ``self.artifacts`` in the body of the method. These will be added
        to the step manifest.

        Optionally, an `abort` event can be watched to stop the task if another
        fails. This is useful for long running work that can be stopped midway
        once the run is deemed to be a failure.

        :return: The `Task` instance itself must be returned.
        :rtype: Self
        """
        return self

    @report
    def validate(self) -> Self:
        """Validate the task result.

        This method should be implemented if the task requires validation. If not
        implemented, the task will always be considered valid.

        The validate method should make use of the `v` method from the validators
        module to invoke a series of validators. See :func:`otter.validators.v`.

        :return: The `Task` instance itself must be returned.
        :rtype: Self
        """
        return self

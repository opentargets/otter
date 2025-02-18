"""Manifest data models."""

from datetime import UTC, datetime
from enum import StrEnum, auto

from pydantic import BaseModel, computed_field


class Result(StrEnum):
    """Result enumeration.

    The result of a `Task`, `Step` or the whole set of steps. Used in the manifest
    to track the status of the run.

    .. seealso:: :class:`TaskManifest`, :class:`StepManifest` and :class:`RootManifest`.
    """

    PENDING = auto()  # not yet started
    SUCCESS = auto()  # completed successfully
    FAILURE = auto()  # failed to run or validate
    ABORTED = auto()  # stopped before completion because of external reasons


class Artifact(BaseModel):
    """Artifact model.

    An `Artifact` is the resulting product of a task. Tasks can produce none, one
    or more artifacts. It is not necesarily a file, it could be a folder or a
    database. If a task creates an `Artifact`, it will be logged into the manifest
    for tracking.

    The `Artifact` class can be subclassed to extend the data model with additional
    fields to add to the manifest.

    Artifacts must have a source and a destination, which can be used to track them
    through the flow of the pipeline.
    """

    source: str
    """The source of the resource."""

    destination: str
    """The destination of the resource."""


class TaskManifest(BaseModel, extra='allow'):
    """Model representing a task in a step of the manifest."""

    name: str
    result: Result = Result.PENDING
    started_run_at: datetime | None = None
    finished_run_at: datetime | None = None
    started_validation_at: datetime | None = None
    finished_validation_at: datetime | None = None
    log: list[str] = []
    # spec: dict[str, Any] = {}
    artifacts: list[Artifact] = []

    @computed_field
    @property
    def run_elapsed(self) -> float | None:
        """Calculate the elapsed time for the run."""
        if self.started_run_at and self.finished_run_at:
            return (self.finished_run_at - self.started_run_at).total_seconds()

    @computed_field
    @property
    def validation_elapsed(self) -> float | None:
        """Calculate the elapsed time for the validation."""
        if self.started_validation_at and self.finished_validation_at:
            return (self.finished_validation_at - self.started_validation_at).total_seconds()

    @computed_field
    @property
    def elapsed(self) -> float | None:
        """Calculate the elapsed time."""
        if self.run_elapsed and self.validation_elapsed:
            return self.run_elapsed + self.validation_elapsed


class StepManifest(BaseModel):
    """Model representing a step in the manifest."""

    name: str
    result: Result = Result.PENDING
    started_run_at: datetime | None = None
    finished_run_at: datetime | None = None
    log: list[str] = []
    tasks: list[TaskManifest] = []
    artifacts: list[Artifact] = []

    @computed_field
    @property
    def elapsed(self) -> float | None:
        """Calculate the elapsed time."""
        if self.started_run_at and self.finished_run_at:
            return (self.finished_run_at - self.started_run_at).total_seconds()


class RootManifest(BaseModel):
    """Model representing the root of the manifest."""

    result: Result = Result.PENDING
    started_at: datetime = datetime.now(UTC)
    modified_at: datetime = datetime.now(UTC)
    log: list[str] = []
    steps: dict[str, StepManifest] = {}

"""Manifest data models."""

import asyncio
import random
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import TYPE_CHECKING

from loguru import logger
from pydantic import BaseModel, ValidationError, computed_field

from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle
from otter.util.errors import ManifestError, NotFoundError, PreconditionFailedError, StorageError

if TYPE_CHECKING:
    from otter.storage.model import Revision

MANIFEST_FILENAME = 'manifest.json'
RETRY_BASE_DELAY = 0.5


class Result(StrEnum):
    """Result enumeration.

    The result of a `Task`, `Step` or the whole set of steps. Used in the manifest
        to track the status of the run.

    .. seealso:: :class:`TaskManifest`, :class:`StepManifest` and:class:`RootManifest`.
    """

    PENDING = auto()  # not yet started
    SUCCESS = auto()  # completed successfully
    FAILURE = auto()  # failed to run or validate
    ABORTED = auto()  # stopped before completion because of external reasons


class Artifact(BaseModel):
    """Artifact model.

    An ``Artifact`` is the resulting product of a task. Tasks can produce none,
    one or more artifacts. It is not necesarily a file, it can be a folder or a
    database.

    The ``Artifact`` class can be subclassed to add additional fields that better
    describe things.

    Artifacts must have a source and a destination, which can be used to track
    them through the flow of the pipeline.
    """

    source: str | list[str]
    """The sources of the resource."""

    destination: str | list[str]
    """The destinations of the resource."""


class TaskManifest(BaseModel, extra='allow'):
    """Model representing a task in a step of the manifest."""

    name: str
    result: Result = Result.PENDING
    started_run_at: datetime | None = None
    finished_run_at: datetime | None = None
    started_validation_at: datetime | None = None
    finished_validation_at: datetime | None = None
    log: list[str] = []
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


class Manifest:
    """Manifest class that handles operations.

    This class wraps a RootManifest and provides atomic read-modify-write
    operations with optimistic locking for concurrent access.
    """

    def __init__(self, config: Config):
        """Initialize with config."""
        self.config = config
        self._root: RootManifest
        self._revision: Revision

    def _create_empty(self) -> RootManifest:
        """Create empty manifest with steps from config."""
        root = RootManifest()
        for step in self.config.steps:
            step_name = f'{self.config.runner_name}_{step}'
            root.steps[step_name] = StepManifest(name=step)
        return root

    def _recalculate_result(self) -> None:
        """Update root result based on step results."""
        steps = self._root.steps.values()
        if any(s.result in [Result.FAILURE, Result.ABORTED] for s in steps):
            self._root.result = Result.FAILURE
            logger.warning('there are failed or aborted steps in the manifest')
        elif all(s.result == Result.SUCCESS for s in steps):
            self._root.result = Result.SUCCESS
            logger.success('all steps in the manifest completed successfully')
        else:
            self._root.result = Result.PENDING
            logger.info('some steps in the manifest are still pending')

    def _serialize(self) -> str:
        """Serialize the manifest to a JSON string."""
        try:
            return self._root.model_dump_json(indent=2, serialize_as_any=True)
        except ValueError as e:
            logger.critical(f'error serializing manifest: {e}')
            raise ManifestError('error serializing manifest') from e

    def update(self, step_manifest: StepManifest) -> None:
        """Update manifest with the given step and save it.

        :param step_manifest: The :class:`otter.manifest.model.StepManifest` to
            update in the manifest.
        :type step_manifest: StepManifest
        :raises ManifestError: If an error occurs during the update
        """
        step_name = f'{self.config.runner_name}_{step_manifest.name}'

        if step_manifest.result == Result.SUCCESS:
            logger.success(f'step {step_manifest.name} ran successfully')
        elif step_manifest.result in [Result.FAILURE, Result.ABORTED]:
            logger.error(f'step {step_manifest.name} failed')
        else:
            logger.info(f'step {step_manifest.name} is pending')

        if self.config.release_uri:
            remote_uri = f'{self.config.release_uri}/{MANIFEST_FILENAME}'
            h = StorageHandle(remote_uri, self.config)
        else:
            h = StorageHandle(MANIFEST_FILENAME, self.config, force_local=True)

        try:
            h.stat()
        except NotFoundError:
            logger.info(f'no manifest found at {h.absolute}, creating new one')
            self._root = self._create_empty()
            self._root.steps[step_name] = step_manifest
            self._root.modified_at = datetime.now()
            self._recalculate_result()
            h.write_text(self._serialize())
            logger.success(f'step {step_manifest.name} updated successfully')
            return

        while True:
            try:
                manifest, revision = h.read_text()
            except StorageError as e:
                logger.critical(f'error reading manifest from {h.absolute}: {e}')
                raise ManifestError('error reading manifest') from e
            try:
                self._root = RootManifest().model_validate_json(manifest)
            except ValidationError as e:
                logger.critical(f'error validating manifest: {e}')
                raise ManifestError('invalid manifest format')
            self._root.steps[step_name] = step_manifest
            self._root.modified_at = datetime.now()
            self._recalculate_result()
            try:
                h.write_text(self._serialize(), expected_revision=revision)
                logger.success(f'step {step_manifest.name} updated successfully')
                return
            except PreconditionFailedError:
                logger.warning(f'manifest at {h.absolute} was modified by another process, retrying')
                asyncio.sleep(RETRY_BASE_DELAY + random.uniform(0, RETRY_BASE_DELAY))
            except StorageError as e:
                logger.critical(f'error writing manifest to {h.absolute}: {e}')
                raise ManifestError(f'error writing manifest: {e}') from e

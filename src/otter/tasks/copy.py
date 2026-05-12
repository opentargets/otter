"""Copy a file."""

from typing import Any, Self

from loguru import logger

from otter.manifest.model import Artifact
from otter.storage.storage_context import storage_context
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import TaskRunError
from otter.validators import file


class CopySpec(Spec):
    """Configuration fields for the copy task."""

    source: str
    """The source URI of the file to copy. Must be absolute."""
    destination: str
    """The destination for the file, relative to the release root."""
    settings: dict[str, Any] | None = None
    """Optional storage context settings for backend-specific configuration.

    The allowed settings depend on the storage backend being used:
        - For Google Cloud Storage (gs://): See :class:`otter.storage.settings.GoogleStorageSettings`
        - For other backends: Check the backend's documentation for supported settings

    Example:
        settings={'billing_project': 'my-billing-project'}  # For GCS requester-pays buckets
    """


class Copy(Task):
    """Copy a file.

    Copies a file from an external source to a destination inside the release. If
    no `release_uri` is provided in the configuration, the file will be downloaded
    to the local `work_path`.

    .. note:: `source` must be absolute. This task is intended for external resources.

    .. note:: `destination` will be prepended with either :py:obj:`otter.config.model.Config.release_uri`
        or :py:obj:`otter.config.model.Config.work_path` config fields.
    """

    def __init__(self, spec: CopySpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CopySpec

    @report
    def run(self) -> Self:
        logger.info(f'copying file from {self.spec.source} to {self.spec.destination}')

        with storage_context(**(self.spec.settings or {})):
            try:
                src = StorageHandle(self.spec.source)
            except ValueError:
                raise TaskRunError(
                    f'source {self.spec.source} is relative, copy task is intended for external resources'
                )
            dst = StorageHandle(self.spec.destination, config=self.context.config)

            src.copy_to(dst)

        self.artifacts = [Artifact(source=src.absolute, destination=dst.absolute)]
        return self

    @report
    def validate(self) -> Self:
        """Check that the copied file exists and has a valid size."""
        with storage_context(**(self.spec.settings or {})):
            file.exists(
                self.spec.destination,
                config=self.context.config,
            )

            file.size(
                self.spec.source,
                self.spec.destination,
                config=self.context.config,
            )

        return self

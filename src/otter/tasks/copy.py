"""Copy a file."""

from pathlib import Path
from typing import Self

from loguru import logger

from otter.manifest.model import Artifact
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.download import download
from otter.validators import v
from otter.validators.file import file_exists, file_size


class CopySpec(Spec):
    """Configuration fields for the copy task."""

    source: str
    """The URL of the file to download."""
    destination: str
    """The path, relative to `release_uri` to upload the file to."""


class Copy(Task):
    """Copy a file.

    Downloads a file from `source`, then uploads it to `destination`.

    .. note:: `destination` will be prepended with the :py:obj:`otter.config.model.Config.release_uri`
        config field.

    If no `release_uri` is provided in the configuration, the file will only be
    downloaded locally. This is useful for local runs or debugging. The local path
    will be created by prepeding :py:obj:`otter.config.model.Config.work_path` to the
    destination field.
    """

    def __init__(self, spec: CopySpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CopySpec
        self.local_path: Path = context.config.work_path / spec.destination
        self.remote_uri: str | None = None
        if context.config.release_uri:
            self.remote_uri = f'{context.config.release_uri}/{spec.destination}'

    def _is_google_spreadsheet(self) -> bool:
        return self.spec.source.startswith('https://docs.google.com/spreadsheets/')

    @report
    def run(self) -> Self:
        download(self.spec.source, self.local_path, abort=self.context.abort)
        logger.debug('download successful')

        if self.remote_uri:
            remote_storage = get_remote_storage(self.remote_uri)
            remote_storage.upload(self.local_path, self.remote_uri)
            logger.debug('upload successful')

        self.artifacts = [Artifact(source=self.spec.source, destination=self.remote_uri or str(self.local_path))]
        return self

    @report
    def validate(self) -> Self:
        """Check that the downloaded file exists and has a valid size."""
        v(file_exists, self.local_path)

        # skip size validation for google spreadsheet
        if self._is_google_spreadsheet():
            logger.warning('skipping validation for google spreadsheet')
            return self

        v(file_size, self.spec.source, self.local_path)

        return self

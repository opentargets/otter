"""Download a file."""

from pathlib import Path
from typing import Self

from loguru import logger

from otter.manifest.model import Artifact
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.download import download
from otter.validators import v
from otter.validators.file import file_exists, file_size


class DownloadSpec(Spec):
    """Configuration fields for the download task.

    This task has the following custom configuration fields:
        - source (str): The URL of the file to download.
    """

    source: str
    destination: Path


class Download(Task):
    """Download a file.

    Downloads a file from a URL to a local destination.
    """

    def __init__(self, spec: DownloadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: DownloadSpec
        self.full_destination = context.config.work_path / spec.destination

    def _is_google_spreadsheet(self) -> bool:
        return self.spec.source.startswith('https://docs.google.com/spreadsheets/')

    @report
    def run(self) -> Self:
        """Download a file from the source URL to the destination path."""
        download(self.spec.source, self.full_destination, abort=self.context.abort)
        self.artifact = Artifact(source=self.spec.source, destination=str(self.full_destination))
        logger.debug('download successful')
        return self

    @report
    def validate(self) -> Self:
        """Check that the downloaded file exists and has a valid size."""
        v(file_exists, self.full_destination)

        # skip size validation for google spreadsheet
        if self._is_google_spreadsheet():
            logger.warning('skipping validation for google spreadsheet')
            return self

        v(file_size, self.spec.source, self.full_destination)

        return self

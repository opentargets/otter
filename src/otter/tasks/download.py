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
    """Configuration fields for the download task."""

    source: str
    """The URL of the file to download. If it looks like a relative path, it will
        be prepended the release_uri."""
    destination: Path | None = None
    """The local path to download the file to. If ommitted, the file will be
        downloaded to the same path as the source."""


class Download(Task):
    """Download a file.

    Downloads a file from `source` to `destination`. There are a few defaults and
    conveniences built in to the task:

    - If `source` does not contain a protocol (``://`` not present), the `release_uri` \
        will be prepended to the source.

    - If `destination` is not provided, the file will be downloaded to the same path \
        as the source, prepending the work path.

    Those two together are useful for downloading files from the release bucket.
    """

    def __init__(self, spec: DownloadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: DownloadSpec
        self.source = spec.source
        if '://' not in self.source:
            if not context.config.release_uri:
                raise ValueError('source must be a full url if release_uri is not provided')

            self.source = f'{context.config.release_uri}/{spec.source}'
            logger.info(f'prepending release_uri to source: {spec.source}')

        self.destination = spec.destination
        if not self.destination:
            if '://' in self.spec.source:
                raise ValueError('destination must be provided when source is a full url')
            self.destination = context.config.work_path / spec.source
            logger.info(f'using work_path as destination: {self.destination}')

    def _is_google_spreadsheet(self) -> bool:
        return self.spec.source.startswith('https://docs.google.com/spreadsheets/')

    @report
    def run(self) -> Self:
        assert self.destination is not None
        download(self.source, self.destination, abort=self.context.abort)
        self.artifact = Artifact(source=self.source, destination=str(self.destination))
        logger.debug('download successful')
        return self

    @report
    def validate(self) -> Self:
        """Check that the downloaded file exists and has a valid size."""
        v(file_exists, self.destination)

        # skip size validation for google spreadsheet
        if self._is_google_spreadsheet():
            logger.warning('skipping validation for google spreadsheet')
            return self

        v(file_size, self.source, self.destination)

        return self

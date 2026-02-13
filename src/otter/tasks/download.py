"""Download a file."""

from pathlib import Path
from typing import Self

from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.validators import file


class DownloadSpec(Spec):
    """Configuration fields for the download task."""

    source: str
    """The source location, relative to the release, of the file to download."""


class Download(Task):
    """Download a file.

    Downloads a file from ``source`` to a local destination. The source **must**
    be relative to the release root. This should be used after a ``copy`` task
    has put that artifact into the release, as modifying an external resource
    would break reproducibility.

    The destination will be the same as the source, but relative to the local
    ``work_path``.

    This task should only be used when a later task needs an artifact to exist
    locally for some reason. In most cases, downloading files should be avoided.
    Instead:

    * For copying artifacts into a release, use the ``copy`` task.
    * For transforming data, whenever possible, open files already copied and work
        with them directly.

    .. note:: This task will not generate an artifact, as the downloaded file will
        stay local only. It is a responsibility of subsequent tasks to put the
        file in the release and generate the corresponding artifact. This is easy
        because the relative part of the path once downloaded will be the same as
        the one in the release.
    """

    def __init__(self, spec: DownloadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: DownloadSpec
        self.destination = Path(f'{self.context.config.work_path}/{self.spec.source}')

    @report
    def run(self) -> Self:
        src = StorageHandle(self.spec.source, config=self.context.config)
        if src.is_absolute:
            raise ValueError('source must be relative to the release root')
        dst = StorageHandle(self.spec.source, config=self.context.config, force_local=True)

        src.copy_to(dst)

        return self

    @report
    def validate(self) -> Self:
        """Check that the downloaded file exists and has a valid size."""
        # remember: self.spec.source is relative, so destination is work_dir/{source}
        file.exists(
            str(self.spec.source),
            config=self.context.config,
            force_local=True,
        )

        file.size(
            self.spec.source,
            self.spec.source,  # same as above
            config=self.context.config,
            force_local=True,
        )

        return self

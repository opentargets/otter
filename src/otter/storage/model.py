"""Abstract base class for storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import IO

Revision = float | str | None
"""Type alias for file revision identifiers."""


@dataclass
class StatResult:
    """Dataclass representing file metadata."""

    is_dir: bool
    """Whether the resource is a directory."""
    is_reg: bool
    """Whether the resource is a regular file."""
    size: int | None
    """The resource size in bytes, `None` if unknown."""
    revision: Revision
    """The resource revision identifier."""
    mtime: float | None = None
    """The resource modification time as a Unix timestamp, `None` if unknown."""


class Storage(ABC):
    """Abstract base class for storage backends."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the storage backend.

        :return: The name of the storage backend.
        :rtype: str
        """

    @abstractmethod
    def stat(self, location: str) -> StatResult:
        """Get metadata for a resource.

        This method returns a :class:`StatResult` object with metadata about the
        resource at the given location. It should raise a NotFoundError if the
        location does not exist.

        :param location: The path or uri to the resource.
        :type location: str
        :return: A :class:`StatResult` object containing the resource metadata.
        :rtype: :class:`StatResult`
        :raises NotFoundError: If the resource does not exist.
        """

    @abstractmethod
    def open(self, location: str, mode: str = 'r', revision: Revision = None) -> IO:
        """Open a resource for reading or writing.

        This method should return a file-like object that can be used to read
        from or write to the resource at the given location. A revision can be
        optionally provided to open a specific version of the resource. If `None`,
        the latest version should be opened.

        :param location: The path or uri to the resource.
        :type location: str
        :param mode: The mode to open the resource in. Defaults to 'r' for read.
        :type mode: str
        :return: A file-like object for the resource.
        :rtype: file-like object
        """

    @abstractmethod
    def glob(self, location: str, pattern: str) -> list[str]:
        """List resources matching a glob pattern under the given location.

        If the pattern contains no wildcards, all resources under it should be
        listed (it should be treated as a prefix or folder).

        :param location: The base path or uri to search under.
        :type location: str
        :param pattern: The pattern to match for.
        :type pattern: str
        :return: A list of absolute file paths or uris.
        :rtype: list[str]
        """

    @abstractmethod
    def download_to_file(self, src: str, dst: Path) -> int:
        """Download a file to the local filesystem.

        This method should use cloud provider SDKs where possible. For example,
        wrapping GCP's `download_to_filename`. This ensures we benefit from all
        optimizations and error handling provided by those instead of using raw
        open/read/write operations.

        This method must ensure that the destination path is ready to receive
        the file (parent directories exist and is writable, file does not exist,
        etc).

        This method should return the revision of the downloaded file if possible.

        :param src: The uri of the file to download.
        :type src: str
        :param dst: The destination path to download the file to.
        :type dst: Path
        :return: The revision of the file.
        :rtype: Revision
        :raises NotFoundError: If the file does not exist.
        :raises DownloadError: If an error occurs during download.
        """

    @abstractmethod
    def download_to_string(self, src: str) -> tuple[str, int]:
        """Download a file and return its contents as a string.

        This method should use cloud provider SDKs where possible. For example,
        wrapping GCP's `download_as_string`. This ensures we benefit from all
        optimizations and error handling provided by those instead of using raw
        open/read operations.

        :param src: The uri of the file to download.
        :type src: str
        :return: A tuple containing the file contents and the revision.
        :rtype: tuple[str, Revision]
        :raises NotFoundError: If the file does not exist.
        :raises DownloadError: If an error occurs during download.
        """

    @abstractmethod
    def upload(self, src: Path, dst: str, revision: Revision = None) -> int:
        """Upload a local file to the remote storage.

        Optionally, a revision can be provided to ensure that the file has
        not been modified since the last time it was read. Implementations must
        check the revision and raise a PreconditionFailedError if it does
        not match.

        :param src: The source path of the file to upload.
        :type src: Path
        :param dst: The uri to upload the file to.
        :type dst: str
        :param revision: Optional. The expected revision of the file.
        :type revision: int | str | None
        :return: The new revision of the file.
        :rtype: int
        :raises UploadError: If an error occurs during upload.
        :raises PreconditionFailedError: If the revision does not match.
        """

    @abstractmethod
    def copy_within(self, src: str, dst: str) -> int:
        """Copy a file within the same storage backend.

        This method allows for efficient copies in the same storage backend, without
        the need for intermediate copy.

        :param src: The source path of the file to copy.
        :type src: str
        :param dst: The destination path to copy the file to.
        :type dst: str
        :return: The revision of the copied file.
        :rtype: int
        :raises NotFoundError: If the source file does not exist.
        :raises CopyError: If an error occurs during the copy operation.
        """

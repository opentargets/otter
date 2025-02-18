"""Abstract base class for remote storage services."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


class RemoteStorage(ABC):
    """Abstract base class for remote storage services."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the storage service.

        :return: The name of the storage service.
        :rtype: str
        """

    @abstractmethod
    def check(self, uri: str) -> bool:
        """Check if the provided storage is valid.

        This method should check if the storage exists and has proper permissions.
        On Google Cloud Storage, for example, this would check if the bucket exists
        and the service account has get, list and create permissions.

        :param uri: The URI to check.
        :type uri: str
        :return: True if the storage exists, False otherwise.
        :rtype: bool
        """

    @abstractmethod
    def stat(self, uri: str) -> dict[str, Any]:
        """Get metadata for a file.

        Currently, only the modification time is required, as it is used for the
        download_latest task. This method should be expanded as needed.

        :param uri: The URI to get metadata for.
        :type uri: str
        :return: A dictionary containing metadata.
        :rtype: dict
        :raises NotFoundError: If the file does not exist.
        """

    @abstractmethod
    def list(self, uri: str, pattern: str | None = None) -> list[str]:
        """List files in prefix URI.

        Optionally, a pattern can be provided to match files against. The pattern
        should be a simple string match, preceded by an exclamation mark to exclude
        files. For example, 'foo' will match all files containing 'foo', while '!foo'
        will exclude all files containing 'foo'.

        :param uri: The prefix URI by which to list files.
        :type uri: str
        :param pattern: Optional. The pattern to match files against.
        :type pattern: str | None
        :return: A list of file URIs.
        :rtype: list[str]
        """
        return []

    @abstractmethod
    def glob(self, uri: str) -> list[str]:
        """List blobs matching a pattern.

        :param uri: The URI with a glob expression to match for.
        :type uri: str
        :return: A list of blob URIs.
        :rtype: list[str]
        """

    @abstractmethod
    def download_to_file(self, uri: str, dst: Path) -> int:
        """Download a file to the local filesystem.

        :param uri: The URI of the file to download.
        :type uri: str
        :param dst: The destination path to download the file to.
        :type dst: Path
        :return: The revision number of the file.
        :rtype: int
        :raises NotFoundError: If the file does not exist.
        :raises DownloadError: If an error occurs during download.
        """

    @abstractmethod
    def download_to_string(self, uri: str) -> tuple[str, int]:
        """Download a file and return its contents as a string.

        :param uri: The URI of the file to download.
        :type uri: str
        :return: A tuple containing the file contents and the revision number.
        :rtype: tuple[str, int]
        :raises NotFoundError: If the file does not exist.
        :raises DownloadError: If an error occurs during download
        """

    @abstractmethod
    def upload(self, src: Path, uri: str, revision: int | None = None) -> int:
        """Upload a file to the remote storage.

        Optionally, a revision number can be provided to ensure that the file has
        not been modified since the last time it was read.

        :param src: The source path of the file to upload.
        :type src: Path
        :param uri: The URI to upload the file to.
        :type uri: str
        :param revision: Optional. The expected revision number of the file.
        :type revision: int | None
        :return: The new revision number of the file.
        :rtype: int
        :raises UploadError: If an error occurs during upload.
        :raises PreconditionFailedError: If the revision number does not match.
        """

    @abstractmethod
    def get_session(self) -> Any:
        """Return a session for making requests.

        :return: The session.
        """

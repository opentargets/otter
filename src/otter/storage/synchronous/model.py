"""Abstract base class for synchronous storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from io import IOBase

from otter.storage.model import Revision, StatResult


class Storage(ABC):
    """Abstract base class for synchronous storage backends."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the storage backend.

        :return: The name of the storage backend.
        :rtype: str
        """

    @abstractmethod
    def stat(
        self,
        location: str,
    ) -> StatResult:
        """Get metadata for a resource.

        :param location: The path or uri to the resource.
        :type location: str
        :return: A :class:`StatResult` object containing the resource metadata.
        :rtype: :class:`StatResult`
        :raises NotFoundError: If the resource does not exist.
        """

    @abstractmethod
    def glob(
        self,
        location: str,
        pattern: str,
    ) -> list[str]:
        """List resources matching a glob pattern under the given location.

        :param location: The base path or uri to search under.
        :type location: str
        :param pattern: The pattern to match for.
        :type pattern: str
        :return: A list of absolute file paths or uris.
        :rtype: list[str]
        """

    @abstractmethod
    def open(
        self,
        location: str,
        mode: str = 'r',
    ) -> IOBase:
        """Open a file-like object for the given location.

        :param location: The path or uri to the file.
        :type location: str
        :param mode: The file mode, e.g. Defaults to 'r' for reading.
        :type mode: str
        :return: A file-like object.
        :rtype: IOBase
        :raises NotFoundError: If the file does not exist (in read mode).
        """

    @abstractmethod
    def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        """Read the contents of a file.

        :param location: The path or uri to the file.
        :type location: str
        :return: A tuple of (file contents as bytes, file revision).
        :rtype: tuple[bytes, Revision]
        :raises NotFoundError: If the file does not exist.
        :raises TimeoutError: If the read operation times out.
        """

    @abstractmethod
    def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        """Read the contents of a file as text.

        :param location: The path or uri to the file.
        :type location: str
        :param encoding: The text encoding. Defaults to 'utf-8'.
        :type encoding: str
        :return: A tuple of (file contents as a string, file revision).
        :rtype: tuple[str, Revision]
        :raises NotFoundError: If the file does not exist.
        """

    @abstractmethod
    def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        """Write data to a file.

        Optionally, an expected revision can be provided to fail the write if
        the current revision does not match.

        .. note:: The locking mechanism to enforce expected revision is
           backend-specific and may be blocking.

        :param location: The path or uri to the file.
        :type location: str
        :param data: The data to write.
        :type data: bytes
        :param expected_revision: (keyword-only) The expected target revision.
        :type expected_revision: Revision
        :return: The revision of the written file.
        :rtype: Revision
        :raises PreconditionFailedError: If expected revision does not match.
        """

    @abstractmethod
    def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision = None,
    ) -> Revision:
        """Write text to a file.

        Optionally, an expected revision can be provided to fail the write if
        the current revision does not match.

        .. note:: The locking mechanism to enforce expected revision is
           backend-specific and may be blocking.

        :param location: The path or uri to the file.
        :type location: str
        :param data: The text to write.
        :type data: str
        :param encoding: (keyword-only) The text encoding. Defaults to 'utf-8'.
        :type encoding: str
        :param expected_revision: (keyword-only) The expected target revision.
        :type expected_revision: Revision
        :return: The revision of the written file.
        :rtype: Revision
        :raises PreconditionFailedError: If expected revision does not match.
        """

    @abstractmethod
    def copy_within(
        self,
        src: str,
        dst: str,
    ) -> Revision:
        """Copy a file within the same storage backend.

        This method allows for efficient copies without downloading/uploading.

        :param src: The source path of the file to copy.
        :type src: str
        :param dst: The destination path to copy the file to.
        :type dst: str
        :return: The revision of the copied file.
        :rtype: Revision
        :raises NotFoundError: If the source file does not exist.
        """

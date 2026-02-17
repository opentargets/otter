"""Noop storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from io import IOBase

from otter.storage.model import Revision, StatResult
from otter.storage.synchronous.model import Storage


class NoopStorage(Storage):
    """No-op storage helper class.

    Used as placeholder for unrecognized protocols.
    """

    @property
    def name(self) -> str:
        """The name of the storage provider."""
        return 'Dummy storage'

    def stat(
        self,
        location: str,
    ) -> StatResult:
        raise NotImplementedError

    def glob(
        self,
        location: str,
        pattern: str,
    ) -> list[str]:
        raise NotImplementedError

    def open(
        self,
        location: str,
        mode: str = 'r',
    ) -> IOBase:
        raise NotImplementedError

    def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        raise NotImplementedError

    def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        raise NotImplementedError

    def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        raise NotImplementedError

    def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision = None,
    ) -> Revision:
        raise NotImplementedError

    def copy_within(self, src: str, dst: str) -> Revision:
        raise NotImplementedError

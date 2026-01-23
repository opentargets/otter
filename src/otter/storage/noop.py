"""Noop storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from otter.storage.model import Revision, StatResult, Storage

if TYPE_CHECKING:
    from typing import IO


class NoopStorage(Storage):
    """No-op storage helper class.

    Used as placeholder in not-implemented storage backends.
    """

    @property
    def name(self) -> str:
        """The name of the storage provider."""
        return 'Dummy storage'

    def stat(self, location: str) -> StatResult:
        raise NotImplementedError

    def open(self, location: str, mode: str = 'r', revision: Revision = None) -> IO:
        raise NotImplementedError

    def glob(self, location: str, pattern: str) -> list[str]:
        raise NotImplementedError

    def download_to_file(self, src: str, dst: Path) -> int:
        raise NotImplementedError

    def download_to_string(self, src: str) -> tuple[str, int]:
        raise NotImplementedError

    def upload(self, src: Path, dst: str, revision: Revision = None) -> int:
        raise NotImplementedError

    def copy_within(self, src: str, dst: str) -> int:
        raise NotImplementedError

"""Noop storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from otter.storage.model import Revision, StatResult, Storage


class NoopStorage(Storage):
    """No-op storage helper class.

    Used as placeholder for unrecognized protocols.
    """

    @property
    def name(self) -> str:
        """The name of the storage provider."""
        return 'Dummy storage'

    async def stat(
        self,
        location: str,
    ) -> StatResult:
        raise NotImplementedError

    async def glob(
        self,
        location: str,
        pattern: str,
    ) -> list[str]:
        raise NotImplementedError

    async def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        raise NotImplementedError

    async def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        raise NotImplementedError

    async def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        raise NotImplementedError

    async def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision = None,
    ) -> Revision:
        raise NotImplementedError

    async def copy_within(self, src: str, dst: str) -> Revision:
        raise NotImplementedError

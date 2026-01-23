"""Local filesystem storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from otter.storage.model import Revision, StatResult, Storage
from otter.util.fs import check_destination

if TYPE_CHECKING:
    from typing import IO


class FilesystemStorage(Storage):
    """Local filesystem storage class."""

    @property
    def name(self) -> str:
        return 'Filesystem Storage'

    def stat(self, location: str) -> StatResult:
        s = Path(location).stat()
        return StatResult(
            is_dir=Path(location).is_dir(),
            is_reg=Path(location).is_file(),
            size=s.st_size,
            revision=s.st_mtime,
            mtime=s.st_mtime,
        )

    def open(self, location: str, mode: str = 'r', revision: Revision = None) -> IO:
        return open(location, mode)

    def glob(self, location: str, pattern: str) -> list[str]:
        if not pattern:
            pattern = '*'
        return [str(p) for p in Path(location).glob(pattern)]

    def download_to_file(self, src: str, dst: Path) -> int:
        if Path(src) == dst:
            logger.debug(f'source and destination are the same file: {src}, download skipped')
            return int(Path(dst).stat().st_mtime)
        return self.copy_within(src, str(dst))

    def download_to_string(self, src: str) -> tuple[str, int]:
        content = Path(src).read_text()
        revision = int(Path(src).stat().st_mtime)
        return (content, revision)

    def upload(self, src: Path, dst: str, revision: Revision = None) -> int:
        raise NotImplementedError

    def copy_within(self, src: str, dst: str) -> int:
        src_path = Path(src)
        dst_path = Path(dst)

        check_destination(dst_path, delete=True)

        # Try hard link first for efficiency
        try:
            dst_path.hardlink_to(src_path)
        except (OSError, NotImplementedError):
            try:
                shutil.copy2(src_path, dst_path)
            except shutil.SameFileError:
                logger.debug(f'source and destination are the same file: {src_path}, copy skipped')

        return int(dst_path.stat().st_mtime)

"""Local filesystem storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import IO, Any, cast

from filelock import FileLock
from loguru import logger

from otter.storage.model import REVISION_ABSENT, Revision, StatResult
from otter.storage.synchronous.model import Storage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError
from otter.util.fs import check_destination

#: the process umask, read once: new files get the mode `write_bytes` would give them
_UMASK = os.umask(0)
os.umask(_UMASK)


def _file_mode(path: Path) -> int:
    """The mode a write to `path` keeps: its current one, or a new file's default."""
    try:
        return path.stat().st_mode & 0o777
    except FileNotFoundError:
        return 0o666 & ~_UMASK


def _replace_atomically(path: Path, data: bytes) -> None:
    """Write `data` to `path` so a concurrent reader sees the old content or the new, never part of it."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        os.chmod(tmp, _file_mode(path))
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


class FilesystemStorage(Storage):
    """Local filesystem storage class."""

    @property
    def name(self) -> str:
        return 'Filesystem Storage'

    def stat(self, location: str) -> StatResult:
        p = Path(location)
        try:
            s = p.stat()
        except FileNotFoundError:
            raise NotFoundError(thing=location)
        return StatResult(
            is_dir=p.is_dir(),
            is_reg=p.is_file(),
            size=s.st_size,
            revision=s.st_mtime,
            mtime=s.st_mtime,
        )

    def glob(
        self,
        location: str,
        pattern: str = '*',
    ) -> list[str]:
        return [str(p) for p in Path(location).glob(pattern)]

    def open(
        self,
        location: str,
        mode: str = 'r',
    ) -> IO[Any]:
        if 'w' in mode:
            Path(location).parent.mkdir(parents=True, exist_ok=True)
        return open(location, mode)

    def _read(
        self,
        location: str,
        mode: str = 'rb',
        encoding: str | None = None,
    ) -> tuple[bytes | str, Revision]:
        while True:
            previous_stat = self.stat(location)
            previous_mtime = previous_stat.mtime
            try:
                with open(location, mode, encoding=encoding) as f:
                    data = f.read()
            except FileNotFoundError:
                raise NotFoundError(thing=location)
            current_stat = self.stat(location)
            current_mtime = current_stat.mtime
            if current_mtime == previous_mtime:
                logger.info(f'downloaded {location}')
                return data, current_stat.revision
            logger.debug(f'file {location} modified during read, retrying')

    def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        data, revision = self._read(location, mode='rb')
        return cast(bytes, data), revision

    def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        try:
            data, revision = self._read(location, mode='r', encoding=encoding)
        except UnicodeDecodeError:
            raise StorageError(f'error decoding {location}')
        return cast(str, data), revision

    def write(
        self,
        location: str,
        data: bytes,
        *,
        expected_revision: Revision = None,
    ) -> Revision:
        p = Path(location)
        p.parent.mkdir(parents=True, exist_ok=True)
        lock_path = p.with_suffix(p.suffix + '.lock')
        if expected_revision is not None:
            lock = FileLock(lock_path, timeout=10)
            try:
                with lock:
                    if expected_revision == REVISION_ABSENT:
                        if p.exists():
                            raise PreconditionFailedError(f'{location} already exists')
                    else:
                        s = self.stat(str(p))
                        r = s.revision
                        if r != expected_revision:
                            raise PreconditionFailedError(f'revision mismatch {expected_revision} {r}')
                    _replace_atomically(p, data)
                    return p.stat().st_mtime
            finally:
                # the lock file is kept: deleting it lets a process waiting on the old file and
                # one creating a new file both hold the lock
                if lock.is_locked:
                    lock.release()
        else:
            _replace_atomically(p, data)
            return p.stat().st_mtime

    def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision = None,
    ) -> Revision:
        return self.write(
            location,
            data.encode(encoding),
            expected_revision=expected_revision,
        )

    def copy_within(self, src: str, dst: str) -> Revision:
        src_path = Path(src)
        dst_path = Path(dst)

        if not src_path.exists():
            raise NotFoundError(thing=src)

        if not src_path.is_file():
            raise ValueError('can only copy regular files')

        check_destination(dst_path, delete=True)

        # try hard linking first
        try:
            dst_path.hardlink_to(src_path)
        except (OSError, NotImplementedError):
            try:
                shutil.copy2(src_path, dst_path)
            except shutil.SameFileError:
                logger.debug(f'copying to same file skipped: {src_path}')

        return dst_path.stat().st_mtime

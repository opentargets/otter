"""Local filesystem storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

import shutil
from pathlib import Path
from stat import S_ISDIR
from typing import IO, Any, cast

from filelock import FileLock
from loguru import logger

from otter.storage.model import Revision, StatResult
from otter.storage.synchronous.model import Storage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError
from otter.util.fs import check_destination


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
                    s = self.stat(str(p))
                    r = s.revision
                    if r != expected_revision:
                        raise PreconditionFailedError(f'revision mismatch {expected_revision} {r}')
                    Path(location).write_bytes(data)
                    return p.stat().st_mtime
            finally:
                if lock.is_locked:
                    lock.release()
                lock_path.unlink(missing_ok=True)
        else:
            Path(location).write_bytes(data)
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

    def delete(self, dst: str, *, is_recursive: bool = False) -> int:
        p = Path(dst)
        try:
            s = p.stat(follow_symlinks=False)
        except FileNotFoundError:
            # The target is already missing; deleting it again is a successful no-op.
            logger.debug(f'{dst} does not exist, nothing to delete')
            return 0
        except OSError as e:
            # Inspection failed for another reason, such as permissions; report the failure.
            raise StorageError(f'error deleting {dst}: {e}')

        # a real directory, as opposed to a symlink pointing at one
        if S_ISDIR(s.st_mode):
            if not is_recursive:
                raise StorageError(f'{dst} is a directory, pass is_recursive to delete it')
            try:
                # count before deleting, rmtree does not report what it removed
                count = sum(1 for f in p.rglob('*') if f.is_file() or f.is_symlink())
                shutil.rmtree(p)
            except FileNotFoundError as e:
                # Counting or deletion tried to access a path that does not exist.
                # Check whether the directory we were asked to delete is also missing,
                # or whether it still exists and deletion failed somewhere inside it.
                try:
                    p.stat(follow_symlinks=False)
                except FileNotFoundError:
                    # The target itself is gone, so there is nothing left to delete.
                    logger.debug(f'{dst} does not exist, nothing to delete')
                    return 0
                except OSError as stat_error:
                    # Checking whether the directory exists failed, for example due to permissions.
                    # Its existence is unknown, so we cannot return 0 to indicate it is missing.
                    # Report the error from this check as a StorageError.
                    raise StorageError(f'error deleting directory {dst}: {stat_error}')
                # The target still exists, so deletion is incomplete; report the original error.
                raise StorageError(f'error deleting directory {dst}: {e}')
            except OSError as e:
                # Counting or removal failed for another reason; report it rather than returning a count.
                raise StorageError(f'error deleting directory {dst}: {e}')
            logger.info(f'deleted directory {dst} and the {count} files in it')
            return count

        # a file or a symlink
        # deleting something already gone is fine, not an error (idempotence)
        try:
            p.unlink()
        except FileNotFoundError:
            # The file or symlink disappeared after stat; there is nothing left to delete.
            logger.debug(f'{dst} does not exist, nothing to delete')
            return 0
        except OSError as e:
            # Unlink failed for another reason, such as permissions; report the failure.
            raise StorageError(f'error deleting {dst}: {e}')
        logger.info(f'deleted {dst}')
        return 1

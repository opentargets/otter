"""Local file system utilities."""

import hashlib
import os
import tempfile
from pathlib import Path

from loguru import logger

from otter.util.errors import FSError


def check_dir(path: Path | str) -> None:
    """Check working conditions for a directory.

    The function will make sure that the directory exists and is writable. If it
    does not exist, the function will attempt to create it.

    :param path: The directory to check.
    :type path: Path | str
    :raises FSError: If the path exists but is not a directory or is not writable;
        or if the directory does not exist and cannot be created.
    """
    if isinstance(path, str):
        path = Path(path)

    if path.is_file():
        raise FSError(f'not a directory: {path}')
    if path.is_dir():
        if not os.access(path, os.W_OK):
            raise FSError('directory not writable')
    else:
        logger.info(f'creating {path}')
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise FSError(f'error creating directory: {e}')
    logger.trace(f'directory {path} passed checks')


def check_source(path: Path | str) -> None:
    """Check working conditions for a file.

    The function will make sure that the file exists and is readable.

    :param path: The path to check. Must be a file.
    :type path: Path | str
    :raises FSError: If the path is not a file, does not exist, or is not readable.
    """
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        raise FSError(f'file not found: {path}')
    if not path.is_file():
        raise FSError(f'not a file: {path}')
    if not os.access(path, os.R_OK):
        raise FSError(f'file not readable: {path}')
    logger.trace('source passed checks')


def check_destination(path: Path | str, *, delete: bool = False) -> None:
    """Check working conditions for a destination path.

    The function will make sure that the file does not exist and that the parent
    directory exists and is writable. If the parent directory does not exist, the
    function will attempt to create it.

    If ``delete`` is ``True``, the function will delete the file if it already
    exists.

    .. warning:: This function can potentially delete files!

    :param path: The path to check. Must be a file.
    :type path: Path | str
    :param delete: Whether to delete the file if it already exists.
    :type delete: bool
    :raises FSError: If the parent directory checks fail, or if the file exists
        and ``delete`` is ``False``.
    """
    if isinstance(path, str):
        path = Path(path)

    check_dir(path.parent)
    if path.is_file():
        if delete:
            logger.warning(f'file {path} already exists, deleting it')
            try:
                path.unlink()
            except OSError as e:
                raise FSError(f'error deleting file {path}: {e}')
        else:
            raise FSError(f'file {path} already exists')
    logger.trace('destination passed checks')


#: where :func:`lock_path` puts lock files
LOCK_DIR = Path(tempfile.gettempdir()) / 'otter-locks'


def lock_path(path: Path | str) -> Path:
    """The lock file that guards reads and writes of a local file.

    It is kept outside the file's directory, so that globs over the data never
    pick it up, and it is named after a hash of the file's resolved path, so
    that every name of the file (a symlink, a relative path) shares one lock.

    Lock files are never deleted: a lock file removed while a process waits on
    it lets that process and a newcomer, who creates a new one, both hold the
    lock. They are empty, and there is one per file ever locked.

    .. note:: The lock only works between processes that share a temporary
       directory (``TMPDIR``), which normally means the same host and user.

    :param path: The file to guard.
    :type path: Path | str
    :return: The lock file's path. Its directory exists.
    :rtype: Path
    """
    key = hashlib.sha256(str(Path(path).resolve()).encode()).hexdigest()
    LOCK_DIR.mkdir(exist_ok=True)
    return LOCK_DIR / f'{key}.lock'

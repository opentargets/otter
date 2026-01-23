"""Local file system utilities."""

import os
from pathlib import Path

from loguru import logger

from otter.util.errors import FSError


def check_dir(path: Path) -> None:
    """Check working conditions for a directory.

    The function will make sure that the directory exists and is writable. If it
    does not exist, the function will attempt to create it.

    :param path: The directory to check.
    :type path: Path
    :raises FSError: If the path exists but is not a directory or is not writable;
        or if the directory does not exist and cannot be created.
    """
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


def check_source(path: Path) -> None:
    """Check working conditions for a file.

    The function will make sure that the file exists and is readable.

    :param path: The path to check. Must be a file.
    :type path: Path
    :raises FSError: If the path is not a file, does not exist, or is not readable.
    """
    if not path.exists():
        raise FSError(f'file not found: {path}')
    if not path.is_file():
        raise FSError(f'not a file: {path}')
    if not os.access(path, os.R_OK):
        raise FSError(f'file not readable: {path}')
    logger.trace('source passed checks')


def check_destination(path: Path, *, delete: bool = False) -> None:
    """Check working conditions for a destination path.

    The function will make sure that the file does not exist and that the parent
    directory exists and is writable. If the parent directory does not exist, the
    function will attempt to create it.

    If ``delete`` is ``True``, the function will delete the file if it already
    exists.

    .. warning:: This function can potentially delete files!

    :param path: The path to check. Must be a file.
    :type path: Path
    :param delete: Whether to delete the file if it already exists.
    :type delete: bool
    :raises FSError: If the parent directory checks fail, or if the file exists
        and ``delete`` is ``False``.
    """
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

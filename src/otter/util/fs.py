"""File system utilities."""

import errno
import os
from pathlib import Path

from loguru import logger


def check_file(path: Path) -> None:
    """Check working conditions for a file.

    The function will make sure that a file does not exist in the given path.

    .. warning:: The function will delete the file if it already exists.

    :param path: The path to check. Must be a file.
    :type path: Path
    :raises SystemExit: If there is an error deleting the file.
    :return: `None` if all checks pass.
    :rtype: None
    """
    if path.is_file():
        logger.warning(f'file {path} already exists, deleting')
        try:
            path.unlink()
        except OSError as e:
            logger.critical(f'error deleting {path}: {e}')
            raise SystemExit(e.errno)
    logger.debug(f'file {path} passed checks')


def check_dir(path: Path) -> None:
    """Check working conditions for a directory.

    The function will make sure that the directory exists and is writable. If it
    does not exist, the function will attempt to create it.

    :param path: The directory to check.
    :type path: Path
    :raises SystemExit: If the directory is not writable.
    :raises SystemExit: If there is an error creating the directory.
    :return: `None` if all checks pass.
    :rtype: None
    """
    if path.is_file():
        logger.critical(f'{path} exists and is a file, expected a directory')
        raise SystemExit(errno.ENOTDIR)

    if path.is_dir():
        if not os.access(path, os.W_OK):
            logger.critical('directory is not writtable')
            raise SystemExit(errno.EEXIST)
    else:
        logger.debug(f'directory {path} does not exist, creating it')
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.critical(f'error creating dir: {e}')
            raise SystemExit(e.errno)
    logger.debug(f'directory {path} passed checks')


def check_fs(path: Path) -> None:
    """Check working conditions for a file and its parent directory.

    The function will make sure that the file does not exist and that the parent
    directory exists and is writable. If the parent directory does not exist, the
    function will attempt to create it.

    .. warning:: The function will delete the file if it already exists.

    :param path: The path to check. Must be a file.
    :type path: Path
    :raises SystemExit: If the file already exists.
    :raises SystemExit: If the parent directory is not writable.
    :raises SystemExit: If there is an error creating the parent directory.
    :return: `None` if all checks pass.
    :rtype: None
    """
    check_dir(path.parent)
    check_file(path)
    logger.debug('file and directory checks passed')

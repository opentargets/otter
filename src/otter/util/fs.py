"""File system utilities."""

import errno
import os
from pathlib import Path

from loguru import logger


def check_file_not_exists(path: Path, *, delete: bool = False) -> None:
    """Ensure a file does not exist, optionally deleting it.

    The function will make sure that a file does not exist in the given path. If
    ``delete`` is ``True``, the function will delete the file if it already exists.

    .. warning:: This function can potentially delete files!

    :param path: The path to check. Must be a file.
    :type path: Path
    :param delete: Whether to delete the file if it already exists.
    :type delete: bool
    :raises SystemExit: If the file exists and ``delete`` is ``False``. Or if
        there is an error deleting the file.
    :return: `None` if all checks pass.
    :rtype: None
    """
    if path.is_file():
        if delete:
            logger.warning(f'file {path} already exists, deleting it')
            try:
                path.unlink()
            except OSError as e:
                logger.critical(f'error deleting {path}: {e}')
                raise SystemExit(e.errno)
        else:
            logger.critical(f'file {path} already exists')
            raise SystemExit(errno.EEXIST)
    logger.debug(f'file {path} passed checks')


def check_file_exists(path: Path) -> None:
    """Ensure a file exists.

    The function will make sure that a file exists in the given path.

    :param path: The path to check. Must be a file.
    :type path: Path
    :raises SystemExit: If the file does not exist.
    :return: `None` if all checks pass.
    :rtype: None
    """
    if not path.is_file():
        logger.critical(f'file {path} does not exist')
        raise SystemExit(errno.ENOENT)
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


def check_source(path: Path) -> None:
    """Check working conditions for a file.

    The function will make sure that the file exists and is readable.

    :param path: The path to check. Must be a file.
    :type path: Path
    :raises SystemExit: If the file does not exist.
    :raises SystemExit: If the file is not readable.
    :return: `None` if all checks pass.
    :rtype: None
    """
    check_dir(path.parent)
    check_file_exists(path)
    logger.debug('source passed checks')


def check_destination(path: Path, *, delete: bool = False) -> None:
    """Check working conditions for a file and its parent directory.

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
    :raises SystemExit: If the file already exists.
    :raises SystemExit: If the parent directory is not writable.
    :raises SystemExit: If there is an error creating the parent directory.
    :return: `None` if all checks pass.
    :rtype: None
    """
    check_dir(path.parent)
    check_file_not_exists(path, delete=delete)
    logger.debug('destination passed checks')

"""Validators for files."""

from loguru import logger

from otter.config.model import Config
from otter.storage.handle import StorageHandle

REQUEST_TIMEOUT = 10


def exists(
    location: str,
    config: Config | None = None,
    force_local: bool = False,
) -> bool:
    """Check if a file exists.

    Optionally takes a :class:`otter.config.model.Config` instance to work with
    relative locations.

    :param location: The file location.
    :type location: str
    :param config: The :class:`otter.config.model.Config` instance.
    :type config: Config | None
    :param force_local: Whether to force local resolution.
    :type local: bool
    :return: True if the file exists, False otherwise.
    :rtype: bool
    """
    logger.trace(location)
    try:
        StorageHandle(location, config=config, force_local=force_local).stat()
        logger.trace(f'file {location} exists')
    except Exception as e:
        logger.warning(f'error when doing stat: {e}')
        return False
    return True


def size(
    one: str,
    two: str,
    config: Config | None = None,
    force_local: bool = False,
) -> bool:
    """Check if two files have the same size.

    Optionally takes a :class:`otter.config.model.Config` instance to work with
    relative locations.

    :param one: The first file location
    :type one: str
    :param two: The second file location
    :type two: str
    :param config: The :class:`otter.config.model.Config` instance.
    :type config: Config | None
    :param force_local: Whether to force local resolution on the second file.
    :type local: bool
    :return: True if the file sizes match, False otherwise.
    :rtype: bool
    """
    logger.debug(f'checking if {one} and {two} are the same size')

    try:
        one_handle = StorageHandle(one, config=config)
        one_stat = one_handle.stat()
        if not one_stat.size:
            logger.warning(f'could not determine size of {one}, skipping size comparison')
            return True
        two_handle = StorageHandle(two, config=config, force_local=force_local)
        two_stat = two_handle.stat()
        if not two_stat.size:
            logger.warning(f'could not determine size of {two}, skipping size comparison')
            return True
        logger.trace(f'size of {one}: {one_stat.size}, size of {two}: {two_stat.size}')
    except Exception as e:
        logger.warning(f'error when doing stat: {e}')
        return False
    return one_stat.size == two_stat.size

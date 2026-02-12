"""File validators."""

from loguru import logger

from otter.config.model import Config
from otter.storage.handle import StorageHandle
from otter.util.errors import TaskValidationError


async def exists(
    location: str,
    config: Config | None = None,
    force_local: bool = False,
) -> None:
    """Check if a file exists.

    Optionally takes a :class:`otter.config.model.Config` instance to work with
    relative locations.

    :param location: The file location.
    :type location: str
    :param config: The :class:`otter.config.model.Config` instance.
    :type config: Config | None
    :param force_local: Whether to force local resolution.
    :type force_local: bool
    :raises TaskValidationError: If the file does not exist.
    """
    logger.debug(f'checking if file exists: {location}')

    try:
        await StorageHandle(location, config=config, force_local=force_local).stat()
        logger.trace(f'file {location} exists')
    except Exception as e:
        # log as warning because this is expected if validator fails
        logger.warning(f'error when doing stat: {e}')
        raise TaskValidationError(f'file does not exist: {location}') from e


async def size(
    one: str,
    two: str,
    config: Config | None = None,
    force_local: bool = False,
) -> None:
    """Check if two files have the same size.

    Optionally takes a :class:`otter.config.model.Config` instance to work with
    relative locations.

    If the file size cannot be determined for either file, it logs a warning but
    does not fail validation, since many sources do not provide file size
    information.

    :param one: The first file location
    :type one: str
    :param two: The second file location
    :type two: str
    :param config: The :class:`otter.config.model.Config` instance.
    :type config: Config | None
    :param force_local: Whether to force local resolution on the second file.
    :type force_local: bool
    :raises TaskValidationError: If the file sizes does not match.
    """
    logger.debug(f'checking if {one} and {two} are the same size')

    try:
        one_handle = StorageHandle(one, config=config)
        one_stat = await one_handle.stat()
        if not one_stat.size:
            logger.warning(f'could not determine size of {one}, skipping size comparison')
            return

        two_handle = StorageHandle(two, config=config, force_local=force_local)
        two_stat = await two_handle.stat()
        if not two_stat.size:
            logger.warning(f'could not determine size of {two}, skipping size comparison')
            return

        logger.trace(f'size of {one}: {one_stat.size}, size of {two}: {two_stat.size}')

        if one_stat.size != two_stat.size:
            raise TaskValidationError(f'file size mismatch: {one} ({one_stat.size}) != {two} ({two_stat.size})')

    except TaskValidationError:
        raise
    except Exception as e:
        logger.warning(f'error when doing stat: {e}')
        raise TaskValidationError(f'could not compare file sizes: {one} and {two}') from e

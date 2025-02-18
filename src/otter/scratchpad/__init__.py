"""Scratchpad package."""

from pathlib import Path

from loguru import logger

from otter.config.yaml import parse_yaml
from otter.scratchpad.model import Scratchpad


def load_scratchpad(config_path: Path) -> Scratchpad:
    """Load the scratchpad.

    See :class:`otter.scratchpad.model.Scratchpad` for the scratchpad object.

    :param config_path: The path to the config file.
    :type config_path: Path
    :return: The scratchpad object.
    :rtype: Scratchpad
    """
    config_dict = parse_yaml(config_path)
    sentinel_dict = config_dict.get('scratchpad', {})

    logger.trace(f'loaded scratchpad: {sentinel_dict}')

    return Scratchpad(sentinel_dict=sentinel_dict)

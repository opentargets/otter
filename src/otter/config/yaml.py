"""YAML config parser."""

from __future__ import annotations

import errno
import sys
from functools import cache
from pathlib import Path
from typing import Any

import yaml
from loguru import logger


@cache
def parse_yaml(path: Path) -> dict[str, Any]:
    """Parse a yaml file.

    Loads a yaml file, parses its content, and returns it as a dictionary. There
    is a cache, so the file is only read once.

    .. warning:: If the file cannot be read or the yaml content cannot be parsed,
        the program will log an error and exit.

    :param path: The path to the yaml file.
    :type path: Path
    :return: The parsed yaml content.
    :rtype: dict
    """
    logger.debug(f'loading yaml file {path}')
    try:
        data: dict[str, Any] = yaml.safe_load(path.read_text())
        if not data:
            logger.critical('empty file configuration file')
            sys.exit(errno.EINVAL)
        return data
    except OSError as e:
        logger.critical(f'error reading config file: {e}')
        sys.exit(errno.EINVAL)
    except (yaml.YAMLError, TypeError) as e:
        logger.critical(f'error parsing config file: {e}')
        sys.exit(errno.EINVAL)

"""Environment variable parser."""

import errno
import inspect
import os

from loguru import logger
from pydantic import ValidationError

from otter.config.model import BaseConfig
from otter.util.errors import log_pydantic


def _get_caller_package() -> str:
    frame = inspect.currentframe()
    while frame:
        if frame.f_back and frame.f_back.f_globals.get('__package__'):
            return frame.f_back.f_globals['__package__'].split('.')[0]
        frame = frame.f_back
    return __name__.split('.')[0]


ENV_PREFIX = _get_caller_package().upper()


def env_to_config(name: str) -> str:
    """Convert an env var to its config variable name."""
    return name[len(f'{ENV_PREFIX}_') :].lower()


def parse_env() -> BaseConfig:
    """Parses the environment variables and returns an BaseConfig object.

    :return: The parsed environment variables.
    :rtype: BaseConfig
    """
    logger.debug('parsing environment variables')

    # this builds a dict of all environment variables that start with the prefix
    config_dict = {env_to_config(k): v for k, v in os.environ.items() if k.startswith(f'{ENV_PREFIX}_')}

    try:
        return BaseConfig.model_validate_strings(config_dict)
    except ValidationError as e:
        logger.critical('config error: invalid env vars')
        logger.error(log_pydantic(e))
        raise SystemExit(errno.EINVAL)

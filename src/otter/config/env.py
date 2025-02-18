"""Environment variable parser."""

import errno
import os

from loguru import logger
from pydantic import ValidationError

from otter.config.model import BaseConfig
from otter.util.errors import log_pydantic


def env_to_config(env_var: str, env_prefix: str) -> str:
    """Convert an env var to its config variable name."""
    return env_var[len(f'{env_prefix}_') :].lower()


def parse_env(runner_name: str) -> BaseConfig:
    """Parses the environment variables and returns an BaseConfig object.

    :param str runner_name: The name of the runner.
    :type runner_name: str
    :return: The parsed environment variables.
    :rtype: BaseConfig
    """
    logger.debug('parsing environment variables')
    env_prefix = runner_name.upper()

    # this builds a dict of all environment variables that start with the prefix
    config_dict = {env_to_config(k, env_prefix): v for k, v in os.environ.items() if k.startswith(f'{env_prefix}_')}

    try:
        return BaseConfig.model_validate_strings(config_dict)
    except ValidationError as e:
        logger.critical('config error: invalid env vars')
        logger.error(log_pydantic(e))
        raise SystemExit(errno.EINVAL)

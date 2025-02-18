"""Configuration package."""

import errno

from loguru import logger
from pydantic import ValidationError

from otter.config.cli import parse_cli
from otter.config.env import parse_env
from otter.config.model import Config, Defaultconfig, YamlConfig
from otter.config.yaml import parse_yaml
from otter.util.errors import StepInvalidError, log_pydantic
from otter.util.logger import init_logger


def load_config() -> Config:
    """Load the config.

    See :class:`otter.config.model.Config` for the `Config` object.

    :return: The config object.
    :rtype: Config
    """
    # start with defaults
    dfl = Defaultconfig()

    # parse env vars and cli args
    env = parse_env()
    cli = parse_cli()

    # get the step
    step = cli.step or env.step
    if not step:
        init_logger(log_level='TRACE')  # must initialize logger to dump queue
        logger.critical('no step provided')
        raise SystemExit(errno.EINVAL)

    # load the yaml config, changing the steps dict into a list of steps
    config_path = (cli.config_path or env.config_path or dfl.config_path).resolve().absolute()
    config_dict = parse_yaml(config_path)
    try:
        yml = YamlConfig(**config_dict | {'steps': list(config_dict.get('steps', {}))})
    except ValidationError as e:
        init_logger(log_level='TRACE')  # must initialize logger to dump queue
        logger.critical('error validating yaml config')
        logger.error(log_pydantic(e))
        raise SystemExit(errno.EINVAL)

    # merge all configs into the final config
    # order or precedence: cli > env > yaml > defaults
    try:
        config = Config(
            step=step,
            steps=yml.steps,
            config_path=config_path,
            work_path=(cli.work_path or env.work_path or yml.work_path or dfl.work_path).resolve().absolute(),
            release_uri=cli.release_uri or env.release_uri or yml.release_uri or dfl.release_uri,
            pool_size=cli.pool_size or env.pool_size or yml.pool_size or dfl.pool_size,
            log_level=cli.log_level or env.log_level or yml.log_level or dfl.log_level,
        )
    except Exception as e:
        init_logger(log_level='TRACE')  # must initialize logger to dump queue
        if type(e) is StepInvalidError:
            logger.critical(f'invalid step: {step}')
            logger.info(f'valid steps are: {yml.steps}')
        else:
            logger.critical('config error')
            logger.error(e)
        raise SystemExit(errno.EINVAL)

    logger.trace(f'loaded settings: {config}')
    if config.release_uri is None:
        logger.info('no release uri provided, run is local')
    return config

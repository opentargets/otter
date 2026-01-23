"""CLI argument parser."""

from __future__ import annotations

import argparse
import os
from typing import TYPE_CHECKING

from loguru import logger

from otter.config.model import BaseConfig, Config

if TYPE_CHECKING:
    from argparse import Action


def config_to_env(var: str, runner_name: str) -> str:
    """Convert a config variable name to its env var."""
    return f'{runner_name.upper()}_{var.upper()}'


def parse_cli(runner_name: str) -> BaseConfig:
    """Parse the command line arguments.

    :param runner_name: The name of the runner.
    :type runner_name: str
    :return: The parsed command line arguments.
    :rtype: BaseConfig
    """
    logger.debug('parsing cli arguments')

    class HelpFormatter(argparse.HelpFormatter):
        def _get_help_string(self, action: Action) -> str:
            if action.default is not argparse.SUPPRESS:
                action.help = f'{action.help} (environment variable: {config_to_env(action.dest, runner_name)})'
                default_value = Config.model_fields[action.dest].default
                if default_value != '':  # noqa: PLC1901
                    action.help += f' (default: {default_value})'
            return f'{action.help}'

    parser = argparse.ArgumentParser(
        description='Open Targets Pipeline Input Stage application.',
        formatter_class=HelpFormatter,
    )

    parser.add_argument(
        '-s',
        '--step',
        required=config_to_env('step', runner_name) not in os.environ,
        help='The step to run',
    )

    parser.add_argument(
        '-c',
        '--config-path',
        help='The path for the configuration file.',
    )

    parser.add_argument(
        '-w',
        '--work-path',
        help='The local working path. This is where files will be downloaded and '
        'the manifest and logs will be written to.',
    )

    parser.add_argument(
        '-r',
        '--release-uri',
        help='If set, this URI will be used as the release location. This is where '
        'files will be uploaded and the manifest and logs will be written to.'
        'If omitted, the run will be local only.',
    )

    parser.add_argument(
        '-p',
        '--pool-size',
        type=int,
        help='The number of worker proccesses that will be spawned to run tasks'
        'in the step in parallel. It should be similar to the number of cores,'
        'but could be higher because there is a lot of I/O blocking. Must be > 1.',
    )

    parser.add_argument(
        '-l',
        '--log-level',
        choices=['TRACE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Log level for the application.',
    )
    parsed_args, unknown_args = parser.parse_known_args()
    if unknown_args:
        logger.warning(f'unknown arguments: {unknown_args}')
    settings_vars = vars(parsed_args)
    settings_dict = {k: v for k, v in settings_vars.items() if v is not None}

    return BaseConfig.model_validate(settings_dict)

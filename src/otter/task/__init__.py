"""Task module."""

import errno
from pathlib import Path

from loguru import logger
from pydantic import ValidationError

from otter.config.yaml import parse_yaml
from otter.task.model import Spec
from otter.util.errors import log_pydantic


def load_specs(config_path: Path, step_name: str) -> list[Spec]:
    """Load `Specs` for a `Step`.

    .. seealso:: :class:`otter.task.model.Spec`, :func:`otter.step.model.Step`

    :return: The task specs.
    :rtype: list[Spec]
    """
    config_dict = parse_yaml(config_path)
    spec_dicts = config_dict.get('steps', {}).get(step_name, [])

    logger.trace(f'loaded task specs for step {step_name}: {spec_dicts}')

    try:
        return [Spec(**s) for s in spec_dicts]
    except ValidationError as e:
        logger.critical('error validating task specs')
        logger.error(log_pydantic(e))
        raise SystemExit(errno.EINVAL)

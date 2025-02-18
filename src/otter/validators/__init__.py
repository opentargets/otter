"""Resource validators.

This module contains the validators for the resources generated by tasks.
"""

from collections.abc import Callable
from typing import Any

from loguru import logger

from otter.util.errors import TaskValidationError


def v(func: Callable[..., bool], *args: Any, **kwargs: Any) -> None:
    """Validate a resource.

    `v` is the way to run a validator inside of the `validate` function of a Task.
    Validators are meant to return a boolean, and v will take care of handling
    it in a common way.

    `v` will run the validator function passed as first parameter passing any args
    and kwargs to it.

    :param func: The validator function to run.
    :type func: Callable[..., bool]

    :raises StepValidationError: If the validator returns False.
    """
    logger.debug(f'running validator {func.__name__}')

    result = func(*args, **kwargs)
    if result is False:
        raise TaskValidationError(f'validator {func.__name__} failed')

    logger.debug(f'validator {func.__name__} passed')

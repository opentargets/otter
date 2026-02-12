"""Models for the config."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlparse

from pydantic import AfterValidator, BaseModel, ConfigDict, model_validator

from otter.util.errors import StepInvalidError

LOG_LEVELS = Literal['TRACE', 'DEBUG', 'INFO', 'SUCCESS', 'WARNING', 'ERROR', 'CRITICAL']
"""The log levels."""


def _validate_uri(uri: str) -> str:
    result = urlparse(uri)
    if not all([result.scheme, result.netloc, result.path]):
        raise ValueError(f'invalid uri: {uri}')
    return uri.rstrip('/')


def _validate_pool_size(size: int) -> int:
    if size < 2:
        raise ValueError('pool_size must be > 1')
    return size


class BaseConfig(BaseModel):
    """Base config model."""

    runner_name: str | None = None
    step: str | None = None
    config_path: Path | None = None
    work_path: Path | None = None
    release_uri: Annotated[str, AfterValidator(_validate_uri)] | None = None
    pool_size: Annotated[int, AfterValidator(_validate_pool_size)] | None = None
    log_level: LOG_LEVELS | None = None


class Defaultconfig(BaseModel):
    """Default config model."""

    runner_name: str = 'otter'
    config_path: Path = Path('config.yaml')
    work_path: Path = Path('./output')
    release_uri: str | None = None
    pool_size: int = 5
    log_level: LOG_LEVELS = 'INFO'


class YamlConfig(BaseModel):
    """YAML config model."""

    work_path: Path | None = None
    release_uri: Annotated[str, AfterValidator(_validate_uri)] | None = None
    pool_size: Annotated[int, AfterValidator(_validate_pool_size)] | None = None
    log_level: LOG_LEVELS | None = None
    steps: list[str] = []


class Config(BaseModel):
    """Config model.

    This model is used to define the config for the application.

    It is constructed by merging the config from the defaults, env vars, CLI, and
    YAML config file. The config is loaded in order of precedence:
    1. Command line arguments
    2. Environment variables
    3. YAML configuration file
    4. Default settings

    The model is frozen after validation to prevent further modification.
    """

    model_config = ConfigDict(frozen=True)

    @model_validator(mode='after')
    def _step_in_step_list(self) -> Config:
        if self.step not in self.steps:
            raise StepInvalidError(f'invalid step: {self.step}')
        return self

    runner_name: str = 'otter'
    """The name of the runner/application."""

    step: str
    """The step to run. This is a required field."""

    steps: list[str] = []
    """The list of steps defined in the configuration file."""

    config_path: Path = Path('config.yaml')
    """The path to the configuration file."""

    work_path: Path = Path('./output')
    """The local working path. This is where resources will be downloaded and
    the manifest and logs will be written to before upload to the GCS bucket."""

    release_uri: Annotated[str, AfterValidator(_validate_uri)] | None = None
    """The release URI. If present, this is where resources, logs and manifest
    will be uploaded to."""

    pool_size: Annotated[int, AfterValidator(_validate_pool_size)] = 5
    """The maximum number of workers in the pool where tasks will run."""

    log_level: LOG_LEVELS = 'INFO'
    """See :data:`LOG_LEVELS`."""

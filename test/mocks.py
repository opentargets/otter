"""Mocks used throughout tests."""

from pathlib import Path

import pytest

from otter.config import Config
from otter.config.model import LOG_LEVELS
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext


def fake_config(
    step: str = 'test-step',
    steps: list[str] | None = None,
    config_path: Path | str = Path('config.yaml'),
    work_path: Path | str = Path('/tmp/otter-test'),
    release_uri: str | None = 'gs://test-bucket/release/path',
    pool_size: int = 2,
    log_level: LOG_LEVELS = 'DEBUG',
) -> Config:
    """Create a mock Config for testing.

    Args:
        step: The step to run. Defaults to 'test-step'.
        steps: The list of steps. Defaults to [step] if not provided.
        config_path: The path to the config file. Defaults to 'config.yaml'.
        work_path: The local working path. Defaults to '/tmp/otter-test'.
        release_uri: The release URI. Defaults to 'gs://test-bucket/release/path'.
        pool_size: The pool size. Defaults to 2.
        log_level: The log level. Defaults to 'DEBUG'.

    Returns:
        A Config instance with the provided values.
    """
    if steps is None:
        steps = [step]

    if isinstance(config_path, str):
        config_path = Path(config_path)

    if isinstance(work_path, str):
        work_path = Path(work_path)

    return Config(
        step=step,
        steps=steps,
        config_path=config_path,
        work_path=work_path,
        release_uri=release_uri,
        pool_size=pool_size,
        log_level=log_level,
    )


@pytest.fixture
def mock_spec():
    """Create a mock spec."""
    return Spec(name='test_task mock task')


@pytest.fixture
def mock_context():
    """Create a mock task context."""
    return TaskContext(
        config=fake_config(),
        scratchpad=Scratchpad(),
    )


class AsyncMockTask(Task):
    """Mock async task for testing."""

    def __init__(self, spec: Spec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.run_called = False
        self.validate_called = False
        self.abort_called = False

    async def run(self):
        """Mock async run method."""
        self.run_called = True
        return self

    async def validate(self):
        """Mock async validate method."""
        self.validate_called = True
        return self

    def abort(self):
        """Mock abort method."""
        self.abort_called = True


class SyncMockTask(Task):
    """Mock sync task for testing."""

    def __init__(self, spec: Spec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.run_called = False
        self.validate_called = False
        self.abort_called = False

    def run(self):  # Not async!
        """Mock sync run method."""
        self.run_called = True
        return self

    def validate(self):  # Not async!
        """Mock validate method."""
        self.validate_called = True
        return self

    def abort(self):
        """Mock abort method."""
        self.abort_called = True

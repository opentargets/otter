"""Fixtures and utilities for end-to-end testing."""

import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from otter.core import Runner
from otter.step.model import Step


@pytest.fixture
def e2e_config_path() -> Path:
    """Path to the e2e test config file."""
    return Path(__file__).parent / 'test_config.yaml'


@pytest.fixture
def e2e_work_dir():
    """Temporary work directory for e2e tests (auto-cleaned)."""
    temp_dir = tempfile.mkdtemp(prefix='otter-e2e-test-')
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


async def run_step_e2e(step_name: str, config_path: Path, work_dir: Path) -> Step:
    """Run a step end-to-end."""
    test_argv = ['otter', '-s', step_name, '-c', str(config_path), '-w', str(work_dir)]
    with patch.object(sys, 'argv', test_argv):
        runner = Runner(name='otter')
        runner.start()
        return await runner.run()

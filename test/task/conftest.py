"""Fixtures for task tests."""

import pytest

from otter.scratchpad.model import Scratchpad
from otter.task.task_registry import TaskRegistry
from test.mocks import fake_config


@pytest.fixture
def task_registry():
    """Create a task registry with default config and scratchpad."""
    config = fake_config()
    scratchpad = Scratchpad()
    return TaskRegistry(config, scratchpad)


@pytest.fixture
def task_registry_with_scratchpad():
    """Create a task registry with scratchpad populated."""
    config = fake_config()
    scratchpad = Scratchpad()
    scratchpad.sentinel_dict = {
        'test_var': 'test_value',
        'source_path': '/path/to/source',
        'dest_path': '/path/to/dest',
    }
    return TaskRegistry(config, scratchpad)

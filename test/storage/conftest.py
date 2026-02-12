"""Shared fixtures for storage tests."""

from pathlib import Path

import pytest


@pytest.fixture
def work_path(tmp_path: Path) -> Path:
    """Create a work path for testing."""
    work = tmp_path / 'work'
    work.mkdir()
    return work

"""End-to-end tests for step execution."""

import pytest

from otter.manifest.model import Result

from .conftest import run_step_e2e


@pytest.mark.e2e
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('step_name', 'expected_task_count'),
    [
        ('simple', 1),
        ('multi_task', 3),
        ('explode_test', 2),
    ],
)
async def test_step_runs_successfully(step_name, expected_task_count, e2e_config_path, e2e_work_dir):
    """Test that steps run and complete successfully."""
    step = await run_step_e2e(step_name, e2e_config_path, e2e_work_dir)

    assert step.manifest.result == Result.SUCCESS
    assert len(step.manifest.tasks) == expected_task_count
    for task in step.manifest.tasks:
        assert task.result == Result.SUCCESS
        assert task.started_run_at is not None
        assert task.finished_run_at is not None

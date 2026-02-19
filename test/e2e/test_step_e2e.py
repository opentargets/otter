"""End-to-end tests for step execution."""

from pathlib import Path

import pytest

from otter.manifest.model import Result
from test.e2e.conftest import run_step_e2e


@pytest.mark.e2e
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('step_name', 'expected_task_count'),
    [
        ('simple', 1),
        ('multi_task', 3),
        ('explode_test', 3),
    ],
)
async def test_step_runs_successfully(
    step_name: str,
    expected_task_count: int,
    e2e_config_path: Path,
    e2e_work_dir: Path,
):
    step = await run_step_e2e(
        step_name,
        e2e_config_path,
        e2e_work_dir,
    )

    assert step.manifest.result == Result.SUCCESS
    assert len(step.manifest.tasks) == expected_task_count
    for task in step.manifest.tasks:
        assert task.result == Result.SUCCESS
        assert task.started_run_at is not None
        assert task.finished_run_at is not None


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_step_fails_when_task_fails(
    e2e_config_path: Path,
    e2e_work_dir: Path,
):
    step = await run_step_e2e(
        'bad_copy',
        e2e_config_path,
        e2e_work_dir,
    )

    assert step.manifest.result == Result.FAILURE
    assert len(step.manifest.tasks) == 1
    task = step.manifest.tasks[0]
    assert task.result == Result.FAILURE
    assert task.failure_reason is not None
    assert step.manifest.failure_reason is not None


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_tasks_with_requires_run_after_dependencies(
    e2e_config_path: Path,
    e2e_work_dir: Path,
):
    step = await run_step_e2e(
        'multi_task',
        e2e_config_path,
        e2e_work_dir,
    )

    assert step.manifest.result == Result.SUCCESS

    by_name = {t.name: t for t in step.manifest.tasks}
    first = by_name['hello_world first']
    second = by_name['hello_world second']
    third = by_name['hello_world third with requirements']

    assert third.started_run_at is not None
    assert first.finished_run_at is not None
    assert second.finished_run_at is not None
    assert third.started_run_at >= first.finished_run_at
    assert third.started_run_at >= second.finished_run_at

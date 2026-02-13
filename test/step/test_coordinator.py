"""Tests for the coordinator module."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from otter.manifest.model import Result
from otter.step.coordinator import Coordinator
from otter.step.model import Step
from otter.task.model import Spec, State, TaskContext
from otter.util.errors import StepFailedError
from test.mocks import SyncMockTask, fake_config


class TestIsSpecReady:
    def test_spec_ready_without_dependencies(
        self,
        coordinator,
    ):
        spec = Spec(name='test_task independent_task')

        assert coordinator._is_spec_ready(spec) is True

    def test_spec_not_ready_when_dependency_missing(
        self,
        coordinator,
    ):
        spec = Spec(
            name='test_task dependent_task',
            requires=['test_task missing_task'],
        )

        assert coordinator._is_spec_ready(spec) is False

    def test_spec_not_ready_when_dependency_not_done(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task dep_task'), mock_context)
        task.context.state = State.RUNNING
        coordinator.step.tasks['test_task dep_task'] = task

        spec = Spec(
            name='test_task waiting_task',
            requires=['test_task dep_task'],
        )

        assert coordinator._is_spec_ready(spec) is False

    def test_spec_ready_when_dependency_done(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task dep_task'), mock_context)
        task.context.state = State.DONE
        coordinator.step.tasks['test_task dep_task'] = task

        spec = Spec(
            name='test_task waiting_task',
            requires=['test_task dep_task'],
        )

        assert coordinator._is_spec_ready(spec) is True


class TestGetReadySpecs:
    def test_get_ready_specs_returns_specs(
        self,
        coordinator,
    ):
        coordinator._remaining_specs.clear()
        coordinator._remaining_specs.append(Spec(name='test_task task_1'))
        coordinator._remaining_specs.append(Spec(name='test_task task_2'))

        ready = coordinator._get_ready_specs()

        assert len(ready) == 2
        assert all(isinstance(s, Spec) for s in ready)

    def test_get_ready_specs_filters_blocked_specs(
        self,
        coordinator,
    ):
        coordinator._remaining_specs.clear()
        coordinator._remaining_specs.append(Spec(name='test_task task_1'))
        coordinator._remaining_specs.append(
            Spec(
                name='test_task task_2',
                requires=['test_task task_1'],
            )
        )

        ready = coordinator._get_ready_specs()

        assert len(ready) == 1
        assert ready[0].name == 'test_task task_1'
        assert len(coordinator._remaining_specs) == 1


class TestBuildSpecIntoTask:
    def test_build_spec_creates_task(
        self,
        coordinator,
    ):
        spec = Spec(name='test_task new_task')

        task = coordinator._build_spec_into_task(spec)

        assert task is not None
        assert coordinator.step.tasks[spec.name] == task

    def test_build_duplicate_spec_raises_error(
        self,
        coordinator,
        mock_context,
    ):
        spec = Spec(name='test_task duplicate_task')
        coordinator.step.tasks[spec.name] = SyncMockTask(spec, mock_context)

        with pytest.raises(ValueError, match='duplicate task'):
            coordinator._build_spec_into_task(spec)


class TestEnqueueTasks:
    def test_enqueue_tasks_adds_to_queue(
        self,
        coordinator,
        mock_context,
    ):
        task1 = SyncMockTask(Spec(name='test_task task_1'), mock_context)
        task2 = SyncMockTask(Spec(name='test_task task_2'), mock_context)

        coordinator._enqueue_tasks([task1, task2])

        assert coordinator._task_queue.qsize() == 2


class TestIsStepComplete:
    def test_step_not_complete_when_tasks_missing(
        self,
        coordinator,
    ):
        assert coordinator._is_step_complete() is False

    def test_step_not_complete_when_tasks_not_done(
        self,
        coordinator,
        mock_context,
    ):
        coordinator.step.tasks['test_task task_1'] = SyncMockTask(
            Spec(name='test_task task_1'),
            mock_context,
        )
        coordinator.step.tasks['test_task task_2'] = SyncMockTask(
            Spec(name='test_task task_2'),
            mock_context,
        )
        coordinator.step.tasks['test_task task_1'].context.state = State.RUNNING
        coordinator.step.tasks['test_task task_2'].context.state = State.RUNNING

        assert coordinator._is_step_complete() is False

    def test_step_complete_when_all_done(
        self,
        coordinator,
        mock_context,
    ):
        coordinator.step.tasks['test_task task_1'] = SyncMockTask(
            Spec(name='test_task task_1'),
            mock_context,
        )
        coordinator.step.tasks['test_task task_2'] = SyncMockTask(
            Spec(name='test_task task_2'),
            mock_context,
        )
        coordinator.step.tasks['test_task task_1'].context.state = State.DONE
        coordinator.step.tasks['test_task task_2'].context.state = State.DONE

        assert coordinator._is_step_complete() is True


class TestGetTaskResults:
    def test_get_task_results_returns_all_done_tasks(
        self,
        coordinator,
        mock_context,
    ):
        task1 = SyncMockTask(Spec(name='test_task task_1'), mock_context)
        task2 = SyncMockTask(Spec(name='test_task task_2'), mock_context)
        coordinator._result_queue.put(task1)
        coordinator._result_queue.put(task2)

        results = coordinator._get_task_results()

        assert len(results) == 2
        assert coordinator._result_queue.qsize() == 0


class TestAddNewSpecsFromTask:
    def test_add_new_specs_from_task(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task parent_task'), mock_context)
        new_specs = [
            Spec(name='test_task child_1'),
            Spec(name='test_task child_2'),
        ]
        task.context.specs = new_specs

        initial_count = coordinator._spec_count
        coordinator._add_new_specs_from_task(task)

        assert coordinator._spec_count == initial_count + 2
        assert len(coordinator._remaining_specs) == len(coordinator.step.specs) + 2
        assert coordinator._task_subtasks['test_task parent_task'] == [
            'test_task child_1',
            'test_task child_2',
        ]


class TestProcessDoneTasks:
    def test_process_done_tasks_updates_state(
        self,
        coordinator,
    ):
        from otter.scratchpad.model import Scratchpad

        context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        task = SyncMockTask(Spec(name='test_task completed_task'), context)
        task.context.state = State.RUNNING
        coordinator.step.tasks[task.spec.name] = task
        coordinator._result_queue.put(task)

        with patch.object(task, 'get_next_state', return_value=State.PENDING_VALIDATION):
            coordinator._process_done_tasks()

        updated_task = coordinator.step.tasks[task.spec.name]
        assert updated_task.context.state == State.PENDING_VALIDATION

    def test_process_done_tasks_enqueues_for_validation(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task task_to_validate'), mock_context)
        task.context.state = State.RUNNING
        coordinator.step.tasks[task.spec.name] = task
        coordinator._result_queue.put(task)

        with patch.object(task, 'get_next_state', return_value=State.PENDING_VALIDATION):
            coordinator._process_done_tasks()

        assert coordinator._task_queue.qsize() == 1

    def test_process_done_tasks_raises_on_failure(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task failed_task'), mock_context)
        task.manifest.result = Result.FAILURE
        task.context.state = State.RUNNING
        coordinator.step.tasks[task.spec.name] = task
        coordinator._result_queue.put(task)

        with patch.object(task, 'get_next_state', return_value=State.DONE):
            with pytest.raises(StepFailedError):
                coordinator._process_done_tasks()


class TestProcessReadySpecs:
    def test_process_ready_specs_builds_and_enqueues(
        self,
        coordinator,
    ):
        coordinator._remaining_specs.clear()
        coordinator._remaining_specs.append(Spec(name='test_task ready_task'))

        coordinator._process_ready_specs()

        assert 'test_task ready_task' in coordinator.step.tasks
        assert coordinator._task_queue.qsize() == 1

    def test_process_no_ready_specs(
        self,
        coordinator,
        mock_context,
    ):
        coordinator._remaining_specs.clear()
        coordinator._remaining_specs.append(
            Spec(
                name='test_task blocked_task',
                requires=['test_task missing_task'],
            )
        )

        coordinator._process_ready_specs()

        assert len(coordinator.step.tasks) == 0
        assert coordinator._task_queue.qsize() == 0


class TestIsTaskFinishedWaitingForSubtasks:
    def test_task_not_in_waiting_state(
        self,
        coordinator,
        mock_context,
    ):
        task = SyncMockTask(Spec(name='test_task parent_task'), mock_context)
        task.context.state = State.RUNNING
        coordinator.step.tasks[task.spec.name] = task

        result = coordinator._is_task_finished_waiting_for_subtasks(
            'test_task parent_task',
            ['test_task child_1'],
        )

        assert result is False

    def test_task_waiting_with_incomplete_subtasks(
        self,
        coordinator,
        mock_context,
    ):
        parent = SyncMockTask(Spec(name='test_task parent_task'), mock_context)
        parent.context.state = State.WAITING_FOR_SUBTASKS
        coordinator.step.tasks[parent.spec.name] = parent

        child = SyncMockTask(Spec(name='test_task child_1'), mock_context)
        child.context.state = State.RUNNING
        coordinator.step.tasks[child.spec.name] = child

        result = coordinator._is_task_finished_waiting_for_subtasks(
            'test_task parent_task',
            ['test_task child_1'],
        )

        assert result is False

    def test_task_waiting_with_all_subtasks_done(
        self,
        coordinator,
    ):
        from otter.scratchpad.model import Scratchpad

        parent_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        parent = SyncMockTask(Spec(name='test_task parent_task'), parent_context)
        parent.context.state = State.WAITING_FOR_SUBTASKS
        coordinator.step.tasks[parent.spec.name] = parent

        child_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        child = SyncMockTask(Spec(name='test_task child_1'), child_context)
        child.context.state = State.DONE
        coordinator.step.tasks[child.spec.name] = child

        result = coordinator._is_task_finished_waiting_for_subtasks(
            'test_task parent_task',
            ['test_task child_1'],
        )

        assert result is True


class TestCompleteTasksWaitingForSubtasks:
    def test_completes_task_when_subtasks_done(
        self,
        coordinator,
    ):
        from otter.scratchpad.model import Scratchpad

        parent_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        parent = SyncMockTask(Spec(name='test_task parent_task'), parent_context)
        parent.context.state = State.WAITING_FOR_SUBTASKS
        coordinator.step.tasks[parent.spec.name] = parent

        child_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        child = SyncMockTask(Spec(name='test_task child_1'), child_context)
        child.context.state = State.DONE
        coordinator.step.tasks[child.spec.name] = child

        coordinator._task_subtasks['test_task parent_task'] = ['test_task child_1']

        with patch.object(parent, 'get_next_state', return_value=State.DONE):
            coordinator._complete_tasks_waiting_for_subtasks()

        assert parent.context.state == State.DONE
        assert 'test_task parent_task' not in coordinator._task_subtasks

    def test_does_not_complete_when_subtasks_not_done(
        self,
        coordinator,
    ):
        from otter.scratchpad.model import Scratchpad

        parent_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        parent = SyncMockTask(Spec(name='test_task parent_task'), parent_context)
        parent.context.state = State.WAITING_FOR_SUBTASKS
        coordinator.step.tasks[parent.spec.name] = parent

        child_context = TaskContext(
            config=fake_config(pool_size=2),
            scratchpad=Scratchpad(),
        )
        child = SyncMockTask(Spec(name='test_task child_1'), child_context)
        child.context.state = State.RUNNING
        coordinator.step.tasks[child.spec.name] = child

        coordinator._task_subtasks['test_task parent_task'] = ['test_task child_1']

        coordinator._complete_tasks_waiting_for_subtasks()

        assert parent.context.state == State.WAITING_FOR_SUBTASKS
        assert 'test_task parent_task' in coordinator._task_subtasks


class TestStartStopWorkers:
    def test_start_workers_creates_processes(
        self,
        coordinator,
    ):
        coordinator._start_workers()

        assert len(coordinator._workers) == coordinator.config.pool_size
        assert all(w.is_alive() for w in coordinator._workers)

        # cleanup
        coordinator._stop_workers()

    def test_stop_workers_terminates_processes(
        self,
        coordinator,
    ):
        coordinator._start_workers()
        coordinator._stop_workers()

        assert all(not w.is_alive() for w in coordinator._workers)


class TestRun:
    @pytest.mark.asyncio
    async def test_run_executes_simple_step(
        self,
        task_registry,
    ):
        specs = [Spec(name='test_task single_task')]
        step = Step(name='simple-step', specs=specs)
        coordinator = Coordinator(
            step=step,
            task_registry=task_registry,
            config=fake_config(pool_size=2),
        )

        with patch.object(coordinator, '_start_workers'):
            with patch.object(coordinator, '_stop_workers'):
                original_process_ready = coordinator._process_ready_specs
                call_count = {'count': 0}

                def mock_process_ready():
                    if call_count['count'] == 0:
                        original_process_ready()
                        for task in coordinator.step.tasks.values():
                            task.context.state = State.DONE
                    call_count['count'] += 1

                with patch.object(coordinator, '_process_ready_specs', side_effect=mock_process_ready):
                    await coordinator.run()

        assert step.manifest.started_run_at is not None
        assert step.manifest.finished_run_at is not None

    @pytest.mark.asyncio
    async def test_run_calls_step_start_and_finish(
        self,
        coordinator,
    ):
        with patch.object(coordinator.step, 'start') as mock_start:
            with patch.object(coordinator.step, 'finish') as mock_finish:
                with patch.object(coordinator, '_is_step_complete', return_value=True):
                    with patch.object(coordinator, '_start_workers'):
                        with patch.object(coordinator, '_stop_workers'):
                            await coordinator.run()

        mock_start.assert_called_once()
        mock_finish.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_stops_workers_on_error(
        self,
        coordinator,
    ):
        with patch.object(coordinator, '_process_done_tasks', side_effect=StepFailedError('test error')):
            with patch.object(coordinator, '_start_workers'):
                with patch.object(coordinator, '_stop_workers') as mock_stop:
                    with pytest.raises(StepFailedError):
                        await coordinator.run()

        mock_stop.assert_called_once()

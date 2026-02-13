"""Tests for the worker module."""

from __future__ import annotations

from otter.step.worker import Worker
from otter.task.model import Spec, State, TaskContext
from test.mocks import AsyncMockTask, SyncMockTask


class TestWorkerProcess:
    def test_worker_executes_task(
        self,
        mock_spec,
        mock_context,
        queues,
        worker,
    ):
        task_queue, result_queue, _ = queues
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)

        task_queue.put(task)
        result = result_queue.get(timeout=2)

        assert result.run_called is True
        assert result.context.state == State.RUNNING

    def test_worker_stops_on_shutdown(
        self,
        queues,
        worker,
    ):
        _, _, shutdown_event = queues

        shutdown_event.set()
        worker.join(timeout=2)

        assert not worker.is_alive()

    def test_worker_processes_multiple_tasks(
        self,
        mock_context,
        queues,
        worker,
    ):
        task_queue, result_queue, _ = queues

        for i in range(3):
            ctx = TaskContext(
                config=mock_context.config,
                scratchpad=mock_context.scratchpad,
            )
            ctx.state = State.PENDING_RUN
            spec = Spec(name=f'test_task task_{i}')
            task = SyncMockTask(spec, ctx)
            task_queue.put(task)

        results = [result_queue.get(timeout=2) for _ in range(3)]

        assert len(results) == 3
        assert all(r.run_called for r in results)


class TestExecuteTask:
    def test_execute_async_task(
        self,
        mock_spec,
        mock_context,
        abort_event,
    ):
        mock_context.state = State.PENDING_RUN
        task = AsyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.run_called is True
        assert task.context.state == State.RUNNING

    def test_execute_sync_task(
        self,
        mock_spec,
        mock_context,
        abort_event,
    ):
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.run_called is True
        assert task.context.state == State.RUNNING

    def test_execute_task_respects_abort(
        self,
        mock_spec,
        mock_context,
        abort_event,
    ):
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)
        abort_event.set()

        worker.execute_task(task, abort_event)

        assert task.abort_called is True
        assert task.run_called is False

    def test_execute_task_validating_state(
        self,
        mock_spec,
        mock_context,
        abort_event,
    ):
        mock_context.state = State.PENDING_VALIDATION
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.validate_called is True
        assert task.run_called is False

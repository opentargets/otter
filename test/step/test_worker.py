"""Tests for the worker module."""

from __future__ import annotations

from otter.step.worker import Worker
from otter.task.model import Spec, State, TaskContext
from test.mocks import AsyncMockTask, SyncMockTask


class TestWorkerProcess:
    """Tests for worker_process function."""

    def test_worker_executes_task(self, mock_spec, mock_context, queues, worker):
        """Test worker retrieves task, executes it, and returns result."""
        task_queue, result_queue, _ = queues
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)

        task_queue.put(task)
        result = result_queue.get(timeout=2)

        assert result.run_called is True
        assert result.context.state == State.RUNNING

    def test_worker_stops_on_shutdown(self, queues, worker):
        """Test worker stops when shutdown event is set."""
        _, _, shutdown_event = queues

        shutdown_event.set()
        worker.join(timeout=2)

        assert not worker.is_alive()

    def test_worker_processes_multiple_tasks(self, mock_context, queues, worker):
        """Test worker processes multiple tasks sequentially."""
        task_queue, result_queue, _ = queues

        # create and enqueue multiple tasks
        for i in range(3):
            ctx = TaskContext(config=mock_context.config, scratchpad=mock_context.scratchpad)
            ctx.state = State.PENDING_RUN
            spec = Spec(name=f'test_task task_{i}')
            task = SyncMockTask(spec, ctx)
            task_queue.put(task)

        # collect results
        results = [result_queue.get(timeout=2) for _ in range(3)]

        assert len(results) == 3
        assert all(r.run_called for r in results)


class TestExecuteTask:
    """Tests for Worker.execute_task method."""

    def test_execute_async_task(self, mock_spec, mock_context, abort_event):
        """Test executing an async task."""
        mock_context.state = State.PENDING_RUN
        task = AsyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.run_called is True
        assert task.context.state == State.RUNNING

    def test_execute_sync_task(self, mock_spec, mock_context, abort_event):
        """Test executing a sync task."""
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.run_called is True
        assert task.context.state == State.RUNNING

    def test_execute_task_respects_abort(self, mock_spec, mock_context, abort_event):
        """Test that task aborts when abort event is set."""
        mock_context.state = State.PENDING_RUN
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        # set abort event before execution
        abort_event.set()

        worker.execute_task(task, abort_event)

        assert task.abort_called is True
        assert task.run_called is False

    def test_execute_task_validating_state(self, mock_spec, mock_context, abort_event):
        """Test task execution in VALIDATING state calls validate method."""
        mock_context.state = State.PENDING_VALIDATION
        task = SyncMockTask(mock_spec, mock_context)
        worker = Worker(0)

        worker.execute_task(task, abort_event)

        assert task.validate_called is True
        assert task.run_called is False

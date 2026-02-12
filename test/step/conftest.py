"""Fixtures for step worker tests."""

from multiprocessing import Manager
from threading import Event, Thread

import pytest

from otter.scratchpad.model import Scratchpad
from otter.step.coordinator import Coordinator
from otter.step.model import Step
from otter.step.worker import worker_process
from otter.task.model import Spec, TaskContext
from otter.task.task_registry import TaskRegistry
from test.mocks import SyncMockTask, fake_config


@pytest.fixture
def abort_event():
    """Create an abort event."""
    return Event()


@pytest.fixture
def queues():
    """Create task and result queues."""
    manager = Manager()
    task_queue = manager.Queue()
    result_queue = manager.Queue()
    shutdown_event = manager.Event()
    yield task_queue, result_queue, shutdown_event
    manager.shutdown()


@pytest.fixture
def worker(queues):
    """Create and start a worker in a thread."""
    task_queue, result_queue, shutdown_event = queues

    worker = Thread(target=worker_process, args=(0, task_queue, result_queue, shutdown_event))
    worker.start()

    yield worker

    shutdown_event.set()
    worker.join(timeout=2)
    if worker.is_alive():
        pass


@pytest.fixture
def task_registry(monkeypatch):
    """Create a task registry with mock tasks."""
    config = fake_config(pool_size=2)
    registry = TaskRegistry(config=config, scratchpad=Scratchpad())

    def build_mock_task(spec: Spec) -> SyncMockTask:
        context = TaskContext(config=config, scratchpad=Scratchpad())
        return SyncMockTask(spec, context)

    monkeypatch.setattr(registry, 'build', build_mock_task)
    return registry


@pytest.fixture
def basic_specs():
    """Create basic specs for testing."""
    return [
        Spec(name='test_task task_1'),
        Spec(name='test_task task_2', requires=['test_task task_1']),
    ]


@pytest.fixture
def step(basic_specs):
    """Create a step with basic specs."""
    return Step(name='test-step', specs=basic_specs)


@pytest.fixture
def coordinator(step, task_registry):
    """Create a coordinator instance."""
    return Coordinator(step=step, task_registry=task_registry, config=fake_config(pool_size=2))

"""Shared fixtures for tests."""

from multiprocessing import Manager
from threading import Event, Thread

import pytest

from otter.step.worker import worker_process

pytest_plugins = ['test.mocks']


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

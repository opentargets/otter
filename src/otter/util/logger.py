"""Configures the logger for the application."""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from queue import Queue
from typing import TYPE_CHECKING

import loguru
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from otter.step.model import Step
    from otter.task.model import Task


def get_exception_info(record_exception: loguru.RecordException | None) -> tuple[str, str, str]:
    """Get fields from the exception record.

    This function extracts the name, function and line number from an exception.
    It will go back in the stack to the first frame originated inside the app,
    that way it will make sure the error is meaningful in the logs. If we don't
    do this, the error will be logged as raising from the in the report decorator,
    which is not very useful.

    Args:
        record_exception: The exception record.
    """
    name = '{name}'
    func = '{function}'
    line = '{line}'

    if record_exception is not None:
        tb = record_exception.traceback

        if tb is None:
            return name, func, line

        # go back in the stack to the first frame originated inside the app
        app_name = globals()['__package__'].split('.')[0]
        while tb.tb_next:
            next_name = tb.tb_next.tb_frame.f_globals.get('__name__', None)
            if app_name not in next_name:
                break
            name = next_name
            tb = tb.tb_next
        func = tb.tb_frame.f_code.co_name
        line = str(tb.tb_lineno)

    return name, func, line


def get_format_log(include_task: bool = True) -> Callable[..., str]:
    """Create the log format function."""

    def format_log(record: loguru.Record) -> str:
        name, func, line = get_exception_info(record.get('exception'))
        task = '<y>{extra[task]}</>::' if include_task and record['extra'].get('task') else ''
        trail = '\n' if include_task else ''

        exception = os.getenv('OTTER_SHOW_EXCEPTIONS')

        # debug flag to hide exceptions in logs (they are too verbose when checking the log flow)
        if exception and include_task:
            trail = '\n{exception}'  # noqa: RUF027

        return (
            '<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</> | '
            '<lvl>{level: <8}</> | '
            f'{task}'
            f'<c>{name}</>:<c>{func}</>:<c>{line}</>'
            ' - <lvl>{message}</>'
            f'{trail}'
        )

    return format_log


@contextmanager
def task_logging(task: Task) -> Generator[None, None, None]:
    """Context manager that appends log messages to the task's manifest.

    :param task: The task to log messages to.
    :type task: Task
    """
    with logger.contextualize(task=task.spec.name):
        sink_task: Callable[[str], None] = lambda message: task.manifest.log.append(message)
        logger.add(
            sink=sink_task,
            filter=lambda record: record['extra'].get('task') == task.spec.name,
            format=get_format_log(include_task=False),
            level=task.context.config.log_level or 'TRACE',
        )

        yield


@contextmanager
def step_logging(step: Step) -> Generator[None, None, None]:
    """Context manager that appends log messages to the step's manifest.

    :param step: The step to log messages to.
    :type step: Step
    """
    with logger.contextualize(step=step.name):
        sink_step: Callable[[str], None] = lambda message: step.manifest.log.append(message)
        logger.add(
            sink=sink_step,
            filter=lambda record: record['extra'].get('step') == step.name,
            format=get_format_log(include_task=False),
            level=step.config.log_level or 'TRACE',
        )

        yield


class MessageQueue:
    """A queue for log messages."""

    def __init__(self) -> None:
        self._log_queue: Queue[loguru.Message] = Queue()

    def put(self, message: loguru.Message) -> None:
        """Put a message in the queue."""
        self._log_queue.put(message)

    def flush(self) -> None:
        """Dump the log messages to stdout."""
        while not self._log_queue.empty():
            msg = self._log_queue.get()
            logger.patch(lambda r: None).log(msg.record['level'].name, msg.rstrip())


_early_logs = MessageQueue()
logger.remove()
logger.add(sink=_early_logs.put, level='TRACE')
logger.debug('early logger configured')


def init_logger(log_level: str = 'INFO') -> None:
    """Initialize the logger.

    Once the logger is set up, dumps the log messages held in the queue.

    :param log_level: The log level to use.
    :type log_level: str
    :param message_queue: The message queue.
    :type message_queue: MessageQueue
    """
    logger.remove()
    logger.add(sink=sys.stdout, level=log_level, format=get_format_log())
    _early_logs.flush()
    logger.debug('logger configured')

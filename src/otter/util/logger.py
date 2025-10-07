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

_runner_name: list[str] = ['otter']

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

    :param record_exception: The exception record to extract the information from.
    :type record_exception: loguru.RecordException | None
    """
    name = '{name}'
    func = '{function}'
    line = '{line}'

    if record_exception is not None:
        tb = record_exception.traceback

        if tb is None:
            return name, func, line

        # go back in the stack to the first frame originated inside the app
        while tb.tb_next:
            next_name = tb.tb_next.tb_frame.f_globals.get('__name__', None)
            if not next_name or not any(next_name.startswith(rn) for rn in _runner_name):
                break
            name = next_name
            tb = tb.tb_next
        func = tb.tb_frame.f_code.co_name
        line = str(tb.tb_lineno)

    return name, func, line


def get_format_log(include_task: bool = True) -> Callable[..., str]:
    """Create the log format function."""

    def _get_process_role() -> str:
        p_role = os.environ.get('OTTER_PROCESS_ROLE', 'M')
        p_role_str = f'{p_role}:{os.getpid()}'.ljust(8)
        color = 'lm' if p_role_str.startswith('W') else 'g'
        return f'<b><{color}>{p_role_str}</{color}></b>'

    def format_log(record: loguru.Record) -> str:
        name, func, line = get_exception_info(record.get('exception'))
        task = '<y>{extra[task]}</>::' if include_task and record['extra'].get('task') else ''
        trail = '\n' if include_task else ''
        p_role = _get_process_role()
        exception = os.getenv('OTTER_SHOW_EXCEPTIONS')

        # debug flag to hide exceptions in logs (they are too verbose when checking the log flow)
        if exception and include_task:
            trail = '\n{exception}'  # noqa: RUF027

        return (
            '<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</> | '
            f'<m>{p_role}</> | '
            '<lvl>{level: <8}</> | '
            f'{task}'
            f'<c>{name}</>:<c>{func}</>:<c>{line}</>'
            ' - <lvl>{message}</>'
            f'{trail}'
        )

    return format_log


@contextmanager
def task_logging(task: Task) -> Generator[None]:
    """Context manager that appends log messages to the task's manifest.

    We check if there is already a stdout logger, and if not, we add one. This
    is added to make sure spawned processes also log to stdout. We've seen that
    sometimes the logger configuration is not inherited by child processes for
    some unknown reason. Probably related to:

    https://github.com/Delgan/loguru/issues/912

    :param task: The task to log messages to.
    :type task: Task
    """
    found = False
    for h in logger._core.handlers.values():
        if hasattr(h, '_sink') and hasattr(h._sink, '_stream') and h._sink._stream.name == '<stdout>':
            logger.debug('found stdout logger')
            found = True
    if not found:
        logger.add(sink=sys.stdout, level='TRACE', format=get_format_log())
        logger.debug('added missing stdout logger')
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
def step_logging(step: Step) -> Generator[None]:
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
    """A queue for log messages.

    This class is used to hold log messages until the logger is configured.
    """

    def __init__(self) -> None:
        self._log_queue: Queue[loguru.Message] = Queue()

    def put(self, message: loguru.Message) -> None:
        """Put a message in the queue."""
        self._log_queue.put(message)

    def flush(self) -> None:
        """Dump the log messages to stdout."""
        while not self._log_queue.empty():
            msg = self._log_queue.get()

            def patcher(record: loguru.Record) -> None:
                record.update(msg.record)  # noqa: B023

            logger.patch(patcher).log(msg.record['level'].name, msg.record['message'])


_early_logs = MessageQueue()
logger.remove()
logger.add(sink=_early_logs.put, level='TRACE')
logger.debug('early logger configured')


def init_logger(log_level: str = 'INFO', app_name: str | None = None) -> None:
    """Initialize the logger.

    Once the logger is set up, dumps the log messages held in the queue.

    :param log_level: The log level to use.
    :type log_level: str
    :param app_name: The name of the Otter application, defaults to `otter`.
    :type app_name: str | None
    """
    if app_name is not None:
        global _runner_name  # noqa: PLW0602
        _runner_name.append(app_name)

    logger.remove()
    logger.add(sink=sys.stdout, level=log_level, format=get_format_log())
    _early_logs.flush()
    logger.debug('logger configured')

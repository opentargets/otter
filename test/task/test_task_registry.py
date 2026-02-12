"""Tests for the task registry module."""

from __future__ import annotations

import errno

import pytest

from otter.task.model import Spec
from otter.task.task_registry import BUILTIN_TASKS_MODULE


class TestRegister:
    def test_register_builtin_tasks(
        self,
        task_registry,
    ):
        task_registry.register(BUILTIN_TASKS_MODULE)

        assert 'hello_world' in task_registry._tasks

    def test_register_nonexistent_package_exits(
        self,
        task_registry,
    ):
        with pytest.raises(SystemExit) as exc_info:
            task_registry.register('nonexistent.package.name')

        assert exc_info.value.code == errno.ENOENT


class TestBuild:
    def test_build_simple_task(
        self,
        task_registry,
    ):
        task_registry.register(BUILTIN_TASKS_MODULE)
        spec = Spec(
            name='hello_world simple task',
        )

        task = task_registry.build(spec)

        assert task is not None
        assert task.spec.name == 'hello_world simple task'
        assert task.spec.task_type == 'hello_world'

    def test_build_task_with_scratchpad_replacement(
        self,
        task_registry_with_scratchpad,
    ):
        task_registry_with_scratchpad.register(BUILTIN_TASKS_MODULE)
        spec = Spec(
            name='copy file with template',
            source='/original/${test_var}/file.txt',  # ty:ignore[unknown-argument]
            destination='/dest/${test_var}/file.txt',  # ty:ignore[unknown-argument]
        )

        task = task_registry_with_scratchpad.build(spec)

        assert task.spec.source == '/original/test_value/file.txt'
        assert task.spec.destination == '/dest/test_value/file.txt'

    def test_build_task_with_scratchpad_ignore_missing(
        self,
        task_registry,
    ):
        task_registry.register(BUILTIN_TASKS_MODULE)
        spec = Spec(
            name='hello_world task with ignore missing',
            who='${missing_var}',  # ty:ignore[unknown-argument]
            scratchpad_ignore_missing=True,
        )

        task = task_registry.build(spec)

        assert task.spec.who == '${missing_var}'

    def test_build_creates_task_context(
        self,
        task_registry,
    ):
        task_registry.register(BUILTIN_TASKS_MODULE)
        spec = Spec(name='hello_world task with context')

        task = task_registry.build(spec)

        assert task.context is not None
        assert task.context.config == task_registry.config
        assert task.context.scratchpad == task_registry.scratchpad

    def test_build_multiple_tasks(
        self,
        task_registry,
    ):
        task_registry.register(BUILTIN_TASKS_MODULE)
        spec1 = Spec(
            name='hello_world task for Alice',
            who='Alice',  # ty:ignore[unknown-argument]
        )
        spec2 = Spec(
            name='hello_world task for Bob',
            who='Bob',  # ty:ignore[unknown-argument]
        )

        task1 = task_registry.build(spec1)
        task2 = task_registry.build(spec2)

        assert task1.spec.who == 'Alice'
        assert task2.spec.who == 'Bob'
        assert task1.spec.name != task2.spec.name

    def test_build_task_with_complex_scratchpad(
        self,
        task_registry_with_scratchpad,
    ):
        task_registry_with_scratchpad.register(BUILTIN_TASKS_MODULE)
        spec = Spec(
            name='copy file with multiple templates',
            source='${source_path}/file.txt',  # ty:ignore[unknown-argument]
            destination='${dest_path}/file.txt',  # ty:ignore[unknown-argument]
        )

        task = task_registry_with_scratchpad.build(spec)

        assert task.spec.source == '/path/to/source/file.txt'
        assert task.spec.destination == '/path/to/dest/file.txt'

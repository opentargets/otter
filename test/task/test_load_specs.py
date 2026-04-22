"""Tests for the load_specs function."""

from __future__ import annotations

import errno

import pytest
import yaml

from otter.task import load_specs


def write_config(tmp_path, steps: dict) -> object:
    """Write a YAML config file and return its path."""
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.dump({'steps': steps}))
    return path


class TestLoadSpecs:
    def test_loads_valid_specs(self, tmp_path):
        path = write_config(tmp_path, {'my-step': [{'name': 'test_task task_a'}]})

        specs = load_specs(path, 'my-step')

        assert len(specs) == 1
        assert specs[0].name == 'test_task task_a'

    def test_loads_valid_requires(self, tmp_path):
        path = write_config(
            tmp_path,
            {
                'my-step': [
                    {'name': 'test_task task_a'},
                    {'name': 'test_task task_b', 'requires': ['test_task task_a']},
                ]
            },
        )

        specs = load_specs(path, 'my-step')

        assert specs[1].requires == ['test_task task_a']

    def test_exits_on_duplicate_task_name(self, tmp_path):
        path = write_config(
            tmp_path,
            {
                'my-step': [
                    {'name': 'test_task task_a'},
                    {'name': 'test_task task_a'},
                ]
            },
        )

        with pytest.raises(SystemExit) as exc_info:
            load_specs(path, 'my-step')

        assert exc_info.value.code == errno.EINVAL

    def test_exits_on_unknown_requires_reference(self, tmp_path):
        path = write_config(
            tmp_path,
            {
                'my-step': [
                    {'name': 'test_task task_a', 'requires': ['test_task nonexistent']},
                ]
            },
        )

        with pytest.raises(SystemExit) as exc_info:
            load_specs(path, 'my-step')

        assert exc_info.value.code == errno.EINVAL

    def test_exits_on_unknown_requires_reference_in_later_task(self, tmp_path):
        path = write_config(
            tmp_path,
            {
                'my-step': [
                    {'name': 'test_task task_a'},
                    {'name': 'test_task task_b', 'requires': ['test_task task_a', 'test_task ghost']},
                ]
            },
        )

        with pytest.raises(SystemExit) as exc_info:
            load_specs(path, 'my-step')

        assert exc_info.value.code == errno.EINVAL

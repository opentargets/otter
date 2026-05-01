"""Tests for the load_specs function."""

from __future__ import annotations

import errno
from pathlib import Path

import pytest

from otter.task import load_specs


@pytest.fixture
def config_yaml(monkeypatch):
    def _config(data):
        monkeypatch.setattr('otter.task.parse_yaml', lambda path: data)

    return _config


class TestLoadSpecs:
    def test_loads_valid_specs(self, config_yaml):
        config_yaml({
            'steps': {
                'my-step': [
                    {
                        'name': 'test_task task_a',
                    },
                ],
            }
        })

        specs = load_specs(Path('config.yaml'), 'my-step')

        assert len(specs) == 1
        assert specs[0].name == 'test_task task_a'

    def test_loads_valid_requires(self, config_yaml):
        config_yaml({
            'steps': {
                'my-step': [
                    {
                        'name': 'test_task task_a',
                    },
                    {
                        'name': 'test_task task_b',
                        'requires': ['test_task task_a'],
                    },
                ]
            }
        })

        specs = load_specs(Path('config.yaml'), 'my-step')

        assert specs[1].requires == ['test_task task_a']

    def test_exits_on_duplicate_task_name(self, config_yaml):
        config_yaml({
            'steps': {
                'my-step': [
                    {'name': 'test_task task_a'},
                    {'name': 'test_task task_a'},
                ]
            }
        })

        with pytest.raises(SystemExit) as exc_info:
            load_specs(Path('config.yaml'), 'my-step')

        assert exc_info.value.code == errno.EINVAL

    def test_exits_on_unknown_requires_reference(self, config_yaml):
        config_yaml({
            'steps': {
                'my-step': [
                    {
                        'name': 'test_task task_a',
                        'requires': ['test_task nonexistent'],
                    },
                ]
            }
        })

        with pytest.raises(SystemExit) as exc_info:
            load_specs(Path('config.yaml'), 'my-step')

        assert exc_info.value.code == errno.EINVAL

    def test_exits_on_unknown_requires_reference_in_later_task(self, config_yaml):
        config_yaml({
            'steps': {
                'my-step': [
                    {'name': 'test_task task_a'},
                    {
                        'name': 'test_task task_b',
                        'requires': ['test_task task_a', 'test_task ghost'],
                    },
                ]
            }
        })

        with pytest.raises(SystemExit) as exc_info:
            load_specs(Path('config.yaml'), 'my-step')

        assert exc_info.value.code == errno.EINVAL

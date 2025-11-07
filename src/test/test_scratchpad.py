"""Test scratchpad functionality."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from otter.config.model import Config
from otter.scratchpad.model import Scratchpad
from otter.step.model import Step
from otter.task.model import State


class TestScratchpad:
    """Test the Scratchpad model."""

    @pytest.mark.parametrize(
        ('dict_to_replace', 'expected_dict'),
        [
            pytest.param({'x1': 'Value ${replace}'}, {'x1': 'Value B'}, id='String replacement'),
            pytest.param({'x1': Path('${replace}')}, {'x1': Path('B')}, id='Path replacement'),
            pytest.param({'x1': 0.1}, {'x1': 0.1}, id='Float - no replacement'),
            pytest.param({'x1': 123}, {'x1': 123}, id='Int - no replacement'),
            pytest.param({'x1': True}, {'x1': True}, id='Bool - no replacement'),
            pytest.param({'x1': None}, {'x1': None}, id='None - no replacement'),
            pytest.param(
                {'x1': ['Value ${replace}', 'Another ${replace}']},
                {'x1': ['Value B', 'Another B']},
                id='List replacement',
            ),
            pytest.param(
                {'x1': {'y1': 'Value ${replace}', 'y2': 'Another ${replace}'}},
                {'x1': {'y1': 'Value B', 'y2': 'Another B'}},
                id='Dict replacement',
            ),
            pytest.param(
                {'x1': 'Value ${replace}', 'x2': 'Another ${replace}'},
                {'x1': 'Value B', 'x2': 'Another B'},
                id='Multiple replacements',
            ),
        ],
    )
    def test_replace_dict(self, dict_to_replace: dict[str, Any], expected_dict: dict[str, Any]) -> None:
        """Test replace_dict method."""
        sp = Scratchpad()
        sp.store('replace', 'B')
        result = sp.replace_dict(dict_to_replace)
        assert result == expected_dict


def test_process_results():
    """Test that the scratchpad is merged correctly after processing results."""

    # the new sentinel should be present here
    global_scratchpad = Scratchpad()

    # we don't care about config
    mock_config = MagicMock(spec=Config)
    task_registry = MagicMock()
    task_registry.scratchpad = global_scratchpad

    # we don't care about the task
    mock_task = MagicMock()
    mock_task.spec.name = 'test_task'
    mock_task.context.state = State.RUNNING
    mock_task.get_next_state.return_value = State.DONE

    # this is where the sentinel should be picked up from
    task_scratchpad = Scratchpad()
    task_scratchpad.store('test_key', 'test_value')
    mock_task.context.scratchpad = task_scratchpad

    # we create a step with no tasks, the registry containing our global scratchpad
    # and the mock config
    step = Step('test_step', [], task_registry, mock_config)

    # call the tested method with our mock task containing that sentinel in its scratchpad
    step._process_results([mock_task])

    # the global scratchpad should now contain that sentinel
    assert global_scratchpad.sentinel_dict.get('test_key') == 'test_value'

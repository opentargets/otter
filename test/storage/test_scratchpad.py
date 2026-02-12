"""Test scratchpad functionality."""

from pathlib import Path
from typing import Any

import pytest

from otter.scratchpad.model import Scratchpad


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

"""Test scratchpad functionality."""

from pathlib import Path
from typing import Any

import pytest

from otter.scratchpad.model import Scratchpad
from otter.util.errors import ScratchpadError


class TestScratchpad:
    @pytest.mark.parametrize(
        ('dict_to_replace', 'expected_dict'),
        [
            (
                {'x1': 'Value ${replace}'},
                {'x1': 'Value B'},
            ),
            (
                {'x1': Path('${replace}')},
                {'x1': Path('B')},
            ),
            (
                {'x1': 0.1},
                {'x1': 0.1},
            ),
            (
                {'x1': 123},
                {'x1': 123},
            ),
            (
                {'x1': True},
                {'x1': True},
            ),
            (
                {'x1': None},
                {'x1': None},
            ),
            (
                {'x1': ['Value ${replace}', 'Another ${replace}']},
                {'x1': ['Value B', 'Another B']},
            ),
            (
                {'x1': {'y1': 'Value ${replace}', 'y2': 'Another ${replace}'}},
                {'x1': {'y1': 'Value B', 'y2': 'Another B'}},
            ),
            (
                {'x1': 'Value ${replace}', 'x2': 'Another ${replace}'},
                {'x1': 'Value B', 'x2': 'Another B'},
            ),
        ],
    )
    def test_replace_dict(
        self,
        dict_to_replace: dict[str, Any],
        expected_dict: dict[str, Any],
    ) -> None:
        sp = Scratchpad()
        sp.store('replace', 'B')
        result = sp.replace_dict(dict_to_replace)
        assert result == expected_dict

    def test_missing_key_raises(self) -> None:
        sp = Scratchpad()
        sp.store('existing_key', 'value')

        with pytest.raises(ScratchpadError):
            sp.replace_dict({'x': 'Value ${missing_key}'})

        with pytest.raises(ScratchpadError):
            sp.replace_dict({'x': 'Value ${existing_key} and ${missing_key}'})

    def test_missing_key_with_ignore_missing_does_not_raise(self) -> None:
        sp = Scratchpad()
        sp.store('existing_key', 'value')

        result = sp.replace_dict({'x': 'Value ${existing_key} and ${missing_key}'}, ignore_missing=True)
        assert result == {'x': 'Value value and ${missing_key}'}

        result = sp.replace_dict({'x': 'Value ${missing_key}'}, ignore_missing=True)
        assert result == {'x': 'Value ${missing_key}'}
        assert result == {'x': 'Value ${missing_key}'}

    def test_merge_no_overlap(self) -> None:
        sp1 = Scratchpad()
        sp1.store('key1', 'value1')
        sp1.store('key2', 'value2')

        sp2 = Scratchpad()
        sp2.store('key3', 'value3')
        sp2.store('key4', 'value4')

        sp1.merge(sp2)

        assert sp1.sentinel_dict == {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
            'key4': 'value4',
        }

    def test_merge_does_not_overwrite_existing_keys(self) -> None:
        sp1 = Scratchpad()
        sp1.store('key1', 'original')
        sp1.store('key2', 'value2')

        sp2 = Scratchpad()
        sp2.store('key1', 'new')  # should not overwrite
        sp2.store('key3', 'value3')  # should be added

        sp1.merge(sp2)

        assert sp1.sentinel_dict == {
            'key1': 'original',
            'key2': 'value2',
            'key3': 'value3',
        }

"""Tests for the copy_many task."""

from threading import Event
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from otter.manifest.model import Artifact, Result
from otter.scratchpad.model import Scratchpad
from otter.task.model import TaskContext
from otter.tasks.copy_many import CopyMany, CopyManySpec
from test.mocks import fake_config

MAPPING_YAML = """
gs://source-bucket/a/pairs.tsv.gz: first.tsv.gz
gs://source-bucket/b/pairs.tsv.gz: second.tsv.gz
"""


def _mapping_handle(content: str) -> MagicMock:
    handle = MagicMock()
    handle.read_text.return_value = (content, 0)
    return handle


class TestCopyManyTask:
    def test_spec_defaults_to_no_settings(self) -> None:
        spec = CopyManySpec(
            name='copy_many test copies',
            sources=['gs://source-bucket/source.txt'],
            destination='dest',
        )

        assert spec.settings is None

    @pytest.mark.parametrize(
        'other',
        [
            {},
            {'sources': ['gs://b/x'], 'source_mapping_file': 'map.yaml'},
            {'sources': ['gs://b/x'], 'source_list_file': 'list.txt'},
            {'source_mapping_file': 'map.yaml', 'source_list_file': 'list.txt'},
            {'sources': ['gs://b/x'], 'source_mapping_file': 'map.yaml', 'source_list_file': 'list.txt'},
        ],
    )
    def test_spec_rejects_source_param_combinations(self, other: dict) -> None:
        with pytest.raises(ValueError, match='exactly one of'):
            CopyManySpec(
                name='copy_many test copies',
                destination='dest',
                **other,
            )

    @pytest.mark.asyncio
    async def test_run_uses_settings_context(self) -> None:
        spec = CopyManySpec(
            name='copy_many test copies',
            sources=['gs://source-bucket/source.txt'],
            destination='dest',
            settings={'billing_project': 'billing-project'},
        )
        task = CopyMany(spec, TaskContext(config=fake_config(), scratchpad=Scratchpad()))

        with (
            patch('otter.tasks.copy_many.storage_context') as mock_storage_context,
            patch.object(
                task,
                '_copy_single_file',
                new=AsyncMock(
                    return_value=Artifact(
                        source='gs://source-bucket/source.txt',
                        destination='gs://test-bucket/release/path/dest/source.txt',
                    )
                ),
            ) as mock_copy_single,
        ):
            await task.run()

        mock_storage_context.assert_called_once_with(billing_project='billing-project')
        mock_copy_single.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_copies_sources_to_new_names(self) -> None:
        spec = CopyManySpec(name='copy_many test copies', destination='dest/', source_mapping_file='map.yaml')
        context = TaskContext(config=fake_config(), scratchpad=Scratchpad())
        context.abort = Event()
        task = CopyMany(spec, context)
        copies: list[tuple[str, str]] = []

        class FakeHandle:
            def __init__(self, path: str, config: object = None) -> None:
                self.absolute = path

            async def copy_to(self, dst: 'FakeHandle') -> None:
                copies.append((self.absolute, dst.absolute))

        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(MAPPING_YAML)),
            patch('otter.tasks.copy_many.AsyncStorageHandle', FakeHandle),
        ):
            await task.run()

        # both sources share the basename `pairs.tsv.gz`, so only the mapping can tell them apart
        assert sorted(copies) == [
            ('gs://source-bucket/a/pairs.tsv.gz', 'dest/first.tsv.gz'),
            ('gs://source-bucket/b/pairs.tsv.gz', 'dest/second.tsv.gz'),
        ]
        assert task.manifest.result != Result.FAILURE

    @pytest.mark.asyncio
    @pytest.mark.parametrize('content', ['- a\n- b\n', 'just a string', 'a: [1, 2]\n', 'a: 1\n'])
    async def test_run_rejects_malformed_mapping(self, content: str) -> None:
        spec = CopyManySpec(name='copy_many test copies', destination='dest', source_mapping_file='map.yaml')
        context = TaskContext(config=fake_config(), scratchpad=Scratchpad())
        context.abort = Event()
        task = CopyMany(spec, context)
        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(content)),
        ):
            await task.run()

        assert task.manifest.result == Result.FAILURE
        assert task.manifest.failure_reason
        assert 'mapping' in task.manifest.failure_reason

    @pytest.mark.asyncio
    async def test_run_rejects_duplicate_new_names(self) -> None:
        spec = CopyManySpec(name='copy_many test copies', destination='dest', source_mapping_file='map.yaml')
        context = TaskContext(config=fake_config(), scratchpad=Scratchpad())
        context.abort = Event()
        task = CopyMany(spec, context)
        content = 'gs://b/a.txt: same.txt\ngs://b/b.txt: same.txt\n'

        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(content)),
        ):
            await task.run()

        assert task.manifest.result == Result.FAILURE
        assert task.manifest.failure_reason
        assert 'duplicate' in task.manifest.failure_reason

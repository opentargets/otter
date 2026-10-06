"""Tests for the copy_many task."""

import asyncio
from threading import Event
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from otter.manifest.model import Artifact, Result
from otter.scratchpad.model import Scratchpad
from otter.task.model import TaskContext
from otter.tasks.copy_many import CopyMany, CopyManySpec
from test.mocks import fake_config


class TestCopyManyTask:
    def test_spec_defaults_to_no_settings(self) -> None:
        spec = CopyManySpec(
            name='copy_many test copies',
            sources=['gs://source-bucket/source.txt'],
            destination='dest',
        )

        assert spec.settings is None

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


MAPPING_YAML = """
gs://source-bucket/a/pairs.tsv.gz: first.tsv.gz
gs://source-bucket/b/pairs.tsv.gz: second.tsv.gz
"""


def _task(**kwargs) -> CopyMany:
    spec = CopyManySpec(name='copy_many test copies', destination='dest', **kwargs)
    context = TaskContext(config=fake_config(), scratchpad=Scratchpad())
    context.abort = Event()
    return CopyMany(spec, context)


def _mapping_handle(content: str) -> MagicMock:
    handle = MagicMock()
    handle.read_text.return_value = (content, 0)
    return handle


class TestCopyManySourceMapping:
    @pytest.mark.parametrize('other', [{'sources': ['gs://b/x']}, {'source_list_file': 'list.txt'}])
    def test_spec_rejects_mapping_with_other_sources(self, other: dict) -> None:
        with pytest.raises(ValueError, match='source_mapping_file'):
            CopyManySpec(
                name='copy_many test copies',
                destination='dest',
                source_mapping_file='map.yaml',
                **other,
            )

    @pytest.mark.asyncio
    async def test_run_copies_sources_with_new_names(self) -> None:
        task = _task(source_mapping_file='map.yaml')

        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(MAPPING_YAML)) as handle,
            patch.object(
                task, '_copy_single_file', new=AsyncMock(return_value=Artifact(source='s', destination='d'))
            ) as mock_copy,
        ):
            await task.run()

        assert handle.call_args.args[0] == 'map.yaml'
        calls = [
            (c.args[0], c.kwargs.get('name', c.args[2] if len(c.args) > 2 else None)) for c in mock_copy.await_args_list
        ]
        assert calls == [
            ('gs://source-bucket/a/pairs.tsv.gz', 'first.tsv.gz'),
            ('gs://source-bucket/b/pairs.tsv.gz', 'second.tsv.gz'),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize('content', ['- a\n- b\n', 'just a string', 'a: [1, 2]\n', 'a: 1\n'])
    async def test_run_rejects_malformed_mapping(self, content: str) -> None:
        task = _task(source_mapping_file='map.yaml')

        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(content)),
        ):
            await task.run()

        assert task.manifest.result == Result.FAILURE
        assert 'mapping' in (task.manifest.failure_reason or '')

    @pytest.mark.asyncio
    async def test_run_rejects_duplicate_new_names(self) -> None:
        task = _task(source_mapping_file='map.yaml')
        content = 'gs://b/a.txt: same.txt\ngs://b/b.txt: same.txt\n'

        with (
            patch('otter.tasks.copy_many.storage_context'),
            patch('otter.tasks.copy_many.StorageHandle', return_value=_mapping_handle(content)),
        ):
            await task.run()

        assert task.manifest.result == Result.FAILURE
        assert 'duplicate' in (task.manifest.failure_reason or '')

    @pytest.mark.asyncio
    async def test_copy_single_file_uses_new_name(self) -> None:
        task = _task(source_mapping_file='map.yaml')
        src, dst = MagicMock(absolute='src-abs'), MagicMock(absolute='dst-abs')
        src.copy_to = AsyncMock()

        with patch('otter.tasks.copy_many.AsyncStorageHandle', side_effect=[src, dst]) as handle:
            await task._copy_single_file('gs://b/x/long.tsv.gz', asyncio.Semaphore(1), name='short.tsv.gz')

        assert handle.call_args_list[1].args[0] == 'dest/short.tsv.gz'

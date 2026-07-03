"""Tests for the copy task."""

from unittest.mock import MagicMock, patch

import pytest

from otter.scratchpad.model import Scratchpad
from otter.task.model import TaskContext
from otter.tasks.copy import Copy, CopySpec
from test.mocks import fake_config


class TestCopyTask:
    def test_spec_defaults_to_no_settings(self) -> None:
        spec = CopySpec(
            name='copy test copy',
            source='gs://source-bucket/source.txt',
            destination='dest.txt',
        )

        assert spec.settings is None

    @pytest.mark.asyncio
    async def test_run_uses_settings_context(self) -> None:
        spec = CopySpec(
            name='copy test copy',
            source='gs://source-bucket/source.txt',
            destination='dest.txt',
            settings={'billing_project': 'billing-project'},
        )
        task = Copy(spec, TaskContext(config=fake_config(), scratchpad=Scratchpad()))

        src_handle = MagicMock()
        src_handle.absolute = 'gs://source-bucket/source.txt'
        dst_handle = MagicMock()
        dst_handle.absolute = 'gs://test-bucket/release/path/dest.txt'

        with (
            patch('otter.tasks.copy.storage_context') as mock_storage_context,
            patch('otter.tasks.copy.StorageHandle') as mock_storage_handle,
        ):
            mock_storage_handle.side_effect = [src_handle, dst_handle]

            await task.run()

        mock_storage_context.assert_called_once_with(billing_project='billing-project')
        src_handle.copy_to.assert_called_once_with(dst_handle)

    @pytest.mark.asyncio
    async def test_validate_uses_settings_context(self) -> None:
        spec = CopySpec(
            name='copy test copy',
            source='gs://source-bucket/source.txt',
            destination='dest.txt',
            settings={'billing_project': 'billing-project'},
        )
        task = Copy(spec, TaskContext(config=fake_config(), scratchpad=Scratchpad()))

        src_handle = MagicMock()
        src_handle.absolute = 'gs://source-bucket/source.txt'
        dst_handle = MagicMock()
        dst_handle.absolute = 'gs://test-bucket/release/path/dest.txt'

        with (
            patch('otter.tasks.copy.storage_context'),
            patch('otter.tasks.copy.StorageHandle') as mock_storage_handle,
        ):
            mock_storage_handle.side_effect = [src_handle, dst_handle]
            await task.run()

        with (
            patch('otter.tasks.copy.storage_context') as mock_storage_context,
            patch('otter.tasks.copy.file.exists') as mock_exists,
            patch('otter.tasks.copy.file.size') as mock_size,
        ):
            await task.validate()

        mock_storage_context.assert_called_once_with(billing_project='billing-project')
        mock_exists.assert_called_once()
        mock_size.assert_called_once()

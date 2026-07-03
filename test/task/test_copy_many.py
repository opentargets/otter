"""Tests for the copy_many task."""

from unittest.mock import AsyncMock, patch

import pytest

from otter.manifest.model import Artifact
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

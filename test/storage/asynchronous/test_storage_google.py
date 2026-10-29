"""Tests for the AsyncGoogleStorage class."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import ClientResponseError

from otter.storage.asynchronous.google import DELETE_CONCURRENCY, AsyncGoogleStorage
from otter.storage.storage_context import storage_context
from otter.util.errors import NotFoundError, PartialDeleteError, PreconditionFailedError, StorageError


class TestGoogleStorage:
    @pytest.fixture
    def storage(self) -> AsyncGoogleStorage:
        return AsyncGoogleStorage()

    @pytest.mark.parametrize(
        ('uri', 'expected_bucket', 'expected_blob'),
        [
            ('gs://bucket/path/file.txt', 'bucket', 'path/file.txt'),
            ('gs://bucket/file.txt', 'bucket', 'file.txt'),
            ('gs://bucket/', 'bucket', ''),
            ('gs://bucket', 'bucket', ''),
        ],
    )
    def test_parse_uri(
        self,
        storage: AsyncGoogleStorage,
        uri: str,
        expected_bucket: str,
        expected_blob: str,
    ) -> None:
        bucket, blob = storage._parse_uri(uri)
        assert bucket == expected_bucket
        assert blob == expected_blob

    @pytest.mark.asyncio
    async def test_stat_regular_blob_populates_mtime(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        # find_latest relies on mtime to pick the newest file; a stat that leaves it None
        # silently drops every candidate
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(
                return_value={
                    'size': '1913915',
                    'generation': '42',
                    'updated': '2026-04-23T07:47:33.195Z',
                }
            )
            mock_get_client.return_value = mock_client

            result = await storage.stat('gs://bucket/file.json.gz')

        assert result.is_reg is True
        assert result.size == 1913915
        assert result.revision == '42'
        assert result.mtime == datetime(2026, 4, 23, 7, 47, 33, 195000, tzinfo=UTC).timestamp()

    @pytest.mark.asyncio
    async def test_stat_prefix(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(side_effect=Exception('Not Found'))
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': 'prefix/file1.txt'}]})
            mock_get_client.return_value = mock_client

            result = await storage.stat('gs://bucket/prefix')

        assert result.is_dir is True
        assert result.is_reg is False
        assert result.size == 0

    @pytest.mark.asyncio
    async def test_stat_not_found(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(side_effect=Exception('Not Found'))
            mock_client.list_objects = AsyncMock(return_value={'items': []})
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                await storage.stat('gs://bucket/not-found.txt')

    @pytest.mark.asyncio
    async def test_glob(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(
                return_value={
                    'items': [
                        {'name': 'data/file1.txt'},
                        {'name': 'data/file2.txt'},
                    ]
                }
            )
            mock_get_client.return_value = mock_client

            result = await storage.glob('gs://bucket/data/', '*.txt')

        assert len(result) == 2
        assert 'gs://bucket/data/file1.txt' in result
        assert 'gs://bucket/data/file2.txt' in result

    @pytest.mark.asyncio
    async def test_glob_raises_on_error(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(side_effect=Exception('API Error'))
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error listing blobs'):
                await storage.glob('gs://bucket/data/', '*.txt')

    @pytest.mark.asyncio
    async def test_read(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_metadata = {'generation': '42'}
        mock_data = b'file content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(return_value=mock_metadata)
            mock_client.download = AsyncMock(return_value=mock_data)
            mock_get_client.return_value = mock_client

            data, revision = await storage.read('gs://bucket/file.txt')

        assert data == mock_data
        assert revision == '42'

    @pytest.mark.asyncio
    async def test_read_retries_on_modification(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_data = b'file content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(
                side_effect=[
                    {'generation': '1'},
                    {'generation': '2'},
                    {'generation': '2'},
                    {'generation': '2'},
                ]
            )
            mock_client.download = AsyncMock(return_value=mock_data)
            mock_get_client.return_value = mock_client

            data, revision = await storage.read('gs://bucket/file.txt')

        assert data == mock_data
        assert revision == '2'
        assert mock_client.download.call_count == 2

    @pytest.mark.asyncio
    async def test_read_not_found(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(side_effect=Exception('404 Not Found'))
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                await storage.read('gs://bucket/not-found.txt')

    @pytest.mark.asyncio
    async def test_read_text_encoding_error(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_metadata = {'generation': '42'}
        mock_data = b'\x80\x81\x82'  # Invalid UTF-8

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(return_value=mock_metadata)
            mock_client.download = AsyncMock(return_value=mock_data)
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error decoding'):
                await storage.read_text('gs://bucket/file.txt')

    @pytest.mark.asyncio
    async def test_write(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_metadata = {'generation': '43'}
        mock_data = b'new content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.upload = AsyncMock(return_value=mock_metadata)
            mock_get_client.return_value = mock_client

            revision = await storage.write('gs://bucket/file.txt', mock_data)

        assert revision == '43'

    @pytest.mark.asyncio
    async def test_write_with_expected_revision(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_metadata = {'generation': '43'}
        mock_data = b'new content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.upload = AsyncMock(return_value=mock_metadata)
            mock_get_client.return_value = mock_client

            await storage.write('gs://bucket/file.txt', mock_data, expected_revision='42')

        mock_client.upload.assert_called_once()
        call_args = mock_client.upload.call_args
        assert call_args[1]['headers'] == {'x-goog-if-generation-match': '42'}

    @pytest.mark.asyncio
    async def test_write_precondition_failed(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            error = ClientResponseError(
                request_info=Mock(),
                history=(),
                status=412,
                message='Precondition Failed',
            )
            mock_client.upload = AsyncMock(side_effect=error)
            mock_get_client.return_value = mock_client

            with pytest.raises(PreconditionFailedError, match='generation mismatch'):
                await storage.write('gs://bucket/file.txt', b'data', expected_revision='99')

    @pytest.mark.asyncio
    async def test_copy_within(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        mock_metadata = {'generation': '44'}

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.copy = AsyncMock()
            mock_client.download_metadata = AsyncMock(return_value=mock_metadata)
            mock_get_client.return_value = mock_client

            revision = await storage.copy_within('gs://bucket/source.txt', 'gs://bucket/dest.txt')

        assert revision == '44'
        mock_client.copy.assert_called_once_with(
            'bucket',
            'source.txt',
            'bucket',
            new_name='dest.txt',
            params=None,
            headers=None,
        )

    @pytest.mark.asyncio
    async def test_copy_within_not_found(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.copy = AsyncMock(side_effect=Exception('404 Not Found'))
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                await storage.copy_within('gs://bucket/not-found.txt', 'gs://bucket/dest.txt')

    @pytest.mark.asyncio
    async def test_read_uses_billing_project_context(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.download_metadata = AsyncMock(return_value={'generation': '42'})
            mock_client.download = AsyncMock(return_value=b'data')
            mock_get_client.return_value = mock_client

            with storage_context(billing_project='billing-project'):
                await storage.read('gs://bucket/file.txt')

        expected_headers = {'x-goog-user-project': 'billing-project'}
        first_call = mock_client.download_metadata.call_args_list[0]
        assert first_call.kwargs['headers'] == expected_headers
        assert mock_client.download.call_args.kwargs['headers'] == expected_headers

    @pytest.mark.asyncio
    async def test_copy_within_uses_billing_project_context(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.copy = AsyncMock()
            mock_client.download_metadata = AsyncMock(return_value={'generation': '44'})
            mock_get_client.return_value = mock_client

            with storage_context(billing_project='billing-project'):
                await storage.copy_within('gs://bucket/source.txt', 'gs://bucket/dest.txt')

        mock_client.copy.assert_called_once_with(
            'bucket',
            'source.txt',
            'bucket',
            new_name='dest.txt',
            params={'userProject': 'billing-project'},
            headers={'x-goog-user-project': 'billing-project'},
        )

    @pytest.mark.asyncio
    async def test_delete_blob(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """Delete a single blob."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/path/file.txt')

        assert mock_client.delete.await_args_list[0].args == ('bucket', 'path/file.txt')

    @pytest.mark.asyncio
    async def test_delete_missing_blob_is_not_an_error(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """A 404 means there was no such blob, which is not an error."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            # nothing sits under the prefix either, so there is nothing to refuse
            mock_client.list_objects = AsyncMock(return_value={'items': []})
            mock_client.delete = AsyncMock(
                side_effect=ClientResponseError(
                    request_info=Mock(),
                    history=(),
                    status=404,
                    message='Not Found',
                )
            )
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/path/gone.txt')

        assert mock_client.delete.await_args_list[0].args == ('bucket', 'path/gone.txt')

    @pytest.mark.asyncio
    async def test_delete_other_error_raises(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """Any status other than 404 is a real failure."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.delete = AsyncMock(
                side_effect=ClientResponseError(
                    request_info=Mock(),
                    history=(),
                    status=403,
                    message='Forbidden',
                )
            )
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error deleting'):
                await storage.delete('gs://bucket/path/file.txt')

    @staticmethod
    def _bucket_holding(*names: str):
        """Fake client.delete for a bucket that holds exactly ``names``.

        Deleting anything else raises a 404, the way the real API would.
        """
        existing = set(names)

        def fake_delete(*args: object, **_kwargs: object) -> str:
            if args[1] not in existing:
                raise ClientResponseError(
                    request_info=Mock(),
                    history=(),
                    status=404,
                    message='Not Found',
                )
            return ''

        return fake_delete

    @pytest.mark.asyncio
    async def test_delete_prefix_recursive(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """Recursive deletion removes every blob sharing the prefix."""
        parts = ['dataset/00000000.parquet', 'dataset/00000001.parquet']

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': n} for n in parts]})
            mock_client.delete = AsyncMock(side_effect=self._bucket_holding(*parts))
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/dataset', is_recursive=True)

        assert mock_client.list_objects.await_args_list[0].kwargs['params']['prefix'] == 'dataset/'
        deleted = [call.args[1] for call in mock_client.delete.await_args_list]
        assert deleted == [*parts, 'dataset']

    @pytest.mark.asyncio
    async def test_delete_prefix_recursive_with_trailing_slash(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """A location written with a trailing slash gives the same prefix."""
        parts = ['dataset/00000000.parquet']

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': n} for n in parts]})
            mock_client.delete = AsyncMock(side_effect=self._bucket_holding(*parts))
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/dataset/', is_recursive=True)

        assert mock_client.list_objects.await_args_list[0].kwargs['params']['prefix'] == 'dataset/'
        deleted = [call.args[1] for call in mock_client.delete.await_args_list]
        assert deleted == [*parts, 'dataset/']

    @pytest.mark.asyncio
    async def test_delete_prefix_recursive_also_deletes_matching_blob(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """A blob named like the prefix goes too, the prefix listing misses it."""
        parts = ['dataset/00000000.parquet']

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': n} for n in parts]})
            mock_client.delete = AsyncMock(side_effect=self._bucket_holding(*parts, 'dataset'))
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/dataset', is_recursive=True)

        deleted = [call.args[1] for call in mock_client.delete.await_args_list]
        assert deleted == [*parts, 'dataset']

    @pytest.mark.asyncio
    async def test_delete_empty_prefix_recursive_is_not_an_error(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """Recursive deletion of a prefix with nothing under it does not raise."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': []})
            mock_client.delete = AsyncMock(side_effect=self._bucket_holding())
            mock_get_client.return_value = mock_client

            await storage.delete('gs://bucket/dataset', is_recursive=True)

        assert mock_client.delete.await_args_list[0].args == ('bucket', 'dataset')

    @pytest.mark.asyncio
    async def test_delete_prefix_without_recursive_raises(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """A prefix with blobs under it refuses to be deleted without is_recursive."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': 'dataset/00000000.parquet'}]})
            mock_client.delete = AsyncMock(side_effect=self._bucket_holding())
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='is_recursive'):
                await storage.delete('gs://bucket/dataset')

        params = mock_client.list_objects.await_args_list[0].kwargs['params']
        assert params['prefix'] == 'dataset/'
        assert params['maxResults'] == '1'

    @pytest.mark.parametrize('uri', ['gs://bucket', 'gs://bucket/'])
    @pytest.mark.asyncio
    async def test_delete_bucket_root_raises(
        self,
        storage: AsyncGoogleStorage,
        uri: str,
    ) -> None:
        """Deleting the root of a bucket is refused, with or without is_recursive."""
        with pytest.raises(StorageError, match='root of bucket'):
            await storage.delete(uri, is_recursive=True)

        with pytest.raises(StorageError, match='root of bucket'):
            await storage.delete(uri)

    @pytest.mark.asyncio
    async def test_delete_stops_at_the_first_failure(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """A blob that will not delete stops the run and reports what already went."""
        parts = [
            'dataset/00000000.parquet',
            'dataset/00000001.parquet',
            'dataset/00000002.parquet',
        ]

        def fail_on_the_second(*args: object, **_kwargs: object) -> str:
            if args[1] == parts[1]:
                raise ClientResponseError(
                    request_info=Mock(),
                    history=(),
                    status=403,
                    message='Forbidden',
                )
            return ''

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_objects = AsyncMock(return_value={'items': [{'name': n} for n in parts]})
            mock_client.delete = AsyncMock(side_effect=fail_on_the_second)
            mock_get_client.return_value = mock_client

            with pytest.raises(PartialDeleteError) as excinfo:
                await storage.delete('gs://bucket/dataset', is_recursive=True)

        assert excinfo.value.deleted == 1
        assert excinfo.value.failed == parts[1]
        # the run stopped, it did not carry on to the third blob
        attempted = [call.args[1] for call in mock_client.delete.await_args_list]
        assert parts[2] not in attempted

    @pytest.mark.asyncio
    async def test_delete_prefix_limits_concurrent_requests(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        parts = [f'dataset/{i}.parquet' for i in range(DELETE_CONCURRENCY * 2 + 1)]
        active = 0
        peak = 0
        first_batch_started = asyncio.Event()

        async def delete_with_overlap(bucket, name, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == DELETE_CONCURRENCY:
                first_batch_started.set()
            try:
                await first_batch_started.wait()
                await asyncio.sleep(0)
            finally:
                active -= 1

        client = AsyncMock()
        client.list_objects.return_value = {'items': [{'name': name} for name in parts]}
        client.delete.side_effect = delete_with_overlap
        with patch.object(storage, '_get_client', return_value=client):
            await asyncio.wait_for(storage.delete('gs://bucket/dataset', is_recursive=True), timeout=5)

        assert peak == DELETE_CONCURRENCY
        assert active == 0
        assert {call.args[1] for call in client.delete.await_args_list} == {*parts, 'dataset'}
        assert client.delete.await_args_list[-1].args[1] == 'dataset'

    @pytest.mark.asyncio
    async def test_delete_failure_waits_for_active_requests_and_skips_queued_requests(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        parts = [f'dataset/{i}.parquet' for i in range(DELETE_CONCURRENCY + 5)]
        started = 0
        first_batch_started = asyncio.Event()
        failure_raised = asyncio.Event()
        completed = []

        async def fail_while_others_are_active(bucket, name, **kwargs):
            nonlocal started
            started += 1
            if started == DELETE_CONCURRENCY:
                first_batch_started.set()
            await first_batch_started.wait()
            if name == parts[0]:
                failure_raised.set()
                raise PermissionError('access denied')
            await failure_raised.wait()
            await asyncio.sleep(0)
            completed.append(name)

        client = AsyncMock()
        client.list_objects.return_value = {'items': [{'name': name} for name in parts]}
        client.delete.side_effect = fail_while_others_are_active
        with patch.object(storage, '_get_client', return_value=client):
            with pytest.raises(PartialDeleteError) as excinfo:
                await asyncio.wait_for(storage.delete('gs://bucket/dataset', is_recursive=True), timeout=5)

        assert excinfo.value.failed == parts[0]
        assert excinfo.value.deleted == len(completed) == DELETE_CONCURRENCY - 1
        assert {call.args[1] for call in client.delete.await_args_list} == set(parts[:DELETE_CONCURRENCY])

    @pytest.mark.asyncio
    async def test_delete_single_blob_failure_is_not_partial(
        self,
        storage: AsyncGoogleStorage,
    ) -> None:
        """Nothing was deleted, so there is no partial state to report."""
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.delete = AsyncMock(
                side_effect=ClientResponseError(
                    request_info=Mock(),
                    history=(),
                    status=403,
                    message='Forbidden',
                )
            )
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error deleting') as excinfo:
                await storage.delete('gs://bucket/path/file.txt')

        assert not isinstance(excinfo.value, PartialDeleteError)

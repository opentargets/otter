"""Tests for the AsyncGoogleStorage class."""

from unittest.mock import AsyncMock, patch

import pytest

from otter.storage.asynchronous.google import AsyncGoogleStorage
from otter.storage.storage_context import storage_context
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError


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
            error = Exception('Precondition Failed')
            error.status = 412  # type: ignore[attr-defined]
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

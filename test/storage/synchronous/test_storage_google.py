"""Tests for the GoogleStorage class."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from otter.storage.synchronous.google import GoogleStorage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError


class TestGoogleStorage:
    @pytest.fixture
    def storage(self) -> GoogleStorage:
        return GoogleStorage()

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
        storage: GoogleStorage,
        uri: str,
        expected_bucket: str,
        expected_blob: str,
    ) -> None:
        bucket, blob = storage._parse_uri(uri)
        assert bucket == expected_bucket
        assert blob == expected_blob

    def test_stat_prefix(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.reload = MagicMock(side_effect=NotFound('Not Found'))
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_bucket.list_blobs = MagicMock(return_value=[MagicMock(), MagicMock()])
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            result = storage.stat('gs://bucket/prefix')

        assert result.is_dir is True
        assert result.is_reg is False
        assert result.size == 0

    def test_stat_not_found(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.reload = MagicMock(side_effect=NotFound('Not Found'))
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_bucket.list_blobs = MagicMock(return_value=[])
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                storage.stat('gs://bucket/not-found.txt')

    def test_glob(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob1 = MagicMock()
            mock_blob1.name = 'data/file1.txt'
            mock_blob2 = MagicMock()
            mock_blob2.name = 'data/file2.txt'
            mock_bucket.list_blobs = MagicMock(return_value=[mock_blob1, mock_blob2])
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            result = storage.glob('gs://bucket/data/', '*.txt')

        assert len(result) == 2
        assert 'gs://bucket/data/file1.txt' in result
        assert 'gs://bucket/data/file2.txt' in result

    def test_glob_raises_on_error(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_bucket.list_blobs = MagicMock(side_effect=Exception('API Error'))
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error listing blobs'):
                storage.glob('gs://bucket/data/', '*.txt')

    def test_read(
        self,
        storage: GoogleStorage,
    ) -> None:
        mock_data = b'file content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.generation = 42
            mock_blob.reload = MagicMock()
            mock_blob.download_as_bytes = MagicMock(return_value=mock_data)
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            data, revision = storage.read('gs://bucket/file.txt')

        assert data == mock_data
        assert revision == '42'

    def test_read_retries_on_modification(
        self,
        storage: GoogleStorage,
    ) -> None:
        mock_data = b'file content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            # First iteration: generation changes during read
            # Second iteration: generation stays stable
            call_count = [0]

            def mock_reload():
                call_count[0] += 1
                if call_count[0] == 1:
                    mock_blob.generation = 1
                elif call_count[0] == 2:
                    mock_blob.generation = 2
                else:
                    mock_blob.generation = 2

            mock_blob.reload = mock_reload
            mock_blob.download_as_bytes = MagicMock(return_value=mock_data)
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            data, revision = storage.read('gs://bucket/file.txt')

        assert data == mock_data
        assert revision == '2'
        assert mock_blob.download_as_bytes.call_count == 2

    def test_read_not_found(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.reload = MagicMock(side_effect=NotFound('404 Not Found'))
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                storage.read('gs://bucket/not-found.txt')

    def test_read_text_encoding_error(
        self,
        storage: GoogleStorage,
    ) -> None:
        mock_data = b'\x80\x81\x82'  # Invalid UTF-8

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.generation = 42
            mock_blob.reload = MagicMock()
            mock_blob.download_as_bytes = MagicMock(return_value=mock_data)
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(StorageError, match='error decoding'):
                storage.read_text('gs://bucket/file.txt')

    def test_write(
        self,
        storage: GoogleStorage,
    ) -> None:
        mock_data = b'new content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.generation = 43
            mock_blob.upload_from_string = MagicMock()
            mock_blob.reload = MagicMock()
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            revision = storage.write('gs://bucket/file.txt', mock_data)

        assert revision == '43'

    def test_write_with_expected_revision(
        self,
        storage: GoogleStorage,
    ) -> None:
        mock_data = b'new content'

        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.generation = 43
            mock_blob.upload_from_string = MagicMock()
            mock_blob.reload = MagicMock()
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            storage.write('gs://bucket/file.txt', mock_data, expected_revision='42')

        mock_blob.upload_from_string.assert_called_once()
        call_args = mock_blob.upload_from_string.call_args
        assert call_args[1]['if_generation_match'] == 42

    def test_write_precondition_failed(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_blob.upload_from_string = MagicMock(side_effect=PreconditionFailed('Precondition Failed'))
            mock_bucket.blob = MagicMock(return_value=mock_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(PreconditionFailedError, match='generation mismatch'):
                storage.write('gs://bucket/file.txt', b'data', expected_revision='99')

    def test_copy_within(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_src_blob = MagicMock()
            mock_dst_blob = MagicMock()
            mock_dst_blob.generation = 44
            mock_dst_blob.reload = MagicMock()
            mock_bucket.blob = MagicMock(return_value=mock_src_blob)
            mock_bucket.copy_blob = MagicMock(return_value=mock_dst_blob)
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            revision = storage.copy_within('gs://bucket/source.txt', 'gs://bucket/dest.txt')

        assert revision == '44'
        mock_bucket.copy_blob.assert_called_once()

    def test_copy_within_not_found(
        self,
        storage: GoogleStorage,
    ) -> None:
        with patch.object(storage, '_get_client') as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_bucket.copy_blob = MagicMock(side_effect=NotFound('404 Not Found'))
            mock_bucket.blob = MagicMock()
            mock_client.bucket = MagicMock(return_value=mock_bucket)
            mock_get_client.return_value = mock_client

            with pytest.raises(NotFoundError):
                storage.copy_within('gs://bucket/not-found.txt', 'gs://bucket/dest.txt')

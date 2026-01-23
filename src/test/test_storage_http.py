"""Unit tests for HTTPStorage."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from otter.storage.http import HTTPStorage


class TestHTTPStorage:
    """Test the HTTPStorage class."""

    @pytest.fixture
    def storage(self) -> HTTPStorage:
        """Create an HTTPStorage instance."""
        return HTTPStorage()

    def test_name(self, storage: HTTPStorage) -> None:
        """Test that the name property returns the expected value."""
        assert storage.name == 'HTTP Storage'

    def test_stat_returns_metadata(self, storage: HTTPStorage) -> None:
        """Test stat returns file metadata from HTTP headers."""
        mock_response = MagicMock()
        mock_response.headers = {
            'Content-Length': '1234',
            'Last-Modified': 'Mon, 20 Jan 2026 12:00:00 GMT',
        }

        with patch.object(requests.Session, 'send', return_value=mock_response):
            result = storage.stat('http://example.com/file.txt')

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 1234
        assert result.revision == 'Mon, 20 Jan 2026 12:00:00 GMT'

    def test_stat_without_content_length(self, storage: HTTPStorage) -> None:
        """Test stat handles missing Content-Length header."""
        mock_response = MagicMock()
        mock_response.headers = {}

        with patch.object(requests.Session, 'send', return_value=mock_response):
            result = storage.stat('http://example.com/file.txt')

        assert result.size is None
        assert result.revision is None

    def test_stat_raises_on_http_error(self, storage: HTTPStorage) -> None:
        """Test stat raises on HTTP error status."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')

        with patch.object(requests.Session, 'send', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                storage.stat('http://example.com/not_found.txt')

    def test_open_write_mode_raises(self, storage: HTTPStorage) -> None:
        """Test opening in write mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match='only supports read mode'):
            storage.open('http://example.com/file.txt', 'w')

    def test_open_raises_on_http_error(self, storage: HTTPStorage) -> None:
        """Test open raises on HTTP error status."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('403 Forbidden')

        with patch.object(requests.Session, 'send', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                storage.open('http://example.com/forbidden.txt', 'r')

    def test_glob_not_implemented(self, storage: HTTPStorage) -> None:
        """Test that glob raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            storage.glob('http://example.com/', '*.txt')

    def test_download_to_file(self, storage: HTTPStorage, tmp_path: Path) -> None:
        """Test download_to_file downloads content to file."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2', b'chunk3']

        dst_file = tmp_path / 'downloaded.txt'

        with patch.object(requests.Session, 'send', return_value=mock_response):
            bytes_written = storage.download_to_file('http://example.com/file.txt', dst_file)

        assert dst_file.exists()
        assert dst_file.read_bytes() == b'chunk1chunk2chunk3'
        assert bytes_written == 18  # len('chunk1chunk2chunk3')

    def test_download_to_file_raises_on_http_error(self, storage: HTTPStorage, tmp_path: Path) -> None:
        """Test download_to_file raises on HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('500 Server Error')

        dst_file = tmp_path / 'downloaded.txt'

        with patch.object(requests.Session, 'send', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                storage.download_to_file('http://example.com/error.txt', dst_file)

    def test_download_to_string(self, storage: HTTPStorage) -> None:
        """Test download_to_string returns content and length."""
        mock_response = MagicMock()
        mock_response.text = 'Hello, World!'

        with patch.object(requests.Session, 'send', return_value=mock_response):
            content, length = storage.download_to_string('http://example.com/file.txt')

        assert content == 'Hello, World!'
        assert length == 13

    def test_download_to_string_raises_on_http_error(self, storage: HTTPStorage) -> None:
        """Test download_to_string raises on HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')

        with patch.object(requests.Session, 'send', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                storage.download_to_string('http://example.com/not_found.txt')

    def test_upload_not_implemented(self, storage: HTTPStorage, tmp_path: Path) -> None:
        """Test that upload raises NotImplementedError."""
        src_file = tmp_path / 'source.txt'
        src_file.write_text('content')

        with pytest.raises(NotImplementedError, match='does not support upload'):
            storage.upload(src_file, 'http://example.com/dest.txt')

    def test_copy_within_not_implemented(self, storage: HTTPStorage) -> None:
        """Test that copy_within raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match='does not support copy within'):
            storage.copy_within('http://example.com/src.txt', 'http://example.com/dst.txt')

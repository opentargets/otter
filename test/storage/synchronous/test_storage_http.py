"""Tests for the HTTPStorage class."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from otter.storage.synchronous.http import HTTPStorage

DATE_RFC_1123 = 'Sun, 06 Nov 1994 08:49:37 GMT'


class TestHTTPStorage:
    @pytest.fixture
    def storage(self) -> HTTPStorage:
        return HTTPStorage()

    def test_stat(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.headers = {
            'Content-Length': '1234',
            'Last-Modified': DATE_RFC_1123,
        }
        mock_response.raise_for_status = MagicMock()
        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.head = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            result = storage.stat('http://example.com/file.txt')

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 1234
        assert result.revision == DATE_RFC_1123

    def test_stat_without_content_length(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.head = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            result = storage.stat('http://example.com/file.txt')

        assert result.size is None
        assert result.revision is None

    def test_stat_raises_on_http_error(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('404 Not Found')

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.head = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            with pytest.raises(requests.exceptions.HTTPError):
                storage.stat('http://example.com/not_found.txt')

    def test_glob_not_implemented(
        self,
        storage: HTTPStorage,
    ) -> None:
        with pytest.raises(NotImplementedError):
            storage.glob('http://example.com/', '*.txt')

    def test_read(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.content = b'file content'
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {'Last-Modified': DATE_RFC_1123}

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            content, revision = storage.read('http://example.com/file.txt')

        assert content == b'file content'
        assert revision == DATE_RFC_1123

    def test_read_raises_on_http_error(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('500 Server Error')

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            with pytest.raises(requests.exceptions.HTTPError):
                storage.read('http://example.com/error.txt')

    def test_read_text(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.content = b'Hello, World!'
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {'Last-Modified': DATE_RFC_1123}

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            content, revision = storage.read_text('http://example.com/file.txt')

        assert content == 'Hello, World!'
        assert revision == DATE_RFC_1123

    def test_read_text_raises_on_http_error(
        self,
        storage: HTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('404 Not Found')

        with patch.object(storage, '_get_session') as mock_get_session:
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            with pytest.raises(requests.exceptions.HTTPError):
                storage.read_text('http://example.com/not_found.txt')

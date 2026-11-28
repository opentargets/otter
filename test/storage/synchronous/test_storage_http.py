"""Tests for the HTTPStorage class."""

from unittest.mock import MagicMock, patch

import pytest
import requests
from freezegun import freeze_time

from otter.storage.synchronous.http import (
    DEFAULT_RATE_LIMIT_WAIT,
    MAX_RATE_LIMIT_RETRIES,
    MAX_RATE_LIMIT_WAIT,
    HTTPStorage,
)

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


def _response(status_code: int, headers: dict[str, str] | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.content = b'file content'
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f'{status_code} Client Error')
    return resp


class TestHTTPStorageRateLimit:
    @pytest.fixture
    def storage(self) -> HTTPStorage:
        return HTTPStorage()

    @pytest.fixture
    def sleep(self):
        with patch('otter.storage.synchronous.http.time.sleep') as mock_sleep:
            yield mock_sleep

    def _session(self, storage: HTTPStorage, method: str, responses: list[MagicMock]):
        mock_session = MagicMock()
        getattr(mock_session, method).side_effect = responses
        return patch.object(storage, '_get_session', return_value=mock_session), mock_session

    def test_read_retries_after_429_honouring_retry_after(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        p, session = self._session(storage, 'get', [_response(429, {'Retry-After': '600'}), _response(200)])
        with p:
            content, _ = storage.read('http://example.com/file.txt')

        assert content == b'file content'
        assert session.get.call_count == 2
        sleep.assert_called_once_with(600.0)

    def test_stat_retries_after_429(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        ok = _response(200, {'Content-Length': '1234'})
        p, session = self._session(storage, 'head', [_response(429, {'Retry-After': '5'}), ok])
        with p:
            result = storage.stat('http://example.com/file.txt')

        assert result.size == 1234
        assert session.head.call_count == 2
        sleep.assert_called_once_with(5.0)

    def test_retry_after_as_http_date(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        with freeze_time('2026-11-28 18:00:00'):
            p, _ = self._session(
                storage, 'get', [_response(429, {'Retry-After': 'Sat, 28 Nov 2026 18:02:00 GMT'}), _response(200)]
            )
            with p:
                storage.read('http://example.com/file.txt')

        sleep.assert_called_once_with(120.0)

    def test_missing_or_bad_retry_after_uses_default(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        p, _ = self._session(storage, 'get', [_response(429), _response(429, {'Retry-After': 'soon'}), _response(200)])
        with p:
            storage.read('http://example.com/file.txt')

        assert [c.args[0] for c in sleep.call_args_list] == [DEFAULT_RATE_LIMIT_WAIT, DEFAULT_RATE_LIMIT_WAIT]

    def test_wait_is_capped(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        p, _ = self._session(storage, 'get', [_response(429, {'Retry-After': '86400'}), _response(200)])
        with p:
            storage.read('http://example.com/file.txt')

        sleep.assert_called_once_with(float(MAX_RATE_LIMIT_WAIT))

    def test_gives_up_after_max_retries(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        responses = [_response(429, {'Retry-After': '1'}) for _ in range(MAX_RATE_LIMIT_RETRIES + 1)]
        p, session = self._session(storage, 'get', responses)
        with p, pytest.raises(requests.exceptions.HTTPError, match='429'):
            storage.read('http://example.com/file.txt')

        assert session.get.call_count == MAX_RATE_LIMIT_RETRIES + 1
        assert sleep.call_count == MAX_RATE_LIMIT_RETRIES

    def test_other_errors_are_not_retried(self, storage: HTTPStorage, sleep: MagicMock) -> None:
        p, session = self._session(storage, 'get', [_response(503)])
        with p, pytest.raises(requests.exceptions.HTTPError):
            storage.read('http://example.com/file.txt')

        assert session.get.call_count == 1
        sleep.assert_not_called()

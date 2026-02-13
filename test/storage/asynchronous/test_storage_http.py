"""Tests for the AsyncHTTPStorage class."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from otter.storage.asynchronous.http import AsyncHTTPStorage

DATE_RFC_1123 = 'Sun, 06 Nov 1994 08:49:37 GMT'


class TestHTTPStorage:
    @pytest.fixture
    def storage(self) -> AsyncHTTPStorage:
        return AsyncHTTPStorage()

    @pytest.mark.asyncio
    async def test_stat(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.headers = {
            'Content-Length': '1234',
            'Last-Modified': DATE_RFC_1123,
        }
        mock_response.raise_for_status = MagicMock()
        with patch.object(
            httpx.AsyncClient,
            'request',
            return_value=mock_response,
        ):
            result = await storage.stat('http://example.com/file.txt')

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 1234
        assert result.revision == DATE_RFC_1123

    @pytest.mark.asyncio
    async def test_stat_without_content_length(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, 'request', return_value=mock_response):
            result = await storage.stat('http://example.com/file.txt')

        assert result.size is None
        assert result.revision is None

    @pytest.mark.asyncio
    async def test_stat_raises_on_http_error(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            '404 Not Found', request=MagicMock(), response=MagicMock()
        )

        with patch.object(httpx.AsyncClient, 'request', return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await storage.stat('http://example.com/not_found.txt')

    @pytest.mark.asyncio
    async def test_glob_not_implemented(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        with pytest.raises(NotImplementedError):
            await storage.glob('http://example.com/', '*.txt')

    @pytest.mark.asyncio
    async def test_read(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.content = b'file content'
        mock_response.raise_for_status = MagicMock()
        mock_response.headers.get = MagicMock(return_value=DATE_RFC_1123)

        with patch.object(httpx.AsyncClient, 'get', return_value=mock_response):
            content, revision = await storage.read('http://example.com/file.txt')

        assert content == b'file content'
        assert revision == DATE_RFC_1123

    @pytest.mark.asyncio
    async def test_read_raises_on_http_error(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            '500 Server Error', request=MagicMock(), response=MagicMock()
        )

        with patch.object(httpx.AsyncClient, 'get', return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await storage.read('http://example.com/error.txt')

    @pytest.mark.asyncio
    async def test_read_text(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.content = b'Hello, World!'
        mock_response.raise_for_status = MagicMock()
        mock_response.headers.get = MagicMock(return_value=DATE_RFC_1123)

        with patch.object(httpx.AsyncClient, 'get', return_value=mock_response):
            content, revision = await storage.read_text('http://example.com/file.txt')

        assert content == 'Hello, World!'
        assert revision == DATE_RFC_1123

    @pytest.mark.asyncio
    async def test_read_text_raises_on_http_error(
        self,
        storage: AsyncHTTPStorage,
    ) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            '404 Not Found', request=MagicMock(), response=MagicMock()
        )

        with patch.object(httpx.AsyncClient, 'get', return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await storage.read_text('http://example.com/not_found.txt')

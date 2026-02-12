"""HTTP Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from datetime import datetime

import httpx

from otter.storage.model import Revision, StatResult, Storage

REQUEST_TIMEOUT = 60


class HTTPStorage(Storage):
    """HTTP Storage class.

    This class implements the Storage interface for HTTP resources.
    Uses httpx.AsyncClient for async HTTP operations with connection pooling.
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        return self._client

    @property
    def name(self) -> str:
        return 'HTTP Storage'

    async def stat(self, location: str) -> StatResult:
        client = self._get_client()
        resp = await client.request(
            'HEAD',
            location,
            headers={'Accept-Encoding': 'identity'},  # prevent compression to get real size
            follow_redirects=True,
        )
        resp.raise_for_status()

        if 'Content-Length' not in resp.headers:
            size = None
        else:
            size = int(resp.headers.get('Content-Length'))

        last_modified = resp.headers.get('Last-Modified', None)
        if last_modified is not None:
            mtime = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z').timestamp()
        else:
            mtime = None

        return StatResult(
            is_dir=False,
            is_reg=True,
            size=size,
            revision=resp.headers.get('Last-Modified', None),
            mtime=mtime,
        )

    async def glob(self, location: str, pattern: str) -> list[str]:
        """Glob is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage does not support globbing.
        """
        raise NotImplementedError

    async def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        try:
            resp = await self._get_client().get(
                location,
                follow_redirects=True,
            )
            resp.raise_for_status()
            return resp.content, resp.headers.get('Last-Modified', None)
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.TimeoutException):
            raise TimeoutError(f'timeout while reading {location}')

    async def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        data, revision = await self.read(location)
        return data.decode(encoding), revision

    async def write(
        self,
        location: str,
        data: bytes,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision | None = None,
    ) -> Revision:
        """Writing is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage is read-only.
        """
        raise NotImplementedError

    async def write_text(
        self,
        location: str,
        data: str,
        *,
        encoding: str = 'utf-8',
        expected_revision: Revision | None = None,
    ) -> Revision:
        """Writing is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage is read-only.
        """
        raise NotImplementedError

    async def copy_within(self, src: str, dst: str) -> Revision:
        """Copying is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage is read-only.
        """
        raise NotImplementedError

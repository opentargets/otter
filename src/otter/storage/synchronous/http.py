"""HTTP Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from datetime import datetime

import requests

from otter.storage.model import Revision, StatResult
from otter.storage.synchronous.model import Storage

REQUEST_TIMEOUT = 60


class HTTPStorage(Storage):
    """HTTP Storage class.

    This class implements the Storage interface for HTTP resources.
    Uses requests.Session for HTTP operations with connection pooling.
    """

    def __init__(self) -> None:
        self._session: requests.Session | None = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    @property
    def name(self) -> str:
        return 'HTTP Storage'

    def stat(self, location: str) -> StatResult:
        session = self._get_session()
        resp = session.head(
            location,
            headers={'Accept-Encoding': 'identity'},  # prevent compression to get real size
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
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

    def open(
        self,
        location: str,
        mode: str = 'r',
    ):
        """Open is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage does not support
            opening file-like objects.
        """
        raise NotImplementedError

    def glob(self, location: str, pattern: str) -> list[str]:
        """Glob is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage does not support
            globbing.
        """
        raise NotImplementedError

    def read(
        self,
        location: str,
    ) -> tuple[bytes, Revision]:
        try:
            resp = self._get_session().get(
                location,
                allow_redirects=True,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.content, resp.headers.get('Last-Modified', None)
        except requests.exceptions.Timeout:
            raise TimeoutError(f'timeout while reading {location}')

    def read_text(
        self,
        location: str,
        encoding: str = 'utf-8',
    ) -> tuple[str, Revision]:
        data, revision = self.read(location)
        return data.decode(encoding), revision

    def write(
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

    def write_text(
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

    def copy_within(self, src: str, dst: str) -> Revision:
        """Copying is not supported for HTTP storage.

        :raises NotImplementedError: Always, since HTTP storage is read-only.
        """
        raise NotImplementedError

"""HTTP Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import IO, Any

import requests
from loguru import logger

from otter.storage.model import Revision, StatResult
from otter.storage.synchronous.model import Storage

REQUEST_TIMEOUT = 300

TOO_MANY_REQUESTS = 429
MAX_RATE_LIMIT_RETRIES = 10
"""How many times a request rate-limited with HTTP 429 is retried before giving up."""
DEFAULT_RATE_LIMIT_WAIT = 60
"""Seconds to wait after a 429 without a usable ``Retry-After`` header."""
MAX_RATE_LIMIT_WAIT = 900
"""Upper bound, in seconds, for a single wait after a 429."""


def _retry_after_seconds(resp: requests.Response) -> float:
    """Seconds to wait before retrying a rate-limited response.

    Reads ``Retry-After`` as either delay-seconds or an HTTP-date, falling back
    to :data:`DEFAULT_RATE_LIMIT_WAIT`, and caps it at :data:`MAX_RATE_LIMIT_WAIT`.
    """
    value = resp.headers.get('Retry-After')
    wait: float = DEFAULT_RATE_LIMIT_WAIT
    if value:
        try:
            wait = float(value)
        except ValueError:
            try:
                wait = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError):
                wait = DEFAULT_RATE_LIMIT_WAIT
    return min(max(wait, 0.0), MAX_RATE_LIMIT_WAIT)


def _send_with_rate_limit_retry(send: Callable[[], requests.Response], location: str) -> requests.Response:
    """Send a request, waiting and retrying while the server answers HTTP 429.

    The wait honours ``Retry-After``. After :data:`MAX_RATE_LIMIT_RETRIES`
    retries the last response is returned, so the caller's error handling
    applies to it unchanged.
    """
    for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
        resp = send()
        if resp.status_code != TOO_MANY_REQUESTS:
            return resp
        wait = _retry_after_seconds(resp)
        logger.warning(
            f'rate limited (HTTP 429) on {location}, retry {attempt}/{MAX_RATE_LIMIT_RETRIES} in {wait:.0f}s'
        )
        resp.close()
        time.sleep(wait)
    return send()


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
        resp = _send_with_rate_limit_retry(
            lambda: session.head(
                location,
                headers={'Accept-Encoding': 'identity'},  # prevent compression to get real size
                allow_redirects=True,
                timeout=REQUEST_TIMEOUT,
            ),
            location,
        )
        resp.raise_for_status()

        if 'Content-Length' not in resp.headers:
            size = None
        else:
            content_length = resp.headers.get('Content-Length')
            if content_length is None:
                size = None
            else:
                size = int(content_length)

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
    ) -> IO[Any]:
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
        session = self._get_session()
        try:
            resp = _send_with_rate_limit_retry(
                lambda: session.get(
                    location,
                    allow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                ),
                location,
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

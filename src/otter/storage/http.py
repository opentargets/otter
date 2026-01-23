"""HTTP Storage class."""
# ruff: noqa: D102 # docstring inheritance

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import requests

from otter.storage.google import GoogleStorage
from otter.storage.model import Revision, StatResult, Storage

if TYPE_CHECKING:
    from typing import IO

REQUEST_TIMEOUT = 10  # seconds


class HTTPStorage(Storage):
    """HTTP Storage class.

    This class implements the Storage interface for HTTP resources.
    """

    def __init__(self) -> None:
        self.session = None

    def _get_session_for_url(self, src: str) -> requests.Session:
        if self.session is not None:
            return self.session
        if src.startswith('https://docs.google.com/spreadsheets/'):  # special case for google sheets
            google_storage = GoogleStorage()
            session = google_storage.get_session()
        else:
            session = requests.Session()
        self.session = session
        return session

    @property
    def name(self) -> str:
        return 'HTTP Storage'

    def stat(self, location: str) -> StatResult:
        s = self._get_session_for_url(location)
        resp = s.request(
            'HEAD',
            location,
            timeout=REQUEST_TIMEOUT,
            headers={'Accept-Encoding': 'identity'},  # prevent compression to get real size
        )
        resp.raise_for_status()

        if 'Content-Length' not in resp.headers:
            size = None
        else:
            size = int(resp.headers.get('Content-Length'))

        last_modified = resp.headers.get('Last-Modified', None)
        if last_modified is not None:
            mtime = datetime.strptime(last_modified, '%a, %d %b %Y %I:%M:%S %Z').timestamp()
        else:
            mtime = None

        return StatResult(
            is_dir=False,
            is_reg=True,
            size=size,
            revision=resp.headers.get('Last-Modified', None),
            mtime=mtime,
        )

    def open(self, location: str, mode: str = 'r', revision: Revision = None) -> IO:
        if 'r' not in mode:
            raise NotImplementedError('http storage only supports read mode')

        s = self._get_session_for_url(location)
        resp = s.request('GET', location, stream=True, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.raw

    def glob(self, location: str, pattern: str) -> list[str]:
        raise NotImplementedError

    def download_to_file(self, src: str, dst: Path) -> int:
        s = self._get_session_for_url(src)
        resp = s.request('GET', src, stream=True, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        with open(dst, 'wb') as f:
            total_bytes = 0
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                total_bytes += len(chunk)
        return total_bytes

    def download_to_string(self, src: str) -> tuple[str, int]:
        s = self._get_session_for_url(src)
        resp = s.request('GET', src, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        content = resp.text
        return content, len(content)

    def upload(self, src: Path, dst: str, revision: Revision = None) -> int:
        raise NotImplementedError('HTTP storage does not support upload')

    def copy_within(self, src: str, dst: str) -> int:
        raise NotImplementedError('HTTP storage does not support copy within')

"""Tests for the StorageHandle class."""

from pathlib import Path

import pytest

from otter.storage.synchronous.filesystem import FilesystemStorage
from otter.storage.synchronous.google import GoogleStorage
from otter.storage.synchronous.handle import StorageHandle
from otter.storage.synchronous.http import HTTPStorage
from otter.storage.synchronous.noop import NoopStorage
from otter.util.errors import NotFoundError
from test.mocks import fake_config


class TestStorageHandleResolution:
    @pytest.mark.parametrize(
        'url',
        [
            'gs://bucket/path/file.txt',
            'https://example.com/file.txt',
            'http://example.com/file.txt',
        ],
    )
    def test_absolute_url_used_as_is(
        self,
        url: str,
    ) -> None:
        handle = StorageHandle(url)

        assert handle.absolute == url
        assert handle.is_absolute is True

    def test_relative_with_release_uri_resolves_to_remote(self) -> None:
        config = fake_config(release_uri='gs://bucket/release/path')
        handle = StorageHandle('data/file.txt', config)

        assert handle.absolute == 'gs://bucket/release/path/data/file.txt'
        assert handle.is_absolute is False

    def test_relative_without_release_uri_resolves_to_work_path(self) -> None:
        config = fake_config(work_path='/tmp/work', release_uri=None)
        handle = StorageHandle('data/file.txt', config)

        assert handle.absolute == '/tmp/work/data/file.txt'
        assert handle.is_absolute is False

    def test_absolute_local_path_raises(self) -> None:
        with pytest.raises(ValueError, match='absolute local paths are not allowed'):
            StorageHandle('/absolute/path/file.txt')

    def test_relative_without_config_raises(self) -> None:
        with pytest.raises(ValueError, match='config must be provided'):
            StorageHandle('relative/path/file.txt')


class TestStorageHandleStorageSelection:
    @pytest.mark.parametrize(
        ('url', 'expected_storage'),
        [
            ('gs://bucket/path/file.txt', GoogleStorage),
            ('http://example.com/file.txt', HTTPStorage),
            ('https://example.com/file.txt', HTTPStorage),
            ('ftp://example.com/file.txt', NoopStorage),
        ],
    )
    def test_protocol_selects_correct_storage(
        self,
        url: str,
        expected_storage: type,
    ) -> None:
        handle = StorageHandle(url)

        assert isinstance(handle.storage, expected_storage)

    def test_no_protocol_selects_filesystem_storage(self) -> None:
        config = fake_config(work_path='/tmp/work', release_uri=None)
        handle = StorageHandle('data/file.txt', config)

        assert isinstance(handle.storage, FilesystemStorage)


class TestStorageHandleCopyTo:
    def test_copy_to_same_backend(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_dir = work_path / 'src'
        src_dir.mkdir()
        src_file = src_dir / 'file.txt'
        src_file.write_text('source content')
        dst_dir = work_path / 'dst'
        dst_dir.mkdir()
        src_handle = StorageHandle('src/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        revision = src_handle.copy_to(dst_handle)

        dst_file = dst_dir / 'file.txt'
        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert revision is not None

    def test_copy_to_creates_hard_link(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_dir = work_path / 'src'
        src_dir.mkdir()
        src_file = src_dir / 'file.txt'
        src_file.write_text('source content')
        dst_dir = work_path / 'dst'
        dst_dir.mkdir()
        src_handle = StorageHandle('src/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        src_handle.copy_to(dst_handle)

        dst_file = dst_dir / 'file.txt'
        assert src_file.stat().st_ino == dst_file.stat().st_ino

    def test_copy_to_nonexistent_source_raises(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_handle = StorageHandle('nonexistent/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        with pytest.raises(NotFoundError, match='not found'):
            src_handle.copy_to(dst_handle)

    def test_copy_to_directory_raises(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_dir = work_path / 'srcdir'
        src_dir.mkdir()
        src_handle = StorageHandle('srcdir', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        with pytest.raises(ValueError, match='only copy regular files'):
            src_handle.copy_to(dst_handle)


class TestStorageHandleDownload:
    def test_read(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_file = work_path / 'source.txt'
        src_file.write_text('file content')
        handle = StorageHandle('source.txt', config)

        content, revision = handle.read()

        assert content == b'file content'
        assert revision is not None

    def test_read_text(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        src_file = work_path / 'source.txt'
        src_file.write_text('string content')
        handle = StorageHandle('source.txt', config)

        content, revision = handle.read_text()

        assert content == 'string content'
        assert revision is not None

    def test_stat_file(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        test_file = work_path / 'test.txt'
        test_file.write_text('hello')
        handle = StorageHandle('test.txt', config)

        result = handle.stat()

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 5

    def test_stat_directory(
        self,
        work_path: Path,
    ) -> None:
        config = fake_config(work_path=work_path, release_uri=None)
        test_dir = work_path / 'subdir'
        test_dir.mkdir()
        handle = StorageHandle('subdir', config)

        result = handle.stat()

        assert result.is_dir is True
        assert result.is_reg is False

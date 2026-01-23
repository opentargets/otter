"""Unit tests for StorageHandle."""

from pathlib import Path

import pytest

from otter.storage.filesystem import FilesystemStorage
from otter.storage.google import GoogleStorage
from otter.storage.handle import StorageHandle
from otter.storage.http import HTTPStorage
from otter.storage.noop import NoopStorage
from otter.util.errors import NotFoundError
from test.mocks.config import mock_config


class TestStorageHandleResolution:
    """Test StorageHandle location resolution logic."""

    @pytest.mark.parametrize(
        'url',
        [
            'gs://bucket/path/file.txt',
            'https://example.com/file.txt',
            'http://example.com/file.txt',
        ],
    )
    def test_absolute_url_used_as_is(self, url: str) -> None:
        """Test that absolute URLs are used without modification."""
        handle = StorageHandle(url)

        assert handle.absolute == url
        assert handle.is_absolute is True

    def test_relative_with_release_uri_resolves_to_remote(self) -> None:
        """Test that relative paths resolve to release_uri when present."""
        config = mock_config(release_uri='gs://bucket/release/path')
        handle = StorageHandle('data/file.txt', config)

        assert handle.absolute == 'gs://bucket/release/path/data/file.txt'
        assert handle.is_absolute is False

    def test_relative_without_release_uri_resolves_to_work_path(self) -> None:
        """Test that relative paths resolve to work_path when no release_uri."""
        config = mock_config(work_path='/tmp/work', release_uri=None)
        handle = StorageHandle('data/file.txt', config)

        assert handle.absolute == '/tmp/work/data/file.txt'
        assert handle.is_absolute is False

    def test_absolute_local_path_raises(self) -> None:
        """Test that absolute local paths are disallowed."""
        with pytest.raises(ValueError, match='absolute local paths are not allowed'):
            StorageHandle('/absolute/path/file.txt')

    def test_relative_without_config_raises(self) -> None:
        """Test that relative paths without config raise an error."""
        with pytest.raises(ValueError, match='config must be provided'):
            StorageHandle('relative/path/file.txt')


class TestStorageHandleStorageSelection:
    """Test StorageHandle storage backend selection."""

    @pytest.mark.parametrize(
        ('url', 'expected_storage'),
        [
            ('gs://bucket/path/file.txt', GoogleStorage),
            ('http://example.com/file.txt', HTTPStorage),
            ('https://example.com/file.txt', HTTPStorage),
            ('ftp://example.com/file.txt', NoopStorage),
        ],
    )
    def test_protocol_selects_correct_storage(self, url: str, expected_storage: type) -> None:
        """Test that URLs select the correct storage backend."""
        handle = StorageHandle(url)

        assert isinstance(handle.storage, expected_storage)

    def test_no_protocol_selects_filesystem_storage(self) -> None:
        """Test that paths without protocol select FilesystemStorage."""
        config = mock_config(work_path='/tmp/work', release_uri=None)
        handle = StorageHandle('data/file.txt', config)

        assert isinstance(handle.storage, FilesystemStorage)


class TestStorageHandleCopyTo:
    """Test StorageHandle.copy_to() with real FilesystemStorage."""

    @pytest.fixture
    def work_path(self, tmp_path: Path) -> Path:
        """Create a work path for testing."""
        work = tmp_path / 'work'
        work.mkdir()
        return work

    def test_copy_to_same_backend(self, work_path: Path) -> None:
        """Test copy_to between two filesystem locations."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create source file
        src_dir = work_path / 'src'
        src_dir.mkdir()
        src_file = src_dir / 'file.txt'
        src_file.write_text('source content')

        # create destination directory
        dst_dir = work_path / 'dst'
        dst_dir.mkdir()

        src_handle = StorageHandle('src/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        revision = src_handle.copy_to(dst_handle)

        dst_file = dst_dir / 'file.txt'
        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert isinstance(revision, (int, float))

    def test_copy_to_creates_hard_link_when_possible(self, work_path: Path) -> None:
        """Test that copy_to creates a hard link for filesystem copies."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create source file
        src_dir = work_path / 'src'
        src_dir.mkdir()
        src_file = src_dir / 'file.txt'
        src_file.write_text('source content')

        # create destination directory
        dst_dir = work_path / 'dst'
        dst_dir.mkdir()

        src_handle = StorageHandle('src/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        src_handle.copy_to(dst_handle)

        dst_file = dst_dir / 'file.txt'
        # same inode
        assert src_file.stat().st_ino == dst_file.stat().st_ino

    def test_copy_to_nonexistent_source_raises(self, work_path: Path) -> None:
        """Test that copy_to raises NotFoundError for missing source."""
        config = mock_config(work_path=work_path, release_uri=None)

        src_handle = StorageHandle('nonexistent/file.txt', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        with pytest.raises(NotFoundError, match='does not exist'):
            src_handle.copy_to(dst_handle)

    def test_copy_to_directory_raises(self, work_path: Path) -> None:
        """Test that copy_to raises for directories."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create a directory as source
        src_dir = work_path / 'srcdir'
        src_dir.mkdir()

        src_handle = StorageHandle('srcdir', config)
        dst_handle = StorageHandle('dst/file.txt', config)

        with pytest.raises(ValueError, match='only regular files'):
            src_handle.copy_to(dst_handle)


class TestStorageHandleDownload:
    """Test StorageHandle download methods with real FilesystemStorage."""

    @pytest.fixture
    def work_path(self, tmp_path: Path) -> Path:
        """Create a work path for testing."""
        work = tmp_path / 'work'
        work.mkdir()
        return work

    def test_download_to_file(self, work_path: Path, tmp_path: Path) -> None:
        """Test download_to_file with filesystem storage."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create source file
        src_file = work_path / 'source.txt'
        src_file.write_text('file content')

        handle = StorageHandle('source.txt', config)
        dst_file = tmp_path / 'downloaded.txt'

        handle.download_to_file(dst_file)

        assert dst_file.exists()
        assert dst_file.read_text() == 'file content'

    def test_download_to_string(self, work_path: Path) -> None:
        """Test download_to_string with filesystem storage."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create source file
        src_file = work_path / 'source.txt'
        src_file.write_text('string content')

        handle = StorageHandle('source.txt', config)

        content, revision = handle.download_to_string()

        assert content == 'string content'
        assert isinstance(revision, int)

    def test_download_directory_raises(self, work_path: Path, tmp_path: Path) -> None:
        """Test that downloading a directory raises ValueError."""
        config = mock_config(work_path=work_path, release_uri=None)

        # create a directory
        src_dir = work_path / 'subdir'
        src_dir.mkdir()

        handle = StorageHandle('subdir', config)

        with pytest.raises(ValueError, match='is not a regular file'):
            handle.download_to_file(tmp_path / 'dest.txt')


class TestStorageHandleStat:
    """Test StorageHandle.stat() with real FilesystemStorage."""

    @pytest.fixture
    def work_path(self, tmp_path: Path) -> Path:
        """Create a work path for testing."""
        work = tmp_path / 'work'
        work.mkdir()
        return work

    def test_stat_file(self, work_path: Path) -> None:
        """Test stat on a regular file."""
        config = mock_config(work_path=work_path, release_uri=None)

        test_file = work_path / 'test.txt'
        test_file.write_text('hello')

        handle = StorageHandle('test.txt', config)
        result = handle.stat()

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 5

    def test_stat_directory(self, work_path: Path) -> None:
        """Test stat on a directory."""
        config = mock_config(work_path=work_path, release_uri=None)

        test_dir = work_path / 'subdir'
        test_dir.mkdir()

        handle = StorageHandle('subdir', config)
        result = handle.stat()

        assert result.is_dir is True
        assert result.is_reg is False

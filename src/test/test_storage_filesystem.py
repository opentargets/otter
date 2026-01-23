from pathlib import Path

import pytest

from otter.storage.filesystem import FilesystemStorage


class TestFilesystemStorage:
    """Test the FilesystemStorage class."""

    @pytest.fixture
    def storage(self) -> FilesystemStorage:
        """Create a FilesystemStorage instance."""
        return FilesystemStorage()

    def test_name(self, storage: FilesystemStorage) -> None:
        """Test that the name property returns the expected value."""
        assert storage.name == 'Filesystem Storage'

    def test_stat_file(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test stat on a regular file."""
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        result = storage.stat(str(test_file))

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 11
        assert result.revision is not None

    def test_stat_directory(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test stat on a directory."""
        test_dir = tmp_path / 'subdir'
        test_dir.mkdir()

        result = storage.stat(str(test_dir))

        assert result.is_dir is True
        assert result.is_reg is False
        assert result.revision is not None

    def test_stat_nonexistent_raises(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test that stat raises FileNotFoundError for nonexistent paths."""
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(FileNotFoundError):
            storage.stat(str(nonexistent))

    def test_open_read(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test opening a file for reading."""
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        with storage.open(str(test_file), 'r') as f:
            content = f.read()

        assert content == 'hello world'

    def test_open_write(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test opening a file for writing."""
        test_file = tmp_path / 'test.txt'

        with storage.open(str(test_file), 'w') as f:
            f.write('new content')

        assert test_file.read_text() == 'new content'

    def test_open_binary(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test opening a file in binary mode."""
        test_file = tmp_path / 'test.bin'
        test_file.write_bytes(b'\x00\x01\x02')

        with storage.open(str(test_file), 'rb') as f:
            content = f.read()

        assert content == b'\x00\x01\x02'

    def test_open_nonexistent_raises(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test that opening a nonexistent file for reading raises."""
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(FileNotFoundError):
            storage.open(str(nonexistent), 'r')

    def test_glob_wildcard(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test glob with wildcard pattern."""
        (tmp_path / 'file1.txt').write_text('a')
        (tmp_path / 'file2.txt').write_text('b')
        (tmp_path / 'file3.json').write_text('{}')

        result = storage.glob(str(tmp_path), '*.txt')

        assert len(result) == 2
        assert any('file1.txt' in r for r in result)
        assert any('file2.txt' in r for r in result)

    def test_glob_recursive(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test glob with recursive pattern."""
        (tmp_path / 'file1.txt').write_text('a')
        subdir = tmp_path / 'subdir'
        subdir.mkdir()
        (subdir / 'file2.txt').write_text('b')

        result = storage.glob(str(tmp_path), '**/*.txt')

        assert len(result) == 2

    def test_glob_no_match(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test glob returns empty list when no matches."""
        result = storage.glob(str(tmp_path), '*.nonexistent')

        assert result == []

    def test_download_to_file(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test download_to_file copies content correctly."""
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        revision = storage.download_to_file(str(src_file), dst_file)

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert isinstance(revision, int)

    def test_download_to_string(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test download_to_string returns content and revision."""
        test_file = tmp_path / 'test.txt'
        test_file.write_text('file content')

        content, revision = storage.download_to_string(str(test_file))

        assert content == 'file content'
        assert isinstance(revision, int)

    def test_download_to_string_nonexistent_raises(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test download_to_string raises for nonexistent file."""
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(FileNotFoundError):
            storage.download_to_string(str(nonexistent))

    def test_upload_not_implemented(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test that upload raises NotImplementedError."""
        src_file = tmp_path / 'source.txt'
        src_file.write_text('content')

        with pytest.raises(NotImplementedError):
            storage.upload(src_file, str(tmp_path / 'dest.txt'))

    def test_copy_within(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test copy_within copies file correctly."""
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        revision = storage.copy_within(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert isinstance(revision, int)

    def test_copy_within_prefers_hardlink(self, storage: FilesystemStorage, tmp_path: Path) -> None:
        """Test that copy_within creates a hard link when possible."""
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        storage.copy_within(str(src_file), str(dst_file))

        # Check that they share the same inode (hard link)
        assert src_file.stat().st_ino == dst_file.stat().st_ino

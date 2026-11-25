"""Tests for the FilesystemStorage class."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from filelock import Timeout

from otter.storage.synchronous.filesystem import FilesystemStorage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError


class TestFilesystemStorage:
    @pytest.fixture
    def storage(self) -> FilesystemStorage:
        return FilesystemStorage()

    def test_stat_file(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        result = storage.stat(str(test_file))

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 11
        assert result.revision is not None

    def test_stat_directory(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_dir = tmp_path / 'subdir'
        test_dir.mkdir()

        result = storage.stat(str(test_dir))

        assert result.is_dir is True
        assert result.is_reg is False
        assert result.revision is not None

    def test_stat_nonexistent_raises(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(NotFoundError):
            storage.stat(str(nonexistent))

    def test_read_text(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        content, revision = storage.read_text(str(test_file))

        assert content == 'hello world'
        assert revision is not None

    def test_write_text(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'

        revision = storage.write_text(str(test_file), 'new content')

        assert test_file.read_text() == 'new content'
        assert revision is not None

    def test_read_binary(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_file.write_bytes(b'\x00\x01\x02')

        content, revision = storage.read(str(test_file))

        assert content == b'\x00\x01\x02'
        assert revision is not None

    def test_read_retries(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        os.utime(test_file, (0, 0))  # ensure write changes mtime
        attempt_count = 0
        original_stat = storage.stat

        def mock_stat(location: str):
            nonlocal attempt_count
            result = original_stat(location)
            # second stat will touch so mtime changes
            attempt_count += 1
            if attempt_count == 1:
                test_file.touch()
            return result

        with patch.object(storage, 'stat', side_effect=mock_stat):
            content, _ = storage.read_text(str(test_file))

        assert attempt_count >= 3
        assert content == 'initial content'

    def test_read_nonexistent_raises(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(NotFoundError):
            storage.read_text(str(nonexistent))

    def test_read_text_with_invalid_encoding(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'invalid.txt'
        test_file.write_bytes(b'\x80\x81\x82\x83')

        with pytest.raises(StorageError, match='error decoding'):
            storage.read_text(str(test_file))

    def test_glob_wildcard(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        (tmp_path / 'file1.txt').write_text('a')
        (tmp_path / 'file2.txt').write_text('b')
        (tmp_path / 'file3.json').write_text('{}')

        result = storage.glob(str(tmp_path), '*.txt')

        assert len(result) == 2
        assert any('file1.txt' in r for r in result)
        assert any('file2.txt' in r for r in result)

    def test_glob_recursive(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        (tmp_path / 'file1.txt').write_text('a')
        subdir = tmp_path / 'subdir'
        subdir.mkdir()
        (subdir / 'file2.txt').write_text('b')

        result = storage.glob(str(tmp_path), '**/*.txt')

        assert len(result) == 2

    def test_glob_no_match(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        result = storage.glob(str(tmp_path), '*.nonexistent')

        assert result == []

    def test_write(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_data = b'binary content'

        revision = storage.write(str(test_file), test_data)

        assert test_file.exists()
        assert test_file.read_bytes() == test_data
        assert revision is not None

    def test_read(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_data = b'binary content'
        test_file.write_bytes(test_data)

        content, revision = storage.read(str(test_file))

        assert content == test_data
        assert revision is not None

    def test_write_with_expected_revision_succeeds(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        os.utime(test_file, (0, 0))  # ensure write changes mtime
        stat = storage.stat(str(test_file))
        current_revision = stat.revision

        new_revision = storage.write(
            str(test_file),
            b'updated content',
            expected_revision=current_revision,
        )

        assert test_file.read_bytes() == b'updated content'
        assert new_revision != current_revision

    def test_write_with_wrong_expected_revision_fails(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        with pytest.raises(PreconditionFailedError, match='revision mismatch'):
            storage.write(
                str(test_file),
                b'updated content',
                expected_revision=999.0,
            )

        assert test_file.read_text() == 'initial content'

    def test_write_with_expected_revision_on_nonexistent_file_fails(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'nonexistent.txt'

        with pytest.raises(NotFoundError):
            storage.write(
                str(test_file),
                b'content',
                expected_revision=123.0,
            )

    def test_write_with_lock_timeout(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')

        with patch('otter.storage.synchronous.filesystem.FileLock') as mock_lock_class:
            mock_lock = mock_lock_class.return_value
            mock_lock.__enter__.side_effect = Timeout(str(test_file))

            stat = storage.stat(str(test_file))
            with pytest.raises(Timeout):
                storage.write(
                    str(test_file),
                    b'new content',
                    expected_revision=stat.revision,
                )

    def test_write_cleans_up_lock_on_error(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        mock_lock = MagicMock()
        mock_lock.is_locked = True

        with patch('otter.storage.synchronous.filesystem.FileLock', return_value=mock_lock):
            with patch('pathlib.Path.write_bytes', side_effect=OSError):
                stat = storage.stat(str(test_file))
                with pytest.raises(OSError):
                    storage.write(
                        str(test_file),
                        b'new content',
                        expected_revision=stat.revision,
                    )

        mock_lock.release.assert_called_once()

    def test_write_creates_parent_directories(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'deep' / 'nested' / 'path' / 'file.txt'

        revision = storage.write(str(test_file), b'content')

        assert test_file.exists()
        assert test_file.read_bytes() == b'content'
        assert revision is not None

    def test_copy_within_source_not_found(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'nonexistent.txt'
        dst_file = tmp_path / 'dest.txt'

        with pytest.raises(NotFoundError):
            storage.copy_within(str(src_file), str(dst_file))

    def test_copy_within_source_is_directory_raises(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_dir = tmp_path / 'srcdir'
        src_dir.mkdir()
        dst_file = tmp_path / 'dest.txt'

        with pytest.raises(ValueError, match='only copy regular files'):
            storage.copy_within(str(src_dir), str(dst_file))

    def test_copy_within_falls_back_to_copy_when_hardlink_fails(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        with patch(
            'pathlib.Path.hardlink_to',
            side_effect=OSError,
        ):
            revision = storage.copy_within(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert src_file.stat().st_ino != dst_file.stat().st_ino
        assert revision is not None

    def test_copy_within(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        revision = storage.copy_within(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert revision is not None

    def test_copy_within_prefers_hardlink(
        self,
        storage: FilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        storage.copy_within(str(src_file), str(dst_file))

        assert src_file.stat().st_ino == dst_file.stat().st_ino  # test same inode

"""Tests for the AsyncFilesystemStorage class."""

import os
import shutil
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from filelock import Timeout

from otter.storage.asynchronous.filesystem import AsyncFilesystemStorage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError


class TestFilesystemStorage:
    @pytest.fixture
    def storage(self) -> AsyncFilesystemStorage:
        return AsyncFilesystemStorage()

    @pytest.mark.asyncio
    async def test_stat_file(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        result = await storage.stat(str(test_file))

        assert result.is_reg is True
        assert result.is_dir is False
        assert result.size == 11
        assert result.revision is not None

    @pytest.mark.asyncio
    async def test_stat_directory(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_dir = tmp_path / 'subdir'
        test_dir.mkdir()

        result = await storage.stat(str(test_dir))

        assert result.is_dir is True
        assert result.is_reg is False
        assert result.revision is not None

    @pytest.mark.asyncio
    async def test_stat_nonexistent_raises(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(NotFoundError):
            await storage.stat(str(nonexistent))

    @pytest.mark.asyncio
    async def test_read_text(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        content, revision = await storage.read_text(str(test_file))

        assert content == 'hello world'
        assert revision is not None

    @pytest.mark.asyncio
    async def test_write_text(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'

        revision = await storage.write_text(str(test_file), 'new content')

        assert test_file.read_text() == 'new content'
        assert revision is not None

    @pytest.mark.asyncio
    async def test_read_binary(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_file.write_bytes(b'\x00\x01\x02')

        content, revision = await storage.read(str(test_file))

        assert content == b'\x00\x01\x02'
        assert revision is not None

    @pytest.mark.asyncio
    async def test_read_retries(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        os.utime(test_file, (0, 0))  # ensure write changes mtime
        attempt_count = 0
        original_stat = storage.stat

        async def mock_stat(location: str):
            nonlocal attempt_count
            result = await original_stat(location)
            # second stat will touch so mtime changes
            attempt_count += 1
            if attempt_count == 1:
                test_file.touch()
            return result

        with patch.object(storage, 'stat', side_effect=mock_stat):
            content, _ = await storage.read_text(str(test_file))

        assert attempt_count >= 3
        assert content == 'initial content'

    @pytest.mark.asyncio
    async def test_read_nonexistent_raises(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / 'does_not_exist.txt'

        with pytest.raises(NotFoundError):
            await storage.read_text(str(nonexistent))

    @pytest.mark.asyncio
    async def test_read_text_with_invalid_encoding(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'invalid.txt'
        test_file.write_bytes(b'\x80\x81\x82\x83')

        with pytest.raises(StorageError, match='error decoding'):
            await storage.read_text(str(test_file))

    @pytest.mark.asyncio
    async def test_glob_wildcard(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        (tmp_path / 'file1.txt').write_text('a')
        (tmp_path / 'file2.txt').write_text('b')
        (tmp_path / 'file3.json').write_text('{}')

        result = await storage.glob(str(tmp_path), '*.txt')

        assert len(result) == 2
        assert any('file1.txt' in r for r in result)
        assert any('file2.txt' in r for r in result)

    @pytest.mark.asyncio
    async def test_glob_recursive(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        (tmp_path / 'file1.txt').write_text('a')
        subdir = tmp_path / 'subdir'
        subdir.mkdir()
        (subdir / 'file2.txt').write_text('b')

        result = await storage.glob(str(tmp_path), '**/*.txt')

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_glob_no_match(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        result = await storage.glob(str(tmp_path), '*.nonexistent')

        assert result == []

    @pytest.mark.asyncio
    async def test_write(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_data = b'binary content'

        revision = await storage.write(str(test_file), test_data)

        assert test_file.exists()
        assert test_file.read_bytes() == test_data
        assert revision is not None

    @pytest.mark.asyncio
    async def test_read(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.bin'
        test_data = b'binary content'
        test_file.write_bytes(test_data)

        content, revision = await storage.read(str(test_file))

        assert content == test_data
        assert revision is not None

    @pytest.mark.asyncio
    async def test_write_with_expected_revision_succeeds(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        os.utime(test_file, (0, 0))  # ensure write changes mtime
        stat = await storage.stat(str(test_file))
        current_revision = stat.revision

        new_revision = await storage.write(
            str(test_file),
            b'updated content',
            expected_revision=current_revision,
        )

        assert test_file.read_bytes() == b'updated content'
        assert new_revision != current_revision

    @pytest.mark.asyncio
    async def test_write_with_wrong_expected_revision_fails(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        with pytest.raises(PreconditionFailedError, match='revision mismatch'):
            await storage.write(
                str(test_file),
                b'updated content',
                expected_revision=999.0,
            )

        assert test_file.read_text() == 'initial content'

    @pytest.mark.asyncio
    async def test_write_with_expected_revision_on_nonexistent_file_fails(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'nonexistent.txt'

        with pytest.raises(NotFoundError):
            await storage.write(
                str(test_file),
                b'content',
                expected_revision=123.0,
            )

    @pytest.mark.asyncio
    async def test_write_with_lock_timeout(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')

        with patch('otter.storage.asynchronous.filesystem.FileLock') as mock_lock_class:
            mock_lock = mock_lock_class.return_value
            mock_lock.__enter__.side_effect = Timeout(str(test_file))

            stat = await storage.stat(str(test_file))
            with pytest.raises(Timeout):
                await storage.write(
                    str(test_file),
                    b'new content',
                    expected_revision=stat.revision,
                )

    @pytest.mark.asyncio
    async def test_write_cleans_up_lock_on_error(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'test.txt'
        test_file.write_text('initial content')
        mock_lock = MagicMock()
        mock_lock.is_locked = True

        with patch('otter.storage.asynchronous.filesystem.FileLock', return_value=mock_lock):
            with patch('aiofiles.open', side_effect=OSError):
                stat = await storage.stat(str(test_file))
                with pytest.raises(OSError):
                    await storage.write(
                        str(test_file),
                        b'new content',
                        expected_revision=stat.revision,
                    )

        mock_lock.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_creates_parent_directories(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'deep' / 'nested' / 'path' / 'file.txt'

        revision = await storage.write(str(test_file), b'content')

        assert test_file.exists()
        assert test_file.read_bytes() == b'content'
        assert revision is not None

    @pytest.mark.asyncio
    async def test_copy_within_source_not_found(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'nonexistent.txt'
        dst_file = tmp_path / 'dest.txt'

        with pytest.raises(NotFoundError):
            await storage.copy_within(str(src_file), str(dst_file))

    @pytest.mark.asyncio
    async def test_copy_within_source_is_directory_raises(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_dir = tmp_path / 'srcdir'
        src_dir.mkdir()
        dst_file = tmp_path / 'dest.txt'

        with pytest.raises(ValueError, match='only copy regular files'):
            await storage.copy_within(str(src_dir), str(dst_file))

    @pytest.mark.asyncio
    async def test_copy_within_falls_back_to_copy_when_hardlink_fails(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        with patch(
            'pathlib.Path.hardlink_to',
            side_effect=OSError,
        ):
            revision = await storage.copy_within(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert src_file.stat().st_ino != dst_file.stat().st_ino
        assert revision is not None

    @pytest.mark.asyncio
    async def test_copy_within(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        revision = await storage.copy_within(str(src_file), str(dst_file))

        assert dst_file.exists()
        assert dst_file.read_text() == 'source content'
        assert revision is not None

    @pytest.mark.asyncio
    async def test_copy_within_prefers_hardlink(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        src_file = tmp_path / 'source.txt'
        src_file.write_text('source content')
        dst_file = tmp_path / 'dest.txt'

        await storage.copy_within(str(src_file), str(dst_file))

        assert src_file.stat().st_ino == dst_file.stat().st_ino  # test same inode

    @pytest.mark.asyncio
    async def test_delete_file(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """Deleting a file removes the file."""
        test_file = tmp_path / 'test.txt'
        test_file.write_text('hello world')

        await storage.delete(str(test_file))

        assert not test_file.exists()

    @pytest.mark.asyncio
    async def test_delete_missing_location_is_noop(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """Deleting a file that is not there does nothing and does not raise."""
        nonexistent = tmp_path / 'does_not_exist.txt'

        await storage.delete(str(nonexistent))

    @pytest.mark.asyncio
    async def test_delete_directory_recursive(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """With recursive, a directory and everything under directory gets deleted."""
        test_dir = tmp_path / 'dataset'
        (test_dir / 'nested').mkdir(parents=True)
        (test_dir / '00000000.parquet').write_text('part one')
        (test_dir / 'nested' / '00000001.parquet').write_text('part two')

        await storage.delete(str(test_dir), recursive=True)

        assert not test_dir.exists()

    @pytest.mark.asyncio
    async def test_delete_directory_without_recursive_raises(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """Without recursive, a directory raises instead of being skipped."""
        test_dir = tmp_path / 'dataset'
        test_dir.mkdir()
        (test_dir / '00000000.parquet').write_text('part one')

        with pytest.raises(StorageError, match='recursive'):
            await storage.delete(str(test_dir))

        assert test_dir.exists()

    @pytest.mark.asyncio
    async def test_delete_directory_runs_in_worker_thread(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_dir = tmp_path / 'dataset'
        test_dir.mkdir()
        (test_dir / 'part.txt').write_text('content')
        event_loop_thread = threading.get_ident()
        original_rmtree = shutil.rmtree

        def checked_rmtree(path):
            assert threading.get_ident() != event_loop_thread
            original_rmtree(path)

        with patch('otter.storage.asynchronous.filesystem.shutil.rmtree', side_effect=checked_rmtree):
            await storage.delete(str(test_dir), recursive=True)

        assert not test_dir.exists()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('target_kind', ['file', 'directory', 'missing'])
    async def test_delete_symlink_preserves_target(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
        target_kind: str,
    ) -> None:
        target = tmp_path / 'target'
        if target_kind == 'file':
            target.write_text('content')
        elif target_kind == 'directory':
            target.mkdir()
            (target / 'part.txt').write_text('content')
        link = tmp_path / 'link'
        link.symlink_to(target)

        await storage.delete(str(link))

        assert not link.is_symlink()
        if target_kind == 'file':
            assert target.read_text() == 'content'
        elif target_kind == 'directory':
            assert (target / 'part.txt').read_text() == 'content'

    @pytest.mark.asyncio
    async def test_delete_file_error_raises_storage_error(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_file = tmp_path / 'file.txt'
        test_file.write_text('content')

        with patch('aiofiles.os.unlink', side_effect=PermissionError('access denied')) as unlink:
            with pytest.raises(StorageError, match=r'error deleting .*access denied'):
                await storage.delete(str(test_file))

        unlink.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_directory_error_raises_storage_error(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        test_dir = tmp_path / 'dataset'
        test_dir.mkdir()

        with patch('otter.storage.asynchronous.filesystem.shutil.rmtree', side_effect=PermissionError('access denied')):
            with pytest.raises(StorageError, match=r'error deleting directory .*access denied'):
                await storage.delete(str(test_dir), recursive=True)

    @pytest.mark.asyncio
    async def test_delete_directory_removed_by_someone_else_is_not_an_error(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """A directory that goes away between the stat and the rmtree is still gone."""
        test_dir = tmp_path / 'dataset'
        test_dir.mkdir()
        (test_dir / 'part.txt').write_text('content')

        original_rmtree = shutil.rmtree

        def rmtree_after_someone_else_won(path: Path) -> None:
            original_rmtree(path)
            raise FileNotFoundError(2, 'No such file or directory', str(path))

        with patch(
            'otter.storage.asynchronous.filesystem.shutil.rmtree',
            side_effect=rmtree_after_someone_else_won,
        ):
            await storage.delete(str(test_dir), recursive=True)

        assert not test_dir.exists()

    @pytest.mark.asyncio
    async def test_delete_directory_missing_child_raises_storage_error(
        self,
        storage: AsyncFilesystemStorage,
        tmp_path: Path,
    ) -> None:
        """A missing child must not hide an incomplete directory deletion."""
        test_dir = tmp_path / 'dataset'
        test_dir.mkdir()
        child = test_dir / 'part.txt'
        child.write_text('content')
        remaining = test_dir / 'remaining.txt'
        remaining.write_text('keep me')

        def rmtree_after_child_disappeared(path: Path) -> None:
            child.unlink()
            raise FileNotFoundError(2, 'No such file or directory', str(child))

        with patch(
            'otter.storage.asynchronous.filesystem.shutil.rmtree',
            side_effect=rmtree_after_child_disappeared,
        ):
            with pytest.raises(StorageError, match=r'error deleting directory .*part\.txt'):
                await storage.delete(str(test_dir), recursive=True)

        assert test_dir.is_dir()
        assert not child.exists()
        assert remaining.read_text() == 'keep me'

"""Tests for manifest updates racing with other processes.

Each test makes "another process" change the manifest at one exact point of an
update, by wrapping `StorageHandle.read_text`, and records the
`expected_revision` of every write. They run on the local filesystem backend,
but what they check is `Manifest.update` and the handle: the conditional
writes they rely on are the ones Google Cloud Storage enforces with
`if_generation_match`.
"""

import asyncio
import json
import os
from collections.abc import Callable
from pathlib import Path

import pytest

from otter.manifest.model import MANIFEST_FILENAME, Manifest, Result, StepManifest
from otter.storage.model import REVISION_ABSENT
from otter.storage.synchronous.handle import StorageHandle
from test.mocks import fake_config

STEPS = ['a', 'b']


def _config(work_path: Path, step: str):
    return fake_config(step=step, steps=STEPS, work_path=work_path, release_uri=None)


def _update(work_path: Path, step: str, result: Result = Result.SUCCESS) -> None:
    asyncio.run(Manifest(_config(work_path, step)).update(StepManifest(name=step, result=result)))


def _manifest(work_path: Path) -> Path:
    return work_path / MANIFEST_FILENAME


def _recorded(work_path: Path) -> dict:
    return json.loads(_manifest(work_path).read_text())['steps']


def _write_as_other_process(work_path: Path, step: str) -> None:
    """Record `step` the way another process would: a plain read-modify-write of the file."""
    path = _manifest(work_path)
    root = (
        json.loads(path.read_text())
        if path.exists()
        else json.loads(Manifest(_config(work_path, step))._create_empty().model_dump_json())
    )
    root['steps'][f'otter_{step}'] = json.loads(StepManifest(name=step, result=Result.SUCCESS).model_dump_json())
    before = path.stat().st_mtime_ns if path.exists() else None
    path.write_text(json.dumps(root))
    if before is not None:
        # the filesystem backend's revision is the mtime, which on Linux can stay the same for
        # two writes milliseconds apart (a backend limitation, not what these tests are about);
        # move it on, as the time between two real steps would
        os.utime(path, ns=(before + 10**9, before + 10**9))


@pytest.fixture
def writes(monkeypatch: pytest.MonkeyPatch) -> list:
    """The `expected_revision` of every manifest write, in order."""
    monkeypatch.setattr('otter.manifest.model.RETRY_BASE_DELAY', 0)
    revisions = []
    write_text = StorageHandle.write_text

    def spy(self, data, *args, expected_revision=None, **kwargs):
        revisions.append(expected_revision)
        return write_text(self, data, *args, expected_revision=expected_revision, **kwargs)

    monkeypatch.setattr(StorageHandle, 'write_text', spy)
    return revisions


@pytest.fixture
def after_first_read(monkeypatch: pytest.MonkeyPatch) -> Callable[[Callable[[], None]], list]:
    """Run an action once, right after the first manifest read (found or not); return the read attempts."""

    def install(action: Callable[[], None]) -> list:
        attempts = []
        read_text = StorageHandle.read_text

        def wrapped(self, *args, **kwargs):
            attempts.append(1)
            try:
                return read_text(self, *args, **kwargs)
            finally:
                if len(attempts) == 1:
                    action()

        monkeypatch.setattr(StorageHandle, 'read_text', wrapped)
        return attempts

    return install


class TestManifestUpdateRaces:
    def test_update_written_after_our_read_is_kept(self, tmp_path: Path, writes: list, after_first_read) -> None:
        """The lost update seen in production: `b` records itself between `a`'s read and `a`'s write."""
        _update(tmp_path, 'a', Result.PENDING)
        writes.clear()

        attempts = after_first_read(lambda: _write_as_other_process(tmp_path, 'b'))
        _update(tmp_path, 'a')

        assert len(attempts) == 2  # `a`'s first write was refused, so it read again
        assert len(writes) == 2
        recorded = _recorded(tmp_path)
        assert recorded['otter_a']['result'] == Result.SUCCESS
        assert recorded['otter_b']['result'] == Result.SUCCESS

    def test_manifest_deleted_after_our_read_is_created_again(
        self, tmp_path: Path, writes: list, after_first_read
    ) -> None:
        _update(tmp_path, 'a')
        read_revision = StorageHandle(MANIFEST_FILENAME, _config(tmp_path, 'a'), force_local=True).stat().revision
        writes.clear()

        attempts = after_first_read(_manifest(tmp_path).unlink)
        _update(tmp_path, 'b')

        assert len(attempts) == 2
        assert writes == [read_revision, REVISION_ABSENT]
        assert _recorded(tmp_path)['otter_b']['result'] == Result.SUCCESS

    def test_manifest_created_by_another_process_first_is_updated(
        self, tmp_path: Path, writes: list, after_first_read
    ) -> None:
        """Both find no manifest; `a` creates it first, so `b`'s create is refused and `b` updates it."""
        attempts = after_first_read(lambda: _write_as_other_process(tmp_path, 'a'))
        _update(tmp_path, 'b')

        assert len(attempts) == 2
        assert writes[0] == REVISION_ABSENT
        assert len(writes) == 2
        assert writes[1] not in {None, REVISION_ABSENT}
        recorded = _recorded(tmp_path)
        assert recorded['otter_a']['result'] == Result.SUCCESS
        assert recorded['otter_b']['result'] == Result.SUCCESS

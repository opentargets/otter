"""Tests for concurrent manifest updates."""

import asyncio
import json
import multiprocessing as mp
from pathlib import Path

import pytest

from otter.manifest.model import MANIFEST_FILENAME, Manifest, Result, StepManifest
from otter.storage.synchronous.handle import StorageHandle
from test.mocks import fake_config

N_PROCESSES = 20
TIMEOUT = 120


def _update(work_path: str, step: str, steps: list[str], barrier) -> None:
    """Record `step` in the manifest at `work_path`, starting together with the other processes."""
    config = fake_config(step=step, steps=steps, work_path=work_path, release_uri=None)
    barrier.wait(timeout=TIMEOUT)
    asyncio.run(Manifest(config).update(StepManifest(name=step, result=Result.SUCCESS)))


def _run_concurrently(work_path: Path, steps: list[str]) -> dict:
    ctx = mp.get_context('spawn')
    barrier = ctx.Barrier(len(steps))
    procs = [ctx.Process(target=_update, args=(str(work_path), step, steps, barrier)) for step in steps]
    try:
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=TIMEOUT)
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
                p.join()
    assert all(p.exitcode == 0 for p in procs), [p.exitcode for p in procs]
    return json.loads((work_path / MANIFEST_FILENAME).read_text())['steps']


class TestConcurrentManifestUpdates:
    """Steps finishing at the same moment must all be recorded (no lost read-modify-write updates)."""

    @pytest.fixture
    def steps(self) -> list[str]:
        return [f'step{i:02d}' for i in range(N_PROCESSES)]

    def test_updates_to_an_existing_manifest_are_all_recorded(self, tmp_path: Path, steps: list[str]) -> None:
        config = fake_config(step=steps[0], steps=steps, work_path=tmp_path, release_uri=None)
        asyncio.run(Manifest(config).update(StepManifest(name=steps[0])))  # create it, step pending

        recorded = _run_concurrently(tmp_path, steps)

        assert sorted(recorded) == [f'otter_{s}' for s in steps]
        assert all(s['result'] == Result.SUCCESS for s in recorded.values())

    def test_concurrent_creates_are_all_recorded(self, tmp_path: Path, steps: list[str]) -> None:
        recorded = _run_concurrently(tmp_path, steps)

        assert all(recorded[f'otter_{s}']['result'] == Result.SUCCESS for s in steps)

    def test_updates_leave_nothing_but_the_manifest(self, tmp_path: Path, steps: list[str]) -> None:
        _run_concurrently(tmp_path, steps)

        assert [p.name for p in tmp_path.iterdir()] == [MANIFEST_FILENAME]


class TestManifestRemovedDuringUpdate:
    """A manifest deleted between the read and the write is created again, not an error."""

    def test_manifest_deleted_after_read_is_recreated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config = fake_config(step='a', steps=['a', 'b'], work_path=tmp_path, release_uri=None)
        asyncio.run(Manifest(config).update(StepManifest(name='a', result=Result.SUCCESS)))
        monkeypatch.setattr('otter.manifest.model.RETRY_BASE_DELAY', 0)

        read_text = StorageHandle.read_text
        calls = []

        def read_then_delete(self, *args, **kwargs):
            result = read_text(self, *args, **kwargs)
            if not calls:
                (tmp_path / MANIFEST_FILENAME).unlink()
            calls.append(1)
            return result

        monkeypatch.setattr(StorageHandle, 'read_text', read_then_delete)
        config_b = fake_config(step='b', steps=['a', 'b'], work_path=tmp_path, release_uri=None)
        asyncio.run(Manifest(config_b).update(StepManifest(name='b', result=Result.SUCCESS)))

        recorded = json.loads((tmp_path / MANIFEST_FILENAME).read_text())['steps']
        assert recorded['otter_b']['result'] == Result.SUCCESS
        assert len(calls) == 1  # the second pass found no manifest and created it

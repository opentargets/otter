"""Tests for concurrent manifest updates."""

import asyncio
import json
import multiprocessing as mp
from pathlib import Path

import pytest

from otter.manifest.model import MANIFEST_FILENAME, Manifest, Result, StepManifest
from test.mocks import fake_config

N_PROCESSES = 20


def _update(work_path: str, step: str, steps: list[str], barrier) -> None:
    """Record `step` in the manifest at `work_path`, starting together with the other processes."""
    config = fake_config(step=step, steps=steps, work_path=work_path, release_uri=None)
    barrier.wait()
    asyncio.run(Manifest(config).update(StepManifest(name=step, result=Result.SUCCESS)))


def _run_concurrently(work_path: Path, steps: list[str]) -> dict:
    ctx = mp.get_context('spawn')
    barrier = ctx.Barrier(len(steps))
    procs = [ctx.Process(target=_update, args=(str(work_path), step, steps, barrier)) for step in steps]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
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

"""Stress test: many processes update one manifest on the local filesystem at the same moment.

On Google Cloud Storage the conditional writes are atomic; this exercises the
local filesystem backend, where a reader can still see a write half done
(``invalid manifest format``). See the xfail reason.
"""

import asyncio
import json
import multiprocessing as mp
import time
from pathlib import Path

import pytest

from otter.manifest.model import MANIFEST_FILENAME, Manifest, Result, StepManifest
from test.mocks import fake_config

N_PROCESSES = 20
TIMEOUT = 120

pytestmark = pytest.mark.xfail(
    reason='the filesystem backend writes in place without readers taking its lock, so a reader can '
    'see a partial manifest; how to fix that is the open question of this PR',
    strict=False,
)


def _update(work_path: str, step: str, steps: list[str], barrier) -> None:
    """Record `step` in the manifest at `work_path`, starting together with the other processes."""
    config = fake_config(step=step, steps=steps, work_path=work_path, release_uri=None)
    barrier.wait(timeout=TIMEOUT)
    asyncio.run(Manifest(config).update(StepManifest(name=step, result=Result.SUCCESS)))


def _run_concurrently(work_path: Path, steps: list[str]) -> dict:
    ctx = mp.get_context('spawn')
    barrier = ctx.Barrier(len(steps))
    procs = [ctx.Process(target=_update, args=(str(work_path), step, steps, barrier)) for step in steps]
    deadline = time.monotonic() + TIMEOUT
    try:
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=max(0, deadline - time.monotonic()))
    finally:
        for p in procs:
            if p.is_alive():
                p.kill()
                p.join(timeout=5)
    assert all(p.exitcode == 0 for p in procs), [p.exitcode for p in procs]
    return json.loads((work_path / MANIFEST_FILENAME).read_text())['steps']


class TestConcurrentManifestUpdates:
    """Steps finishing at the same moment must all be recorded."""

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

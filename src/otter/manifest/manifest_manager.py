"""Manifest class for managing the manifest file."""

from __future__ import annotations

import errno
import time
from datetime import datetime
from pathlib import Path

from filelock import FileLock, Timeout
from loguru import logger
from pydantic import ValidationError

from otter.manifest.model import Result, RootManifest, StepManifest
from otter.step.model import Step
from otter.storage import get_remote_storage
from otter.util.errors import NotFoundError, PreconditionFailedError, StorageError
from otter.util.logger import step_logging

MANIFEST_FILENAME = 'manifest.json'
UPLOAD_COOLDOWN = 3


class ManifestManager:
    """Class that manages the manifest file."""

    def __init__(
        self,
        runner_name: str,
        remote_uri: str | None,
        local_path: Path,
        relevant_step: Step,
        steps: list[str],
    ) -> None:
        self.runner_name = runner_name
        self._steps = steps
        self.remote_uri = f'{remote_uri}/{MANIFEST_FILENAME}' if remote_uri else None
        self.local_path = local_path / MANIFEST_FILENAME
        self.relevant_step = relevant_step
        self._revision: int = 0
        self.manifest = self._load_remote() or self._load_local() or self._create_empty()

    def _load_remote(self) -> RootManifest | None:
        if not self.remote_uri:
            logger.info('no release uri provided, skipping remote manifest load')
            return None
        remote_storage = get_remote_storage(self.remote_uri)
        try:
            manifest_str, self._revision = remote_storage.download_to_string(self.remote_uri)
            logger.info(f'remote manifest read from {self.remote_uri} (revision {self._revision})')
            return self._validate(manifest_str)
        except NotFoundError:
            logger.info(f'no remote manifest found in {self.remote_uri}')
            return None
        except StorageError as e:
            logger.critical(f'error reading manifest from {self.remote_uri}: {e}')
            raise SystemExit(errno.EIO)

    def _load_local(self) -> RootManifest | None:
        try:
            manifest_str = self.local_path.read_text()
            logger.info(f'local manifest read from {self.local_path}')
            return self._validate(manifest_str)
        except FileNotFoundError:
            logger.info(f'no local manifest found in {self.local_path}')
            return None
        except (ValueError, OSError) as e:
            logger.critical(f'error reading manifest from {self.local_path}: {e}')
            raise SystemExit(e.errno if isinstance(e, OSError) else errno.EIO)

    def _create_empty(self) -> RootManifest:
        logger.info('creating empty manifest')
        manifest = RootManifest()
        for step in self._steps:
            step_name = f'{self.runner_name}.{step}'
            manifest.steps[step_name] = StepManifest(name=step)
        return manifest

    def _validate(self, manifest_str: str) -> RootManifest:
        try:
            return RootManifest().model_validate_json(manifest_str)
        except ValidationError as e:
            logger.critical(f'error validating manifest: {e}')
            raise SystemExit(errno.EINVAL)

    def _serialize(self) -> str:
        try:
            return self.manifest.model_dump_json(indent=2, serialize_as_any=True)
        except ValueError as e:
            logger.critical(f'error serializing manifest: {e}')
            raise SystemExit(errno.EINVAL)

    def _refresh_from_remote(self) -> None:
        self._load_remote()
        self._update_step(self.relevant_step)
        self._save_local()

    def _save_remote(self) -> None:
        if not self.remote_uri:
            return
        uploaded = False
        remote_storage = get_remote_storage(self.remote_uri)
        while not uploaded:
            try:
                logger.debug(f'uploading manifest to {self.remote_uri} (revision {self._revision})')
                remote_storage.upload(self.local_path, self.remote_uri, self._revision)
                uploaded = True
                logger.success(f'remote manifest {self.remote_uri} uploaded')
            except PreconditionFailedError as e:
                logger.debug(f'{e}, retrying after {UPLOAD_COOLDOWN} seconds...')
                time.sleep(UPLOAD_COOLDOWN)
                self._refresh_from_remote()
            except StorageError as e:
                logger.critical(f'error uploading manifest: {e}')
                raise SystemExit(errno.EIO)

    def _save_local(self) -> None:
        lock_path = f'{self.local_path}.lock'
        manifest_str = self._serialize()
        lock = FileLock(lock_path, timeout=5)
        try:
            lock.acquire()
            self.local_path.write_text(manifest_str)
            lock.release()
            Path(lock_path).unlink(missing_ok=True)
            logger.debug(f'local manifest {self.local_path} written')
        except (OSError, Timeout) as e:
            logger.critical(f'error writing local manifest to {self.local_path}: {e}')
            raise SystemExit(e.errno)

    def _check_steps(self) -> None:
        """Check the result for all steps and update the root."""
        step_manifests = self.manifest.steps.values()
        if any(step.result in [Result.FAILURE, Result.ABORTED] for step in step_manifests):
            logger.warning('there are failed steps in the manifest')
            self.manifest.result = Result.FAILURE
        elif all(step.result is Result.SUCCESS for step in step_manifests):
            self.manifest.result = Result.SUCCESS
        else:
            self.manifest.result = Result.PENDING

    def _update_step(self, step: Step) -> None:
        self.relevant_step = step
        relevant_step_name = f'{self.runner_name}_{step.name}'
        self.manifest.steps[relevant_step_name] = step.manifest
        self.manifest.modified_at = datetime.now()
        self._check_steps()

    def complete(self, step: Step) -> Result:
        """Complete the manifest.

        Updates the step in the manifest, and then saves it locally and in the
        remote storage if a release URI is provided.

        Returns the result of the step.

        :param step: The step to update.
        :type step: Step
        :return: The result of the step.
        :rtype: Result
        """
        self._update_step(step)

        with step_logging(step):
            if step.manifest.result is not Result.SUCCESS:
                logger.warning(f'step {step.name} did not complete successfully')
            else:
                logger.success(f'step {step.name} completed successfully')
                if self.manifest.result is Result.SUCCESS:
                    logger.success('all steps are now complete')

        self._save_local()
        self._save_remote()

        return step.manifest.result

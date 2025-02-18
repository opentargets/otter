"""Main module."""

from otter.config import load_config
from otter.manifest.manifest_manager import ManifestManager
from otter.scratchpad import load_scratchpad
from otter.step.model import Step
from otter.task import load_specs
from otter.task.task_registry import TaskRegistry
from otter.util.fs import check_dir
from otter.util.logger import init_logger


class Runner:
    """Main class."""

    def __init__(self) -> None:
        self.config = load_config()
        init_logger(self.config.log_level)
        self.scratchpad = load_scratchpad(self.config.config_path)
        self.specs = load_specs(config_path=self.config.config_path, step_name=self.config.step)
        self.task_registry = TaskRegistry(self.config, self.scratchpad)
        self.task_registry.register('otter.tasks')

    def start(self) -> None:
        """Start a run."""
        check_dir(self.config.work_path)

    def register_tasks(self, task_module: str) -> None:
        """Register tasks."""
        self.task_registry.register(task_module)

    def run(self) -> None:
        """Run the step."""
        step = Step(
            name=self.config.step,
            specs=self.specs,
            task_registry=self.task_registry,
            config=self.config,
        )

        manifest = ManifestManager(
            remote_uri=self.config.release_uri,
            local_path=self.config.work_path,
            relevant_step=step,
            steps=self.config.steps,
        )

        step.run()
        manifest.complete(step)

"""Main module."""

from importlib.metadata import version

from loguru import logger

from otter.config import load_config
from otter.manifest.manifest_manager import ManifestManager
from otter.manifest.model import Result
from otter.scratchpad import load_scratchpad
from otter.step.model import Step
from otter.task import load_specs
from otter.task.task_registry import TaskRegistry
from otter.util.fs import check_dir
from otter.util.logger import init_logger, init_logger_early


class Runner:
    """Main class.

    This class is the main entry point for Otter.

    Upon instantiation, it will load the configuration, the scratchpad, and the
    specs. It will initialize the logger, create a task registry and register the
    built-in tasks.

    .. warning:: The instantiation will raise `SystemExit` and end the program if
        any of the listed actions fail.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        """The name of the runner.

        This will identify the application using Otter. Usually an application
        will have a single runner.

        The is used as the prefix in environment variables and will also be
        prepended to step names in the manifest. That way, multiple applications
        can report steps with the same name without colliding.

        The name should beetween 2 and 32 characters and only contain lowercase
        letters, numbers and the underscore character.
        """
        init_logger_early()
        self.config = load_config(self.name)
        init_logger(self.config.log_level, self.name)
        logger.info(f'otter v{version("opentargets-otter")} starting!')
        self.scratchpad = load_scratchpad(self.config.config_path)
        self.specs = load_specs(config_path=self.config.config_path, step_name=self.config.step)
        self.task_registry = TaskRegistry(self.config, self.scratchpad)
        self.task_registry.register('otter.tasks')

    def start(self) -> None:
        """Start a run.

        This method is used to start a run. It will check if the work path exists,
        and is writable, and create it if it doesn't.
        """
        check_dir(self.config.work_path)

    def register_tasks(self, task_package: str) -> None:
        """Register tasks.

        This method is used to register task classes. Otter implements a set of
        built-in tasks, but applications will likely want to define their own.
        This method allows users to register these by passing a package name.
        Usually, Otter applications will have a ``tasks`` package that contains
        the task classes in separate modules.

        The package must be importable and the modules must contain a class with
        the same name as the module in camel case, which must subclass
        :py:class:`otter.task.model.Task`.

        .. warning:: This method will raise `SystemExit` and end the program if
            the package cannot be imported or the modules are missing required
            classes.

        :param task_package: The package containing the tasks.
        :type task_package: str
        """
        self.task_registry.register(task_package)

    def run(self) -> None:
        """Run the step."""
        step = Step(
            name=self.config.step,
            specs=self.specs,
            task_registry=self.task_registry,
            config=self.config,
        )
        step.check_cycles()

        manifest = ManifestManager(
            runner_name=self.name,
            relevant_step=step,
            steps=self.config.steps,
            config=self.config,
        )

        step.run()

        manifest.complete(step)

        if manifest.manifest.result not in [Result.PENDING, Result.SUCCESS]:
            logger.warning('there are failed steps in the manifest')
        if step.manifest.result == Result.SUCCESS:
            logger.success(f'step {step.name} ran successfully')
        else:
            logger.error(f'step {step.name} failed')
            raise SystemExit(1)


def main() -> None:
    """Main function.

    This main function runs a simple otter program for development and testing.
    """
    runner = Runner(name='otter')
    runner.start()
    runner.run()

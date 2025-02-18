"""Generate more tasks based on a list."""

from typing import Any, Self

from loguru import logger

from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class ExplodeSpec(Spec):
    """Configuration fields for the explode task."""

    do: list[Spec]
    """The tasks to explode. Each task in the list will be duplicated for each
        iteration of the foreach list."""
    foreach: list[str]
    """The list to iterate over."""

    def model_post_init(self, __context: Any) -> None:
        # allows keys to be missing from the global scratchpad
        self.scratchpad_ignore_missing = True


class Explode(Task):
    """Generate more tasks based on a list.

    This task will duplicate the specs in the `do` list for each entry in the
    `foreach` list.

    Inside of the specs in the `do` list, the string `each` can be used as as a
    sentinel to refer to the current iteration value.

    .. warning:: The `${each}` placeholder **MUST** be present in the :py:obj:`otter.task.model.Spec.name`
        of the new specs defined inside `do`, as otherwise all of them will have
        the same name, and it must be unique.

    Example:

    .. code-block:: yaml

        steps:
            - explode species:
            foreach:
                - homo_sapiens
                - mus_musculus
                - drosophila_melanogaster
            do:
                - name: copy ${each} genes
                source: https://example.com/genes/${each}/file.tsv
                destination: genes-${each}.tsv
                - name: copy ${each} proteins
                source: https://example.com/proteins/${each}/file.tsv
                destination: proteins-${each}.tsv


    Keep in mind this replacement of `each` will only be done in strings, not lists
    or sub-objects.
    """

    def __init__(self, spec: ExplodeSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ExplodeSpec
        self.scratchpad = Scratchpad({})

    @report
    def run(self) -> Self:
        description = self.spec.name.split(' ', 1)[1]
        logger.debug(f'exploding {description} into {len(self.spec.do)} tasks by {len(self.spec.foreach)} iterations')
        new_tasks = 0

        for i in self.spec.foreach:
            self.scratchpad.store('each', i)

            for do_spec in self.spec.do:
                replaced_do_spec = Spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                self.context.specs.append(replaced_do_spec)
                new_tasks += 1

        logger.info(f'exploded into {new_tasks} new tasks')
        return self

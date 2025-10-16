"""Generate more tasks based on a list."""

from queue import Queue
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
    each_placeholder: str = 'each'
    """The placeholder string to use for the current iteration value.
        The value of this field, e.g. `each`, will be replaced by each of the entries in the
        `foreach` list."""

    def model_post_init(self, __context: Any) -> None:
        # allows keys to be missing from the global scratchpad
        self.scratchpad_ignore_missing = True


class Explode(Task):
    """Generate more tasks based on a list.

    This task will duplicate the specs in the `do` list for each entry in the
    `foreach` list.

    Inside of the specs in the `do` list, the string `each_placeholder` can be used as as a
    sentinel to refer to the current iteration value.

    .. warning:: The `${each_placeholder}` placeholder **MUST** be present in the :py:obj:`otter.task.model.Spec.name`
        of the new specs defined inside `do`, as otherwise all of them will have
        the same name, and it must be unique.

        If you do a nested explode, the inner explode will have spec names identical to
        it's sibling specs spawned during the outer explode. Since the spec names need to be unique,
        you should include the outer explode's placeholder in the inner explode's spec names to avoid
        conflicts.
        For example, if you have an outer explode with `each_placeholder: outer` and an inner explode
        with `each_placeholder: inner`, you might name a spec in the inner explode as
        `name: process ${outer} and ${inner} data` to ensure uniqueness.

    Example:

    .. code-block:: yaml

        steps:
            - explode species:
            foreach:
                - homo_sapiens
                - mus_musculus
                - drosophila_melanogaster
            each_placeholder: explode_each
            do:
                - name: copy ${explode_each} genes
                  source: https://example.com/genes/${explode_each}/file.tsv
                  destination: genes-${explode_each}.tsv
                - name: copy ${explode_each} proteins
                  source: https://example.com/proteins/${explode_each}/file.tsv
                  destination: proteins-${explode_each}.tsv


    Keep in mind this replacement of `explode_each` will only be done in strings, not lists
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
        subtask_queue: Queue[Spec] = self.context.sub_queue
        for i in self.spec.foreach:
            for do_spec in self.spec.do:
                self.scratchpad.store(self.spec.each_placeholder, i)
                subtask_spec = do_spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                subtask_spec.task_queue = subtask_queue
                subtask_queue.put(subtask_spec)
                new_tasks += 1
        logger.info(f'exploded into {new_tasks} new tasks')
        subtask_queue.shutdown()
        subtask_queue.join()
        return self

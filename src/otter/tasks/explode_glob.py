"""Generate more tasks based on a glob."""

from pathlib import Path
from typing import Any, Self
from uuid import uuid4

from loguru import logger

from otter.scratchpad.model import Scratchpad
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


def _split_glob(glob: str) -> tuple[str, str]:
    """Get the prefix of a glob expression."""
    i = 0
    while i < len(glob):
        if glob[i] in ['*', '?', '{', '['] and (i == 0 or glob[i - 1] != '\\'):
            return glob[:i], glob[i:]
        i += 1
    return glob, ''


class ExplodeGlobSpec(Spec):
    """Configuration fields for the explode task."""

    glob: str
    """The glob expression."""
    do: list[Spec]
    """The tasks to explode. Each task in the list will be duplicated for each
        iteration of the foreach list."""

    def model_post_init(self, __context: Any) -> None:
        # allows keys to be missing from the global scratchpad
        self.scratchpad_ignore_missing = True


class ExplodeGlob(Task):
    """Generate more tasks based on a glob.

    This task will duplicate the specs in the ``do`` list for each entry in a list
    coming from a glob expression.

    The task will add the following keys to a local scratchpad:

    - ``prefix``: the path up to the glob pattern and relative to either the \
        :py:obj:`otter.config.model.Config.release_uri` or the :py:obj:`otter.config.model.Config.work_path`.
    - ``match_path``: the part of the path that the glob matched **without** the \
        file name.
    - ``match_stem``: the file name of the matched file **without** the extension.
    - ``match_ext``: the file extensions of the matched file, with the dot.
    - ``uuid``: an UUID4, in case it is needed to generate unique names.

    .. code-block:: yaml

        - name: explode_glob things
          glob: 'gs://release-25/input/items/**/*.json'
          do:
            - name: transform ${match_stem} into parquet
              source: ${prefix}${match_path}${match_stem}${match_ext}
              destination: intermediate/${match_path}${math_stem}.parquet

    for a bucket containing two files:

    | gs://release-25/input/items/furniture/chair.json
    | gs://release-25/input/items/furniture/table.json

    And `release_uri` set to ``gs://release-25``

    the values will be:

    .. table:: Scratchpad values
    ==============  ================
    key               value
    ==============  ================
    ``prefix``      ``input/items/``
    ``match_path``  ``furniture/``
    ``match_stem``  ``chair``
    ``match_ext``   ``.json``
    ==============  ================

    the first task will be duplicated twice, with the following specs:

        .. code-block:: yaml

            - name: transform chair into parquet
            source: input/items/furniture/chair.json
            destination: intermediate/furniture/chair.parquet
            - name: transform table into parquet
            source: input/items/furniture/table.json
            destination: intermediate/furniture/table.parquet
    """

    def __init__(self, spec: ExplodeGlobSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ExplodeGlobSpec
        self.prefix, _ = _split_glob(self.spec.glob)
        self.scratchpad = Scratchpad({'prefix': self.prefix})

        if not self.context.config.release_uri:
            raise ValueError('release_uri is required for explode_glob')

        prefix = self.context.config.release_uri or self.context.config.work_path
        self.full_glob_path = f'{prefix}/{self.spec.glob}'

    @report
    def run(self) -> Self:
        if self.context.config.release_uri:
            # when release_uri is set, we glob from the remote storage
            remote_storage = get_remote_storage(self.full_glob_path)
            files = remote_storage.glob(self.full_glob_path)
        else:
            # when release_uri is not set, we glob from the local filesystem
            prefix, glob = _split_glob(self.full_glob_path)
            files = list(Path(prefix).glob(glob))

        new_tasks = 0

        for f in files:
            # store the part of the path that the glob matched
            relative_path = Path(str(f).split(self.prefix)[1])
            match_path = str(relative_path.parent) + '/'
            # remove empty path parts
            if match_path == './':
                match_path = ''
            match_stem = relative_path.stem
            match_ext = relative_path.suffix
            self.scratchpad.store('match_path', match_path)
            self.scratchpad.store('match_stem', match_stem)
            self.scratchpad.store('match_ext', match_ext)
            self.scratchpad.store('uuid', str(uuid4()))

            for do_spec in self.spec.do:
                replaced_do_spec = Spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                self.context.specs.append(replaced_do_spec)
                new_tasks += 1

        logger.info(f'exploded into {new_tasks} new tasks')
        return self

"""Generate more tasks based on a glob."""

from typing import Any, Self
from uuid import uuid4

from loguru import logger

from otter.scratchpad.model import Scratchpad
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


def _split_glob(s: str) -> tuple[str, str]:
    """Return the prefix of a glob expression."""
    i = 0
    while i < len(s):
        if s[i] in ['*', '[', '{', '?'] and (i == 0 or s[i - 1] != '\\'):
            return s[:i], s[i:].strip('/')
        i += 1
    return s, ''


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

    - ``uri``: the full file path
    - ``match_prefix``: the path up to the glob pattern and, in cases where possible,
        relative to :py:obj:`otter.config.model.Config.release_uri`.
    - ``match_path``: the part of the path that the glob matched **without** the
        file name.
    - ``match_stem``: the file name of the matched file **without** the extension.
    - ``match_ext``: the file extensions of the matched file, with the dot.
    - ``uuid``: an UUID4, in case it is needed to generate unique names.

    .. code-block:: yaml

        - name: explode_glob things
          glob: 'gs://release-25/input/items/**/*.json'
          do:
            - name: transform ${match_stem} into parquet
              source: ${uri}
              destination: intermediate/${match_path}${math_stem}.parquet

    for a bucket containing two files:

    | gs://release-25/input/items/furniture/chair.json
    | gs://release-25/input/items/furniture/table.json

    And `release_uri` set to ``gs://release-25``

    the values will be:

    .. table:: Scratchpad values for the first task
        :class: custom

        =================  =====================================================
         key               value
        =================  =====================================================
         ``uri``           ``gs://release-25/input/items/furniture/chair.json``
         ``match_prefix``  ``input/items``
         ``match_path``    ``furniture``
         ``match_stem``    ``chair``
         ``match_ext``     ``.json``
         ``uuid``          ``<uuid>``
        =================  =====================================================

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
        self.scratchpad = Scratchpad()

        if not self.context.config.release_uri:
            raise ValueError('release_uri is required for explode_glob')

        # if glob is relative, we append release_uri
        self.glob = self.spec.glob
        if '://' not in self.glob:
            self.glob = f'{self.context.config.release_uri}/{self.spec.glob}'

    @report
    def run(self) -> Self:
        remote_storage = get_remote_storage(self.glob)
        files = remote_storage.glob(self.glob)

        new_tasks = 0

        for f in files:
            uri = f
            match_prefix, _ = _split_glob(self.glob)
            match = uri.replace(match_prefix, '')
            split_match = match.rsplit('/', 1)
            if len(split_match) == 2:
                match_path, match_filename = split_match
            else:
                match_path = ''
                match_filename = split_match[0]
            split_filename = match_filename.rsplit('.', 1)
            if len(split_filename) == 2:
                match_stem, match_ext = split_filename
            else:
                match_stem = split_filename[0]
                match_ext = ''
            if self.context.config.release_uri:
                match_prefix = match_prefix.replace(self.context.config.release_uri, '').strip('/')

            self.scratchpad.store('uri', uri)
            self.scratchpad.store('match_prefix', match_prefix)
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

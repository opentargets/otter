"""Generate more tasks based on a glob."""

import re
from queue import Queue
from typing import Any, Self
from uuid import uuid4

from loguru import logger

from otter.scratchpad.model import Scratchpad
from otter.storage.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.util import split_glob


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

    - ``match_prefix``: the path up to the glob pattern and relative to
        :py:obj:`otter.config.model.Config.release_uri` or
        :py:obj:`otter.config.model.Config.work_path` if the source is a relative
        location.
    - ``match_path``: the part of the path that the glob matched **without** the \
        file name.
    - ``match_stem``: the file name of the matched file **without** the extension.
    - ``match_ext``: the file extensions of the matched file, **without** the dot.
    - ``uri``: ${match_prefix}/${match_path}/${match_stem}.${match_ext}
    - ``uuid``: an UUID4, in case it is needed to generate unique names.

    .. note:: ${uri} will be either an absolute URL or a path relative to either
        :py:obj:`otter.config.model.Config.release_uri` or
        :py:obj:`otter.config.model.Config.work_path` depending on whether the
        source itself is absolute or relative.

    .. note:: Forming a path with ``${match_prefix}/${match_path}${match_stem}``
        when ``match_path`` is empty would cause double slashes to be introduced.
        These are automatically removed. GCS paths like ``gs://bucket/////file``
        are not supported by this task.

    .. code-block:: yaml

        - name: explode_glob items
          glob: 'gs://release-25/input/items/**/*.json'
          do:
            - name: transform ${match_stem} into parquet
              source: ${match_path}/${match_stem}.${match_ext}
              destination: intermediate/${match_path}/${math_stem}.parquet

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
         ``match_prefix``  ``input/items/``
         ``match_path``    ``furniture``
         ``match_stem``    ``chair``
         ``match_ext``     ``json``
         ``uri``           ``input/items/furniture/chair.json``
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
        """Internal scratchpad used to replace values in subtask specs."""

    @report
    def run(self) -> Self:
        prefix, glob = split_glob(self.spec.glob)
        h = StorageHandle(prefix, config=self.context.config)
        files = h.glob(glob)
        release_uri = self.context.config.release_uri
        work_path = str(self.context.config.work_path)

        new_tasks = 0
        for f in files:
            # make uri relative to release_uri or work_path if possible
            if release_uri and f.startswith(release_uri):
                uri = f[len(release_uri) :].lstrip('/')
            elif work_path and f.startswith(work_path):
                uri = f[len(work_path) :].lstrip('/')
            else:
                uri = f

            relative_path = uri.removeprefix(prefix).lstrip('/')
            match_prefix = uri.removesuffix(relative_path).rstrip('/')
            match_path, _, filename = relative_path.rpartition('/')
            match_stem, _, match_ext = filename.rpartition('.')
            if not match_stem:  # dotfile or no extension
                match_stem, match_ext = filename, ''

            self.scratchpad.store('uri', uri)
            self.scratchpad.store('match_prefix', match_prefix)
            self.scratchpad.store('match_path', match_path)
            self.scratchpad.store('match_stem', match_stem)
            self.scratchpad.store('match_ext', match_ext)
            self.scratchpad.store('uuid', str(uuid4()))

            subtask_queue: Queue[Spec] = self.context.sub_queue
            for do_spec in self.spec.do:
                replaced_model = self.scratchpad.replace_dict(do_spec.model_dump())
                # clean up any double slashes that may have been introduced
                for k, v in replaced_model.items():
                    if isinstance(v, str):
                        replaced_model[k] = re.sub(r'(?<!:)//+', '/', v)
                subtask_spec = do_spec.model_validate(replaced_model)
                subtask_spec.task_queue = subtask_queue
                subtask_queue.put(subtask_spec)
                new_tasks += 1

        logger.info(f'exploded into {new_tasks} new tasks')
        # disabled for now to allow python versions < 3.13
        # subtask_queue.shutdown()
        subtask_queue.join()
        return self

Changelog
=========


Version 26.0.1
--------------

*Released on February 19, 2026*

Improvements
^^^^^^^^^^^^

- **Improved manifest handling**: Add tasks to the manifest when they are built, add
  ``failure_reason`` field, and improve step finish info (:commit:`715cbe3`,
  :commit:`f867f8e`, :commit:`7e51c95`)
- **Improved errors**: Error messages are now more descriptive and actionable
  (:commit:`584846e`)
- **Step run always returns now**: Instead of exiting with a code 1 on error,
  :py:meth:`otter.step.coordinator.Coordinator.run` always finishes and will return a
  :py:class:`otter.step.model.Step` object with the final state of the step, so
  applications can handle errors as they see fit instead (:commit:`1368108`)

Bugfixes
^^^^^^^^

- **Coordinator now stops immediately on error**: Previously a failing task would not
  halt the run; now the coordinator kills all workers on any error (:commit:`1639da3`,
  :commit:`acfd557`, :commit:`3177c8f`)
- Tasks that are done waiting for subtasks are now properly marked as complete in the
  manifest (:commit:`95ab8d3`, :commit:`c95e1e2`)


Version 26.0.0
--------------

*Released on February 18, 2026*

Improvements
^^^^^^^^^^^^

- **Asynchronous task execution**: Otter now supports both synchronous and asynchronous
  tasks, with improved run logic and better coordination by using pull-style workers and
  a more robust process pool management system (:commit:`086911a`, :commit:`b5d7dea`,
  :commit:`2c8f37e`, :commit:`f0e697e`)
- **Improved storage handling**: File operations are now handled through "StorageHandle"
  and "AsyncStorageHandle" classes which provide a consistent interface for local and
  cloud storage. (:commit:`aa6da20`, :commit:`7e84c40`, :commit:`dc2e4d2`, :commit:`0c37f92`,
  :commit:`6e62d59`)
- **New copy_many task for batch operations**: A new builtin task enables efficient copying
  of multiple files at once. This task is better suited for copying a large amount (100+) of
  relatively small (tens of MB) files. It avoids spawning a new process and a new client for
  each file. It supports passing a list of sources, a path to a file with a list of sources,
  or a glob (when copying from backends that can handle those, like cloud services).
  (:commit:`db0a2b9`, :commit:`917e91f`, :commit:`21a707e`)

Bugfixes
^^^^^^^^

- Do not fail validation when file size cannot be determined (:commit:`8a3164f`)
- Fix ``find_latest`` task functionality (:commit:`b5d774d`)
- Fix Google Cloud Storage glob pattern handling (:commit:`5de3d9d`)
- Fix scratchpad and add comprehensive tests (:commit:`1b26ee2`)
- Ensure mtime changes are properly tracked (:commit:`991bb9e`, :commit:`71ee4d8`)
- Fix help not showing defaults (:commit:`4c8e07f`)

Internal Changes
^^^^^^^^^^^^^^^^

- Switch to IOBase for improved I/O handling (:commit:`8b344f3`)
- Refactor builtin tasks for improved maintainability (:commit:`eb9701b`)
- Refactor manifest handling (:commit:`58a4a9e`)
- Make async and improve run logic (:commit:`086911a`)
- Improved storage handling (:commit:`bf5e167`, :commit:`4dfd507`)
- Remove gsheets special case and clean logs (:commit:`522e899`)
- Logger rename for clarity (:commit:`3fa8456`)
- Update dependencies and fix linting and tests (:commit:`3792e29`)
- Ensure process pool is larger than one (:commit:`f4a9be2`)
- Update dependencies (:commit:`50456a5`)
- Simplify download method (:commit:`87318af`)
- Switch to ty type system with type checking improvements (:commit:`3d29142`, :commit:`4262c9c`)
- Update documentation (:commit:`843d36e`)

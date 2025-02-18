Otter — Open Targets Task ExecutoR
===================================

.. toctree::
   :maxdepth: 1
   :hidden:

   otter.core
   otter.step
   otter.task
   otter.manifest
   otter.scratchpad
   otter.tasks
   otter.validators
   otter.config
   otter.storage
   otter.util


Otter is a the task execution framework used in the Open Targets data Pipeline.

It provides an easy to use API to implement generic tasks that are then used by
describing the flow in a YAML configuration file.

Take a look at a :ref:`otter.example`.


Overview
--------

The hierarchy is as follows:

A run of the Pipeline is composed of a series of Steps, and those are a bunch
of Tasks.

Otter is meant to be launched once per step, and it will execute the tasks in a
given step. Orchestrating the execution of steps is outside of the scope of
Otter.


Features
--------

This is a list of what you get *for free* by using Otter:

- **Parallel execution**: Tasks are run in parallel, and Otter will take care
  of the dependency planning.

- **Declarative configuration**: Steps are described in a YAML file, as list of
  tasks with different specifications. The task themselves are implemented in
  Python enabling a lot of flexibility.

- **Logging**: Otter uses the `loguru library
  <https://loguru.readthedocs.io/en/stable/>`_ for logging. It handles all the
  logging related the task flow, and also logs into the manifest (see next item).

- **Manifest**: Otter manages a manifest file that describes a pipeline run. It
  is used to both for debugging and for tracking the provenance of the data. A
  series of simple JQ queries can be used to extract information from it (see
  :ref:`otter.manifest.jq_queries`).

- **Error management**: Otter will stop the execution of the pipeline if a task
  fails, and will log the error in the manifest.

- **Scratchpad**: A place to store variables that can be overwritten into the
  configuration file (something like a very simple templating engine), enabling
  easy parametrization of runs, and passing of data between tasks.

- **Utilities**: Otter provides interfaces to use Google Cloud Storage and other
  remote storage services, and a bunch of utilities to help you write tasks.

Of course, "for free" means there is not an extreme degree of flexibility some of
these are limited in scope. The aim is ease of use and simplicity. You can jump
down to read more about the :ref:`philosophy behind Otter <otter.philosophy>`.


The model
---------

The main elements used when writing an application using Otter are:

- :class:`otter.core.Runner` — Handles the application lifecycle.
- :class:`otter.task.model.Spec` — Holds the task specification.
- :class:`otter.task.model.Task` — A Task itself.
- :class:`otter.task.model.TaskContext` — Holds the context of a task.
- :meth:`otter.validators.v` — The method used to run validators for a task.
- :class:`otter.scratchpad.model.Scratchpad` — A place to store variables to overwrite in the config file.
- :mod:`otter.util` — A bunch of utilities to help you write tasks.
- :mod:`otter.storage` — Remote storage interfaces to use Google Cloud Storage and similar services.


.. _otter.example:

Simple example
--------------

Here is an example of a configuration file for a really simple pipeline:

.. code-block:: yaml

   ---
   work_path: ./work
   log_level: DEBUG
   scratchpad:
   steps:
     my_step:
       - name: hello_world task one
         who: World
       - name: hello_world task two
         requires:
           - hello_world task one
         who: Universe


It defines a ``my_step`` step that has two tasks, out of which the second one will
only run once the first has finished. An application to run this step would be:

.. code-block:: python

   from otter import Runner

   def main() -> None:
      runner = Runner()
      runner.run()

.. tip::
   | Some other examples:
   | `HelloWorld <_modules/otter/tasks/hello_world.html>`_ — Example Task.


.. _otter.philosophy:

Philosophy
----------

One of the reasons for implementing another task execution framework instead of
using something like Celery is, we wanted have a very basic set of features
baked into all tasks (traceability, logging, error management, etc). Although
most of those are already there in many frameworks, we would not be using them
to their full extent, effectively increasing the complexity of the system
unnecessarily.

Using something like Apache Airflow alone to handle the pipeline was another
option. But then we would be completely unable to execute any part of it outside
of the enourmous Airflow ecosystem, which —although amazingly powerful— is also
unwieldy.

Otter provides us with a way to wrap steps in an executable, independent unit
that can be run in any environment, and that can be orchestrated by any other
system (even a simple bash ``for`` loop). It also encapsulates many tools and
interfaces that are common to all tasks, making it easier to write and maintain
them.

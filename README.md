# Otter — Open Targets' Task ExecutoR

[![pypi](https://raster.shields.io/pypi/v/opentargets-otter.png)](https://pypi.org/project/opentargets-otter/)
[![docs status](https://github.com/opentargets/otter/actions/workflows/docs.yaml/badge.svg)](https://opentargets.github.io/otter)
[![build status](https://github.com/opentargets/otter/actions/workflows/build.yaml/badge.svg)](https://github.com/opentargets/otter/actions/workflows/build.yaml)
[![license](https://img.shields.io/github/license/opentargets/otter.svg)](LICENSE)

Otter is a the task execution framework used in the Open Targets data Pipeline.

It provides an easy to use API to implement generic tasks that are then used by
describing the flow in a YAML configuration file.

Take a look at a [Simple example](https://opentargets.github.io/otter/#otter-example).


## Features

This is a list of what you get for free by using Otter:
  * **Parallel execution**: Tasks are run in parallel, and Otter will take care of
    the dependency planning.
  * **Declarative configuration**: Steps are described in a YAML file, as list of
    tasks with different specifications. The task themselves are implemented
    in Python enabling a lot of flexibility.
  * **Logging**: Otter uses the [loguru library](https://github.com/delgan/loguru)
    for logging. It handles all the logging related the task flow, and also logs
    into the manifest (see next item).
  * **Manifest**: Otter manages a manifest file that describes a pipeline run. It
    is used to both for debugging and for tracking the provenance of the data. A series of simple JQ queries can be used to extract information from it (see Useful JQ queries).
  * **Error management**: Otter will stop the execution of the pipeline if a task fails,
    and will log the error in the manifest.
  * **Scratchpad**: A place to store variables that can be overwritten into the
    configuration file (something like a very simple templating engine), enabling
    easy parametrization of runs, and passing of data between tasks.
  * **Utilities**: Otter provides interfaces to use Google Cloud Storage and other
    remote storage services, and a bunch of utilities to help you write tasks.


## Documentation

See it in [here](https://opentargets.github.io/otter).


## Development

> [!IMPORTANT]
> Remember to run `make dev` before starting development. This will set up a very
> simple git hook that does a few checks before committing.

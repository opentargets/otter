"""Main module."""

from importlib.metadata import version

from otter.core import Runner

__all__ = ['Runner']

__version__ = version('opentargets-otter')

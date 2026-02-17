"""Utility functions for storage."""

from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle


def make_absolute(files: str | dict[str, str], config: Config) -> str | dict[str, str]:
    """Make a file path or a dictionary of file paths absolute.

    :param files: A file path or a dictionary of file paths to make absolute.
    :type files: str or dict[str, str]
    :param config: The configuration object containing the work path.
    :type config: Config
    :return: The absolute file path or a dictionary of absolute file paths.
    :rtype: str or dict[str, str]
    """
    if isinstance(files, dict):
        absolute_files = {}
        for k, v in files.items():
            h = StorageHandle(v, config=config)
            absolute_files[k] = h.absolute
        return absolute_files
    else:
        h = StorageHandle(files, config=config)
        return h.absolute

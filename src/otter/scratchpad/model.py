"""Scratchpad module.

This module defines the `Scratchpad` class, which is a centralized place to store
key-value pairs in the configuration of the application. It provides utilities to
perform template substition.
"""

from __future__ import annotations

from pathlib import Path
from string import Template
from typing import Any

from loguru import logger

from otter.util.errors import ScratchpadError


class ScratchpadTemplate(Template):
    """Scratchpad Template class.

    A subclass of `string.Template` that allows dots and spaces in placeholders.
    """

    idpattern = r'(?a:[_a-z][._a-z0-9\s]*[_a-z0-9])'


class Scratchpad:
    """A class to store and replace placeholders in strings.

    This class is used to store key-value pairs and replace placeholders in
    strings with the corresponding values. The placeholders are defined in the
    strings using the dollar sign followed by the placeholder name enclosed in
    curly braces, e.g., ``${person.name}``. The placeholders can include dots to
    represent nested dictionaries or objects.

    Example:
        >>> scratchpad = Scratchpad()
        >>> scratchpad.store('person.name', 'Alice')
        >>> scratchpad.replace('Hello, ${person.name}!')
        'Hello, Alice!'

    The `Scratchpad` class is used to perform template substitution in a `Spec`,
    right before the task is built.

    .. important:: You can use internal scratchpads in tasks. For example, to have
        private variables that are only replaced in one of the specs.

        To be able to do this, however, you must set the
        ``scratchpad_ignore_missing`` attribute to ``True`` in the task model.
        This will allow the task to be built even if keys are missing from the
        global scratchpad.

        You are responsible, however, of ensuring that the keys are present in
        the internal scratchpad. It is good practice to leave the internal
        scratchpad set to not ignore, so that you can catch missing keys.

        To set the `scratchpad_ignore_missing` attribute, you can add this to
        the spec definition:

        .. code-block:: python

            def model_post_init(self, __context: Any) -> None:
                self.scratchpad_ignore_missing = True

    .. seealso::  :py:meth:`otter.task.task_registry.TaskRegistry.build`
    """

    def __init__(self, sentinel_dict: dict[str, Any] | None = None) -> None:
        self.sentinel_dict: dict[str, Any] = sentinel_dict or {}
        """A dictionary to store the key-value pairs."""

    def store(self, key: str, value: str | list[str]) -> None:
        """Store a key-value pair in the scratchpad.

        Both strings and lists of strings are accepted as values. It might be
        useful to extend it to accept dicts as well.

        :param key: The key to store.
        :type key: str
        :param value: The value to store.
        :type value: str | list[str]
        """
        self.sentinel_dict[key] = value

    def merge(self, other: Scratchpad) -> None:
        """Merge the sentinels from another scratchpad.

        :param other: The scratchpad to merge.
        :type other: Scratchpad
        """
        added = 0
        for key, value in other.sentinel_dict.items():
            if key not in self.sentinel_dict:
                added += 1
                self.sentinel_dict[key] = value
        logger.info(f'added {added} new entries to scratchpad')

    def _replace_str(self, string: str, *, ignore_missing: bool = False) -> str:
        """Replace placeholders in a string.

        :param string: The string with placeholders to replace.
        :type string: str
        :return: The string with the placeholders replaced.
        :rtype: str
        :param ignore_missing: Whether to ignore missing keys in the scratchpad.
            Defaults to ``False``.
        :type ignore_missing: bool
        :raises ScratchpadError: If ``self.ignore_missing`` is ``False`` and one
            of the placeholder in the string does not have a corresponding key in
            the scratchpad.
        """
        replacer = ScratchpadTemplate(string)

        if ignore_missing:
            return replacer.safe_substitute(self.sentinel_dict)
        else:
            try:
                return replacer.substitute(self.sentinel_dict)
            except KeyError as e:
                logger.critical(f'key {e} not found in scratchpad')
                raise ScratchpadError(e)

    def _replace_any(self, v: Any, *, ignore_missing: bool = False) -> Any:
        """Replace placeholders in a value.

        :param v: The value with placeholders to replace.
        :type v: Any
        :return: The value with the placeholders replaced.
        :rtype: Any
        :param ignore_missing: Whether to ignore missing keys in the scratchpad.
            Defaults to ``False``.
        :type ignore_missing: bool
        :raises ScratchpadError: If ``self.ignore_missing`` is ``False`` and one of
            the placeholder in the value does not have a corresponding key in the
            scratchpad.
        """
        match v:
            case None:
                return None
            case dict():
                return self.replace_dict(v, ignore_missing=ignore_missing)
            case list():
                return [self._replace_any(e, ignore_missing=ignore_missing) for e in v]
            case Path():
                return Path(self._replace_str(str(v), ignore_missing=ignore_missing))
            case str():
                return self._replace_str(v, ignore_missing=ignore_missing)
            case _:
                return v

    def replace_dict(self, d: dict[str, Any], *, ignore_missing: bool = False) -> dict[str, Any]:
        """Replace placeholders in a dictionary with the corresponding values.

        :param d: The dictionary with placeholders to replace.
        :type d: dict[str, Any]
        :param ignore_missing: Whether to ignore missing keys in the scratchpad.
            Defaults to ``False``.
        :type ignore_missing: bool
        :return: The dictionary with the placeholders replaced by their values.
        :rtype: dict[str, Any]
        """
        keys_to_ignore = {'task_type', 'scratchpad_ignore_missing'}

        replaced_dict: dict[str, Any] = {}
        for key, value in d.items():
            if key in keys_to_ignore:
                replaced_dict[key] = value
                continue
            replaced_dict[key] = self._replace_any(value, ignore_missing=ignore_missing)
        return replaced_dict

"""Utility functions."""


def split_glob(s: str) -> tuple[str, str]:
    """Return the prefix of a glob expression."""
    i = 0
    while i < len(s):
        if s[i] in ['*', '[', '{', '?'] and (i == 0 or s[i - 1] != '\\'):
            return s[:i], s[i:].lstrip('/')
        i += 1
    return s, ''

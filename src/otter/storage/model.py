"""Storage model definitions."""

from dataclasses import dataclass

Revision = float | str | None
"""Type alias for file revision identifiers."""


@dataclass
class StatResult:
    """Dataclass representing file metadata."""

    is_dir: bool
    """Whether the resource is a directory."""
    is_reg: bool
    """Whether the resource is a regular file."""
    size: int | None = None
    """The resource size in bytes, `None` if unknown."""
    revision: Revision = None
    """The resource revision identifier."""
    mtime: float | None = None
    """The resource modification time as a Unix timestamp, `None` if unknown."""

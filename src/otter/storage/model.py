"""Storage model definitions."""

from dataclasses import dataclass

from pydantic import BaseModel

Revision = float | str | None
"""Type alias for file revision identifiers."""


class StorageSettings(BaseModel):
    """Base class for storage settings models.

    Storage backends that support context settings should define a subclass of StorageSettings
    to specify the allowed context parameters. These settings are used for validating the
    context passed via the storage_context() context manager when performing storage operations.

    Example:
        class GoogleStorageSettings(StorageSettings):
            billing_project: str | None = Field(
                default=None,
                description='Project ID for requester-pays bucket billing',
            )
    """


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

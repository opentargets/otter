"""Custom exceptions."""

from pydantic import ValidationError


def log_pydantic(e: ValidationError) -> str:
    """Log a pydantic validation error correctly."""
    errors = e.errors()
    return '. '.join([f'{err["loc"][0]}: {err["msg"]}' for err in errors])


class OtterError(Exception):
    """Base class for all application-specific exceptions."""


# Step-related errors
class StepInvalidError(OtterError):
    """Raise when a step is invalid somehow."""


class StepFailedError(OtterError):
    """Raise when a step fails somehow."""


# Task-related errors
class TaskAbortedError(OtterError):
    """Raise when a task is aborted."""

    def __init__(self) -> None:
        super().__init__('another task failed, task aborted')


class TaskValidationError(OtterError):
    """Raise when a task fails validation."""


# Other errors
class DownloadError(OtterError):
    """Raise when an error occurs during a download."""


class NotFoundError(OtterError):
    """Raise when something is not found."""

    def __init__(self, thing: str | None = None) -> None:
        if thing is None:
            thing = 'item'
        super().__init__(f'{thing} not found')


class PreconditionFailedError(OtterError):
    """Raise when a precondition fails."""


class ScratchpadError(OtterError):
    """Raise when a key is not found in the scratchpad."""


class StorageError(OtterError):
    """Raise when an error occurs in a storage class."""

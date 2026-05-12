"""Storage context for backend-specific configuration.

This module provides a flexible context manager for passing backend-specific
configuration to storage operations. Storage backends can define and use their
own context parameters without modifying core APIs.

Examples:
    - Google Cloud Storage: billing_project for requester-pays buckets
    - AWS S3: role_arn for assumed roles (future)
    - Azure Blob: tenant_id for multi-tenant access (future)

The context variable approach allows configuration to be set at the task level
without modifying storage backend APIs.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any


class StorageContext:
    """Generic storage context holding backend-specific configuration.

    Storage backends can query this context for the parameters they need,
    making the system extensible without tight coupling to specific providers.

    Example:
        ctx = StorageContext(billing_project='my-project', timeout=300)
        project = ctx.get('billing_project')  # Returns 'my-project'
        timeout = ctx.get('timeout', 60)   # Returns 300
        role = ctx.get('role_arn')         # Returns None
    """

    def __init__(self, **settings: Any) -> None:
        """Initialize storage context with arbitrary settings.

        Args:
            **settings: Backend-specific configuration parameters.
        """
        self._settings = settings

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key.

        Args:
            key: Configuration parameter name.
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        return self._settings.get(key, default)

    def __getitem__(self, key: str) -> Any:
        """Get a configuration value by key (dict-style access).

        Args:
            key: Configuration parameter name.

        Returns:
            Configuration value.

        Raises:
            KeyError: If key not found.
        """
        return self._settings[key]

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the context.

        Args:
            key: Configuration parameter name.

        Returns:
            True if key exists, False otherwise.
        """
        return key in self._settings


_storage_context: ContextVar[StorageContext | None] = ContextVar('storage_context', default=None)


def get_storage_context() -> StorageContext | None:
    """Get the current storage context.

    Returns the active StorageContext instance for the current execution context,
    or None if no context is set. Storage backends should check for None and
    handle missing configuration gracefully.

    Returns:
        Active StorageContext or None.
    """
    return _storage_context.get()


@contextmanager
def storage_context(**settings: Any) -> Generator[None]:
    """Set storage context for operations within the block.

    This context manager allows task-level configuration to be passed to
    storage backends without modifying function signatures. Backends query
    the context for parameters they understand.

    Args:
        **settings: Backend-specific configuration parameters. Common examples:
            - billing_project: Billing project for GCS requester-pays buckets
            - timeout: Custom timeout for operations
            - retry_config: Custom retry configuration

    Example:
        with storage_context(billing_project='my-billing-project'):
            # Storage operations within this block can access the context
            handle.copy_to(destination)

        with storage_context(billing_project='project-1', timeout=300):
            # Multiple parameters can be passed
            handle.read()
    """
    if not settings:
        yield
        return

    ctx = StorageContext(**settings)
    token = _storage_context.set(ctx)
    try:
        yield
    finally:
        _storage_context.reset(token)

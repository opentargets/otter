"""Storage settings models for different storage backends."""

from pydantic import BaseModel, Field

from otter.storage.model import StorageSettings


class GoogleStorageSettings(StorageSettings):
    """Settings model for Google Cloud Storage context.

    Defines the allowed context parameters that can be used with Google Cloud Storage
    operations. These settings are passed via the storage_context() context manager.

    Attributes:
        billing_project: Google Cloud project ID to use for requester-pays bucket access.
                     When accessing requester-pays buckets, this project will be billed
                     for the API requests and data egress costs.

    Example:
        with storage_context(billing_project='my-billing-project'):
            # Operations on requester-pays buckets will bill to 'my-billing-project'
            handle.copy_to(destination)
    """

    billing_project: str | None = Field(
        default=None,
        description='Project ID for requester-pays bucket billing',
    )

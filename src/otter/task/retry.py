"""Retry configuration models and backoff strategy definitions."""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class BackoffStrategy(StrEnum):
    """Supported retry backoff calculation strategies.

    Attributes:
        FIXED: Use the same delay between each retry attempt.
        LINEAR: Increase the delay by a fixed amount each retry.
        EXPONENTIAL: Increase the delay exponentially after each retry.
    """

    FIXED = 'fixed'
    LINEAR = 'linear'
    EXPONENTIAL = 'exponential'


class RetryPolicy(BaseModel):
    """Configuration for retry behaviour.

    Attributes:
        retries: Maximum number of retry attempts.
        backoff: Strategy used to calculate delay between retries.
        initial_delay: Delay in seconds before the first retry.
        max_delay: Maximum allowed delay in seconds between retries.
        jitter: Whether to randomize delays slightly to reduce retry spikes.
    """

    retries: int = Field(default=0, ge=0)
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay: float = Field(default=5.0, ge=0.0)
    max_delay: float = Field(default=120.0, ge=0.0)
    jitter: bool = False

    @model_validator(mode='after')
    def max_delay_gte_initial(self) -> 'RetryPolicy':
        """Validate that max_delay is not smaller than initial_delay.

        Returns:
            RetryPolicy: The validated retry policy instance.

        Raises:
            ValueError: If max_delay is less than initial_delay.
        """
        if self.max_delay < self.initial_delay:
            raise ValueError('max_delay must be >= initial_delay')
        return self


class RetryConfig(BaseModel):
    """Retry policies grouped by task lifecycle stage.

    Attributes:
        run: Retry policy applied during task execution.
        validation: Retry policy applied during task validation.
    """

    run: RetryPolicy = RetryPolicy()
    validation: RetryPolicy = RetryPolicy()

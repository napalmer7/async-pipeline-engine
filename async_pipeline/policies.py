import asyncio
import random
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, Tuple, Type, TypeVar
from .exceptions import PipelineTimeoutError

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Configures exponential backoff retry behavior for transient stage errors."""

    max_attempts: int = 1
    base_delay: float = 0.05
    max_delay: float = 5.0
    backoff_multiplier: float = 2.0
    jitter: bool = False
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")

    def compute_delay(self, attempt: int) -> float:
        """Compute delay before the next attempt (1-indexed attempt number)."""
        raw_delay = min(
            self.max_delay,
            self.base_delay * (self.backoff_multiplier ** max(0, attempt - 1)),
        )
        if self.jitter:
            return random.uniform(0, raw_delay)
        return raw_delay


@dataclass(frozen=True)
class TimeoutPolicy:
    """Configures stage-level and pipeline-level timeout bounds."""

    stage_timeout_seconds: Optional[float] = None
    pipeline_timeout_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        if self.stage_timeout_seconds is not None and self.stage_timeout_seconds <= 0:
            raise ValueError("stage_timeout_seconds must be > 0")
        if self.pipeline_timeout_seconds is not None and self.pipeline_timeout_seconds <= 0:
            raise ValueError("pipeline_timeout_seconds must be > 0")


async def execute_with_policies(
    coro_factory: Callable[[], Awaitable[T]],
    stage_name: str,
    retry_policy: Optional[RetryPolicy] = None,
    timeout_policy: Optional[TimeoutPolicy] = None,
) -> Tuple[T, int]:
    """Execute an async callable honoring RetryPolicy and TimeoutPolicy.

    Returns a tuple of (result, attempts_used).
    """
    policy = retry_policy or RetryPolicy(max_attempts=1)
    stage_timeout = timeout_policy.stage_timeout_seconds if timeout_policy else None

    last_exc: Optional[Exception] = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            coro = coro_factory()
            if stage_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=stage_timeout)
            else:
                result = await coro
            return result, attempt
        except asyncio.TimeoutError as exc:
            timeout_err = PipelineTimeoutError(
                f"Stage '{stage_name}' timed out after {stage_timeout}s"
            )
            last_exc = timeout_err
            if not isinstance(timeout_err, policy.retryable_exceptions) or attempt >= policy.max_attempts:
                raise timeout_err from exc
        except policy.retryable_exceptions as exc:
            last_exc = exc
            if attempt >= policy.max_attempts:
                raise
        except Exception:
            # Non-retryable exception fails immediately
            raise

        delay = policy.compute_delay(attempt)
        if delay > 0:
            await asyncio.sleep(delay)

    assert last_exc is not None
    raise last_exc

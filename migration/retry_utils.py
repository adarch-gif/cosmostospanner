from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Protocol, TypeVar

T = TypeVar("T")
SECURE_RNG = random.SystemRandom()


class SupportsRetryConfig(Protocol):
    retry_attempts: int
    retry_initial_delay_seconds: float
    retry_max_delay_seconds: float
    retry_backoff_multiplier: float
    retry_jitter_seconds: float


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    initial_delay_seconds: float
    max_delay_seconds: float
    backoff_multiplier: float
    jitter_seconds: float

    @classmethod
    def from_runtime(cls, runtime: SupportsRetryConfig) -> "RetryPolicy":
        return cls(
            attempts=runtime.retry_attempts,
            initial_delay_seconds=runtime.retry_initial_delay_seconds,
            max_delay_seconds=runtime.retry_max_delay_seconds,
            backoff_multiplier=runtime.retry_backoff_multiplier,
            jitter_seconds=runtime.retry_jitter_seconds,
        )


def run_with_retry(
    operation: Callable[[], T],
    *,
    operation_name: str,
    policy: RetryPolicy,
    logger: Any,
    retriable_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    last_error: BaseException | None = None
    for attempt in range(1, policy.attempts + 1):
        try:
            return operation()
        except retriable_exceptions as exc:  # noqa: PERF203
            last_error = exc
            if attempt >= policy.attempts:
                raise
            _sleep_before_retry(
                operation_name=operation_name,
                policy=policy,
                logger=logger,
                attempt=attempt,
                error=exc,
            )

    raise RuntimeError(f"Unexpected retry state for operation {operation_name}.") from last_error


def iter_with_retry(
    open_stream: Callable[[], Iterable[T]],
    *,
    operation_name: str,
    policy: RetryPolicy,
    logger: Any,
    retriable_exceptions: tuple[type[BaseException], ...] = (Exception,),
    on_retry: Callable[[BaseException, int], None] | None = None,
) -> Iterator[T]:
    last_error: BaseException | None = None
    attempt = 1
    while attempt <= policy.attempts:
        try:
            yield from open_stream()
            return
        except retriable_exceptions as exc:  # noqa: PERF203
            last_error = exc
            if attempt >= policy.attempts:
                raise
            if on_retry:
                on_retry(exc, attempt)
            _sleep_before_retry(
                operation_name=operation_name,
                policy=policy,
                logger=logger,
                attempt=attempt,
                error=exc,
            )
            attempt += 1

    raise RuntimeError(f"Unexpected retry state for operation {operation_name}.") from last_error


def _retry_delay_seconds(policy: RetryPolicy, attempt: int) -> float:
    delay = min(
        policy.max_delay_seconds,
        policy.initial_delay_seconds * (policy.backoff_multiplier ** (attempt - 1)),
    )
    if policy.jitter_seconds > 0:
        delay += SECURE_RNG.uniform(0, policy.jitter_seconds)
    return delay


def _sleep_before_retry(
    *,
    operation_name: str,
    policy: RetryPolicy,
    logger: Any,
    attempt: int,
    error: BaseException,
) -> None:
    delay = _retry_delay_seconds(policy, attempt)
    logger.warning(
        "Retrying %s after attempt %s/%s due to %s: %s (sleep %.2fs)",
        operation_name,
        attempt,
        policy.attempts,
        type(error).__name__,
        error,
        delay,
    )
    time.sleep(delay)

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

# Overridable in tests.
_sleep = asyncio.sleep
_monotonic = time.monotonic


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How the client retries requests the server could not take right now.

    Only overload (``503`` and ``429``) is retried, never other errors. The wait
    follows the server's ``Retry-After`` when there is one. Otherwise it is an
    exponential backoff with full jitter: ``random(0, min(max_delay,
    base_delay * 2**n))`` for the n-th retry, counted from 0.

    When the policy gives up, the request is dropped and the server's last error
    is raised.

    Attributes:
        max_attempts: Total tries, the first one included. ``1`` disables retries.
        base_delay: Starting point of the backoff, in seconds.
        max_delay: Longest single wait, in seconds. A ``Retry-After`` above it is
            not waited for: the request is dropped.
        total_timeout: Seconds after which no new attempt is started.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    total_timeout: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0 or self.max_delay < 0 or self.total_timeout < 0:
            raise ValueError("delays and timeouts must not be negative")


DEFAULT_RETRY = RetryPolicy()
NO_RETRY = RetryPolicy(max_attempts=1)

_OVERLOAD_STATUSES = frozenset({429, 503})


def retry_after_seconds(response: Any) -> float | None:
    """Return ``Retry-After`` in seconds, `None` if absent or not a number."""
    value = response.headers.get("retry-after")
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None  # HTTP-date form: ignored, we fall back to our own backoff.
    return seconds if seconds >= 0 else None


def is_retryable(response: Any, *, idempotent: bool) -> bool:
    """Whether the answer means "not now, try again".

    A write is only resent when the server said so with ``Retry-After``: SensApp
    sends it when it rejects a write before reading it. A bare ``503`` (storage
    unavailable) may follow a partial write, and SensApp keeps duplicate samples,
    so it is not resent. Reads are always safe to resend.
    """
    if response.status_code not in _OVERLOAD_STATUSES:
        return False
    return idempotent or retry_after_seconds(response) is not None


async def send_with_retry(
    send: Callable[[], Awaitable[Any]],
    policy: RetryPolicy,
    *,
    idempotent: bool,
) -> Any:
    """Call `send` until it succeeds, is not retryable, or the policy gives up.

    Giving up returns the last response, so the caller reports the server's own
    error. Connection errors and timeouts are not retried: a write may already
    have been processed.
    """
    started = _monotonic()
    attempt = 0
    while True:
        response = await send()
        attempt += 1
        if attempt >= policy.max_attempts:
            return response
        if not is_retryable(response, idempotent=idempotent):
            return response

        delay = retry_after_seconds(response)
        if delay is None:
            ceiling = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1))
            delay = random.uniform(0, ceiling)
        elif delay > policy.max_delay:
            return response

        if _monotonic() - started + delay > policy.total_timeout:
            return response
        await _sleep(delay)

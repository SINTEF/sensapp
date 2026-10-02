from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import niquests.exceptions as http_errors

# Overridable in tests.
_sleep = asyncio.sleep
_monotonic = time.monotonic


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How the client retries requests that could not be served right now.

    Retried: the server is overloaded or unavailable (``503``, ``429``, ``504``), the
    connection fails, or the request times out. Never retried: other errors, which would
    come back the same. The wait follows the server's ``Retry-After`` when there is one.
    Otherwise it is an exponential backoff with full jitter: ``random(0, min(max_delay,
    base_delay * 2**n))`` for the n-th retry, counted from 0.

    A write that timed out may have been stored, so a retry can store its samples twice:
    that is accepted, the vacuum operation of the server removes duplicate samples.

    When the policy gives up, the request is dropped and the last error is raised: the
    server's error, or the exception of the connection.

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

_RETRYABLE_STATUSES = frozenset({429, 503, 504})

# A failure of the connection or a timeout. A bad certificate or a bad proxy
# configuration will not get better by waiting.
_RETRYABLE_ERRORS = (
    http_errors.ConnectionError,
    http_errors.Timeout,
    http_errors.ChunkedEncodingError,
)
_PERMANENT_ERRORS = (http_errors.SSLError, http_errors.ProxyError)


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


def is_retryable_status(response: Any) -> bool:
    """Whether the answer means "not now, try again"."""
    return response.status_code in _RETRYABLE_STATUSES


def is_retryable_error(error: BaseException) -> bool:
    """Whether the connection failed in a way that may not happen again."""
    return isinstance(error, _RETRYABLE_ERRORS) and not isinstance(
        error, _PERMANENT_ERRORS
    )


def _give_up(error: BaseException | None, response: Any) -> Any:
    """End the retries: raise the last exception, or return the last response."""
    if error is not None:
        raise error
    return response


async def send_with_retry(
    send: Callable[[], Awaitable[Any]],
    policy: RetryPolicy,
) -> Any:
    """Call `send` until it succeeds, fails for good, or the policy gives up.

    Giving up returns the last response, so that the caller reports the server's own
    error, or raises the last exception of the connection.
    """
    started = _monotonic()
    attempt = 0
    while True:
        attempt += 1
        error: BaseException | None = None
        response: Any = None
        try:
            response = await send()
        except Exception as caught:
            if not is_retryable_error(caught):
                raise
            error = caught
        else:
            if not is_retryable_status(response):
                return response

        if attempt >= policy.max_attempts:
            return _give_up(error, response)

        delay = retry_after_seconds(response) if response is not None else None
        if delay is None:
            ceiling = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1))
            delay = random.uniform(0, ceiling)
        elif delay > policy.max_delay:
            return _give_up(error, response)

        if _monotonic() - started + delay > policy.total_timeout:
            return _give_up(error, response)
        await _sleep(delay)

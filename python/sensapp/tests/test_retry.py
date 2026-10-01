from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from sensapp import RetryPolicy, SensAppClient, _retry
from sensapp._exceptions import SensAppHTTPError

from .conftest import MockResponse

OVERLOADED = {"retry-after": "2"}


def _busy(retry_after: str | None = "2") -> MockResponse:
    headers = {} if retry_after is None else {"retry-after": retry_after}
    response = MockResponse.error(503, "busy")
    response.headers = headers
    return response


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr(_retry, "_sleep", fake_sleep)
    return recorded


def _client(
    retry: RetryPolicy | None = _retry.DEFAULT_RETRY,
) -> tuple[SensAppClient, AsyncMock]:
    client = SensAppClient("http://sensapp.test", retry=retry)
    session = AsyncMock()
    client._session = session
    return client, session


async def test_write_rejected_with_retry_after_is_resent_after_that_delay(
    sleeps: list[float],
) -> None:
    client, session = _client()
    session.post.side_effect = [_busy("2"), MockResponse.text_ok("ok")]

    assert await client.publish("temperature", 21.5) == "ok"
    assert session.post.await_count == 2
    assert sleeps == [2.0]


async def test_bare_503_on_a_write_is_not_resent(sleeps: list[float]) -> None:
    # No Retry-After: the storage may have been reached, SensApp keeps duplicates.
    client, session = _client()
    session.post.return_value = _busy(None)

    with pytest.raises(SensAppHTTPError) as exc:
        await client.publish("temperature", 21.5)
    assert exc.value.status_code == 503
    assert session.post.await_count == 1
    assert sleeps == []


async def test_reads_are_retried_even_without_retry_after(
    sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_retry.random, "uniform", lambda low, high: high)
    client, session = _client()
    session.get.side_effect = [_busy(None), MockResponse.json_ok({"status": "ok"})]

    assert (await client.health_live()).status == "ok"
    assert session.get.await_count == 2
    assert sleeps == [0.1]


async def test_client_errors_are_never_retried(sleeps: list[float]) -> None:
    client, session = _client()
    bad_request = MockResponse.error(400, "nope")
    bad_request.headers = OVERLOADED
    session.post.return_value = bad_request

    with pytest.raises(SensAppHTTPError):
        await client.publish("temperature", 21.5)
    assert session.post.await_count == 1


async def test_gives_up_after_max_attempts_with_the_servers_error(
    sleeps: list[float],
) -> None:
    client, session = _client(RetryPolicy(max_attempts=3))
    session.post.return_value = _busy("1")

    with pytest.raises(SensAppHTTPError) as exc:
        await client.publish("temperature", 21.5)
    assert exc.value.status_code == 503
    assert session.post.await_count == 3
    assert sleeps == [1.0, 1.0]


async def test_backoff_is_exponential_with_full_jitter(
    sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    bounds: list[tuple[float, float]] = []

    def fake_uniform(low: float, high: float) -> float:
        bounds.append((low, high))
        return high

    monkeypatch.setattr(_retry.random, "uniform", fake_uniform)
    client, session = _client(
        RetryPolicy(max_attempts=5, base_delay=0.1, max_delay=0.5)
    )
    session.get.return_value = _busy(None)

    with pytest.raises(SensAppHTTPError):
        await client.health_live()
    # 0.1, 0.2, 0.4, then capped at max_delay: the lower bound is always 0.
    assert bounds == [(0, 0.1), (0, 0.2), (0, 0.4), (0, 0.5)]


async def test_retry_after_longer_than_max_delay_drops_the_request(
    sleeps: list[float],
) -> None:
    client, session = _client(RetryPolicy(max_delay=10.0))
    session.post.return_value = _busy("3600")

    with pytest.raises(SensAppHTTPError):
        await client.publish("temperature", 21.5)
    assert session.post.await_count == 1
    assert sleeps == []


async def test_stops_retrying_when_the_total_timeout_would_be_exceeded(
    sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = iter([0.0, 9.0, 9.0, 9.0])
    monkeypatch.setattr(_retry, "_monotonic", lambda: next(clock))
    client, session = _client(RetryPolicy(max_attempts=10, total_timeout=10.0))
    session.post.return_value = _busy("2")

    with pytest.raises(SensAppHTTPError):
        await client.publish("temperature", 21.5)
    # 9 s elapsed + a 2 s wait would pass the 10 s budget.
    assert session.post.await_count == 1
    assert sleeps == []


async def test_retry_none_disables_retries(sleeps: list[float]) -> None:
    client, session = _client(retry=None)
    session.post.return_value = _busy("1")

    with pytest.raises(SensAppHTTPError):
        await client.publish("temperature", 21.5)
    assert session.post.await_count == 1


def test_policy_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="negative"):
        RetryPolicy(base_delay=-1)


def test_http_date_retry_after_is_ignored() -> None:
    response = _busy("Wed, 21 Oct 2026 07:28:00 GMT")
    assert _retry.retry_after_seconds(response) is None

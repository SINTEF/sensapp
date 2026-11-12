from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from niquests import exceptions as http_errors

from sensapp import RetryPolicy, SensAppClient, SensAppValidationError, _retry
from sensapp._exceptions import SensAppHTTPError

from .conftest import MockResponse

SERIES_UUID = "9d87123d-9b47-466d-9eda-001c2ecf9c54"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(_retry, "_sleep", fake_sleep)


def _client(token: str | None = None) -> tuple[SensAppClient, AsyncMock]:
    client = SensAppClient("http://sensapp.test", token=token)
    session = AsyncMock()
    client._session = session
    return client, session


# ── vacuum ──────────────────────────────────────────────────────


async def test_vacuum_posts_and_parses_the_count() -> None:
    client, session = _client(token="t")
    session.post.return_value = MockResponse.json_ok(
        {"status": "ok", "duplicates_removed": 12}
    )

    result = await client.admin.vacuum()

    assert (result.status, result.duplicates_removed) == ("ok", 12)
    assert session.post.call_args[0][0] == "http://sensapp.test/api/v1/admin/vacuum"
    assert session.post.call_args[1]["headers"] == {"authorization": "Bearer t"}


async def test_vacuum_count_is_none_where_the_backend_cannot_deduplicate() -> None:
    client, session = _client()
    session.post.return_value = MockResponse.json_ok(
        {"status": "ok", "duplicates_removed": None}
    )

    assert (await client.admin.vacuum()).duplicates_removed is None


async def test_vacuum_waits_longer_than_the_server_maintenance_timeout() -> None:
    client, session = _client()
    session.post.return_value = MockResponse.json_ok({"status": "ok"})

    await client.admin.vacuum()
    assert session.post.call_args[1]["timeout"] > 3600.0

    await client.admin.vacuum(timeout=60.0)
    assert session.post.call_args[1]["timeout"] == 60.0


async def test_vacuum_is_not_retried_after_a_504() -> None:
    # The server carries on after a 504: a second vacuum would compete with the first
    client, session = _client()
    session.post.return_value = MockResponse.error(504, "timeout")

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.vacuum()

    assert raised.value.status_code == 504
    assert session.post.await_count == 1


async def test_vacuum_is_not_retried_after_a_lost_connection() -> None:
    client, session = _client()
    session.post.side_effect = http_errors.ConnectionError("reset")

    with pytest.raises(http_errors.ConnectionError):
        await client.admin.vacuum()

    assert session.post.await_count == 1


async def test_vacuum_403_raises() -> None:
    client, session = _client()
    session.post.return_value = MockResponse.error(
        403, '{"error": "Sensor-scoped tokens cannot run database-wide maintenance"}'
    )

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.vacuum()

    assert raised.value.status_code == 403


# ── deletes ─────────────────────────────────────────────────────


async def test_delete_series_sends_delete_and_returns_nothing() -> None:
    client, session = _client(token="t")
    session.delete.return_value = MockResponse(status_code=204)

    assert await client.admin.delete_series(SERIES_UUID) is None

    assert session.delete.call_args[0][0] == (
        f"http://sensapp.test/series/{SERIES_UUID}"
    )
    assert session.delete.call_args[1]["headers"] == {"authorization": "Bearer t"}


async def test_delete_series_unknown_is_a_404() -> None:
    client, session = _client()
    session.delete.return_value = MockResponse.error(404, '{"error": "not found"}')

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.delete_series(SERIES_UUID)

    assert raised.value.status_code == 404


async def test_delete_samples_sends_inclusive_bounds_and_parses_the_count() -> None:
    client, session = _client()
    session.delete.return_value = MockResponse.json_ok(
        {"series_uuid": SERIES_UUID, "deleted_samples": 3}
    )

    result = await client.admin.delete_samples(
        SERIES_UUID,
        start=datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
        end="2026-03-16T13:00:00Z",
    )

    assert (result.series_uuid, result.deleted_samples) == (SERIES_UUID, 3)
    assert session.delete.call_args[0][0] == (
        f"http://sensapp.test/series/{SERIES_UUID}/samples"
    )
    assert session.delete.call_args[1]["params"] == {
        "start": "2026-03-16T12:00:00+00:00",
        "end": "2026-03-16T13:00:00Z",
    }


async def test_delete_samples_refuses_a_naive_datetime() -> None:
    client, session = _client()

    with pytest.raises(SensAppValidationError):
        await client.admin.delete_samples(
            SERIES_UUID,
            start=datetime(2026, 3, 16, 12, 0),
            end=datetime(2026, 3, 16, 13, 0, tzinfo=UTC),
        )

    session.delete.assert_not_called()


@pytest.mark.parametrize("naive", ["2026-03-16T12:00:00", "2026-03-16"])
async def test_delete_samples_refuses_a_time_string_without_a_time_zone(
    naive: str,
) -> None:
    # The server would read it as UTC
    client, session = _client()

    with pytest.raises(SensAppValidationError):
        await client.admin.delete_samples(
            SERIES_UUID, start=naive, end="2026-03-17T00:00:00Z"
        )

    session.delete.assert_not_called()


async def test_delete_samples_keeps_the_offset_of_the_datetime() -> None:
    client, session = _client()
    session.delete.return_value = MockResponse.json_ok(
        {"series_uuid": SERIES_UUID, "deleted_samples": 0}
    )
    paris = timezone(timedelta(hours=2))

    await client.admin.delete_samples(
        SERIES_UUID,
        start=datetime(2026, 3, 16, 14, 0, tzinfo=paris),
        end=datetime(2026, 3, 16, 15, 0, tzinfo=paris),
    )

    assert session.delete.call_args[1]["params"]["start"] == "2026-03-16T14:00:00+02:00"


async def test_deletes_retry_an_overloaded_server() -> None:
    client, session = _client()
    session.delete.side_effect = [
        MockResponse.error(503, "busy"),
        MockResponse.json_ok({"series_uuid": SERIES_UUID, "deleted_samples": 1}),
    ]

    result = await client.admin.delete_samples(
        SERIES_UUID, start="2026-03-16T12:00:00Z", end="2026-03-16T13:00:00Z"
    )

    assert result.deleted_samples == 1
    assert session.delete.await_count == 2


async def test_deletes_are_not_retried_after_a_504() -> None:
    # The server may still be deleting: a second delete would compete with it
    client, session = _client()
    session.delete.return_value = MockResponse.error(504, "timeout")

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.delete_series(SERIES_UUID)

    assert raised.value.status_code == 504
    assert session.delete.await_count == 1


async def test_deletes_are_not_retried_after_a_timeout() -> None:
    client, session = _client()
    session.delete.side_effect = http_errors.ReadTimeout("slow")

    with pytest.raises(http_errors.ReadTimeout):
        await client.admin.delete_samples(
            SERIES_UUID, start="2026-03-16T12:00:00Z", end="2026-03-16T13:00:00Z"
        )

    assert session.delete.await_count == 1


async def test_deletes_retry_a_connection_that_could_not_open() -> None:
    client, session = _client()
    session.delete.side_effect = [
        http_errors.ConnectTimeout("no route"),
        MockResponse(status_code=204),
    ]

    await client.admin.delete_series(SERIES_UUID)

    assert session.delete.await_count == 2


async def test_deletes_follow_the_retry_policy_of_the_client() -> None:
    client = SensAppClient("http://sensapp.test", retry=RetryPolicy(max_attempts=1))
    session = AsyncMock()
    session.delete.return_value = MockResponse.error(503, "busy")
    client._session = session

    with pytest.raises(SensAppHTTPError):
        await client.admin.delete_series(SERIES_UUID)

    assert session.delete.await_count == 1


async def test_delete_501_raises_where_the_backend_cannot_delete() -> None:
    client, session = _client()
    session.delete.return_value = MockResponse.error(501, "unsupported")

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.delete_series(SERIES_UUID)

    assert raised.value.status_code == 501


# ── tokens ──────────────────────────────────────────────────────

CREATED = {
    "token": "eyJ.secret.token",
    "subject": "edge-device-7",
    "scope": ["write"],
    "sensors": ["temperature"],
    "expires_at": 1_800_000_000,
    "jti": "5d5f7d2e",
}


async def test_create_token_sends_the_request_and_parses_the_answer() -> None:
    client, session = _client(token="admin-token")
    session.post.return_value = MockResponse.json_ok(CREATED)

    created = await client.admin.create_token(
        "edge-device-7", ["write"], duration_seconds=86400, sensors=["temperature"]
    )

    assert created.token == "eyJ.secret.token"
    assert created.scope == ["write"]
    assert created.sensors == ["temperature"]
    assert created.expires_at == 1_800_000_000
    assert session.post.call_args[0][0] == "http://sensapp.test/api/v1/admin/tokens"
    assert session.post.call_args[1]["json"] == {
        "subject": "edge-device-7",
        "scope": ["write"],
        "duration_seconds": 86400,
        "sensors": ["temperature"],
    }
    assert session.post.call_args[1]["headers"] == {
        "authorization": "Bearer admin-token"
    }


async def test_create_token_without_sensors_leaves_them_out() -> None:
    client, session = _client()
    session.post.return_value = MockResponse.json_ok({**CREATED, "sensors": None})

    created = await client.admin.create_token("me", ["read"], duration_seconds=60)

    assert "sensors" not in session.post.call_args[1]["json"]
    assert created.sensors is None


async def test_created_token_is_not_in_its_repr() -> None:
    client, session = _client()
    session.post.return_value = MockResponse.json_ok(CREATED)

    created = await client.admin.create_token("me", ["read"], duration_seconds=60)

    assert "secret" not in repr(created)
    assert "edge-device-7" in repr(created)


async def test_create_token_is_not_retried() -> None:
    # A retry after a lost answer would make a second token, valid until it expires
    client, session = _client()
    session.post.side_effect = http_errors.ConnectionError("reset")

    with pytest.raises(http_errors.ConnectionError):
        await client.admin.create_token("me", ["read"], duration_seconds=60)

    assert session.post.await_count == 1


@pytest.mark.parametrize("status", [403, 404])
async def test_create_token_refusals_raise(status: int) -> None:
    # 403: not an admin token, or an admin one was asked for; 404: authentication is off
    client, session = _client()
    session.post.return_value = MockResponse.error(status, '{"error": "no"}')

    with pytest.raises(SensAppHTTPError) as raised:
        await client.admin.create_token("me", ["admin"], duration_seconds=60)

    assert raised.value.status_code == status


# ── last sample and availability ────────────────────────────────


def _last(sensor_type: str, value: object) -> dict[str, object]:
    return {
        "series_uuid": SERIES_UUID,
        "sensor_name": "temperature",
        "sensor_type": sensor_type,
        "unit": "degC",
        "timestamp": "2026-03-16T12:00:00.250000+00:00",
        "value": value,
    }


async def test_last_sample_parses_the_answer() -> None:
    client, session = _client()
    session.get.return_value = MockResponse.json_ok(_last("float", 21.5))

    sample = await client.last_sample(SERIES_UUID)

    assert session.get.call_args[0][0] == (
        f"http://sensapp.test/series/{SERIES_UUID}/last"
    )
    assert session.get.call_args[1]["params"] == {}
    assert sample.timestamp == datetime(2026, 3, 16, 12, 0, 0, 250000, tzinfo=UTC)
    assert (sample.sensor_name, sample.unit, sample.value) == (
        "temperature",
        "degC",
        21.5,
    )


async def test_last_sample_sends_its_window() -> None:
    client, session = _client()
    session.get.return_value = MockResponse.json_ok(_last("integer", 4))

    await client.last_sample(
        SERIES_UUID, start="2026-03-16T00:00:00Z", end=datetime(2026, 3, 17, tzinfo=UTC)
    )

    assert session.get.call_args[1]["params"] == {
        "start": "2026-03-16T00:00:00Z",
        "end": "2026-03-17T00:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("sensor_type", "sent", "expected"),
    [
        ("float", "NaN", "nan"),
        ("float", "+Inf", float("inf")),
        ("float", "-Inf", float("-inf")),
        ("numeric", "12.345678901234567890", Decimal("12.345678901234567890")),
        ("string", "NaN", "NaN"),
        ("boolean", True, True),
        ("blob", [104, 105], b"hi"),
        ("location", {"longitude": 10.4, "latitude": 63.4}, None),
    ],
)
async def test_last_sample_values_by_type(
    sensor_type: str, sent: object, expected: object
) -> None:
    client, session = _client()
    session.get.return_value = MockResponse.json_ok(_last(sensor_type, sent))

    value = (await client.last_sample(SERIES_UUID)).value

    if expected == "nan":
        assert value != value  # NaN is the only float not equal to itself
    elif expected is None:
        assert value == sent
    else:
        assert value == expected
        assert type(value) is type(expected)


async def test_last_sample_without_a_sample_is_a_404() -> None:
    client, session = _client()
    session.get.return_value = MockResponse.error(404, '{"error": "no sample"}')

    with pytest.raises(SensAppHTTPError) as raised:
        await client.last_sample(SERIES_UUID)

    assert raised.value.status_code == 404


AVAILABILITY = {
    "series_uuid": SERIES_UUID,
    "sensor_name": "temperature",
    "start": "2026-03-16T12:00:00+00:00",
    "end": "2026-03-16T13:00:00+00:00",
    "present": True,
    "sample_count": 4,
    "first_sample_at": "2026-03-16T12:01:00+00:00",
    "last_sample_at": "2026-03-16T12:40:00+00:00",
    "step": "10m",
    "covered_buckets": 4,
    "total_buckets": 7,
    "coverage_ratio": 4 / 7,
}


async def test_availability_with_a_step_reports_the_buckets() -> None:
    client, session = _client()
    session.get.return_value = MockResponse.json_ok(AVAILABILITY)

    result = await client.availability(
        SERIES_UUID,
        start=datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
        end=datetime(2026, 3, 16, 13, 0, tzinfo=UTC),
        step="10m",
    )

    assert session.get.call_args[0][0] == (
        f"http://sensapp.test/series/{SERIES_UUID}/availability"
    )
    assert session.get.call_args[1]["params"] == {
        "start": "2026-03-16T12:00:00+00:00",
        "end": "2026-03-16T13:00:00+00:00",
        "step": "10m",
    }
    assert result.present is True
    assert result.sample_count == 4
    assert result.first_sample_at == datetime(2026, 3, 16, 12, 1, tzinfo=UTC)
    assert (result.covered_buckets, result.total_buckets) == (4, 7)
    assert result.coverage_ratio == pytest.approx(4 / 7)


async def test_availability_without_a_step_has_no_bucket_fields() -> None:
    client, session = _client()
    session.get.return_value = MockResponse.json_ok(
        {
            **AVAILABILITY,
            "present": False,
            "sample_count": 0,
            "first_sample_at": None,
            "last_sample_at": None,
            "step": None,
            "covered_buckets": None,
            "total_buckets": None,
            "coverage_ratio": None,
        }
    )

    result = await client.availability(
        SERIES_UUID, start="2026-03-16T12:00:00Z", end="2026-03-16T13:00:00Z"
    )

    assert "step" not in session.get.call_args[1]["params"]
    assert result.present is False
    assert result.first_sample_at is None
    assert result.coverage_ratio is None

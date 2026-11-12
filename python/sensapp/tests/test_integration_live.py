from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import polars as pl
import pytest

from sensapp import (
    SamplePoint,
    SensAppClient,
    SensAppHTTPError,
    build_upload_table_from_polars,
    serialize_arrow_table,
)


def _utc_timestamps(frame: pl.DataFrame) -> list[datetime]:
    """Decoded timestamps, sorted. They must equal what was published: the
    Arrow export used to shift them by the TAI - UTC offset (37 s)."""
    return [
        ts.replace(tzinfo=UTC) for ts in frame.get_column("timestamp").sort().to_list()
    ]


@pytest.mark.integration
async def test_live_publish_polars_and_query_timeseries(
    live_server_url: str,
) -> None:
    sensor_name = f"sdk-polars-{uuid4().hex}"
    now = datetime.now(UTC).replace(microsecond=0)
    frame = pl.DataFrame(
        {
            "timestamp": [now - timedelta(seconds=1), now],
            "value": [21.5, 21.7],
        }
    )

    async with SensAppClient(live_server_url) as client:
        await client.publish(sensor_name, frame)
        result = await client.query(f'{{__name__="{sensor_name}"}}[1h]')

    assert len(result) == 1
    series = result[0]
    assert series.name == sensor_name
    assert series.sensor_type == "float"
    assert series.to_polars().columns == ["timestamp", "value"]
    assert series.to_polars().height == 2
    assert set(series.to_polars().get_column("value").to_list()) == {21.5, 21.7}
    assert _utc_timestamps(series.to_polars()) == [now - timedelta(seconds=1), now]


@pytest.mark.integration
async def test_live_polars_upload_helper_and_get_series(
    live_server_url: str,
) -> None:
    sensor_name = f"sdk-polars-helper-{uuid4().hex}"
    sensor_id = str(uuid4())
    now = datetime.now(UTC).replace(microsecond=0)
    frame = pl.DataFrame(
        {
            "timestamp": [now - timedelta(seconds=1), now],
            "value": [45, 46],
            "sensor_name": [sensor_name, sensor_name],
            "sensor_id": [sensor_id, sensor_id],
        }
    )
    upload_table = build_upload_table_from_polars(frame)

    async with SensAppClient(live_server_url) as client:
        response = await client._post(
            "/publish",
            data=serialize_arrow_table(upload_table),
            headers={"content-type": "application/vnd.apache.arrow.stream"},
        )
        assert response.text == "ok"

        catalog = await client.list_series(metric=sensor_name)
        assert catalog.series, "expected at least one series in the catalog"

        series = await client.get_series(catalog.series[0].uuid)
        simplified = await client.get_series(
            catalog.series[0].uuid, simplify=True, simplify_tolerance=0.001
        )

    assert series.sensor_id == sensor_id
    assert series.name == sensor_name
    assert series.to_polars().columns == ["timestamp", "value"]
    assert set(series.to_polars().get_column("value").to_list()) == {45, 46}
    assert _utc_timestamps(series.to_polars()) == [now - timedelta(seconds=1), now]
    assert simplified.sensor_id == sensor_id


async def _publish_three(
    client: SensAppClient, now: datetime
) -> tuple[str, str, list[datetime]]:
    """Three float samples one second apart, the name and the UUID of the series."""
    name = f"sdk-admin-{uuid4().hex}"
    times = [now - timedelta(seconds=2), now - timedelta(seconds=1), now]
    await client.publish(
        name, [SamplePoint(t, float(i)) for i, t in enumerate(times, start=1)]
    )
    catalog = await client.list_series(metric=name)
    return name, catalog.series[0].uuid, times


async def _count(client: SensAppClient, name: str) -> int:
    series = await client.query(f'{{__name__="{name}"}}[1h]')
    return sum(item.to_polars().height for item in series)


@pytest.mark.integration
async def test_live_last_sample_and_availability(live_server_url: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    async with SensAppClient(live_server_url) as client:
        name, uuid, times = await _publish_three(client, now)

        last = await client.last_sample(uuid)
        before_last = await client.last_sample(uuid, end=times[1])
        window = await client.availability(
            uuid,
            start=now - timedelta(minutes=1),
            end=now + timedelta(minutes=1),
            step="1m",
        )
        elsewhere = await client.availability(
            uuid,
            start=now - timedelta(days=2),
            end=now - timedelta(days=1),
        )

    assert (last.sensor_name, last.value, last.timestamp) == (name, 3.0, times[2])
    assert (before_last.value, before_last.timestamp) == (2.0, times[1])
    assert window.present
    assert window.sample_count == 3
    assert (window.first_sample_at, window.last_sample_at) == (times[0], times[2])
    assert window.total_buckets == 3
    assert window.coverage_ratio is not None and 0 < window.coverage_ratio <= 1
    assert not elsewhere.present
    assert elsewhere.step is None
    assert elsewhere.coverage_ratio is None


@pytest.mark.integration
async def test_live_delete_samples_is_inclusive(live_server_url: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    async with SensAppClient(live_server_url) as client:
        name, uuid, times = await _publish_three(client, now)

        deleted = await client.admin.delete_samples(uuid, start=times[1], end=times[2])
        remaining = await client.query(f'{{__name__="{name}"}}[1h]')

        with pytest.raises(SensAppHTTPError) as raised:
            await client.admin.delete_samples(
                "00000000-0000-4000-8000-000000000000", start=times[0], end=times[2]
            )

    assert (deleted.series_uuid, deleted.deleted_samples) == (uuid, 2)
    assert remaining[0].to_polars().get_column("value").to_list() == [1.0]
    assert raised.value.status_code == 404


@pytest.mark.integration
async def test_live_delete_series_then_it_is_gone(live_server_url: str) -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    async with SensAppClient(live_server_url) as client:
        name, uuid, _ = await _publish_three(client, now)

        await client.admin.delete_series(uuid)
        left = await client.list_series(metric=name)

        with pytest.raises(SensAppHTTPError) as raised:
            await client.admin.delete_series(uuid)

    assert left.series == []
    assert raised.value.status_code == 404


@pytest.mark.integration
async def test_live_vacuum_removes_a_duplicated_sample(live_server_url: str) -> None:
    sample = [SamplePoint(datetime.now(UTC).replace(microsecond=0), 21.5)]
    name = f"sdk-vacuum-{uuid4().hex}"

    async with SensAppClient(live_server_url) as client:
        await client.publish(name, sample)
        await client.publish(name, sample)

        result = await client.admin.vacuum()
        count = await _count(client, name)

    assert result.status == "ok"
    # The backends of CI remove duplicates. ClickHouse only estimates how many when
    # something else writes during the vacuum, so the count of samples is the check.
    assert isinstance(result.duplicates_removed, int)
    assert count == 1


@pytest.mark.integration
async def test_live_create_token_needs_authentication(live_server_url: str) -> None:
    # The live server runs without authentication: there is nothing to sign a token with
    async with SensAppClient(live_server_url) as client:
        with pytest.raises(SensAppHTTPError) as raised:
            await client.admin.create_token("me", ["read"], duration_seconds=60)

    assert raised.value.status_code == 404

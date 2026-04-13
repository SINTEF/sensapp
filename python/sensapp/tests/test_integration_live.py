from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import polars as pl
import pytest

from sensapp import (
    SensAppClient,
    build_upload_table_from_polars,
    serialize_arrow_table,
)


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

    assert series.sensor_id == sensor_id
    assert series.name == sensor_name
    assert series.to_polars().columns == ["timestamp", "value"]
    assert set(series.to_polars().get_column("value").to_list()) == {45, 46}

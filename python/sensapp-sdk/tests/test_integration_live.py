from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from sensapp_sdk import SamplePoint, SensAppClient


@pytest.mark.integration
def test_live_publish_and_query_arrow(live_server_url: str) -> None:
    now = datetime.now(timezone.utc)

    with SensAppClient(live_server_url) as client:
        client.publish_samples(
            sensor_name="temperature",
            samples=[
                SamplePoint(now - timedelta(seconds=1), 21.5),
                SamplePoint(now, 21.7),
            ],
        )

        rows = client.query_rows("temperature[1h]")

    assert len(rows) == 2
    assert all(row["sensor_name"] == "temperature" for row in rows)
    assert {row["value"] for row in rows} == {"21.5", "21.7"}


@pytest.mark.integration
def test_live_list_series_and_fetch_series_rows(live_server_url: str) -> None:
    with SensAppClient(live_server_url) as client:
        client.publish_csv(
            "datetime,sensor_name,value,unit\n"
            "2026-03-16T12:00:00Z,humidity,45.0,%\n"
            "2026-03-16T12:01:00Z,humidity,46.0,%\n"
        )

        catalog = client.list_series(metric="humidity")
        datasets = catalog.get("dcat:dataset", [])

        assert datasets, "Expected at least one humidity series in the catalog"

        series_uuid = datasets[0]["dct:identifier"]
        rows = client.get_series_rows(series_uuid)

    assert len(rows) == 2
    assert {row["value"] for row in rows} == {45.0, 46.0}


@pytest.mark.integration
def test_live_query_csv_and_jsonl_formats(live_server_url: str) -> None:
    now = datetime.now(timezone.utc)

    with SensAppClient(live_server_url) as client:
        client.publish_csv(
            f"datetime,sensor_name,value,unit\n{now.isoformat()},pressure,1013.2,hPa\n"
        )

        csv_payload = client.query_csv("pressure[1h]")
        jsonl_rows = client.query_jsonl("pressure[1h]")

    assert csv_payload.startswith("timestamp,sensor_id,sensor_name,value,type")
    assert len(jsonl_rows) == 1
    assert jsonl_rows[0]["sensor_name"] == "pressure"


@pytest.mark.integration
def test_live_query_sensor_rows_supports_hyphenated_sensor_names(
    live_server_url: str,
) -> None:
    now = datetime.now(timezone.utc)

    with SensAppClient(live_server_url) as client:
        client.publish_samples(
            sensor_name="demo-temperature",
            samples=[
                SamplePoint(now - timedelta(seconds=1), 20.5),
                SamplePoint(now, 20.7),
            ],
        )

        rows = client.query_sensor_rows("demo-temperature", window="1h")

    assert len(rows) == 2
    assert all(row["sensor_name"] == "demo-temperature" for row in rows)

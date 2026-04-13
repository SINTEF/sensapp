from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import polars as pl
import pyarrow as pa
import pytest

from sensapp import (
    SamplePoint,
    SensAppClient,
    __version__,
    serialize_arrow_table,
)
from sensapp._exceptions import SensAppHTTPError

from .conftest import MockResponse

# ── helpers ─────────────────────────────────────────────────────


def _mock_client(
    token: str | None = None,
) -> tuple[SensAppClient, AsyncMock]:
    """Return a client with a fully-mocked session."""
    client = SensAppClient("http://sensapp.test", token=token)
    mock_session = AsyncMock()
    client._session = mock_session
    return client, mock_session


def _arrow_response_table() -> pa.Table:
    return pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 0, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["sensor-1"],
            "sensor_name": ["temperature"],
            "sensor_type": ["float"],
            "unit": ["degC"],
            "labels": ['{"room":"lab"}'],
            "float_value": [21.5],
        }
    )


def _single_series_response_table() -> pa.Table:
    schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us"), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ],
        metadata={
            b"sensapp.sensor.uuid": b"series-uuid",
            b"sensapp.sensor.name": b"temperature",
            b"sensapp.sensor.type": b"float",
            b"sensapp.sensor.unit": b"degC",
            b"sensapp.sensor.labels": b'{"room":"lab"}',
        },
    )
    return pa.Table.from_arrays(
        [
            pa.array(
                [datetime(2026, 3, 16, 12, 0, tzinfo=UTC)],
                type=pa.timestamp("us"),
            ),
            pa.array([21.5], type=pa.float64()),
        ],
        schema=schema,
    )


# ── health ──────────────────────────────────────────────────────


async def test_health_live_parses_response() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.json_ok({"status": "ok"})

    result = await client.health_live()

    assert result.status == "ok"
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert url.endswith("/health/live")


async def test_health_ready_parses_response() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.json_ok(
        {"status": "ready", "database": "ok", "error": None}
    )

    result = await client.health_ready()

    assert result.status == "ready"
    assert result.database == "ok"
    assert result.error is None


# ── publish ─────────────────────────────────────────────────────


async def test_publish_single_value_sends_arrow() -> None:
    client, session = _mock_client()
    session.post.return_value = MockResponse.text_ok("ok")

    result = await client.publish("temperature", 21.5)

    assert result == "ok"
    session.post.assert_called_once()
    call_kwargs = session.post.call_args
    assert call_kwargs[1]["headers"]["content-type"] == (
        "application/vnd.apache.arrow.stream"
    )
    body = call_kwargs[1]["data"]
    payload = pa.ipc.open_stream(pa.py_buffer(body)).read_all()
    assert payload.column_names == ["timestamp", "value", "sensor_name"]


async def test_publish_sample_list_sends_arrow() -> None:
    client, session = _mock_client()
    session.post.return_value = MockResponse.text_ok("ok")

    await client.publish(
        "temperature",
        [
            SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=UTC), 21.5),
            SamplePoint(datetime(2026, 3, 16, 12, 1, tzinfo=UTC), 21.7),
        ],
    )

    body = session.post.call_args[1]["data"]
    payload = pa.ipc.open_stream(pa.py_buffer(body)).read_all()
    assert payload.num_rows == 2


async def test_publish_polars_frame_sends_arrow() -> None:
    client, session = _mock_client()
    session.post.return_value = MockResponse.text_ok("ok")

    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                datetime(2026, 3, 16, 12, 1, tzinfo=UTC),
            ],
            "value": [21.5, 21.7],
        }
    )

    result = await client.publish("temperature", frame, sensor_id="sensor-1")

    assert result == "ok"
    body = session.post.call_args[1]["data"]
    payload = pa.ipc.open_stream(pa.py_buffer(body)).read_all()
    assert payload["sensor_name"].to_pylist() == ["temperature", "temperature"]
    assert payload["sensor_id"].to_pylist() == ["sensor-1", "sensor-1"]


async def test_publish_with_token_sends_auth_header() -> None:
    client, session = _mock_client(token="secret-token")
    session.post.return_value = MockResponse.text_ok("ok")

    await client.publish("temperature", 21.5)

    headers = session.post.call_args[1]["headers"]
    assert headers["authorization"] == "Bearer secret-token"


# ── query ───────────────────────────────────────────────────────


async def test_query_returns_compact_time_series() -> None:
    table = _arrow_response_table()
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(table))

    result = await client.query("temperature[1h]")

    assert len(result) == 1
    series = result[0]
    assert series.sensor_id == "sensor-1"
    assert series.name == "temperature"
    assert series.sensor_type == "float"
    assert series.unit == "degC"
    assert series.labels == {"room": "lab"}
    assert series.rows == [
        {"timestamp": datetime(2026, 3, 16, 12, 0, tzinfo=UTC), "value": 21.5}
    ]
    url = session.get.call_args[0][0]
    assert "/api/v1/query" in url


async def test_query_returns_multiple_series() -> None:
    table = _arrow_response_table()
    extra = pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 1, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["sensor-2"],
            "sensor_name": ["humidity"],
            "sensor_type": ["integer"],
            "unit": [None],
            "labels": ['{"room":"lab"}'],
            "float_value": [None],
            "integer_value": [45],
        }
    )
    merged = pa.concat_tables(
        [
            table.append_column("integer_value", pa.array([None], type=pa.int64())),
            extra,
        ],
        promote_options="default",
    )
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(merged))

    result = await client.query('{__name__=~"temperature|humidity"}[1h]')

    assert [series.name for series in result] == ["temperature", "humidity"]
    assert result[1].rows == [
        {"timestamp": datetime(2026, 3, 16, 12, 1, tzinfo=UTC), "value": 45}
    ]


async def test_query_returns_polars_and_pandas() -> None:
    table = _arrow_response_table()
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(table))

    [series] = await client.query("temperature[5m]")

    assert isinstance(series.to_polars(), pl.DataFrame)
    assert series.to_polars().columns == ["timestamp", "value"]
    pandas_frame = series.to_pandas()
    assert list(pandas_frame.columns) == ["timestamp", "value"]


async def test_query_one_returns_single_series() -> None:
    table = _arrow_response_table()
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(table))

    series = await client.query_one("temperature[5m]")

    assert series.name == "temperature"
    assert series.rows == [
        {"timestamp": datetime(2026, 3, 16, 12, 0, tzinfo=UTC), "value": 21.5}
    ]


async def test_query_one_rejects_no_series() -> None:
    empty = pa.table(
        {
            "timestamp": pa.array([], type=pa.timestamp("us", tz="UTC")),
            "sensor_id": pa.array([], type=pa.string()),
            "sensor_name": pa.array([], type=pa.string()),
            "sensor_type": pa.array([], type=pa.string()),
            "unit": pa.array([], type=pa.string()),
            "labels": pa.array([], type=pa.string()),
            "float_value": pa.array([], type=pa.float64()),
        }
    )
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(empty))

    with pytest.raises(ValueError, match="no series"):
        await client.query_one("temperature[5m]")


async def test_query_one_rejects_multiple_series() -> None:
    table = _arrow_response_table()
    extra = pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 1, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["sensor-2"],
            "sensor_name": ["humidity"],
            "sensor_type": ["integer"],
            "unit": [None],
            "labels": ['{"room":"lab"}'],
            "float_value": [None],
            "integer_value": [45],
        }
    )
    merged = pa.concat_tables(
        [
            table.append_column("integer_value", pa.array([None], type=pa.int64())),
            extra,
        ],
        promote_options="default",
    )
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(merged))

    with pytest.raises(ValueError, match="expected exactly one series"):
        await client.query_one('{__name__=~"temperature|humidity"}[1h]')


# ── catalog ─────────────────────────────────────────────────────


async def test_list_metrics_returns_typed_catalog() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.json_ok(
        {
            "@type": "dcat:Catalog",
            "dcat:dataset": [
                {
                    "dct:title": "temperature",
                    "dct:identifier": "metric:temperature",
                    "dct:description": "Aggregated metric",
                    "sensor:type": "float",
                    "sensor:seriesCount": 2,
                    "sensor:labelDimensions": ["room"],
                    "dcat:keyword": ["metric", "float"],
                    "dcat:distribution": [],
                }
            ],
        }
    )

    catalog = await client.list_metrics()

    assert len(catalog.metrics) == 1
    assert catalog.metrics[0].name == "temperature"
    assert catalog.metrics[0].series_count == 2


async def test_list_series_returns_typed_catalog_with_pagination() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.json_ok(
        {
            "dcat:dataset": [
                {
                    "dct:identifier": "uuid-1",
                    "dct:title": "temperature",
                    "dct:description": "Sensor data",
                    "sensor:type": "float",
                    "sensor:labels": [{"room": "lab"}],
                    "dcat:keyword": [],
                    "dcat:distribution": [],
                }
            ],
            "hydra:view": {
                "hydra:next": "/series?limit=5&bookmark=page2",
            },
        }
    )

    catalog = await client.list_series(metric="temperature", limit=5)

    assert len(catalog.series) == 1
    assert catalog.series[0].uuid == "uuid-1"
    assert catalog.series[0].labels == {"room": "lab"}
    assert catalog.next_bookmark == "page2"


# ── series data ─────────────────────────────────────────────────


async def test_get_series_returns_arrow_table() -> None:
    table = _single_series_response_table()
    client, session = _mock_client()
    session.get.return_value = MockResponse.bytes_ok(serialize_arrow_table(table))

    result = await client.get_series("series-uuid", step="5m")

    assert result.sensor_id == "series-uuid"
    assert result.name == "temperature"
    assert result.sensor_type == "float"
    assert result.unit == "degC"
    assert result.labels == {"room": "lab"}
    assert result.rows == [{"timestamp": datetime(2026, 3, 16, 12, 0), "value": 21.5}]
    params = session.get.call_args[1]["params"]
    assert params["format"] == "arrow"
    assert params["step"] == "5m"


# ── errors ──────────────────────────────────────────────────────


async def test_http_error_raised_with_json_body() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.error(
        400, '{"BadRequest": "Unsupported format"}'
    )

    with pytest.raises(SensAppHTTPError) as exc:
        await client.query("temperature")

    assert exc.value.status_code == 400
    assert "BadRequest" in str(exc.value)


async def test_http_error_raised_with_plain_text() -> None:
    client, session = _mock_client()
    session.get.return_value = MockResponse.error(500, "server error")

    with pytest.raises(SensAppHTTPError) as exc:
        await client.health_live()

    assert exc.value.status_code == 500
    assert exc.value.details is None


# ── from_env ────────────────────────────────────────────────────


async def test_from_env_reads_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SENSAPP_URL", "http://from-env.test")
    monkeypatch.setenv("SENSAPP_TOKEN", "env-token")

    client = SensAppClient.from_env()

    assert client._base_url == "http://from-env.test"
    assert client._token == "env-token"
    await client.close()


async def test_base_url_property_returns_normalized_value() -> None:
    client, _session = _mock_client()

    assert client.base_url == "http://sensapp.test"
    await client.close()


def test_public_version_is_exposed() -> None:
    assert isinstance(__version__, str)
    assert __version__


# ── context manager ─────────────────────────────────────────────


async def test_async_context_manager_closes_session() -> None:
    client, session = _mock_client()

    async with client:
        pass

    session.close.assert_awaited_once()

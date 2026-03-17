from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeAlias

import httpx
import pyarrow as pa
import pytest

from sensapp_sdk import SamplePoint, SensAppClient, serialize_arrow_table
from sensapp_sdk._exceptions import SensAppHTTPError, SensAppValidationError

Handler: TypeAlias = Callable[[httpx.Request], httpx.Response]
ClientFactory: TypeAlias = Callable[[Handler, str | None], SensAppClient]


@pytest.fixture
def client_factory() -> ClientFactory:
    def factory(handler: Handler, token: str | None = None) -> SensAppClient:
        transport = httpx.MockTransport(handler)
        return SensAppClient("http://sensapp.test", token=token, transport=transport)

    return factory


def test_health_ready_parses_response(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/health/ready"
        return httpx.Response(
            200,
            json={"status": "ready", "database": "ok", "error": None},
        )

    client = client_factory(handler)
    readiness = client.health_ready()

    assert readiness.status == "ready"
    assert readiness.database == "ok"


def test_from_env_reads_url_and_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENSAPP_URL", "http://from-env.test")
    monkeypatch.setenv("SENSAPP_TOKEN", "token-from-env")

    client = SensAppClient.from_env()

    assert str(client._client.base_url) == "http://from-env.test"
    assert client._token == "token-from-env"
    client.close()


def test_constructor_rejects_http_client_and_transport_together() -> None:
    with pytest.raises(SensAppValidationError, match="either http_client or transport"):
        SensAppClient(
            http_client=httpx.Client(),
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
        )


def test_query_arrow_returns_table(client_factory: ClientFactory) -> None:
    table = pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["sensor-1"],
            "sensor_name": ["temperature"],
            "value": ["21.5"],
            "type": ["float"],
            "labels": ['{"room":"lab"}'],
        }
    )
    body = serialize_arrow_table(table)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        assert request.url.params["query"] == "temperature[1h]"
        assert request.url.params["format"] == "arrow"
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "application/vnd.apache.arrow.file"},
        )

    client = client_factory(handler)
    result = client.query_arrow("temperature[1h]")

    assert result.to_pylist() == table.to_pylist()


def test_query_sensor_rows_uses_name_matcher_for_hyphenated_sensor(
    client_factory: ClientFactory,
) -> None:
    table = pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["sensor-1"],
            "sensor_name": ["demo-temperature"],
            "value": ["21.5"],
            "type": ["float"],
            "labels": ['{"room":"lab"}'],
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        assert request.url.params["query"] == '{__name__="demo-temperature"}[5m]'
        assert request.url.params["format"] == "arrow"
        return httpx.Response(200, content=serialize_arrow_table(table))

    client = client_factory(handler)

    assert client.query_sensor_rows("demo-temperature", window="5m") == table.to_pylist()


def test_query_sensor_csv_includes_label_matchers(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["query"] == '{__name__="temperature",room="lab",site="north"}[1h]'
        assert request.url.params["format"] == "csv"
        return httpx.Response(200, text="timestamp,value\n2026-03-16T12:00:00Z,21.5\n")

    client = client_factory(handler)

    assert "timestamp,value" in client.query_sensor_csv(
        "temperature",
        window="1h",
        labels={"room": "lab", "site": "north"},
    )


def test_publish_samples_sends_arrow_with_auth_header(
    client_factory: ClientFactory,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/publish"
        assert request.headers["authorization"] == "Bearer token-123"
        assert request.headers["content-type"] == "application/vnd.apache.arrow.file"
        assert request.content.startswith(b"ARROW1")
        return httpx.Response(200, text="ok")

    client = client_factory(handler, token="token-123")
    response = client.publish_samples(
        sensor_name="temperature",
        samples=[SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc), 21.5)],
    )

    assert response == "ok"


def test_publish_csv_sets_content_type(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"] == "text/csv"
        assert request.content == b"datetime,sensor_name,value\n"
        return httpx.Response(200, text="ok")

    client = client_factory(handler)

    assert client.publish_csv("datetime,sensor_name,value\n") == "ok"


def test_publish_senml_sets_json_content_type(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"] == "application/json"
        assert request.content == b'[{"n":"temperature","v":21.5}]'
        return httpx.Response(200, text="ok")

    client = client_factory(handler)

    assert client.publish_senml([{"n": "temperature", "v": 21.5}]) == "ok"


def test_write_influx_passes_expected_query_parameters(
    client_factory: ClientFactory,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v2/write"
        assert request.url.params["bucket"] == "sensapp"
        assert request.url.params["org"] == "demo"
        assert request.url.params["precision"] == "ms"
        assert request.headers["content-type"] == "text/plain"
        return httpx.Response(204)

    client = client_factory(handler)
    client.write_influx(
        "temperature value=21.5 1710590400000",
        bucket="sensapp",
        org="demo",
        precision="ms",
    )


def test_get_series_arrow_passes_advanced_query_params(
    client_factory: ClientFactory,
) -> None:
    table = pa.table(
        {
            "timestamp": pa.array([], type=pa.timestamp("us", tz="UTC")),
            "sensor_id": pa.array([], type=pa.string()),
            "sensor_name": pa.array([], type=pa.string()),
            "value": pa.array([], type=pa.string()),
            "type": pa.array([], type=pa.string()),
            "labels": pa.array([], type=pa.string()),
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/series/series-123"
        params = request.url.params
        assert params["format"] == "arrow"
        assert params["step"] == "5m"
        assert params["aggregation"] == "avg"
        assert params["simplify"] == "true"
        assert params["simplify_tolerance"] == "0.1"
        return httpx.Response(200, content=serialize_arrow_table(table))

    client = client_factory(handler)
    client.get_series_arrow(
        "series-123",
        step="5m",
        aggregation="avg",
        simplify=True,
        simplify_tolerance=0.1,
    )


def test_get_series_csv_returns_plain_text(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/series/series-123"
        assert request.url.params["format"] == "csv"
        return httpx.Response(200, text="timestamp,value\n2026-03-16T12:00:00Z,21.5\n")

    client = client_factory(handler)

    assert "timestamp,value" in client.get_series_csv("series-123")


def test_get_series_senml_returns_json(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["format"] == "senml"
        return httpx.Response(200, json=[{"n": "temperature", "v": 21.5}])

    client = client_factory(handler)

    assert client.get_series_senml("series-123") == [{"n": "temperature", "v": 21.5}]


def test_list_series_passes_bookmark_and_limit(client_factory: ClientFactory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/series"
        assert request.url.params["limit"] == "5"
        assert request.url.params["bookmark"] == "next-page"
        return httpx.Response(200, json={"dcat:dataset": []})

    client = client_factory(handler)

    assert client.list_series(limit=5, bookmark="next-page") == {"dcat:dataset": []}


def test_jsonl_endpoint_is_parsed_line_by_line(
    client_factory: ClientFactory,
) -> None:
    payload = (
        '{"sensor_name":"temperature","value":21.5}\n'
        '{"sensor_name":"temperature","value":21.7}\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["format"] == "jsonl"
        return httpx.Response(200, text=payload)

    client = client_factory(handler)
    rows = client.query_jsonl("temperature")

    assert rows == [
        {"sensor_name": "temperature", "value": 21.5},
        {"sensor_name": "temperature", "value": 21.7},
    ]


def test_http_errors_are_raised_with_backend_message(
    client_factory: ClientFactory,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"BadRequest": "Unsupported export format"})

    client = client_factory(handler)

    with pytest.raises(SensAppHTTPError) as exc:
        client.query_arrow("temperature")

    assert exc.value.status_code == 400
    assert "BadRequest: Unsupported export format" in str(exc.value)


def test_http_error_preserves_plain_text_response(
    client_factory: ClientFactory,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="plain backend failure")

    client = client_factory(handler)

    with pytest.raises(SensAppHTTPError) as exc:
        client.health_live()

    assert exc.value.details is None
    assert exc.value.response_text == "plain backend failure"

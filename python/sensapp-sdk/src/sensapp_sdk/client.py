from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import httpx
import pyarrow as pa

from ._arrow import build_upload_table, read_arrow_table, serialize_arrow_table
from ._exceptions import SensAppHTTPError, SensAppValidationError
from ._models import (
    HealthStatus,
    JsonValue,
    ReadinessStatus,
    SamplePoint,
    SeriesRequest,
)


class SensAppClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:3000",
        *,
        token: str | None = None,
        timeout: float = 10.0,
        http_client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if http_client is not None and transport is not None:
            raise SensAppValidationError(
                "pass either http_client or transport, not both"
            )

        self._token = token
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
        )

    @classmethod
    def from_env(
        cls,
        *,
        url_var: str = "SENSAPP_URL",
        token_var: str = "SENSAPP_TOKEN",
        default_url: str = "http://127.0.0.1:3000",
    ) -> SensAppClient:
        return cls(
            os.environ.get(url_var, default_url),
            token=os.environ.get(token_var),
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> SensAppClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def health_live(self) -> HealthStatus:
        payload = self._request_json("GET", "/health/live")
        return HealthStatus(status=str(payload["status"]))

    def health_ready(self) -> ReadinessStatus:
        payload = self._request_json("GET", "/health/ready")
        return ReadinessStatus(
            status=str(payload["status"]),
            database=str(payload["database"]),
            error=payload.get("error"),
        )

    def list_metrics(
        self,
        *,
        name: str | None = None,
        name_regex: str | None = None,
        sensor_type: str | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/metrics",
            params={"name": name, "name_regex": name_regex, "type": sensor_type},
        )

    def list_series(
        self,
        *,
        metric: str | None = None,
        selector: str | None = None,
        limit: int | None = None,
        bookmark: str | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/series",
            params={
                "metric": metric,
                "selector": selector,
                "limit": limit,
                "bookmark": bookmark,
            },
        )

    def publish_csv(self, csv_data: str | bytes) -> str:
        return self._request_text(
            "POST",
            "/publish",
            headers={"content-type": "text/csv"},
            content=csv_data,
        )

    def publish_senml(self, payload: JsonValue) -> str:
        return self._request_text(
            "POST",
            "/publish",
            headers={"content-type": "application/json"},
            json_body=payload,
        )

    def publish_arrow(self, table_or_bytes: pa.Table | pa.RecordBatch | bytes) -> str:
        content = (
            table_or_bytes
            if isinstance(table_or_bytes, bytes)
            else serialize_arrow_table(table_or_bytes)
        )
        return self._request_text(
            "POST",
            "/publish",
            headers={"content-type": "application/vnd.apache.arrow.file"},
            content=content,
        )

    def publish_samples(
        self,
        *,
        sensor_name: str,
        samples: list[SamplePoint | tuple[Any, Any] | dict[str, Any]],
        sensor_id: str | UUID | None = None,
        sensor_type: str | None = None,
    ) -> str:
        table = build_upload_table(
            sensor_name,
            samples,
            sensor_id=sensor_id,
            sensor_type=sensor_type,
        )
        return self.publish_arrow(table)

    def write_influx(
        self,
        line_protocol: str,
        *,
        bucket: str,
        org: str | None = None,
        org_id: str | None = None,
        precision: str | None = None,
    ) -> None:
        self._request(
            "POST",
            "/api/v2/write",
            params={
                "bucket": bucket,
                "org": org,
                "org_id": org_id,
                "precision": precision,
            },
            headers={"content-type": "text/plain"},
            content=line_protocol,
        )

    def query_arrow(self, query: str) -> pa.Table:
        response = self._request(
            "GET",
            "/api/v1/query",
            params={"query": query, "format": "arrow"},
        )
        return read_arrow_table(response.content)

    def query_sensor_arrow(
        self,
        sensor_name: str,
        *,
        window: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> pa.Table:
        return self.query_arrow(
            self._build_sensor_query(sensor_name, window=window, labels=labels)
        )

    def query_rows(self, query: str) -> list[dict[str, Any]]:
        return self.query_arrow(query).to_pylist()

    def query_sensor_rows(
        self,
        sensor_name: str,
        *,
        window: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        return self.query_sensor_arrow(
            sensor_name,
            window=window,
            labels=labels,
        ).to_pylist()

    def query_csv(self, query: str) -> str:
        return self._request_text(
            "GET",
            "/api/v1/query",
            params={"query": query, "format": "csv"},
        )

    def query_sensor_csv(
        self,
        sensor_name: str,
        *,
        window: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> str:
        return self.query_csv(
            self._build_sensor_query(sensor_name, window=window, labels=labels)
        )

    def query_senml(self, query: str) -> JsonValue:
        return self._request_json(
            "GET",
            "/api/v1/query",
            params={"query": query, "format": "senml"},
        )

    def query_sensor_senml(
        self,
        sensor_name: str,
        *,
        window: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> JsonValue:
        return self.query_senml(
            self._build_sensor_query(sensor_name, window=window, labels=labels)
        )

    def query_jsonl(self, query: str) -> list[dict[str, Any]]:
        return self._parse_jsonl(
            self._request_text(
                "GET",
                "/api/v1/query",
                params={"query": query, "format": "jsonl"},
            )
        )

    def query_sensor_jsonl(
        self,
        sensor_name: str,
        *,
        window: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        return self.query_jsonl(
            self._build_sensor_query(sensor_name, window=window, labels=labels)
        )

    def get_series_arrow(
        self,
        series_uuid: str | UUID,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        step: str | None = None,
        aggregation: str | None = None,
        simplify: bool | None = None,
        simplify_tolerance: float | None = None,
        simplify_high_quality: bool | None = None,
    ) -> pa.Table:
        request = SeriesRequest(
            series_uuid=series_uuid,
            start=start,
            end=end,
            limit=limit,
            step=step,
            aggregation=aggregation,
            simplify=simplify,
            simplify_tolerance=simplify_tolerance,
            simplify_high_quality=simplify_high_quality,
        )
        response = self._request(
            "GET",
            f"/series/{request.series_uuid}",
            params=self._series_params(request, format_name="arrow"),
        )
        return read_arrow_table(response.content)

    def get_series_rows(
        self,
        series_uuid: str | UUID,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return self.get_series_arrow(series_uuid, **kwargs).to_pylist()

    def get_series_csv(self, series_uuid: str | UUID, **kwargs: Any) -> str:
        request = SeriesRequest(series_uuid=series_uuid, **kwargs)
        return self._request_text(
            "GET",
            f"/series/{request.series_uuid}",
            params=self._series_params(request, format_name="csv"),
        )

    def get_series_senml(self, series_uuid: str | UUID, **kwargs: Any) -> JsonValue:
        request = SeriesRequest(series_uuid=series_uuid, **kwargs)
        return self._request_json(
            "GET",
            f"/series/{request.series_uuid}",
            params=self._series_params(request, format_name="senml"),
        )

    def get_series_jsonl(
        self,
        series_uuid: str | UUID,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        request = SeriesRequest(series_uuid=series_uuid, **kwargs)
        return self._parse_jsonl(
            self._request_text(
                "GET",
                f"/series/{request.series_uuid}",
                params=self._series_params(request, format_name="jsonl"),
            )
        )

    def _series_params(
        self,
        request: SeriesRequest,
        *,
        format_name: str,
    ) -> dict[str, Any]:
        return {
            "format": format_name,
            "start": request.start,
            "end": request.end,
            "limit": request.limit,
            "step": request.step,
            "aggregation": request.aggregation,
            "simplify": request.simplify,
            "simplify_tolerance": request.simplify_tolerance,
            "simplify_high_quality": request.simplify_high_quality,
        }

    def _build_sensor_query(
        self,
        sensor_name: str,
        *,
        window: str | None,
        labels: Mapping[str, str] | None,
    ) -> str:
        matcher_parts = [
            f'__name__="{self._escape_matcher_value(sensor_name)}"',
        ]
        if labels is not None:
            matcher_parts.extend(
                f'{label_name}="{self._escape_matcher_value(label_value)}"'
                for label_name, label_value in labels.items()
            )

        selector = "{" + ",".join(matcher_parts) + "}"
        if window is None:
            return selector
        return f"{selector}[{window}]"

    def _escape_matcher_value(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: str | bytes | None = None,
        json_body: JsonValue | None = None,
    ) -> Any:
        response = self._request(
            method,
            path,
            params=params,
            headers=headers,
            content=content,
            json_body=json_body,
        )
        return response.json()

    def _request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: str | bytes | None = None,
        json_body: JsonValue | None = None,
    ) -> str:
        response = self._request(
            method,
            path,
            params=params,
            headers=headers,
            content=content,
            json_body=json_body,
        )
        return response.text

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: str | bytes | None = None,
        json_body: JsonValue | None = None,
    ) -> httpx.Response:
        merged_headers = dict(headers or {})
        if self._token:
            merged_headers.setdefault("authorization", f"Bearer {self._token}")

        response = self._client.request(
            method,
            path,
            params={
                key: value for key, value in (params or {}).items() if value is not None
            },
            headers=merged_headers,
            content=content,
            json=json_body,
        )
        if response.status_code >= 400:
            raise SensAppHTTPError.from_response(response)
        return response

    def _parse_jsonl(self, payload: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in payload.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(json.loads(stripped))
        return rows

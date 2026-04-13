from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import niquests
import polars as pl

from ._arrow import (
    build_upload_table,
    build_upload_table_from_polars,
    decode_query_time_series,
    decode_single_time_series,
    serialize_arrow_table,
)
from ._exceptions import SensAppHTTPError
from ._models import (
    HealthStatus,
    MetricsCatalog,
    ReadinessStatus,
    SamplePoint,
    SeriesCatalog,
    TimeSeries,
    UploadSensorType,
    UploadValue,
    parse_metrics_catalog,
    parse_series_catalog,
)

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


class SensAppClient:
    """Async client for the SensApp HTTP API.

    Usage::

        async with SensAppClient("http://127.0.0.1:3000") as client:
            await client.publish("temperature", 21.5)
            series = await client.query_one("temperature[1h]")
            print(series.frame)
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:3000",
        *,
        token: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._session = niquests.AsyncSession(timeout=timeout)

    @classmethod
    def from_env(
        cls,
        *,
        url_var: str = "SENSAPP_URL",
        token_var: str = "SENSAPP_TOKEN",
        default_url: str = "http://127.0.0.1:3000",
    ) -> SensAppClient:
        """Build a client from environment variables."""
        return cls(
            os.environ.get(url_var, default_url),
            token=os.environ.get(token_var),
        )

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> SensAppClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def health_live(self) -> HealthStatus:
        data = await self._get_json("/health/live")
        return HealthStatus(status=str(data["status"]))

    async def health_ready(self) -> ReadinessStatus:
        data = await self._get_json("/health/ready")
        return ReadinessStatus(
            status=str(data["status"]),
            database=str(data["database"]),
            error=data.get("error"),
        )

    async def publish(
        self,
        sensor: str,
        value: UploadValue | list[SamplePoint] | pl.DataFrame,
        *,
        sensor_id: str | UUID | None = None,
        sensor_type: UploadSensorType | None = None,
    ) -> str:
        """Publish one sensor's samples as Arrow IPC."""
        if isinstance(value, pl.DataFrame):
            table = build_upload_table_from_polars(
                value,
                sensor_name=sensor,
                sensor_id=sensor_id,
                sensor_type=sensor_type,
            )
        elif isinstance(value, list):
            samples = value
            table = build_upload_table(
                sensor,
                samples,
                sensor_id=sensor_id,
                sensor_type=sensor_type,
            )
        else:
            samples = [SamplePoint(datetime.now(UTC), value)]
            table = build_upload_table(
                sensor,
                samples,
                sensor_id=sensor_id,
                sensor_type=sensor_type,
            )
        body = serialize_arrow_table(table)
        response = await self._post(
            "/publish",
            data=body,
            headers={"content-type": _ARROW_CONTENT_TYPE},
        )
        return response.text

    async def query(self, query: str) -> list[TimeSeries]:
        """Run a query and return all matching series."""
        response = await self._get(
            "/api/v1/query",
            params={"query": query, "format": "arrow"},
        )
        return decode_query_time_series(response.content)

    async def query_one(self, query: str) -> TimeSeries:
        """Run a query that must resolve to exactly one series."""
        series_list = await self.query(query)
        if not series_list:
            raise ValueError(f"query returned no series: {query}")
        if len(series_list) != 1:
            raise ValueError(
                f"query expected exactly one series, got {len(series_list)}: {query}"
            )
        return series_list[0]

    async def list_metrics(
        self,
        *,
        name: str | None = None,
        name_regex: str | None = None,
        sensor_type: str | None = None,
    ) -> MetricsCatalog:
        """Fetch the typed DCAT metrics catalog."""
        data = await self._get_json(
            "/metrics",
            params={
                "name": name,
                "name_regex": name_regex,
                "type": sensor_type,
            },
        )
        return parse_metrics_catalog(data)

    async def list_series(
        self,
        *,
        metric: str | None = None,
        selector: str | None = None,
        limit: int | None = None,
        bookmark: str | None = None,
    ) -> SeriesCatalog:
        """Fetch the typed DCAT series catalog."""
        data = await self._get_json(
            "/series",
            params={
                "metric": metric,
                "selector": selector,
                "limit": limit,
                "bookmark": bookmark,
            },
        )
        return parse_series_catalog(data)

    async def get_series(
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
    ) -> TimeSeries:
        """Fetch one series by UUID and decode it into a compact TimeSeries."""
        params: dict[str, Any] = {
            "format": "arrow",
            "start": start,
            "end": end,
            "limit": limit,
            "step": step,
            "aggregation": aggregation,
            "simplify": simplify,
            "simplify_tolerance": simplify_tolerance,
            "simplify_high_quality": simplify_high_quality,
        }
        response = await self._get(f"/series/{series_uuid}", params=params)
        return decode_single_time_series(response.content)

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        response = await self._get(path, params=params)
        return response.json()

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> niquests.Response:
        url = f"{self._base_url}{path}"
        clean_params = (
            {key: value for key, value in params.items() if value is not None}
            if params
            else None
        )
        response = await self._session.get(
            url,
            params=clean_params,
            headers=self._auth_headers(),
        )
        _check_response(response)
        return response  # type: ignore[return-value]

    async def _post(
        self,
        path: str,
        *,
        data: bytes | str | None = None,
        json_body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> niquests.Response:
        url = f"{self._base_url}{path}"
        merged_headers = {**self._auth_headers(), **(headers or {})}
        response = await self._session.post(
            url,
            data=data,
            json=json_body,
            headers=merged_headers,
        )
        _check_response(response)
        return response  # type: ignore[return-value]

    def _auth_headers(self) -> dict[str, str]:
        if self._token:
            return {"authorization": f"Bearer {self._token}"}
        return {}

    @property
    def base_url(self) -> str:
        """Return the normalized SensApp base URL for this client."""
        return self._base_url


def _check_response(resp: niquests.Response) -> None:
    if resp.status_code < 400:
        return

    details: dict[str, Any] | None = None
    message = resp.text or f"HTTP {resp.status_code}"
    try:
        payload = resp.json()
    except (ValueError, json.JSONDecodeError):
        payload = None

    if isinstance(payload, dict):
        details = payload
        if len(payload) == 1:
            key, value = next(iter(payload.items()))
            message = f"{key}: {value}"
        else:
            message = str(payload)

    raise SensAppHTTPError(
        status_code=resp.status_code,
        message=message,
        details=details,
    )

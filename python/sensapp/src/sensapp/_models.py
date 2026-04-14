from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, TypeAlias
from urllib.parse import parse_qs, urlparse

import polars as pl
import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd

SensorType: TypeAlias = Literal[
    "boolean",
    "integer",
    "float",
    "numeric",
    "string",
    "blob",
    "location",
    "json",
]

UploadSensorType: TypeAlias = Literal[
    "boolean",
    "integer",
    "float",
    "numeric",
    "string",
    "blob",
    "location",
]

UploadValue: TypeAlias = (
    bool | int | float | Decimal | str | bytes | tuple[float, float]
)

TimestampLike: TypeAlias = datetime | str | int | float


@dataclass(slots=True)
class SamplePoint:
    timestamp: TimestampLike
    value: UploadValue


@dataclass(slots=True, frozen=True)
class HealthStatus:
    status: str


@dataclass(slots=True, frozen=True)
class ReadinessStatus:
    status: str
    database: str
    error: str | None = None


@dataclass(slots=True)
class TimeSeries:
    sensor_id: str
    name: str
    sensor_type: SensorType
    frame: pl.DataFrame
    labels: dict[str, str] = field(default_factory=dict)
    unit: str | None = None

    @property
    def rows(self) -> list[dict[str, Any]]:
        return self.frame.to_dicts()

    def __len__(self) -> int:
        return self.frame.height

    def to_arrow(self) -> pa.Table:
        return self.frame.to_arrow()

    def to_polars(self) -> pl.DataFrame:
        return self.frame.clone()

    def to_pandas(self) -> pd.DataFrame:
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "Install `sensapp[pandas]` to use TimeSeries.to_pandas()."
            ) from exc

        return self.frame.to_pandas(use_pyarrow_extension_array=True)


@dataclass(slots=True, frozen=True)
class Distribution:
    url: str
    media_type: str
    format: str
    description: str | None = None


@dataclass(slots=True, frozen=True)
class MetricInfo:
    name: str
    identifier: str
    description: str
    sensor_type: SensorType
    series_count: int
    label_dimensions: list[str]
    unit: str | None = None
    keywords: list[str] = field(default_factory=list)
    distributions: list[Distribution] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class SeriesInfo:
    uuid: str
    name: str
    description: str
    sensor_type: SensorType
    labels: dict[str, str] = field(default_factory=dict)
    unit: str | None = None
    keywords: list[str] = field(default_factory=list)
    distributions: list[Distribution] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class MetricsCatalog:
    metrics: list[MetricInfo]


@dataclass(slots=True, frozen=True)
class SeriesCatalog:
    series: list[SeriesInfo]
    next_bookmark: str | None = None


def parse_metrics_catalog(data: dict[str, Any]) -> MetricsCatalog:
    metrics: list[MetricInfo] = []
    for ds in data.get("dcat:dataset", []):
        metrics.append(
            MetricInfo(
                name=ds["dct:title"],
                identifier=ds["dct:identifier"],
                description=ds.get("dct:description", ""),
                sensor_type=ds["sensor:type"],
                series_count=ds.get("sensor:seriesCount", 0),
                label_dimensions=ds.get("sensor:labelDimensions", []),
                unit=ds.get("sensor:unit"),
                keywords=ds.get("dcat:keyword", []),
                distributions=_parse_distributions(ds.get("dcat:distribution", [])),
            )
        )
    return MetricsCatalog(metrics=metrics)


def parse_series_catalog(data: dict[str, Any]) -> SeriesCatalog:
    series: list[SeriesInfo] = []
    for ds in data.get("dcat:dataset", []):
        labels: dict[str, str] = {}
        for label_obj in ds.get("sensor:labels", []):
            if isinstance(label_obj, dict):
                labels.update(label_obj)

        series.append(
            SeriesInfo(
                uuid=ds["dct:identifier"],
                name=ds["dct:title"],
                description=ds.get("dct:description", ""),
                sensor_type=ds["sensor:type"],
                labels=labels,
                unit=ds.get("sensor:unit"),
                keywords=ds.get("dcat:keyword", []),
                distributions=_parse_distributions(ds.get("dcat:distribution", [])),
            )
        )

    next_bookmark = None
    view = data.get("hydra:view")
    if isinstance(view, dict):
        next_url = view.get("hydra:next", "")
        next_bookmark = _extract_bookmark(next_url)

    return SeriesCatalog(series=series, next_bookmark=next_bookmark)


def _parse_distributions(raw: list[dict[str, Any]]) -> list[Distribution]:
    result: list[Distribution] = []
    for d in raw:
        url = d.get("dcat:downloadURL") or d.get("dcat:accessURL", "")
        result.append(
            Distribution(
                url=url,
                media_type=d.get("dcat:mediaType", ""),
                format=d.get("dct:format", ""),
                description=d.get("dct:description"),
            )
        )
    return result


def _extract_bookmark(url: str) -> str | None:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    bookmarks = params.get("bookmark", [])
    return bookmarks[0] if bookmarks else None

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, overload
from urllib.parse import parse_qs, urlparse

import polars as pl
import pyarrow as pa

from ._exceptions import SensAppValidationError

if TYPE_CHECKING:
    import pandas as pd

type SensorType = Literal[
    "boolean",
    "integer",
    "float",
    "numeric",
    "string",
    "blob",
    "location",
    "json",
]

type UploadSensorType = Literal[
    "boolean",
    "integer",
    "float",
    "numeric",
    "string",
    "blob",
    "location",
]

type UploadValue = bool | int | float | Decimal | str | bytes | tuple[float, float]

type TimestampLike = datetime | str | int | float


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


@dataclass(slots=True, frozen=True)
class LastSample:
    """The most recent sample of a series.

    ``value`` is what the server sent, except for a float (the server writes ``NaN``
    and the infinities as text, here they are floats), a numeric (a `Decimal`) and a
    blob (`bytes`), the types `TimeSeries` has. A location is
    ``{"longitude": ..., "latitude": ...}``.
    """

    series_uuid: str
    sensor_name: str
    sensor_type: SensorType
    timestamp: datetime
    value: Any
    unit: str | None = None


@dataclass(slots=True, frozen=True)
class Availability:
    """Whether a series has samples in a time window, and how well it is covered.

    The bucket fields are ``None`` unless a ``step`` was asked for: the window is cut
    in buckets of that width, and ``coverage_ratio`` is the share of them that hold a
    sample.
    """

    series_uuid: str
    sensor_name: str
    start: datetime
    end: datetime
    present: bool
    sample_count: int
    first_sample_at: datetime | None = None
    last_sample_at: datetime | None = None
    step: str | None = None
    covered_buckets: int | None = None
    total_buckets: int | None = None
    coverage_ratio: float | None = None


@dataclass(slots=True, frozen=True)
class VacuumResult:
    """What a vacuum did.

    ``duplicates_removed`` is ``None`` on a backend that cannot remove duplicates.
    """

    status: str
    duplicates_removed: int | None = None


@dataclass(slots=True, frozen=True)
class DeletedSamples:
    series_uuid: str
    deleted_samples: int


@dataclass(slots=True, frozen=True)
class CreatedToken:
    """A token the server just made. It is shown once: the server keeps nothing."""

    # Out of the repr, so that a token does not end up in a log or a notebook
    token: str = field(repr=False)
    subject: str
    scope: list[str]
    expires_at: int
    jti: str
    sensors: list[str] | None = None


@overload
def format_time(value: datetime | str) -> str: ...
@overload
def format_time(value: None) -> None: ...
def format_time(value: datetime | str | None) -> str | None:
    """The text the server expects for a time bound: ISO 8601 with its offset.

    ``None`` stays ``None``, a bound left out.

    A time without a time zone is refused, a naive `datetime` or an ISO 8601 string
    without an offset (``"2026-03-16T12:00:00"``, ``"2026-03-16"``): the server would
    read it as UTC, and guessing would delete or read the wrong window. Other strings
    go to the server as they are.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value  # Not ISO 8601: the server parses it, or refuses it
        if _is_naive(parsed):
            raise SensAppValidationError(
                f"time {value!r} has no time zone, e.g. '2026-03-16T12:00:00Z'"
            )
        return value
    if _is_naive(value):
        raise SensAppValidationError(
            "datetime must be timezone-aware, e.g. datetime(..., tzinfo=UTC)"
        )
    return value.isoformat()


def _is_naive(value: datetime) -> bool:
    return value.tzinfo is None or value.utcoffset() is None


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _parse_optional_time(value: str | None) -> datetime | None:
    return None if value is None else _parse_time(value)


def _parse_sample_value(sensor_type: str, value: Any) -> Any:
    if sensor_type == "blob" and isinstance(value, list):
        # The server writes the bytes as a list of numbers, `TimeSeries` has `bytes`
        return bytes(value)
    if isinstance(value, str):
        if sensor_type == "float":
            # "NaN", "+Inf" and "-Inf" are texts in the JSON of the server
            return float(value)
        if sensor_type == "numeric":
            return Decimal(value)
    return value


def parse_last_sample(data: dict[str, Any]) -> LastSample:
    sensor_type = data["sensor_type"]
    return LastSample(
        series_uuid=data["series_uuid"],
        sensor_name=data["sensor_name"],
        sensor_type=sensor_type,
        unit=data.get("unit"),
        timestamp=_parse_time(data["timestamp"]),
        value=_parse_sample_value(sensor_type, data["value"]),
    )


def parse_availability(data: dict[str, Any]) -> Availability:
    return Availability(
        series_uuid=data["series_uuid"],
        sensor_name=data["sensor_name"],
        start=_parse_time(data["start"]),
        end=_parse_time(data["end"]),
        present=bool(data["present"]),
        sample_count=int(data["sample_count"]),
        first_sample_at=_parse_optional_time(data.get("first_sample_at")),
        last_sample_at=_parse_optional_time(data.get("last_sample_at")),
        step=data.get("step"),
        covered_buckets=data.get("covered_buckets"),
        total_buckets=data.get("total_buckets"),
        coverage_ratio=data.get("coverage_ratio"),
    )


def parse_vacuum_result(data: dict[str, Any]) -> VacuumResult:
    return VacuumResult(
        status=str(data["status"]),
        duplicates_removed=data.get("duplicates_removed"),
    )


def parse_deleted_samples(data: dict[str, Any]) -> DeletedSamples:
    return DeletedSamples(
        series_uuid=str(data["series_uuid"]),
        deleted_samples=int(data["deleted_samples"]),
    )


def parse_created_token(data: dict[str, Any]) -> CreatedToken:
    return CreatedToken(
        token=data["token"],
        subject=data["subject"],
        scope=list(data["scope"]),
        sensors=data.get("sensors"),
        expires_at=int(data["expires_at"]),
        jti=data["jti"],
    )

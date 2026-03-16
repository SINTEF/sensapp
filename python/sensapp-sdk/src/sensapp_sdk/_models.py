from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, TypeAlias
from uuid import UUID

UploadValue: TypeAlias = (
    bool | int | float | Decimal | str | bytes | tuple[float, float]
)
TimestampLike: TypeAlias = datetime | str | int | float
SensorType: TypeAlias = Literal[
    "boolean",
    "integer",
    "float",
    "numeric",
    "string",
    "blob",
    "location",
]


@dataclass(slots=True)
class SamplePoint:
    timestamp: TimestampLike
    value: UploadValue


@dataclass(slots=True)
class HealthStatus:
    status: str


@dataclass(slots=True)
class ReadinessStatus:
    status: str
    database: str
    error: str | None = None


@dataclass(slots=True)
class SeriesRequest:
    series_uuid: str | UUID
    start: str | None = None
    end: str | None = None
    limit: int | None = None
    step: str | None = None
    aggregation: str | None = None
    simplify: bool | None = None
    simplify_tolerance: float | None = None
    simplify_high_quality: bool | None = None


JsonValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None

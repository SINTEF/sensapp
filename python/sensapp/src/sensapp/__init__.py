"""Async Python client for the SensApp HTTP API."""

from importlib.metadata import PackageNotFoundError, version

from ._arrow import (
    build_upload_table,
    build_upload_table_from_polars,
    read_arrow_table,
    serialize_arrow_table,
)
from ._exceptions import SensAppError, SensAppHTTPError, SensAppValidationError
from ._models import (
    Distribution,
    HealthStatus,
    MetricInfo,
    MetricsCatalog,
    ReadinessStatus,
    SamplePoint,
    SensorType,
    SeriesCatalog,
    SeriesInfo,
    TimeSeries,
    UploadValue,
)
from .client import SensAppClient

try:
    __version__ = version("sensapp")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "Distribution",
    "HealthStatus",
    "MetricInfo",
    "MetricsCatalog",
    "ReadinessStatus",
    "SamplePoint",
    "SensAppClient",
    "SensAppError",
    "SensAppHTTPError",
    "SensAppValidationError",
    "SensorType",
    "SeriesCatalog",
    "SeriesInfo",
    "TimeSeries",
    "UploadValue",
    "__version__",
    "build_upload_table",
    "build_upload_table_from_polars",
    "read_arrow_table",
    "serialize_arrow_table",
]

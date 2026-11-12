"""Async Python client for the SensApp HTTP API."""

from importlib.metadata import PackageNotFoundError, version

from ._admin import AdminClient
from ._arrow import (
    build_upload_table,
    build_upload_table_from_polars,
    read_arrow_table,
    serialize_arrow_table,
)
from ._exceptions import SensAppError, SensAppHTTPError, SensAppValidationError
from ._models import (
    Availability,
    CreatedToken,
    DeletedSamples,
    Distribution,
    HealthStatus,
    LastSample,
    MetricInfo,
    MetricsCatalog,
    ReadinessStatus,
    SamplePoint,
    SensorType,
    SeriesCatalog,
    SeriesInfo,
    TimeSeries,
    UploadValue,
    VacuumResult,
)
from ._retry import RetryPolicy
from .client import SensAppClient

try:
    __version__ = version("sensapp")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "AdminClient",
    "Availability",
    "CreatedToken",
    "DeletedSamples",
    "Distribution",
    "HealthStatus",
    "LastSample",
    "MetricInfo",
    "MetricsCatalog",
    "ReadinessStatus",
    "RetryPolicy",
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
    "VacuumResult",
    "__version__",
    "build_upload_table",
    "build_upload_table_from_polars",
    "read_arrow_table",
    "serialize_arrow_table",
]

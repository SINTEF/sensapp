from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from ._exceptions import SensAppValidationError
from ._models import (
    SamplePoint,
    SensorType,
    TimeSeries,
    TimestampLike,
    UploadSensorType,
)


def serialize_arrow_table(table: pa.Table | pa.RecordBatch) -> bytes:
    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])

    sink = pa.BufferOutputStream()
    options = _stream_write_options()
    with ipc.new_stream(sink, table.schema, options=options) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def read_arrow_table(data: bytes) -> pa.Table:
    source = pa.py_buffer(data)
    try:
        return ipc.open_stream(source).read_all()
    except (pa.ArrowInvalid, OSError):
        return ipc.open_file(source).read_all()


def decode_query_time_series(data: bytes) -> list[TimeSeries]:
    table = read_arrow_table(data)
    if table.num_rows == 0:
        return []

    if "sensor_id" not in table.column_names:
        return [decode_single_time_series(data)]

    ordered_sensor_ids: list[str] = []
    seen: set[str] = set()
    for sensor_id in table.column("sensor_id").to_pylist():
        if sensor_id not in seen:
            seen.add(sensor_id)
            ordered_sensor_ids.append(sensor_id)

    return [_extract_multi_series(table, sensor_id) for sensor_id in ordered_sensor_ids]


def decode_single_time_series(data: bytes) -> TimeSeries:
    table = read_arrow_table(data)
    compact_table = _compact_single_series_table(table)
    metadata = _decode_schema_metadata(table.schema.metadata)

    sensor_id = metadata.get("sensapp.sensor.uuid") or _first_scalar(table, "sensor_id")
    sensor_name = metadata.get("sensapp.sensor.name") or _first_scalar(
        table, "sensor_name"
    )
    sensor_type = metadata.get("sensapp.sensor.type") or _first_scalar(
        table, "sensor_type"
    )
    if sensor_type is None:
        sensor_type = _infer_sensor_type_from_arrow(
            compact_table.schema.field("value").type
        )

    return TimeSeries(
        sensor_id=sensor_id or "",
        name=sensor_name or sensor_id or "",
        sensor_type=sensor_type,
        unit=metadata.get("sensapp.sensor.unit") or _first_scalar(table, "unit"),
        labels=_parse_labels_json(
            metadata.get("sensapp.sensor.labels") or _first_scalar(table, "labels")
        ),
        frame=pl.from_arrow(compact_table),
    )


def build_upload_table(
    sensor_name: str,
    samples: Sequence[SamplePoint | tuple[TimestampLike, Any] | Mapping[str, Any]],
    *,
    sensor_id: UUID | str | None = None,
    sensor_type: UploadSensorType | None = None,
) -> pa.Table:
    normalized = [_normalize_sample(s) for s in samples]
    if not normalized:
        raise SensAppValidationError("samples must not be empty")

    inferred_type = sensor_type or _infer_sensor_type(normalized[0][1])
    timestamps = [_coerce_timestamp(ts) for ts, _ in normalized]
    values = [v for _, v in normalized]

    value_array = _build_value_array(values, inferred_type)
    columns: dict[str, pa.Array] = {
        "timestamp": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
        "value": value_array,
        "sensor_name": pa.array([sensor_name] * len(values), type=pa.string()),
    }

    if sensor_id is not None:
        columns["sensor_id"] = pa.array(
            [str(sensor_id)] * len(values), type=pa.string()
        )

    return pa.table(columns)


def build_upload_table_from_polars(
    frame: pl.DataFrame,
    *,
    sensor_name: str | None = None,
    sensor_id: UUID | str | None = None,
    sensor_type: UploadSensorType | None = None,
) -> pa.Table:
    if frame.is_empty():
        raise SensAppValidationError("frame must not be empty")

    if "timestamp" not in frame.columns or "value" not in frame.columns:
        raise SensAppValidationError(
            "frame must contain 'timestamp' and 'value' columns"
        )

    resolved_sensor_name = sensor_name or _resolve_uniform_string_column(
        frame,
        "sensor_name",
    )
    if resolved_sensor_name is None:
        raise SensAppValidationError(
            "pass sensor_name or include a uniform 'sensor_name' column in the frame"
        )

    resolved_sensor_id = sensor_id or _resolve_uniform_string_column(
        frame,
        "sensor_id",
    )

    samples = [
        SamplePoint(timestamp=row["timestamp"], value=row["value"])
        for row in frame.select(["timestamp", "value"]).to_dicts()
    ]
    return build_upload_table(
        resolved_sensor_name,
        samples,
        sensor_id=resolved_sensor_id,
        sensor_type=sensor_type,
    )


# ── internal helpers ────────────────────────────────────────────


def _normalize_sample(
    sample: SamplePoint | tuple[TimestampLike, Any] | Mapping[str, Any],
) -> tuple[TimestampLike, Any]:
    if isinstance(sample, SamplePoint):
        return sample.timestamp, sample.value

    if isinstance(sample, tuple) and len(sample) == 2:
        return sample[0], sample[1]

    if isinstance(sample, Mapping):
        if "timestamp" not in sample or "value" not in sample:
            raise SensAppValidationError(
                "mapping samples must contain 'timestamp' and 'value' keys"
            )
        return sample["timestamp"], sample["value"]

    raise SensAppValidationError(
        "samples must be SamplePoint, (timestamp, value) tuples, or mappings"
    )


def _coerce_timestamp(value: TimestampLike) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)

    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise SensAppValidationError(f"invalid timestamp string: {value}") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    raise SensAppValidationError(f"unsupported timestamp type: {type(value)!r}")


def _infer_sensor_type(value: Any) -> UploadSensorType:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "blob"
    if _is_location_value(value):
        return "location"
    raise SensAppValidationError(f"cannot infer sensor type for {type(value)!r}")


def _build_value_array(
    values: Iterable[Any],
    sensor_type: UploadSensorType,
) -> pa.Array:
    materialized = list(values)

    if sensor_type == "boolean":
        return pa.array([bool(v) for v in materialized], type=pa.bool_())
    if sensor_type == "integer":
        return pa.array([int(v) for v in materialized], type=pa.int64())
    if sensor_type == "float":
        return pa.array([float(v) for v in materialized], type=pa.float64())
    if sensor_type == "numeric":
        return pa.array(materialized, type=pa.decimal128(38, 18))
    if sensor_type == "string":
        return pa.array([str(v) for v in materialized], type=pa.string())
    if sensor_type == "blob":
        return pa.array([bytes(v) for v in materialized], type=pa.binary())
    if sensor_type == "location":
        latitudes: list[float] = []
        longitudes: list[float] = []
        for v in materialized:
            lat, lon = _coerce_location(v)
            latitudes.append(lat)
            longitudes.append(lon)
        return pa.StructArray.from_arrays(
            [
                pa.array(latitudes, type=pa.float64()),
                pa.array(longitudes, type=pa.float64()),
            ],
            fields=[
                pa.field("latitude", pa.float64()),
                pa.field("longitude", pa.float64()),
            ],
        )

    raise SensAppValidationError(f"unsupported sensor type: {sensor_type}")


def _is_location_value(value: Any) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and all(isinstance(item, (int, float)) for item in value)
    ) or (isinstance(value, Mapping) and "latitude" in value and "longitude" in value)


def _coerce_location(value: Any) -> tuple[float, float]:
    if isinstance(value, tuple) and len(value) == 2:
        return float(value[0]), float(value[1])
    if isinstance(value, Mapping) and "latitude" in value and "longitude" in value:
        return float(value["latitude"]), float(value["longitude"])
    raise SensAppValidationError(
        "location values must be (latitude, longitude) tuples or mappings"
    )


def _stream_write_options() -> ipc.IpcWriteOptions:
    try:
        return ipc.IpcWriteOptions(compression="lz4")
    except (TypeError, ValueError):
        return ipc.IpcWriteOptions()


def _extract_multi_series(table: pa.Table, sensor_id: str) -> TimeSeries:
    mask = pc.equal(table.column("sensor_id"), sensor_id)
    group = table.filter(mask)

    sensor_name = _first_scalar(group, "sensor_name") or sensor_id
    sensor_type = _first_scalar(group, "sensor_type") or _first_scalar(group, "type")
    if sensor_type is None:
        raise SensAppValidationError("query response is missing 'sensor_type'")

    compact_table = _compact_multi_series_table(group, sensor_type)
    return TimeSeries(
        sensor_id=sensor_id,
        name=sensor_name,
        sensor_type=sensor_type,
        unit=_first_scalar(group, "unit"),
        labels=_parse_labels_json(_first_scalar(group, "labels")),
        frame=pl.from_arrow(compact_table),
    )


def _compact_single_series_table(table: pa.Table) -> pa.Table:
    if "timestamp" not in table.column_names or "value" not in table.column_names:
        raise SensAppValidationError(
            "single-series Arrow response must contain 'timestamp' and 'value' columns"
        )
    return table.select(["timestamp", "value"])


def _compact_multi_series_table(table: pa.Table, sensor_type: SensorType) -> pa.Table:
    timestamp_column = table.column("timestamp").combine_chunks()

    if "value" in table.column_names:
        value_column = table.column("value").combine_chunks()
    elif sensor_type == "integer":
        value_column = _require_column(table, "integer_value").combine_chunks()
    elif sensor_type == "numeric":
        value_column = _require_column(table, "numeric_value").combine_chunks()
    elif sensor_type == "float":
        value_column = _require_column(table, "float_value").combine_chunks()
    elif sensor_type == "string":
        value_column = _require_column(table, "string_value").combine_chunks()
    elif sensor_type == "boolean":
        value_column = _require_column(table, "boolean_value").combine_chunks()
    elif sensor_type == "blob":
        value_column = _require_column(table, "blob_value").combine_chunks()
    elif sensor_type == "json":
        value_column = _require_column(table, "json_value").combine_chunks()
    elif sensor_type == "location":
        latitude = _require_column(table, "latitude").combine_chunks()
        longitude = _require_column(table, "longitude").combine_chunks()
        value_column = pa.StructArray.from_arrays(
            [latitude, longitude],
            fields=[
                pa.field("latitude", pa.float64(), nullable=True),
                pa.field("longitude", pa.float64(), nullable=True),
            ],
        )
    else:
        raise SensAppValidationError(
            f"unsupported sensor type in query response: {sensor_type}"
        )

    return pa.table([timestamp_column, value_column], names=["timestamp", "value"])


def _require_column(table: pa.Table, column_name: str) -> pa.ChunkedArray:
    if column_name not in table.column_names:
        raise SensAppValidationError(
            f"query response is missing required column '{column_name}'"
        )
    return table.column(column_name)


def _decode_schema_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> dict[str, str]:
    if metadata is None:
        return {}
    return {
        key.decode("utf-8"): value.decode("utf-8") for key, value in metadata.items()
    }


def _parse_labels_json(payload: str | None) -> dict[str, str]:
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SensAppValidationError(f"invalid labels JSON: {payload}") from exc
    if not isinstance(parsed, dict):
        raise SensAppValidationError("labels metadata must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def _first_scalar(table: pa.Table, column_name: str) -> str | None:
    if column_name not in table.column_names or table.num_rows == 0:
        return None
    scalar = table.column(column_name)[0]
    if not scalar.is_valid:
        return None
    value = scalar.as_py()
    return None if value is None else str(value)


def _infer_sensor_type_from_arrow(data_type: pa.DataType) -> SensorType:
    if pa.types.is_int64(data_type):
        return "integer"
    if pa.types.is_decimal(data_type):
        return "numeric"
    if pa.types.is_float64(data_type):
        return "float"
    if pa.types.is_string(data_type):
        return "string"
    if pa.types.is_boolean(data_type):
        return "boolean"
    if pa.types.is_struct(data_type):
        return "location"
    if pa.types.is_binary(data_type):
        return "blob"
    raise SensAppValidationError(f"unsupported Arrow value type: {data_type}")


def _resolve_uniform_string_column(
    frame: pl.DataFrame,
    column_name: str,
) -> str | None:
    if column_name not in frame.columns:
        return None

    values = frame.get_column(column_name).drop_nulls().unique().to_list()
    if not values:
        return None
    if len(values) != 1:
        raise SensAppValidationError(
            f"frame column '{column_name}' must contain a single value"
        )
    return str(values[0])

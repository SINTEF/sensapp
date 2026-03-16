from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.ipc as ipc

from ._exceptions import SensAppValidationError
from ._models import SamplePoint, SensorType, TimestampLike


def serialize_arrow_table(table: pa.Table | pa.RecordBatch) -> bytes:
    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])

    sink = pa.BufferOutputStream()
    with ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def read_arrow_table(data: bytes) -> pa.Table:
    reader = ipc.open_file(pa.py_buffer(data))
    return reader.read_all()


def build_upload_table(
    sensor_name: str,
    samples: Sequence[SamplePoint | tuple[TimestampLike, Any] | Mapping[str, Any]],
    *,
    sensor_id: UUID | str | None = None,
    sensor_type: SensorType | None = None,
) -> pa.Table:
    normalized = [_normalize_sample(sample) for sample in samples]
    if not normalized:
        raise SensAppValidationError("samples must not be empty")

    inferred_type = sensor_type or _infer_sensor_type(normalized[0][1])
    timestamps = [_coerce_timestamp(value[0]) for value in normalized]
    values = [value[1] for value in normalized]

    value_array = _build_value_array(values, inferred_type)
    columns: dict[str, pa.Array] = {
        "timestamp": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
        "value": value_array,
        "sensor_name": pa.array([sensor_name] * len(values), type=pa.string()),
    }

    if sensor_id is not None:
        columns["sensor_id"] = pa.array(
            [str(sensor_id)] * len(values),
            type=pa.string(),
        )

    return pa.table(columns)


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
        "samples must be SamplePoint objects, (timestamp, value) tuples, or mappings"
    )


def _coerce_timestamp(value: TimestampLike) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise SensAppValidationError(f"invalid timestamp string: {value}") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    raise SensAppValidationError(f"unsupported timestamp type: {type(value)!r}")


def _infer_sensor_type(value: Any) -> SensorType:
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


def _build_value_array(values: Iterable[Any], sensor_type: SensorType) -> pa.Array:
    materialized = list(values)

    if sensor_type == "boolean":
        return pa.array([bool(value) for value in materialized], type=pa.bool_())
    if sensor_type == "integer":
        return pa.array([int(value) for value in materialized], type=pa.int64())
    if sensor_type == "float":
        return pa.array([float(value) for value in materialized], type=pa.float64())
    if sensor_type == "numeric":
        return pa.array(materialized, type=pa.decimal128(38, 18))
    if sensor_type == "string":
        return pa.array([str(value) for value in materialized], type=pa.string())
    if sensor_type == "blob":
        return pa.array([bytes(value) for value in materialized], type=pa.binary())
    if sensor_type == "location":
        latitudes: list[float] = []
        longitudes: list[float] = []
        for value in materialized:
            latitude, longitude = _coerce_location(value)
            latitudes.append(latitude)
            longitudes.append(longitude)
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

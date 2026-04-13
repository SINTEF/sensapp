from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from sensapp import (
    SamplePoint,
    build_upload_table,
    build_upload_table_from_polars,
    read_arrow_table,
    serialize_arrow_table,
)
from sensapp._exceptions import SensAppValidationError


def test_build_upload_table_for_float_samples() -> None:
    table = build_upload_table(
        "temperature",
        [
            SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=UTC), 21.5),
            SamplePoint("2026-03-16T12:01:00Z", 21.7),
        ],
        sensor_id="9d87123d-9b47-466d-9eda-001c2ecf9c54",
    )

    assert table.column_names == [
        "timestamp",
        "value",
        "sensor_name",
        "sensor_id",
    ]
    assert table.schema.field("value").type == pa.float64()
    assert table["sensor_name"].to_pylist() == ["temperature", "temperature"]


def test_build_upload_table_for_numeric_samples() -> None:
    table = build_upload_table(
        "power_cost",
        [
            SamplePoint(
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                Decimal("12.34"),
            ),
            SamplePoint(
                datetime(2026, 3, 16, 12, 1, tzinfo=UTC),
                Decimal("12.56"),
            ),
        ],
    )

    assert table.schema.field("value").type == pa.decimal128(38, 18)


def test_build_upload_table_for_location_samples() -> None:
    table = build_upload_table(
        "vehicle_position",
        [
            SamplePoint(
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                (63.4305, 10.3951),
            ),
        ],
    )

    value_field = table.schema.field("value")
    assert pa.types.is_struct(value_field.type)
    assert table["value"].to_pylist() == [{"latitude": 63.4305, "longitude": 10.3951}]


@pytest.mark.parametrize(
    ("sample_value", "expected_type"),
    [
        (True, pa.bool_()),
        (7, pa.int64()),
        ("ok", pa.string()),
        (b"abc", pa.binary()),
    ],
)
def test_build_upload_table_supports_multiple_scalar_types(
    sample_value: Any,
    expected_type: pa.DataType,
) -> None:
    table = build_upload_table(
        "generic_sensor",
        [
            SamplePoint(
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                sample_value,
            ),
        ],
    )

    assert table.schema.field("value").type == expected_type


def test_build_upload_table_accepts_mapping_samples() -> None:
    table = build_upload_table(
        "temperature",
        [{"timestamp": "2026-03-16T12:00:00Z", "value": 21.5}],
    )

    assert table["value"].to_pylist() == [21.5]


def test_build_upload_table_from_polars_uses_explicit_sensor_name() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                datetime(2026, 3, 16, 12, 1, tzinfo=UTC),
            ],
            "value": [21.5, 21.7],
        }
    )

    table = build_upload_table_from_polars(
        frame,
        sensor_name="temperature",
        sensor_id="sensor-123",
    )

    assert table.column_names == ["timestamp", "value", "sensor_name", "sensor_id"]
    assert table["sensor_name"].to_pylist() == ["temperature", "temperature"]
    assert table["sensor_id"].to_pylist() == ["sensor-123", "sensor-123"]


def test_build_upload_table_from_polars_uses_uniform_sensor_name_column() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 3, 16, 12, 0, tzinfo=UTC)],
            "value": [21.5],
            "sensor_name": ["temperature"],
        }
    )

    table = build_upload_table_from_polars(frame)

    assert table["sensor_name"].to_pylist() == ["temperature"]


def test_build_upload_table_from_polars_uses_uniform_sensor_id_column() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 3, 16, 12, 0, tzinfo=UTC)],
            "value": [21.5],
            "sensor_name": ["temperature"],
            "sensor_id": ["sensor-123"],
        }
    )

    table = build_upload_table_from_polars(frame)

    assert table["sensor_id"].to_pylist() == ["sensor-123"]


def test_build_upload_table_from_polars_rejects_missing_required_columns() -> None:
    frame = pl.DataFrame({"value": [21.5]})

    with pytest.raises(SensAppValidationError, match="timestamp"):
        build_upload_table_from_polars(frame, sensor_name="temperature")


def test_build_upload_table_from_polars_rejects_multiple_sensor_names() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                datetime(2026, 3, 16, 12, 1, tzinfo=UTC),
            ],
            "value": [21.5, 21.7],
            "sensor_name": ["temperature", "humidity"],
        }
    )

    with pytest.raises(SensAppValidationError, match="single value"):
        build_upload_table_from_polars(frame)


def test_build_upload_table_from_polars_rejects_multiple_sensor_ids() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                datetime(2026, 3, 16, 12, 1, tzinfo=UTC),
            ],
            "value": [21.5, 21.7],
            "sensor_name": ["temperature", "temperature"],
            "sensor_id": ["sensor-1", "sensor-2"],
        }
    )

    with pytest.raises(SensAppValidationError, match="single value"):
        build_upload_table_from_polars(frame)


def test_build_upload_table_rejects_mapping_without_required_keys() -> None:
    with pytest.raises(SensAppValidationError, match="timestamp"):
        build_upload_table("temperature", [{"value": 21.5}])


def test_build_upload_table_rejects_invalid_timestamp_string() -> None:
    with pytest.raises(SensAppValidationError, match="invalid timestamp"):
        build_upload_table(
            "temperature",
            [SamplePoint("not-a-timestamp", 21.5)],
        )


def test_build_upload_table_rejects_unsupported_value_type() -> None:
    with pytest.raises(SensAppValidationError, match="cannot infer"):
        build_upload_table(
            "temperature",
            [
                SamplePoint(
                    datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
                    object(),
                )
            ],
        )


def test_build_upload_table_rejects_empty_samples() -> None:
    with pytest.raises(SensAppValidationError):
        build_upload_table("temperature", [])


def test_arrow_round_trip() -> None:
    source = build_upload_table(
        "temperature",
        [SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=UTC), 21.5)],
        sensor_id="abc",
    )

    encoded = serialize_arrow_table(source)
    decoded = read_arrow_table(encoded)

    assert decoded.to_pylist() == source.to_pylist()
    assert encoded != b""


def test_read_arrow_table_rejects_invalid_bytes() -> None:
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError, OSError)):
        read_arrow_table(b"not-arrow")

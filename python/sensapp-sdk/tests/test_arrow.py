from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from sensapp_sdk import (
    SamplePoint,
    build_upload_table,
    read_arrow_table,
    serialize_arrow_table,
)
from sensapp_sdk._exceptions import SensAppValidationError


def test_build_upload_table_for_float_samples() -> None:
    table = build_upload_table(
        "temperature",
        [
            SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc), 21.5),
            SamplePoint("2026-03-16T12:01:00Z", 21.7),
        ],
        sensor_id="9d87123d-9b47-466d-9eda-001c2ecf9c54",
    )

    assert table.column_names == ["timestamp", "value", "sensor_name", "sensor_id"]
    assert table.schema.field("value").type == pa.float64()
    assert table["sensor_name"].to_pylist() == ["temperature", "temperature"]


def test_build_upload_table_for_numeric_samples() -> None:
    table = build_upload_table(
        "power_cost",
        [
            SamplePoint(
                datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc),
                Decimal("12.34"),
            ),
            SamplePoint(
                datetime(2026, 3, 16, 12, 1, tzinfo=timezone.utc),
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
                datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc),
                (63.4305, 10.3951),
            ),
        ],
    )

    value_field = table.schema.field("value")
    assert pa.types.is_struct(value_field.type)
    assert table["value"].to_pylist() == [{"latitude": 63.4305, "longitude": 10.3951}]


def test_arrow_round_trip() -> None:
    source = pa.table(
        {
            "timestamp": pa.array(
                [datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "sensor_id": ["abc"],
            "sensor_name": ["temperature"],
            "value": ["21.5"],
            "type": ["float"],
            "labels": ['{"room":"lab"}'],
        }
    )

    encoded = serialize_arrow_table(source)
    decoded = read_arrow_table(encoded)

    assert decoded.to_pylist() == source.to_pylist()


def test_build_upload_table_rejects_empty_samples() -> None:
    with pytest.raises(SensAppValidationError):
        build_upload_table("temperature", [])

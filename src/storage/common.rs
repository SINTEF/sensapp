use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, SensorData, SensorType, TypedSamples};
use crate::storage::{
    Aggregation, SensorAvailabilitySummary, SensorDataQueryOptions, SimplifyOptions,
};
use anyhow::{Result, anyhow};
use hifitime::Unit;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use simplify_polyline::{Point, simplify};
use smallvec::smallvec;
use std::collections::BTreeSet;
use uuid::Uuid;

/// Whether an error comes from a foreign-key violation reported by the database.
///
/// The PostgreSQL-based backends use it to detect a cached sensor id whose sensor was
/// deleted, possibly by another SensApp instance or by hand.
pub fn is_foreign_key_violation(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(|error| error.as_database_error())
            .is_some_and(|error| error.is_foreign_key_violation())
    })
}

/// Whether PostgreSQL aborted a transaction because it was part of a deadlock (SQLSTATE 40P01).
///
/// PostgreSQL rolls the transaction back whole and documents that the application tries again.
pub fn is_deadlock(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(|error| error.as_database_error())
            .is_some_and(|error| error.code().as_deref() == Some("40P01"))
    })
}

/// Names of the per-type sample tables, shared by the SQL backends for bulk deletes.
pub const VALUE_TABLES: [&str; 8] = [
    "blob_values",
    "json_values",
    "location_values",
    "boolean_values",
    "string_values",
    "float_values",
    "numeric_values",
    "integer_values",
];

/// The columns that make two samples of a table the same sample: the series, the time and the value
/// (the coordinates for locations). `time_column` is the name the backend gives the timestamp.
pub fn duplicate_key_columns(table: &str, time_column: &str) -> String {
    match table {
        "location_values" => format!("sensor_id, {time_column}, latitude, longitude"),
        _ => format!("sensor_id, {time_column}, value"),
    }
}

/// Convert a UUID to the 64-bit key of the series in the ClickHouse and BigQuery tables (BigQuery
/// stores the same bits as a signed `INT64`).
///
/// The ids are stored, so this function is part of the on-disk format and must never change:
/// the XOR of the two big-endian 64-bit halves of the UUID. Sensor UUIDs are hashes, so the
/// result is uniformly distributed. Do not use `std`'s `DefaultHasher` here, its algorithm is
/// explicitly allowed to change between Rust releases, which would orphan every stored sample.
pub fn uuid_to_sensor_id(uuid: &Uuid) -> u64 {
    let value = uuid.as_u128();
    ((value >> 64) as u64) ^ (value as u64)
}

/// Id of a unit in the `units` table: the first 8 bytes of the BLAKE3 hash of its name,
/// read as a little-endian integer. Part of the on-disk format, like [`uuid_to_sensor_id`].
pub fn unit_name_to_id(name: &str) -> u64 {
    let hash = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

/// Convert SensAppDateTime to Unix microseconds for database storage
pub fn datetime_to_micros(datetime: &SensAppDateTime) -> i64 {
    // Use to_unix with Microsecond unit to get a f64 in microseconds,
    // then convert to i64. This properly handles the Unix time reference.
    datetime.to_unix(Unit::Microsecond).floor() as i64
}

pub fn apply_query_options(
    mut sensor_data: SensorData,
    options: &SensorDataQueryOptions,
) -> Result<SensorData> {
    if let (Some(step_ms), Some(aggregation)) = (options.step_ms, options.aggregation) {
        sensor_data = aggregate_sensor_data(sensor_data, options.start_time, step_ms, aggregation)?;
    }

    if let Some(simplify_options) = options.simplify {
        sensor_data = simplify_sensor_data(sensor_data, simplify_options)?;
    }

    Ok(sensor_data)
}

pub fn keep_only_last_sample(mut sensor_data: SensorData) -> Option<SensorData> {
    sensor_data.samples = match sensor_data.samples {
        TypedSamples::Integer(mut samples) => TypedSamples::Integer(smallvec![samples.pop()?]),
        TypedSamples::Numeric(mut samples) => TypedSamples::Numeric(smallvec![samples.pop()?]),
        TypedSamples::Float(mut samples) => TypedSamples::Float(smallvec![samples.pop()?]),
        TypedSamples::String(mut samples) => TypedSamples::String(smallvec![samples.pop()?]),
        TypedSamples::Boolean(mut samples) => TypedSamples::Boolean(smallvec![samples.pop()?]),
        TypedSamples::Location(mut samples) => TypedSamples::Location(smallvec![samples.pop()?]),
        TypedSamples::Blob(mut samples) => TypedSamples::Blob(smallvec![samples.pop()?]),
        TypedSamples::Json(mut samples) => TypedSamples::Json(smallvec![samples.pop()?]),
    };

    Some(sensor_data)
}

pub fn summarize_sensor_data_availability(
    sensor_data: SensorData,
    start_time: SensAppDateTime,
    step_ms: Option<i64>,
) -> Result<SensorAvailabilitySummary> {
    let start_us = datetime_to_micros(&start_time);
    let step_us = step_ms
        .map(|value| {
            value
                .checked_mul(1000)
                .ok_or_else(|| anyhow!("step is too large"))
        })
        .transpose()?;

    let (sample_count, first_sample_at, last_sample_at, covered_buckets) = match &sensor_data
        .samples
    {
        TypedSamples::Integer(samples) => {
            availability_stats_for_samples(samples, start_us, step_us)
        }
        TypedSamples::Numeric(samples) => {
            availability_stats_for_samples(samples, start_us, step_us)
        }
        TypedSamples::Float(samples) => availability_stats_for_samples(samples, start_us, step_us),
        TypedSamples::String(samples) => availability_stats_for_samples(samples, start_us, step_us),
        TypedSamples::Boolean(samples) => {
            availability_stats_for_samples(samples, start_us, step_us)
        }
        TypedSamples::Location(samples) => {
            availability_stats_for_samples(samples, start_us, step_us)
        }
        TypedSamples::Blob(samples) => availability_stats_for_samples(samples, start_us, step_us),
        TypedSamples::Json(samples) => availability_stats_for_samples(samples, start_us, step_us),
    };

    Ok(SensorAvailabilitySummary {
        sensor: sensor_data.sensor,
        sample_count,
        first_sample_at,
        last_sample_at,
        covered_buckets,
    })
}

fn availability_stats_for_samples<V>(
    samples: &[Sample<V>],
    start_us: i64,
    step_us: Option<i64>,
) -> (
    usize,
    Option<SensAppDateTime>,
    Option<SensAppDateTime>,
    Option<usize>,
) {
    let covered_buckets = step_us.map(|step_us| {
        samples
            .iter()
            .map(|sample| (datetime_to_micros(&sample.datetime) - start_us).div_euclid(step_us))
            .collect::<BTreeSet<_>>()
            .len()
    });

    (
        samples.len(),
        samples.first().map(|sample| sample.datetime),
        samples.last().map(|sample| sample.datetime),
        covered_buckets,
    )
}

fn aggregate_sensor_data(
    mut sensor_data: SensorData,
    start_time: Option<SensAppDateTime>,
    step_ms: i64,
    aggregation: Aggregation,
) -> Result<SensorData> {
    let origin_us = start_time.as_ref().map(datetime_to_micros).unwrap_or(0);
    let step_us = step_ms
        .checked_mul(1000)
        .ok_or_else(|| anyhow!("step is too large"))?;

    sensor_data.samples = match sensor_data.samples {
        TypedSamples::Float(samples) => {
            aggregate_float_samples(samples.as_slice(), origin_us, step_us, aggregation)?
        }
        TypedSamples::Integer(samples) => {
            aggregate_integer_samples(samples.as_slice(), origin_us, step_us, aggregation)?
        }
        TypedSamples::Numeric(samples) => {
            aggregate_numeric_samples(samples.as_slice(), origin_us, step_us, aggregation)?
        }
        _ => {
            return Err(anyhow!("aggregation is only supported for numeric series"));
        }
    };

    sensor_data.sensor.sensor_type = match &sensor_data.samples {
        TypedSamples::Float(_) => SensorType::Float,
        TypedSamples::Integer(_) => SensorType::Integer,
        TypedSamples::Numeric(_) => SensorType::Numeric,
        _ => sensor_data.sensor.sensor_type,
    };

    if aggregation.output_is_count() {
        sensor_data.sensor.sensor_type = SensorType::Integer;
        sensor_data.sensor.unit = None;
    }

    Ok(sensor_data)
}

pub fn simplify_sensor_data(
    mut sensor_data: SensorData,
    options: SimplifyOptions,
) -> Result<SensorData> {
    sensor_data.samples = match sensor_data.samples {
        TypedSamples::Float(samples) => {
            let keep_indices = simplify_indices_f64(
                &samples,
                |sample| sample.value,
                options.tolerance,
                options.high_quality,
            )?;
            TypedSamples::Float(filter_samples_by_indices(samples, keep_indices))
        }
        TypedSamples::Integer(samples) => {
            let keep_indices = simplify_indices_f64(
                &samples,
                |sample| sample.value as f64,
                options.tolerance,
                options.high_quality,
            )?;
            TypedSamples::Integer(filter_samples_by_indices(samples, keep_indices))
        }
        TypedSamples::Numeric(samples) => {
            let keep_indices = simplify_indices_f64(
                &samples,
                |sample| sample.value.to_f64().unwrap_or(0.0),
                options.tolerance,
                options.high_quality,
            )?;
            TypedSamples::Numeric(filter_samples_by_indices(samples, keep_indices))
        }
        _ => {
            return Err(anyhow!("simplify is only supported for numeric series"));
        }
    };

    Ok(sensor_data)
}

fn filter_samples_by_indices<V>(
    samples: crate::datamodel::sensapp_vec::SensAppVec<Sample<V>>,
    keep_indices: Vec<usize>,
) -> crate::datamodel::sensapp_vec::SensAppVec<Sample<V>> {
    let mut keep_iter = keep_indices.into_iter().peekable();
    let mut filtered = smallvec![];

    for (index, sample) in samples.into_iter().enumerate() {
        if keep_iter.peek().copied() == Some(index) {
            filtered.push(sample);
            keep_iter.next();
        }
    }

    filtered
}

fn simplify_indices_f64<V, F>(
    samples: &[Sample<V>],
    value_fn: F,
    tolerance: f64,
    high_quality: bool,
) -> Result<Vec<usize>>
where
    F: Fn(&Sample<V>) -> f64,
{
    if samples.len() <= 2 {
        return Ok((0..samples.len()).collect());
    }

    let min_ts = datetime_to_micros(&samples[0].datetime);
    let max_ts = datetime_to_micros(&samples[samples.len() - 1].datetime);
    let mut min_val = f64::INFINITY;
    let mut max_val = f64::NEG_INFINITY;
    for sample in samples {
        let value = value_fn(sample);
        min_val = min_val.min(value);
        max_val = max_val.max(value);
    }

    let ts_span = (max_ts - min_ts).max(1) as f64;
    let value_span = (max_val - min_val).abs().max(f64::EPSILON);

    let points: Vec<Point<2, f64>> = samples
        .iter()
        .map(|sample| Point {
            vec: [
                (datetime_to_micros(&sample.datetime) - min_ts) as f64 / ts_span,
                (value_fn(sample) - min_val) / value_span,
            ],
        })
        .collect();

    let simplified = simplify(&points, tolerance, high_quality);
    let mut keep_indices = Vec::with_capacity(simplified.len());
    let mut search_start = 0usize;

    for point in simplified {
        let relative_index = points[search_start..]
            .iter()
            .position(|candidate| *candidate == point)
            .ok_or_else(|| anyhow!("failed to map simplified points back to samples"))?;
        let absolute_index = search_start + relative_index;
        keep_indices.push(absolute_index);
        search_start = absolute_index.saturating_add(1);
    }

    Ok(keep_indices)
}

pub(crate) fn bucket_start(timestamp_us: i64, origin_us: i64, step_us: i64) -> i64 {
    origin_us + (timestamp_us - origin_us).div_euclid(step_us) * step_us
}

fn aggregate_float_samples(
    samples: &[Sample<f64>],
    origin_us: i64,
    step_us: i64,
    aggregation: Aggregation,
) -> Result<TypedSamples> {
    if samples.is_empty() {
        return Ok(TypedSamples::Float(smallvec![]));
    }

    let mut output_float = smallvec![];
    let mut output_int = smallvec![];
    let mut index = 0usize;

    while index < samples.len() {
        let bucket_us = bucket_start(
            datetime_to_micros(&samples[index].datetime),
            origin_us,
            step_us,
        );
        let mut end = index;
        let mut sum = 0.0;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(
                datetime_to_micros(&samples[end].datetime),
                origin_us,
                step_us,
            ) == bucket_us
        {
            let value = samples[end].value;
            sum += value;
            min = min.min(value);
            max = max.max(value);
            last = value;
            count += 1;
            end += 1;
        }

        let datetime = SensAppDateTime::from_unix_microseconds_i64(bucket_us);
        match aggregation {
            Aggregation::Avg => output_float.push(Sample {
                datetime,
                value: sum / count as f64,
            }),
            Aggregation::Min => output_float.push(Sample {
                datetime,
                value: min,
            }),
            Aggregation::Max => output_float.push(Sample {
                datetime,
                value: max,
            }),
            Aggregation::Sum => output_float.push(Sample {
                datetime,
                value: sum,
            }),
            Aggregation::First => output_float.push(Sample {
                datetime,
                value: first,
            }),
            Aggregation::Last => output_float.push(Sample {
                datetime,
                value: last,
            }),
            Aggregation::Latest => output_float.push(Sample {
                datetime: samples[end - 1].datetime,
                value: last,
            }),
            Aggregation::Count => output_int.push(Sample {
                datetime,
                value: count,
            }),
        }

        index = end;
    }

    Ok(if aggregation.output_is_count() {
        TypedSamples::Integer(output_int)
    } else {
        TypedSamples::Float(output_float)
    })
}

fn aggregate_integer_samples(
    samples: &[Sample<i64>],
    origin_us: i64,
    step_us: i64,
    aggregation: Aggregation,
) -> Result<TypedSamples> {
    if samples.is_empty() {
        return Ok(TypedSamples::Integer(smallvec![]));
    }

    let mut output_int = smallvec![];
    let mut output_float = smallvec![];
    let mut index = 0usize;

    while index < samples.len() {
        let bucket_us = bucket_start(
            datetime_to_micros(&samples[index].datetime),
            origin_us,
            step_us,
        );
        let mut end = index;
        let mut sum: i128 = 0;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(
                datetime_to_micros(&samples[end].datetime),
                origin_us,
                step_us,
            ) == bucket_us
        {
            let value = samples[end].value;
            sum += value as i128;
            min = min.min(value);
            max = max.max(value);
            last = value;
            count += 1;
            end += 1;
        }

        let datetime = SensAppDateTime::from_unix_microseconds_i64(bucket_us);
        match aggregation {
            Aggregation::Avg => output_float.push(Sample {
                datetime,
                value: sum as f64 / count as f64,
            }),
            Aggregation::Min => output_int.push(Sample {
                datetime,
                value: min,
            }),
            Aggregation::Max => output_int.push(Sample {
                datetime,
                value: max,
            }),
            Aggregation::Sum => output_int.push(Sample {
                datetime,
                value: i64::try_from(sum).map_err(|_| anyhow!("integer aggregation overflowed"))?,
            }),
            Aggregation::First => output_int.push(Sample {
                datetime,
                value: first,
            }),
            Aggregation::Last => output_int.push(Sample {
                datetime,
                value: last,
            }),
            Aggregation::Latest => output_int.push(Sample {
                datetime: samples[end - 1].datetime,
                value: last,
            }),
            Aggregation::Count => output_int.push(Sample {
                datetime,
                value: count,
            }),
        }

        index = end;
    }

    Ok(if matches!(aggregation, Aggregation::Avg) {
        TypedSamples::Float(output_float)
    } else {
        TypedSamples::Integer(output_int)
    })
}

fn aggregate_numeric_samples(
    samples: &[Sample<Decimal>],
    origin_us: i64,
    step_us: i64,
    aggregation: Aggregation,
) -> Result<TypedSamples> {
    if samples.is_empty() {
        return Ok(TypedSamples::Numeric(smallvec![]));
    }

    let mut output_numeric = smallvec![];
    let mut output_int = smallvec![];
    let mut index = 0usize;

    while index < samples.len() {
        let bucket_us = bucket_start(
            datetime_to_micros(&samples[index].datetime),
            origin_us,
            step_us,
        );
        let mut end = index;
        let mut sum = Decimal::ZERO;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(
                datetime_to_micros(&samples[end].datetime),
                origin_us,
                step_us,
            ) == bucket_us
        {
            let value = samples[end].value;
            sum += value;
            min = min.min(value);
            max = max.max(value);
            last = value;
            count += 1;
            end += 1;
        }

        let datetime = SensAppDateTime::from_unix_microseconds_i64(bucket_us);
        match aggregation {
            Aggregation::Avg => output_numeric.push(Sample {
                datetime,
                value: sum / Decimal::from(count),
            }),
            Aggregation::Min => output_numeric.push(Sample {
                datetime,
                value: min,
            }),
            Aggregation::Max => output_numeric.push(Sample {
                datetime,
                value: max,
            }),
            Aggregation::Sum => output_numeric.push(Sample {
                datetime,
                value: sum,
            }),
            Aggregation::First => output_numeric.push(Sample {
                datetime,
                value: first,
            }),
            Aggregation::Last => output_numeric.push(Sample {
                datetime,
                value: last,
            }),
            Aggregation::Latest => output_numeric.push(Sample {
                datetime: samples[end - 1].datetime,
                value: last,
            }),
            Aggregation::Count => output_int.push(Sample {
                datetime,
                value: count,
            }),
        }

        index = end;
    }

    Ok(if aggregation.output_is_count() {
        TypedSamples::Integer(output_int)
    } else {
        TypedSamples::Numeric(output_numeric)
    })
}

#[cfg(test)]
mod tests {
    // These values are stored in ClickHouse and BigQuery. If one of these tests fails, the on-disk format
    // changed: existing data would no longer be found.
    #[test]
    fn sensor_ids_are_pinned() {
        let id = |uuid: &str| uuid_to_sensor_id(&Uuid::parse_str(uuid).unwrap());
        assert_eq!(
            id("00000000-0000-4000-8000-000000000001"),
            0x8000_0000_0000_4001
        );
        assert_eq!(id("00000000-0000-0000-0000-000000000000"), 0);
        assert_eq!(id("ffffffff-ffff-ffff-ffff-ffffffffffff"), 0);
        assert_eq!(
            id("0123456789abcdeffedcba9876543210"),
            0xffff_ffff_ffff_ffff
        );
        assert_eq!(
            id("9d87123d-9b47-466d-9eda-001c2ecf9c54"),
            0x9d87_123d_9b47_466d ^ 0x9eda_001c_2ecf_9c54
        );
    }

    #[test]
    fn unit_ids_are_pinned() {
        assert_eq!(unit_name_to_id("°C"), UNIT_CELSIUS_ID);
        assert_eq!(unit_name_to_id(""), UNIT_EMPTY_ID);
        assert_ne!(unit_name_to_id("m"), unit_name_to_id("s"));
    }

    const UNIT_CELSIUS_ID: u64 = 14_058_937_354_730_164_320;
    // First 8 bytes of the published BLAKE3 test vector for empty input
    // (af1349b9f5f9a1a6...), read as little-endian.
    const UNIT_EMPTY_ID: u64 = 0xa6a1_f9f5_b949_13af;

    use super::*;
    use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;

    #[test]
    fn test_datetime_to_micros() {
        // Test that the function produces reasonable results
        let datetime = SensAppDateTime::from_unix_seconds(1705315800.0);
        let result = datetime_to_micros(&datetime);
        // Should be in the right ballpark (1705315800 seconds = 1705315800000000 micros)
        assert!((1705315800000000..=1705315800999999).contains(&result));

        // Test that milliseconds convert to reasonable microseconds
        let datetime_millis = SensAppDateTime::from_unix_milliseconds_i64(1705315800123);
        let result = datetime_to_micros(&datetime_millis);
        // Should be reasonable microsecond value
        assert!((1705315800000000..=1705315800999999).contains(&result));
    }

    #[test]
    fn test_datetime_roundtrip_with_microseconds() {
        // Test: from_unix_milliseconds_i64 -> datetime_to_micros -> from_unix_microseconds_i64 -> to_unix_milliseconds
        // This is the path data takes: test creates samples -> storage writes micros -> storage reads micros -> converter to millis
        let input_ms: i64 = 1704067200000; // Jan 1, 2024 00:00:00 UTC

        // Step 1: Create datetime from milliseconds (like test helper does)
        let datetime_in = SensAppDateTime::from_unix_milliseconds_i64(input_ms);

        // Step 2: Convert to microseconds for storage (like postgres publisher does)
        let micros_stored = datetime_to_micros(&datetime_in);

        // Step 3: Read back from storage as microseconds (like postgres queries do)
        let datetime_out = SensAppDateTime::from_unix_microseconds_i64(micros_stored);

        // Step 4: Convert back to milliseconds (like prometheus converter does)
        let output_ms = datetime_out.to_unix_milliseconds().floor() as i64;

        println!("Input ms: {}", input_ms);
        println!("Stored micros: {}", micros_stored);
        println!("Expected micros: {}", input_ms * 1000);
        println!("Output ms: {}", output_ms);
        println!("Diff: {} ms", output_ms - input_ms);

        assert_eq!(
            input_ms, output_ms,
            "Roundtrip should preserve milliseconds"
        );
    }

    #[test]
    fn latest_keeps_the_timestamp_of_the_last_sample_of_each_bucket() {
        use crate::datamodel::{Sensor, SensorType};

        // One sample every 15 s for 3 minutes, from the epoch
        let data = || {
            let samples: smallvec::SmallVec<[Sample<f64>; 4]> = (0..12)
                .map(|index| Sample {
                    datetime: SensAppDateTime::from_unix_seconds(index as f64 * 15.0),
                    value: index as f64,
                })
                .collect();
            let sensor = Sensor {
                uuid: uuid::Uuid::nil(),
                name: "latest".into(),
                sensor_type: SensorType::Float,
                unit: None,
                labels: Default::default(),
            };
            SensorData::new(sensor, TypedSamples::Float(samples))
        };
        let options = |aggregation| SensorDataQueryOptions {
            start_time: Some(SensAppDateTime::from_unix_seconds(0.0)),
            end_time: None,
            limit: None,
            step_ms: Some(60_000),
            aggregation: Some(aggregation),
            simplify: None,
        };
        let seconds_and_values =
            |aggregation| match apply_query_options(data(), &options(aggregation))
                .unwrap()
                .samples
            {
                TypedSamples::Float(samples) => samples
                    .iter()
                    .map(|sample| (sample.datetime.to_unix_seconds() as i64, sample.value))
                    .collect::<Vec<_>>(),
                other => panic!("unexpected samples {other:?}"),
            };

        // `Last` is stamped at the start of its bucket, `Latest` at its sample
        assert_eq!(
            seconds_and_values(Aggregation::Last),
            vec![(0, 3.0), (60, 7.0), (120, 11.0)]
        );
        assert_eq!(
            seconds_and_values(Aggregation::Latest),
            vec![(45, 3.0), (105, 7.0), (165, 11.0)]
        );
    }
}

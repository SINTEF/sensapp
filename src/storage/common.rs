use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use crate::storage::{
    Aggregation, SensorAvailabilitySummary, SensorDataQueryOptions, SimplifyOptions, StorageError,
};
use anyhow::{Result, anyhow};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use uuid::Uuid;

/// The sensors of a batch, one per UUID, in UUID order. A UUID given twice with another name or
/// type is refused: its samples would be written under one identity whatever the other says.
pub fn unique_sensors<'a>(
    sensors: &[&'a Sensor],
) -> std::result::Result<BTreeMap<Uuid, &'a Sensor>, StorageError> {
    let mut unique: BTreeMap<Uuid, &Sensor> = BTreeMap::new();
    for sensor in sensors {
        match unique.get(&sensor.uuid) {
            None => {
                unique.insert(sensor.uuid, sensor);
            }
            Some(first) => {
                let same_name =
                    names_match(sensor, &first.name) || names_match(first, &sensor.name);
                if !same_name || first.sensor_type != sensor.sensor_type {
                    return Err(identity_mismatch(sensor));
                }
            }
        }
    }
    Ok(unique)
}

/// Refuse a write that names a stored sensor UUID with another name or type than the stored ones.
///
/// The name is what the token allow lists check, and the type chooses the table the samples go
/// to: both must be those of the stored sensor. A write that gives no name, only the UUID, appends
/// to the stored series whatever its name (the allow lists then check the UUID, which they do not
/// list unless an administrator put it there). The unit and the labels are not compared, see
/// `docs/DATAMODEL.md`.
pub fn check_sensor_identity(
    sensor: &Sensor,
    stored_name: &str,
    stored_type: &str,
) -> std::result::Result<(), StorageError> {
    let same_type = stored_type.parse::<SensorType>() == Ok(sensor.sensor_type);
    if names_match(sensor, stored_name) && same_type {
        Ok(())
    } else {
        Err(identity_mismatch(sensor))
    }
}

/// Whether the name of `sensor` is `name`, or is no name at all: the importers use the UUID as the
/// name of a series given by its UUID only (SenML without `_name`, Arrow with only `sensor_id`).
fn names_match(sensor: &Sensor, name: &str) -> bool {
    sensor.name == name || sensor.name == sensor.uuid.to_string()
}

fn identity_mismatch(sensor: &Sensor) -> StorageError {
    StorageError::SensorIdentityMismatch {
        uuid: sensor.uuid,
        name: sensor.name.clone(),
        sensor_type: sensor.sensor_type.to_string(),
    }
}

/// Whether a write failed because another transaction deleted one of its sensors: a foreign-key
/// violation on the samples, or a sensor gone during its registration. The write can be tried
/// again.
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub fn sensor_deleted_during_write(error: &anyhow::Error) -> bool {
    is_foreign_key_violation(error)
        || error.is::<crate::storage::pg_sensor_registration::SensorDeletedDuringWrite>()
}

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
/// The ids are stored, so this function is part of the on-disk format and must never change: the
/// first 8 bytes of the BLAKE3 hash of the 16 bytes of the UUID, read as a little-endian integer,
/// like [`unit_name_to_id`]. A hash, and not a fold of the UUID, because SenML and Arrow writers
/// choose their UUIDs, and a fold keeps their structure: the XOR of the two halves gave `(a, b)` and
/// `(b, a)` the same key, and every UUID with equal halves the key `0`. Any 64-bit key can still
/// collide, by chance at billions of series or on purpose in about 2^32 tries, so the registration
/// refuses a UUID whose key belongs to another one (`StorageError::SensorKeyCollision`), and the
/// reads by UUID compare the stored UUID. Do not use `std`'s `DefaultHasher` here, its algorithm is
/// explicitly allowed to change between Rust releases, which would orphan every stored sample.
pub fn uuid_to_sensor_id(uuid: &Uuid) -> u64 {
    let hash = blake3::hash(uuid.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

/// Id of a unit in the `units` table: the first 8 bytes of the BLAKE3 hash of its name,
/// read as a little-endian integer. Part of the on-disk format, like [`uuid_to_sensor_id`].
pub fn unit_name_to_id(name: &str) -> u64 {
    let hash = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

/// Unix microseconds, as every backend stores them: rounded down, exact integer arithmetic.
pub fn datetime_to_micros(datetime: &SensAppDateTime) -> i64 {
    datetime.to_unix_microseconds_i64()
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

/// The indices of the points that a Douglas-Peucker simplification keeps, in order. The same
/// algorithm as `simplify-polyline` (squared distances, a radial distance pass first unless
/// `high_quality`), but iterative: the recursive version went one level deeper per point on some
/// series, and overflowed the stack of a Tokio worker, which aborts the process.
fn simplify_indices(points: &[[f64; 2]], tolerance: f64, high_quality: bool) -> Vec<usize> {
    if points.len() <= 2 {
        return (0..points.len()).collect();
    }
    let tolerance = tolerance * tolerance;
    let sq_dist = |a: [f64; 2], b: [f64; 2]| (a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2);

    // Radial distance: drop the points too close to the last kept one, but keep the last point
    let candidates: Vec<usize> = if high_quality {
        (0..points.len()).collect()
    } else {
        let mut kept = vec![0];
        let mut previous = 0;
        for index in 1..points.len() {
            if sq_dist(points[index], points[previous]) > tolerance {
                kept.push(index);
                previous = index;
            }
        }
        let last = points.len() - 1;
        if previous != last && points[previous] != points[last] {
            kept.push(last);
        }
        kept
    };

    // Squared distance from a point to the segment from `start` to `end`
    let sq_seg_dist = |point: [f64; 2], start: [f64; 2], end: [f64; 2]| {
        let difference = [end[0] - start[0], end[1] - start[1]];
        let mut closest = start;
        let length = difference[0].powi(2) + difference[1].powi(2);
        if length != 0.0 {
            let t = ((point[0] - start[0]) * difference[0] + (point[1] - start[1]) * difference[1])
                / length;
            if t > 1.0 {
                closest = end;
            } else if t > 0.0 {
                closest = [start[0] + difference[0] * t, start[1] + difference[1] * t];
            }
        }
        sq_dist(point, closest)
    };

    // Douglas-Peucker over the candidates, with an explicit stack of segments
    let mut keep = vec![false; candidates.len()];
    keep[0] = true;
    keep[candidates.len() - 1] = true;
    let mut segments = vec![(0, candidates.len() - 1)];
    while let Some((first, last)) = segments.pop() {
        let mut max_sq_dist = tolerance;
        let mut max_index = 0;
        for i in first + 1..last {
            let sq_dist = sq_seg_dist(
                points[candidates[i]],
                points[candidates[first]],
                points[candidates[last]],
            );
            if sq_dist > max_sq_dist {
                max_index = i;
                max_sq_dist = sq_dist;
            }
        }
        if max_sq_dist > tolerance {
            keep[max_index] = true;
            if max_index - first > 1 {
                segments.push((first, max_index));
            }
            if last - max_index > 1 {
                segments.push((max_index, last));
            }
        }
    }
    candidates
        .into_iter()
        .zip(keep)
        .filter_map(|(index, kept)| kept.then_some(index))
        .collect()
}

/// The indices of the samples a simplification keeps. It runs over the finite values, scaled to
/// the unit square: the non-finite ones (an infinity, NaN from an aggregation) have no place on a
/// line, and are all kept.
fn simplify_indices_f64<V, F>(
    samples: &[Sample<V>],
    value_fn: F,
    tolerance: f64,
    high_quality: bool,
) -> Result<Vec<usize>>
where
    F: Fn(&Sample<V>) -> f64,
{
    let finite: Vec<(usize, i64, f64)> = samples
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            (
                index,
                datetime_to_micros(&sample.datetime),
                value_fn(sample),
            )
        })
        .filter(|(_, _, value)| value.is_finite())
        .collect();
    let mut keep: Vec<usize> = if finite.len() <= 2 {
        finite.iter().map(|(index, _, _)| *index).collect()
    } else {
        let min_ts = finite[0].1;
        let max_ts = finite[finite.len() - 1].1;
        let (min_val, max_val) = finite.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(min, max), (_, _, value)| (min.min(*value), max.max(*value)),
        );
        let ts_span = (max_ts - min_ts).max(1) as f64;
        let value_span = (max_val - min_val).abs().max(f64::EPSILON);
        let points: Vec<[f64; 2]> = finite
            .iter()
            .map(|(_, timestamp, value)| {
                [
                    (timestamp - min_ts) as f64 / ts_span,
                    (value - min_val) / value_span,
                ]
            })
            .collect();
        simplify_indices(&points, tolerance, high_quality)
            .into_iter()
            .map(|kept| finite[kept].0)
            .collect()
    };
    if finite.len() < samples.len() {
        keep.extend(
            samples
                .iter()
                .enumerate()
                .filter(|(_, sample)| !value_fn(sample).is_finite())
                .map(|(index, _)| index),
        );
        keep.sort_unstable();
    }
    Ok(keep)
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
                value: i64::try_from(sum).map_err(|_| aggregation_overflow())?,
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

/// A sum of a bucket that the type of the series cannot hold (an `i64`, a `Decimal`): an error of
/// the request, which can ask for a shorter step, and not of the server.
pub(crate) fn aggregation_overflow() -> anyhow::Error {
    crate::storage::StorageError::invalid_data_format(
        "the sum of a bucket does not fit in the type of the series: use a shorter step",
        None,
        None,
    )
    .into()
}

/// The error of a database whose sum left the range of its type: SQLite fails with "integer
/// overflow", PostgreSQL with `numeric_value_out_of_range` (22003). An [`aggregation_overflow`].
#[cfg_attr(
    not(any(feature = "sqlite", feature = "postgres", feature = "timescaledb")),
    allow(dead_code)
)]
pub(crate) fn sql_sum_overflow(error: sqlx::Error) -> anyhow::Error {
    let out_of_range = match &error {
        sqlx::Error::Database(database) => {
            database.code().as_deref() == Some("22003")
                || database.message().contains("integer overflow")
        }
        _ => false,
    };
    if out_of_range {
        aggregation_overflow()
    } else {
        error.into()
    }
}

/// A Numeric aggregate that a database returned as text: a value, or `sum/count` for an average.
/// The average is divided here, so that it is the exact average on every backend: SQL databases
/// give it with their own precision (PostgreSQL with 16 significant digits for large values, DuckDB
/// through a float). A sum that a `Decimal` cannot hold is an [`aggregation_overflow`]; extra
/// digits after the point are rounded.
#[cfg_attr(
    not(any(feature = "postgres", feature = "timescaledb", feature = "duckdb")),
    allow(dead_code)
)]
pub(crate) fn numeric_aggregate_from_text(text: &str) -> Result<Decimal> {
    let (sum, count) = match text.split_once('/') {
        Some((sum, count)) => (sum, Some(count)),
        None => (text, None),
    };
    let sum = Decimal::from_str(sum.trim()).map_err(|error| {
        if error.to_string().contains("overflow") {
            aggregation_overflow()
        } else {
            anyhow!("unreadable Numeric aggregate {text:?}: {error}")
        }
    })?;
    match count {
        None => Ok(sum),
        Some(count) => {
            let count: i64 = count
                .trim()
                .parse()
                .map_err(|error| anyhow!("unreadable count in {text:?}: {error}"))?;
            sum.checked_div(Decimal::from(count))
                .ok_or_else(aggregation_overflow)
        }
    }
}

pub(crate) fn aggregate_numeric_samples(
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
        // `None` once the sum leaves the `Decimal` range: only Sum and Avg need it
        let mut sum = Some(Decimal::ZERO);
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
            sum = sum.and_then(|sum| sum.checked_add(value));
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
                value: sum.ok_or_else(aggregation_overflow)? / Decimal::from(count),
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
                value: sum.ok_or_else(aggregation_overflow)?,
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

    /// The iterative simplification keeps the points that `simplify-polyline` keeps.
    #[test]
    fn simplify_keeps_the_points_of_the_recursive_version() {
        use simplify_polyline::{Point, simplify};
        let mut rng = fastrand::Rng::with_seed(11);
        for round in 0..200 {
            let len = rng.usize(0..300);
            let mut points: Vec<[f64; 2]> = Vec::with_capacity(len);
            let mut x = 0.0;
            for _ in 0..len {
                x += rng.f64() * 0.01;
                // Repeated values too, which the radial pass compares
                points.push([x, (rng.u8(0..20) as f64) / 20.0]);
            }
            let tolerance = [0.0, 0.001, 0.01, 0.1][round % 4];
            let high_quality = round % 2 == 0;
            let reference: Vec<Point<2, f64>> = points.iter().map(|&vec| Point { vec }).collect();
            let expected: Vec<[f64; 2]> = simplify(&reference, tolerance, high_quality)
                .into_iter()
                .map(|point| point.vec)
                .collect();
            let kept: Vec<[f64; 2]> = simplify_indices(&points, tolerance, high_quality)
                .into_iter()
                .map(|index| points[index])
                .collect();
            assert_eq!(kept, expected, "round {round}");
        }
    }

    /// A series whose farthest point is always next to the start of the segment: the recursive
    /// Douglas-Peucker of `simplify-polyline` went one level deeper per point, and 100,000 points
    /// overflowed the 2 MiB stack of a Tokio worker, which aborts the process. On a 128 KiB stack
    /// it already overflows at 1,000 points (measured), so 5,000 points keep the test fast.
    #[test]
    fn simplify_does_not_recurse_with_the_number_of_points() {
        let samples: crate::datamodel::sensapp_vec::SensAppVec<Sample<f64>> = (0..5_000)
            .map(|i| Sample {
                datetime: SensAppDateTime::from_unix_microseconds_i64(i * 1_000_000),
                value: if i % 2 == 0 { 1.0 } else { -1.0 } * 0.99999_f64.powi(i as i32),
            })
            .collect();
        let sensor = Sensor::new(
            uuid::Uuid::new_v4(),
            "zigzag".to_string(),
            SensorType::Float,
            None,
            None,
        );
        let options = SimplifyOptions {
            tolerance: 1e-12,
            high_quality: true,
        };
        let kept = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                simplify_sensor_data(
                    SensorData::new(sensor, TypedSamples::Float(samples)),
                    options,
                )
                .unwrap()
                .samples
                .len()
            })
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(kept, 5_000);
    }
    // These values are stored in ClickHouse and BigQuery. If one of these tests fails, the on-disk format
    // changed: existing data would no longer be found.
    #[test]
    fn sensor_ids_are_pinned() {
        let id = |uuid: &str| uuid_to_sensor_id(&Uuid::parse_str(uuid).unwrap());
        assert_eq!(
            id("00000000-0000-4000-8000-000000000001"),
            0x20f2_b792_5b72_2ee4
        );
        assert_eq!(
            id("9d87123d-9b47-466d-9eda-001c2ecf9c54"),
            0xd44d_b960_a3b6_fd37
        );
        // The XOR of the halves gave these four the keys 0, 0, u64::MAX and u64::MAX
        assert_eq!(
            id("00000000-0000-0000-0000-000000000000"),
            0x0b70_0423_f8df_72e5
        );
        assert_eq!(
            id("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            0x079b_f2bc_6505_87c3
        );
        assert_eq!(
            id("0123456789abcdeffedcba9876543210"),
            0x7b5d_9e94_189f_f3ee
        );
        assert_eq!(
            id("fedcba98765432100123456789abcdef"),
            0x7d78_4ddf_aa2f_9145
        );
    }

    /// The pair of UUIDs that the key collision test of `tests/integration/sensor_identity.rs` uses:
    /// two keys a brute-force search found equal. And the structures that collided under the XOR of
    /// the halves no longer do.
    #[test]
    fn the_pinned_collision_collides_and_structures_do_not() {
        let id = |high: u64, low: u64| uuid_to_sensor_id(&Uuid::from_u64_pair(high, low));
        assert_eq!(id(0, 0xb91c_a660_43fd_e277), 0xb460_1f96_ba04_7f9a);
        assert_eq!(id(0, 0x6cda_2dee_a474_0435), 0xb460_1f96_ba04_7f9a);
        assert_ne!(id(1, 2), id(2, 1));
        assert_ne!(id(3, 3), id(4, 4));
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

    /// Microseconds survive the trip through a `SensAppDateTime`, across 1900 to 2200. The float
    /// conversion lost one microsecond on about a quarter of them outside 2005 to 2038.
    #[test]
    fn microseconds_round_trip_exactly() {
        const YEAR_US: i64 = 31_556_952_000_000;
        let mut rng = fastrand::Rng::with_seed(4);
        let mut values: Vec<i64> = (0..2000)
            .map(|_| rng.i64(-70 * YEAR_US..230 * YEAR_US))
            .collect();
        values.extend([1_073_741_824_001_000, 0, -1, 1]);
        let wrong: Vec<i64> = values
            .into_iter()
            .filter(|&micros| {
                datetime_to_micros(&SensAppDateTime::from_unix_microseconds_i64(micros)) != micros
            })
            .collect();
        assert!(
            wrong.is_empty(),
            "{} wrong, first {:?}",
            wrong.len(),
            wrong.first()
        );
    }

    /// Two values that each fit in a `Decimal` (up to 7.9 × 10^28) but not their sum.
    #[test]
    fn a_numeric_sum_over_the_decimal_range_is_an_error() {
        let large = Decimal::from_i128_with_scale(5 * 10_i128.pow(28), 0);
        let samples = [0.0, 1.0].map(|seconds| Sample {
            datetime: SensAppDateTime::from_unix_seconds(seconds),
            value: large,
        });
        assert!(aggregate_numeric_samples(&samples, 0, 60_000_000, Aggregation::Sum).is_err());
        assert!(aggregate_numeric_samples(&samples, 0, 60_000_000, Aggregation::Avg).is_err());
        assert!(aggregate_numeric_samples(&samples, 0, 60_000_000, Aggregation::Max).is_ok());
    }

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

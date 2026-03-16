use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, SensorData, SensorType, TypedSamples};
use crate::storage::{Aggregation, SensorDataQueryOptions, SimplifyOptions};
use anyhow::{Result, anyhow};
use hifitime::Unit;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use simplify_polyline::{Point, simplify};
use smallvec::smallvec;

/// Convert SensAppDateTime to Unix microseconds for database storage
#[allow(dead_code)] // Used by SQLite backend when enabled
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
            return Err(anyhow!(
                "aggregation is only supported for numeric series"
            ));
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

fn simplify_sensor_data(
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
            return Err(anyhow!(
                "simplify is only supported for numeric series"
            ));
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
        .map(|sample| {
            Point { vec: [
                (datetime_to_micros(&sample.datetime) - min_ts) as f64 / ts_span,
                (value_fn(sample) - min_val) / value_span,
            ] }
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

fn bucket_start(timestamp_us: i64, origin_us: i64, step_us: i64) -> i64 {
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
        let bucket_us = bucket_start(datetime_to_micros(&samples[index].datetime), origin_us, step_us);
        let mut end = index;
        let mut sum = 0.0;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(datetime_to_micros(&samples[end].datetime), origin_us, step_us) == bucket_us
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
            Aggregation::Avg => output_float.push(Sample { datetime, value: sum / count as f64 }),
            Aggregation::Min => output_float.push(Sample { datetime, value: min }),
            Aggregation::Max => output_float.push(Sample { datetime, value: max }),
            Aggregation::Sum => output_float.push(Sample { datetime, value: sum }),
            Aggregation::First => output_float.push(Sample { datetime, value: first }),
            Aggregation::Last => output_float.push(Sample { datetime, value: last }),
            Aggregation::Count => output_int.push(Sample { datetime, value: count }),
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
        let bucket_us = bucket_start(datetime_to_micros(&samples[index].datetime), origin_us, step_us);
        let mut end = index;
        let mut sum: i128 = 0;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(datetime_to_micros(&samples[end].datetime), origin_us, step_us) == bucket_us
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
            Aggregation::Min => output_int.push(Sample { datetime, value: min }),
            Aggregation::Max => output_int.push(Sample { datetime, value: max }),
            Aggregation::Sum => output_int.push(Sample {
                datetime,
                value: i64::try_from(sum).map_err(|_| anyhow!("integer aggregation overflowed"))?,
            }),
            Aggregation::First => output_int.push(Sample { datetime, value: first }),
            Aggregation::Last => output_int.push(Sample { datetime, value: last }),
            Aggregation::Count => output_int.push(Sample { datetime, value: count }),
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
        let bucket_us = bucket_start(datetime_to_micros(&samples[index].datetime), origin_us, step_us);
        let mut end = index;
        let mut sum = Decimal::ZERO;
        let mut min = samples[index].value;
        let mut max = samples[index].value;
        let first = samples[index].value;
        let mut last = samples[index].value;
        let mut count = 0i64;

        while end < samples.len()
            && bucket_start(datetime_to_micros(&samples[end].datetime), origin_us, step_us) == bucket_us
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
            Aggregation::Min => output_numeric.push(Sample { datetime, value: min }),
            Aggregation::Max => output_numeric.push(Sample { datetime, value: max }),
            Aggregation::Sum => output_numeric.push(Sample { datetime, value: sum }),
            Aggregation::First => output_numeric.push(Sample { datetime, value: first }),
            Aggregation::Last => output_numeric.push(Sample { datetime, value: last }),
            Aggregation::Count => output_int.push(Sample { datetime, value: count }),
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
}

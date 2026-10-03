use crate::datamodel::{
    Sample, TypedSamples, sensapp_datetime::sensapp_datetime_to_offset_datetime,
};
use crate::storage::pg_samples::{Window, insert_samples};
use anyhow::Result;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, Transaction, prelude::*};
use std::collections::HashMap;

/*
Each function sends the whole slice of samples as one statement, by binding
one array per column and expanding them with unnest(). That is one round
trip per call instead of one per sample, and it uses a handful of bind
parameters whatever the number of samples.
 */

fn times<T>(values: &[Sample<T>]) -> Result<Vec<OffsetDateTime>> {
    values
        .iter()
        .map(|value| sensapp_datetime_to_offset_datetime(&value.datetime))
        .collect()
}

/// The window of a statement when it deduplicates: its bind parameters are numbered from
/// `first_param`, and the lowest and highest times of the statement are bound there.
fn window(deduplicate: bool, first_param: usize) -> Option<Window> {
    deduplicate.then_some(Window {
        first_param,
        sql_type: "TIMESTAMPTZ",
    })
}

fn bounds(times: &[OffsetDateTime]) -> (OffsetDateTime, OffsetDateTime) {
    (
        times
            .iter()
            .copied()
            .min()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        times
            .iter()
            .copied()
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    )
}

/// The numeric samples (integer, numeric, float) of all the sensors of a batch, with one
/// statement per type: one array per column, expanded with unnest(). A batch of a Prometheus
/// request holds hundreds of sensors with a few samples each, so one statement per sensor was
/// the main cost of a write once the sensors were registered in bulk.
pub async fn publish_numeric_samples(
    transaction: &mut Transaction<'_, Postgres>,
    sensors: &[(i64, &TypedSamples)],
    deduplicate: bool,
) -> Result<()> {
    let mut integers = Columns::<i64>::default();
    let mut numerics = Columns::<rust_decimal::Decimal>::default();
    let mut floats = Columns::<f64>::default();
    for (sensor_id, samples) in sensors {
        match samples {
            TypedSamples::Integer(values) => integers.add(*sensor_id, values)?,
            TypedSamples::Numeric(values) => numerics.add(*sensor_id, values)?,
            TypedSamples::Float(values) => floats.add(*sensor_id, values)?,
            _ => {}
        }
    }

    if !integers.is_empty() {
        let (low, high) = bounds(&integers.times);
        let sql = insert_samples(
            "integer_values",
            "time",
            &["sensor_id", "time", "value"],
            "SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::BIGINT[])",
            window(deduplicate, 4),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(integers.sensor_ids)
            .bind(integers.times)
            .bind(integers.values);
        if deduplicate {
            query = query.bind(low).bind(high);
        }
        transaction.execute(query).await?;
    }
    if !numerics.is_empty() {
        let (low, high) = bounds(&numerics.times);
        let sql = insert_samples(
            "numeric_values",
            "time",
            &["sensor_id", "time", "value"],
            "SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::NUMERIC[])",
            window(deduplicate, 4),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(numerics.sensor_ids)
            .bind(numerics.times)
            .bind(numerics.values);
        if deduplicate {
            query = query.bind(low).bind(high);
        }
        transaction.execute(query).await?;
    }
    if !floats.is_empty() {
        let (low, high) = bounds(&floats.times);
        let sql = insert_samples(
            "float_values",
            "time",
            &["sensor_id", "time", "value"],
            "SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::FLOAT8[])",
            window(deduplicate, 4),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(floats.sensor_ids)
            .bind(floats.times)
            .bind(floats.values);
        if deduplicate {
            query = query.bind(low).bind(high);
        }
        transaction.execute(query).await?;
    }
    Ok(())
}

/// The columns of the samples of many sensors, one entry per sample.
struct Columns<T> {
    sensor_ids: Vec<i64>,
    times: Vec<OffsetDateTime>,
    values: Vec<T>,
}

impl<T> Default for Columns<T> {
    fn default() -> Self {
        Self {
            sensor_ids: Vec::new(),
            times: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl<T: Copy> Columns<T> {
    fn add(&mut self, sensor_id: i64, samples: &[Sample<T>]) -> Result<()> {
        for sample in samples {
            self.sensor_ids.push(sensor_id);
            self.times
                .push(sensapp_datetime_to_offset_datetime(&sample.datetime)?);
            self.values.push(sample.value);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// The string samples of all the sensors of a batch, with one statement. `string_ids` holds the
/// dictionary id of every string of the batch (`pg_strings::ensure_string_ids`).
pub async fn publish_string_samples(
    transaction: &mut Transaction<'_, Postgres>,
    sensors: &[(i64, &TypedSamples)],
    string_ids: &HashMap<String, i64>,
    deduplicate: bool,
) -> Result<()> {
    let mut sensor_ids = Vec::new();
    let mut times = Vec::new();
    let mut values = Vec::new();
    for (sensor_id, samples) in sensors {
        if let TypedSamples::String(samples) = samples {
            for sample in samples {
                sensor_ids.push(*sensor_id);
                times.push(sensapp_datetime_to_offset_datetime(&sample.datetime)?);
                values.push(string_ids[&sample.value]);
            }
        }
    }
    if values.is_empty() {
        return Ok(());
    }
    let (low, high) = bounds(&times);
    let sql = insert_samples(
        "string_values",
        "time",
        &["sensor_id", "time", "value"],
        "SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::BIGINT[])",
        window(deduplicate, 4),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(sensor_ids)
        .bind(times)
        .bind(values);
    if deduplicate {
        query = query.bind(low).bind(high);
    }
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<bool>],
    deduplicate: bool,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let times = times(values)?;
    let (low, high) = bounds(&times);
    let sql = insert_samples(
        "boolean_values",
        "time",
        &["sensor_id", "time", "value"],
        "SELECT $1::BIGINT, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BOOLEAN[]) AS x(t, v)",
        window(deduplicate, 4),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(sensor_id)
        .bind(times)
        .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    if deduplicate {
        query = query.bind(low).bind(high);
    }
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
    deduplicate: bool,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let times = times(values)?;
    let (low, high) = bounds(&times);
    let sql = insert_samples(
        "location_values",
        "time",
        &["sensor_id", "time", "latitude", "longitude"],
        "SELECT $1::BIGINT, t, lat, lon \
         FROM unnest($2::TIMESTAMPTZ[], $3::FLOAT8[], $4::FLOAT8[]) AS x(t, lat, lon)",
        window(deduplicate, 5),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(sensor_id)
        .bind(times)
        .bind(
            values
                .iter()
                .map(|value| value.value.y())
                .collect::<Vec<_>>(),
        )
        .bind(
            values
                .iter()
                .map(|value| value.value.x())
                .collect::<Vec<_>>(),
        );
    if deduplicate {
        query = query.bind(low).bind(high);
    }
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
    deduplicate: bool,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let times = times(values)?;
    let (low, high) = bounds(&times);
    let sql = insert_samples(
        "blob_values",
        "time",
        &["sensor_id", "time", "value"],
        "SELECT $1::BIGINT, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BYTEA[]) AS x(t, v)",
        window(deduplicate, 4),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(sensor_id)
        .bind(times)
        .bind(
            values
                .iter()
                .map(|value| value.value.as_slice())
                .collect::<Vec<_>>(),
        );
    if deduplicate {
        query = query.bind(low).bind(high);
    }
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
    deduplicate: bool,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    // The column is JSONB, the values travel as text and are cast on the way in.
    let times = times(values)?;
    let (low, high) = bounds(&times);
    let sql = insert_samples(
        "json_values",
        "time",
        &["sensor_id", "time", "value"],
        "SELECT $1::BIGINT, t, v::JSONB FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS x(t, v)",
        window(deduplicate, 4),
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(sensor_id)
        .bind(times)
        .bind(
            values
                .iter()
                .map(|value| value.value.to_string())
                .collect::<Vec<_>>(),
        );
    if deduplicate {
        query = query.bind(low).bind(high);
    }
    transaction.execute(query).await?;
    Ok(())
}

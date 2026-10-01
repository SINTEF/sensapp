use super::postgresql_utilities::get_string_value_id_or_create;
use crate::datamodel::{Sample, TypedSamples};
use crate::storage::common::datetime_to_micros;
use anyhow::Result;
use sqlx::{Postgres, Transaction, prelude::*};

/*
Each function sends the whole slice of samples as one statement, by binding
one array per column and expanding them with unnest(). That is one round
trip per call instead of one per sample, and it uses a handful of bind
parameters whatever the number of samples.
 */

fn timestamps_us<T>(values: &[Sample<T>]) -> Vec<i64> {
    values
        .iter()
        .map(|value| datetime_to_micros(&value.datetime))
        .collect()
}

/// The numeric samples (integer, numeric, float) of all the sensors of a batch, with one
/// statement per type: one array per column, expanded with unnest(). A batch of a Prometheus
/// request holds hundreds of sensors with a few samples each, so one statement per sensor was
/// the main cost of a write once the sensors were registered in bulk.
pub async fn publish_numeric_samples(
    transaction: &mut Transaction<'_, Postgres>,
    sensors: &[(i64, &TypedSamples)],
) -> Result<()> {
    let mut integers = Columns::<i64>::default();
    let mut numerics = Columns::<rust_decimal::Decimal>::default();
    let mut floats = Columns::<f64>::default();
    for (sensor_id, samples) in sensors {
        match samples {
            TypedSamples::Integer(values) => integers.add(*sensor_id, values),
            TypedSamples::Numeric(values) => numerics.add(*sensor_id, values),
            TypedSamples::Float(values) => floats.add(*sensor_id, values),
            _ => {}
        }
    }

    if !integers.is_empty() {
        let query = sqlx::query(
            r#"
            INSERT INTO integer_values (sensor_id, timestamp_us, value)
            SELECT * FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[])
            "#,
        )
        .bind(integers.sensor_ids)
        .bind(integers.times)
        .bind(integers.values);
        transaction.execute(query).await?;
    }
    if !numerics.is_empty() {
        let query = sqlx::query(
            r#"
            INSERT INTO numeric_values (sensor_id, timestamp_us, value)
            SELECT * FROM unnest($1::BIGINT[], $2::BIGINT[], $3::NUMERIC[])
            "#,
        )
        .bind(numerics.sensor_ids)
        .bind(numerics.times)
        .bind(numerics.values);
        transaction.execute(query).await?;
    }
    if !floats.is_empty() {
        let query = sqlx::query(
            r#"
            INSERT INTO float_values (sensor_id, timestamp_us, value)
            SELECT * FROM unnest($1::BIGINT[], $2::BIGINT[], $3::FLOAT8[])
            "#,
        )
        .bind(floats.sensor_ids)
        .bind(floats.times)
        .bind(floats.values);
        transaction.execute(query).await?;
    }
    Ok(())
}

/// The columns of the samples of many sensors, one entry per sample.
struct Columns<T> {
    sensor_ids: Vec<i64>,
    times: Vec<i64>,
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
    fn add(&mut self, sensor_id: i64, samples: &[Sample<T>]) {
        for sample in samples {
            self.sensor_ids.push(sensor_id);
            self.times.push(datetime_to_micros(&sample.datetime));
            self.values.push(sample.value);
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub async fn publish_string_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut string_ids = Vec::with_capacity(values.len());
    for value in values {
        string_ids.push(get_string_value_id_or_create(transaction, &value.value).await?);
    }
    let query = sqlx::query(
        r#"
        INSERT INTO string_values (sensor_id, timestamp_us, value)
        SELECT $1, t, v FROM unnest($2::BIGINT[], $3::BIGINT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(timestamps_us(values))
    .bind(string_ids);
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO boolean_values (sensor_id, timestamp_us, value)
        SELECT $1, t, v FROM unnest($2::BIGINT[], $3::BOOLEAN[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(timestamps_us(values))
    .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO location_values (sensor_id, timestamp_us, latitude, longitude)
        SELECT $1, t, lat, lon
        FROM unnest($2::BIGINT[], $3::FLOAT8[], $4::FLOAT8[]) AS u(t, lat, lon)
        "#,
    )
    .bind(sensor_id)
    .bind(timestamps_us(values))
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
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO blob_values (sensor_id, timestamp_us, value)
        SELECT $1, t, v FROM unnest($2::BIGINT[], $3::BYTEA[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(timestamps_us(values))
    .bind(
        values
            .iter()
            .map(|value| value.value.as_slice())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    // The column is JSONB, the values travel as text and are cast on the way in.
    let query = sqlx::query(
        r#"
        INSERT INTO json_values (sensor_id, timestamp_us, value)
        SELECT $1, t, v::JSONB FROM unnest($2::BIGINT[], $3::TEXT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(timestamps_us(values))
    .bind(
        values
            .iter()
            .map(|value| value.value.to_string())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}

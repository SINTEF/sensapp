use crate::datamodel::{
    Sample, TypedSamples, sensapp_datetime::sensapp_datetime_to_offset_datetime,
};
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
            TypedSamples::Integer(values) => integers.add(*sensor_id, values)?,
            TypedSamples::Numeric(values) => numerics.add(*sensor_id, values)?,
            TypedSamples::Float(values) => floats.add(*sensor_id, values)?,
            _ => {}
        }
    }

    if !integers.is_empty() {
        let query = sqlx::query(
            r#"
            INSERT INTO integer_values (sensor_id, time, value)
            SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::BIGINT[])
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
            INSERT INTO numeric_values (sensor_id, time, value)
            SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::NUMERIC[])
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
            INSERT INTO float_values (sensor_id, time, value)
            SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::FLOAT8[])
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
    let query = sqlx::query(
        r#"
        INSERT INTO string_values (sensor_id, time, value)
        SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::BIGINT[])
        "#,
    )
    .bind(sensor_ids)
    .bind(times)
    .bind(values);
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
        INSERT INTO boolean_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BOOLEAN[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
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
        INSERT INTO location_values (sensor_id, time, latitude, longitude)
        SELECT $1, t, lat, lon
        FROM unnest($2::TIMESTAMPTZ[], $3::FLOAT8[], $4::FLOAT8[]) AS u(t, lat, lon)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
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
        INSERT INTO blob_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BYTEA[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
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
        INSERT INTO json_values (sensor_id, time, value)
        SELECT $1, t, v::JSONB FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(
        values
            .iter()
            .map(|value| value.value.to_string())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}

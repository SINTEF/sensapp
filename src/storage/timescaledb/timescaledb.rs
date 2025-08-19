use super::{
    super::StorageInstance, timescaledb_publishers::*,
    timescaledb_utilities::get_sensor_id_or_create_sensor,
};
use crate::datamodel::{TypedSamples, batch::Batch};
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::time::timeout;

#[derive(Debug)]
pub struct TimeScaleDBStorage {
    pool: PgPool,
}

impl TimeScaleDBStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let connect_options = PgConnectOptions::from_str(connection_string)
            .context("Failed to create timescaledb connection options")?;

        let pool = PgPool::connect_with(connect_options)
            .await
            .context("Failed to create timescaledb pool")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageInstance for TimeScaleDBStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        sqlx::migrate!("src/storage/timescaledb/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensors.as_ref() {
            self.publish_single_sensor_batch(&mut transaction, single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        self.sync(sync_sender).await?;
        Ok(())
    }

    async fn sync(&self, sync_sender: Sender<()>) -> Result<()> {
        // timescaledb doesn't need to do anything special for sync
        // as we use transaction
        if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
            let _ = timeout(Duration::from_secs(15), sync_sender.broadcast(())).await?;
        }
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_sensors(&self) -> Result<Vec<crate::datamodel::Sensor>> {
        // TODO: Implement TimescaleDB sensor listing with full metadata
        unimplemented!("TimescaleDB sensor listing not yet implemented");
    }

    async fn query_sensor_data(
        &self,
        _sensor_name: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        unimplemented!("TimescaleDB sensor data querying not yet implemented");
    }

    async fn query_sensor_data_by_uuid(
        &self,
        _sensor_uuid: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        // TODO: Implement TimescaleDB UUID-based sensor data querying
        unimplemented!("TimescaleDB UUID-based sensor data querying not yet implemented");
    }
}

impl TimeScaleDBStorage {
    async fn publish_single_sensor_batch(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        single_sensor_batch: &crate::datamodel::batch::SingleSensorBatch,
    ) -> Result<()> {
        let sensor_id =
            get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor).await?;

        let samples_guard = single_sensor_batch.samples.read().await;
        match &*samples_guard {
            TypedSamples::Integer(values) => {
                publish_integer_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Numeric(values) => {
                publish_numeric_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Float(values) => {
                publish_float_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::String(values) => {
                publish_string_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Boolean(values) => {
                publish_boolean_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Location(values) => {
                publish_location_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Blob(values) => {
                publish_blob_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Json(values) => {
                publish_json_values(transaction, sensor_id, values).await?;
            }
        }

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .context("Failed to vacuum database")?;

        Ok(())
    }

    async fn query_integer_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM integer_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Integer(samples))
    }

    async fn query_numeric_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use rust_decimal::Decimal;
        use smallvec::smallvec;
        use std::str::FromStr;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM numeric_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = Decimal::from_str(&row.value.to_string())
                .context("Failed to parse decimal value")?;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Numeric(samples))
    }

    async fn query_float_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM float_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Float(samples))
    }

    async fn query_string_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT sv.timestamp_ms, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = $1 
            AND ($2::BIGINT IS NULL OR sv.timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR sv.timestamp_ms <= $3)
            ORDER BY sv.timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.string_value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::String(samples))
    }

    async fn query_boolean_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM boolean_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.value.unwrap_or(false);
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Boolean(samples))
    }

    async fn query_location_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, latitude, longitude FROM location_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = geo::Point::new(row.longitude, row.latitude);
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Location(samples))
    }

    async fn query_json_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM json_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value: serde_json::Value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Json(samples))
    }

    async fn query_blob_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM blob_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }
}

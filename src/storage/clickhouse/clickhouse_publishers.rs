use crate::datamodel::{Sample, TypedSamples, batch::SingleSensorBatch};
use crate::storage::clickhouse::clickhouse_utilities::{
    datetime_to_micros, get_sensor_id_or_create_sensor, map_clickhouse_error,
};
use anyhow::Result;
use base64::prelude::*;
use clickhouse::Client;
use serde::Serialize;

// Row structures for ClickHouse inserts
#[derive(Serialize, clickhouse::Row)]
struct IntegerValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: i64,
}

#[derive(Serialize, clickhouse::Row)]
struct NumericValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: rust_decimal::Decimal,
}

#[derive(Serialize, clickhouse::Row)]
struct FloatValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: f64,
}

#[derive(Serialize, clickhouse::Row)]
struct StringValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: String,
}

#[derive(Serialize, clickhouse::Row)]
struct BooleanValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: bool,
}

#[derive(Serialize, clickhouse::Row)]
struct LocationValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    latitude: f64,
    longitude: f64,
}

#[derive(Serialize, clickhouse::Row)]
struct JsonValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: String,
}

#[derive(Serialize, clickhouse::Row)]
struct BlobValueRow {
    sensor_id: u64,
    timestamp_us: i64,
    value: String, // Base64 encoded
}

#[derive(Serialize, clickhouse::Row)]
struct LabelRow {
    sensor_id: u64,
    name: String,
    description: Option<String>,
}

/// Publisher for single sensor batch to ClickHouse
pub struct ClickHousePublisher<'a> {
    client: &'a Client,
}

impl<'a> ClickHousePublisher<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Publish a single sensor batch to ClickHouse
    pub async fn publish_single_sensor_batch(&self, batch: &SingleSensorBatch) -> Result<()> {
        // Get or create sensor_id
        let sensor_id = get_sensor_id_or_create_sensor(
            self.client,
            &batch.sensor.uuid,
            &batch.sensor.name,
            &batch.sensor.sensor_type,
            batch.sensor.unit.as_ref(),
        )
        .await?;

        // Publish labels if any
        // Convert labels to the expected format
        let labels: Vec<(String, String)> = batch
            .sensor
            .labels
            .iter()
            .map(|(name, description)| (name.clone(), description.clone()))
            .collect();
        self.publish_labels(sensor_id, &labels).await?;

        // Publish samples - need to acquire read lock first
        let samples_guard = batch.samples.read().await;
        self.publish_samples(sensor_id, &samples_guard).await?;

        Ok(())
    }

    /// Publish labels for a sensor
    async fn publish_labels(&self, sensor_id: u64, labels: &[(String, String)]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("labels")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for (name, description) in labels {
            let row = LabelRow {
                sensor_id,
                name: name.clone(),
                description: Some(description.clone()),
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish samples by their types
    async fn publish_samples(&self, sensor_id: u64, samples: &TypedSamples) -> Result<()> {
        match samples {
            TypedSamples::Integer(values) => {
                self.publish_integer_values(sensor_id, values).await?;
            }
            TypedSamples::Numeric(values) => {
                self.publish_numeric_values(sensor_id, values).await?;
            }
            TypedSamples::Float(values) => {
                self.publish_float_values(sensor_id, values).await?;
            }
            TypedSamples::String(values) => {
                self.publish_string_values(sensor_id, values).await?;
            }
            TypedSamples::Boolean(values) => {
                self.publish_boolean_values(sensor_id, values).await?;
            }
            TypedSamples::Location(values) => {
                self.publish_location_values(sensor_id, values).await?;
            }
            TypedSamples::Json(values) => {
                self.publish_json_values(sensor_id, values).await?;
            }
            TypedSamples::Blob(values) => {
                self.publish_blob_values(sensor_id, values).await?;
            }
        }

        Ok(())
    }

    /// Publish integer values
    async fn publish_integer_values(&self, sensor_id: u64, samples: &[Sample<i64>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("integer_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = IntegerValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish numeric values
    async fn publish_numeric_values(
        &self,
        sensor_id: u64,
        samples: &[Sample<rust_decimal::Decimal>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("numeric_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = NumericValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish float values
    async fn publish_float_values(&self, sensor_id: u64, samples: &[Sample<f64>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("float_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = FloatValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish string values
    async fn publish_string_values(
        &self,
        sensor_id: u64,
        samples: &[Sample<String>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("string_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = StringValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value.clone(),
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish boolean values
    async fn publish_boolean_values(&self, sensor_id: u64, samples: &[Sample<bool>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("boolean_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = BooleanValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish location values
    async fn publish_location_values(
        &self,
        sensor_id: u64,
        samples: &[Sample<geo::Point>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("location_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = LocationValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                latitude: sample.value.y(),
                longitude: sample.value.x(),
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish JSON values
    async fn publish_json_values(
        &self,
        sensor_id: u64,
        samples: &[Sample<serde_json::Value>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("json_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = JsonValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value.to_string(),
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }

    /// Publish blob values (base64 encoded)
    async fn publish_blob_values(&self, sensor_id: u64, samples: &[Sample<Vec<u8>>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("blob_values")
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        for sample in samples {
            let row = BlobValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: base64::prelude::BASE64_STANDARD.encode(&sample.value),
            };
            insert
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        insert
            .end()
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(())
    }
}

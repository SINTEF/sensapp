use crate::datamodel::{Sample, TypedSamples, batch::SingleSensorBatch};
use crate::storage::clickhouse::clickhouse_utilities::{
    datetime_to_micros, get_sensor_id_or_create_sensor, map_clickhouse_error,
};
use anyhow::Result;
use base64::prelude::*;
use clickhouse::{Client, inserter::Inserter};
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

/// Publisher for single sensor batch to ClickHouse with stateful inserters
pub struct ClickHousePublisher<'a> {
    client: &'a Client,
    // Lazily initialized inserters for each value type
    integer_inserter: Option<Inserter<IntegerValueRow>>,
    numeric_inserter: Option<Inserter<NumericValueRow>>,
    float_inserter: Option<Inserter<FloatValueRow>>,
    string_inserter: Option<Inserter<StringValueRow>>,
    boolean_inserter: Option<Inserter<BooleanValueRow>>,
    location_inserter: Option<Inserter<LocationValueRow>>,
    json_inserter: Option<Inserter<JsonValueRow>>,
    blob_inserter: Option<Inserter<BlobValueRow>>,
    label_inserter: Option<Inserter<LabelRow>>,
}

impl<'a> ClickHousePublisher<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self {
            client,
            integer_inserter: None,
            numeric_inserter: None,
            float_inserter: None,
            string_inserter: None,
            boolean_inserter: None,
            location_inserter: None,
            json_inserter: None,
            blob_inserter: None,
            label_inserter: None,
        }
    }

    /// Publish a single sensor batch to ClickHouse
    pub async fn publish_single_sensor_batch(&mut self, batch: &SingleSensorBatch) -> Result<()> {
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
    async fn publish_labels(&mut self, sensor_id: u64, labels: &[(String, String)]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }

        // Get or create the label inserter
        if self.label_inserter.is_none() {
            self.label_inserter = Some(
                self.client
                    .inserter("labels")
            );
        }

        let inserter = self.label_inserter.as_mut().unwrap();

        for (name, description) in labels {
            let row = LabelRow {
                sensor_id,
                name: name.clone(),
                description: Some(description.clone()),
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish samples by their types
    async fn publish_samples(&mut self, sensor_id: u64, samples: &TypedSamples) -> Result<()> {
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
    async fn publish_integer_values(&mut self, sensor_id: u64, samples: &[Sample<i64>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the integer inserter
        if self.integer_inserter.is_none() {
            self.integer_inserter = Some(
                self.client
                    .inserter("integer_values")
            );
        }

        let inserter = self.integer_inserter.as_mut().unwrap();

        for sample in samples {
            let row = IntegerValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish numeric values
    async fn publish_numeric_values(
        &mut self,
        sensor_id: u64,
        samples: &[Sample<rust_decimal::Decimal>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the numeric inserter
        if self.numeric_inserter.is_none() {
            self.numeric_inserter = Some(
                self.client
                    .inserter("numeric_values")
            );
        }

        let inserter = self.numeric_inserter.as_mut().unwrap();

        for sample in samples {
            let row = NumericValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish float values
    async fn publish_float_values(&mut self, sensor_id: u64, samples: &[Sample<f64>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the float inserter
        if self.float_inserter.is_none() {
            self.float_inserter = Some(
                self.client
                    .inserter("float_values")
            );
        }

        let inserter = self.float_inserter.as_mut().unwrap();

        for sample in samples {
            let row = FloatValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish string values
    async fn publish_string_values(
        &mut self,
        sensor_id: u64,
        samples: &[Sample<String>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the string inserter
        if self.string_inserter.is_none() {
            self.string_inserter = Some(
                self.client
                    .inserter("string_values")
            );
        }

        let inserter = self.string_inserter.as_mut().unwrap();

        for sample in samples {
            let row = StringValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value.clone(),
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish boolean values
    async fn publish_boolean_values(&mut self, sensor_id: u64, samples: &[Sample<bool>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the boolean inserter
        if self.boolean_inserter.is_none() {
            self.boolean_inserter = Some(
                self.client
                    .inserter("boolean_values")
            );
        }

        let inserter = self.boolean_inserter.as_mut().unwrap();

        for sample in samples {
            let row = BooleanValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value,
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish location values
    async fn publish_location_values(
        &mut self,
        sensor_id: u64,
        samples: &[Sample<geo::Point>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the location inserter
        if self.location_inserter.is_none() {
            self.location_inserter = Some(
                self.client
                    .inserter("location_values")
            );
        }

        let inserter = self.location_inserter.as_mut().unwrap();

        for sample in samples {
            let row = LocationValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                latitude: sample.value.y(),
                longitude: sample.value.x(),
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish JSON values
    async fn publish_json_values(
        &mut self,
        sensor_id: u64,
        samples: &[Sample<serde_json::Value>],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the json inserter
        if self.json_inserter.is_none() {
            self.json_inserter = Some(
                self.client
                    .inserter("json_values")
            );
        }

        let inserter = self.json_inserter.as_mut().unwrap();

        for sample in samples {
            let row = JsonValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: sample.value.to_string(),
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Publish blob values (base64 encoded)
    async fn publish_blob_values(&mut self, sensor_id: u64, samples: &[Sample<Vec<u8>>]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        // Get or create the blob inserter
        if self.blob_inserter.is_none() {
            self.blob_inserter = Some(
                self.client
                    .inserter("blob_values")
            );
        }

        let inserter = self.blob_inserter.as_mut().unwrap();

        for sample in samples {
            let row = BlobValueRow {
                sensor_id,
                timestamp_us: datetime_to_micros(&sample.datetime),
                value: base64::prelude::BASE64_STANDARD.encode(&sample.value),
            };
            inserter
                .write(&row)
                .await
                .map_err(|e| map_clickhouse_error(e, None, None))?;
        }

        Ok(())
    }

    /// Commit all pending inserts to ClickHouse in parallel
    pub async fn commit_all(mut self) -> Result<()> {
        // Collect all initialized inserters and commit them in parallel
        let mut tasks = Vec::new();

        if let Some(inserter) = self.integer_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.numeric_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.float_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.string_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.boolean_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.location_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.json_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.blob_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        if let Some(inserter) = self.label_inserter.take() {
            tasks.push(tokio::spawn(async move {
                inserter.end().await.map_err(|e| map_clickhouse_error(e, None, None))
            }));
        }

        // Wait for all tasks to complete and collect results
        for task in tasks {
            task.await??;
        }

        Ok(())
    }
}

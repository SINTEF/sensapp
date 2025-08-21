use anyhow::{Result, anyhow};
use tracing::{debug, info};
use big_decimal_byte_string_encoder::encode_bigdecimal_to_bigquery_bytes;
use bigdecimal::BigDecimal;
use hybridmap::HybridMap;
use std::sync::Arc;
use uuid::Uuid;

use super::BigQueryStorage;
use super::bigquery_prost_structs::{BlobValue, IntegerValue, JsonValue, LocationValue};
use super::bigquery_table_descriptors::{
    BLOB_VALUES_DESCRIPTOR, INTEGER_VALUES_DESCRIPTOR, JSON_VALUES_DESCRIPTOR,
};
use super::bigquery_utilities::publish_rows;
use crate::datamodel::SensAppVec;
use crate::datamodel::{SensorType, TypedSamples, batch::Batch};
use crate::storage::bigquery::bigquery_prost_structs::{
    BooleanValue, FloatValue, NumericValue, StringValue,
};
use crate::storage::bigquery::bigquery_string_values_utilities::get_or_create_string_values_ids;
use crate::storage::bigquery::bigquery_table_descriptors::{
    BOOLEAN_VALUES_DESCRIPTOR, FLOAT_VALUES_DESCRIPTOR, LOCATION_VALUES_DESCRIPTOR,
    NUMERIC_VALUES_DESCRIPTOR, STRING_VALUES_DESCRIPTOR,
};

pub async fn publish_integer_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Integer {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Integer(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        debug!("BigQuery: Publishing integer value with timestamp: {}", timestamp);
                        rows.push(IntegerValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value: value.value,
                        });
                    }
                } else {
                    unreachable!("SensorType is Integer, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "integer_values", &INTEGER_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_numeric_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Numeric {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Numeric(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        // Stupid conversion for now
                        let decimal_string = value.value.to_string();
                        use std::str::FromStr;
                        let decimal_bigdecimal =
                            BigDecimal::from_str(&decimal_string)?.with_scale(9);
                        let value = encode_bigdecimal_to_bigquery_bytes(&decimal_bigdecimal)?;
                        rows.push(NumericValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value,
                        });
                    }
                } else {
                    unreachable!("SensorType is Numeric, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "numeric_values", &NUMERIC_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_float_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Float {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Float(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        rows.push(FloatValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value: value.value as f32, // Stupid conversion for now
                        });
                    }
                } else {
                    unreachable!("SensorType is Float, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "float_values", &FLOAT_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_string_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    struct TmpStringValue {
        sensor_id: i64,
        timestamp: String,
        value_string: String,
    }

    let mut tmp_rows: SensAppVec<TmpStringValue> = SensAppVec::new();

    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::String {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::String(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        tmp_rows.push(TmpStringValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value_string: value.value.clone(),
                        });
                    }
                } else {
                    unreachable!("SensorType is String, but samples are not");
                }
            }
        }
    }

    if tmp_rows.is_empty() {
        debug!("BigQuery: No string values to publish");
        return Ok(());
    }

    info!("BigQuery: Publishing {} string values", tmp_rows.len());

    let only_string_values = tmp_rows
        .iter()
        .map(|row| row.value_string.clone())
        .collect::<SensAppVec<String>>();

    let ids_map = get_or_create_string_values_ids(bqs, only_string_values).await?;

    let rows = tmp_rows
        .into_iter()
        .map(|row| {
            let string_id = ids_map
                .get(&row.value_string)
                .expect("Internal consistency error: String value missing from cached map");
            StringValue {
                sensor_id: row.sensor_id,
                timestamp: row.timestamp,
                value: *string_id,
            }
        })
        .collect::<Vec<StringValue>>();

    publish_rows(bqs, "string_values", &STRING_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_boolean_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Boolean {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Boolean(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        rows.push(BooleanValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value: value.value,
                        });
                    }
                } else {
                    unreachable!("SensorType is Boolean, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "boolean_values", &BOOLEAN_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_location_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Location {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Location(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        rows.push(LocationValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            latitude: value.value.y() as f32,
                            longitude: value.value.x() as f32,
                        });
                    }
                } else {
                    unreachable!("SensorType is Location, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "location_values", &LOCATION_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_json_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Json {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Json(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        rows.push(JsonValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value: value.value.as_str().unwrap_or("").to_string(),
                        });
                    }
                } else {
                    unreachable!("SensorType is Json, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "json_values", &JSON_VALUES_DESCRIPTOR, rows).await
}

pub async fn publish_blob_values(
    bqs: &BigQueryStorage,
    batch: Arc<Batch>,
    sensor_ids: Arc<HybridMap<Uuid, i64>>,
) -> Result<()> {
    let mut rows = vec![];
    for single_sensor_batch in batch.sensors.as_ref() {
        if single_sensor_batch.sensor.sensor_type == SensorType::Blob {
            {
                let samples_guard = single_sensor_batch.samples.read().await;
                if let TypedSamples::Blob(samples) = &*samples_guard {
                    for value in samples {
                        let sensor_id = sensor_ids
                            .get(&single_sensor_batch.sensor.uuid)
                            .ok_or(anyhow!("Sensor not found"))?;
                        let timestamp = value.datetime.to_isoformat();
                        rows.push(BlobValue {
                            sensor_id: *sensor_id,
                            timestamp,
                            value: value.value.clone(),
                        });
                    }
                } else {
                    unreachable!("SensorType is Blob, but samples are not");
                }
            }
        }
    }

    publish_rows(bqs, "blob_values", &BLOB_VALUES_DESCRIPTOR, rows).await
}

use super::{Sensor, TypedSamples, sensapp_vec::SensAppVec};
use anyhow::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct SingleSensorBatch {
    pub sensor: Arc<Sensor>,
    pub samples: RwLock<TypedSamples>,
}

#[derive(Debug)]
pub struct Batch {
    pub sensors: SensAppVec<SingleSensorBatch>,
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            sensors: SensAppVec::new(),
        }
    }
}

impl Batch {
    #[cfg(feature = "test-utils")]
    pub async fn len(&self) -> usize {
        let sensors_len = self.sensors.len();
        if sensors_len == 0 {
            return 0;
        }
        if sensors_len == 1 {
            return self.sensors.iter().next().unwrap().len().await;
        }
        let futures = self.sensors.iter().map(|v| v.len());
        let results = futures::future::join_all(futures).await;
        results.into_iter().sum()
    }

    #[cfg(feature = "test-utils")]
    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl SingleSensorBatch {
    pub fn new(sensor: Arc<Sensor>, samples: TypedSamples) -> Self {
        Self {
            sensor,
            samples: RwLock::new(samples),
        }
    }

    pub async fn append(&mut self, mut new_samples: TypedSamples) -> Result<(), Error> {
        let mut old_samples_guard = self.samples.write().await;
        let old_samples = &mut *old_samples_guard;

        // Beautiful code right there
        match (old_samples, &mut new_samples) {
            (TypedSamples::Integer(old_samples), TypedSamples::Integer(new_samples)) => {
                old_samples.append(&mut *new_samples);
            }
            (TypedSamples::Numeric(old_samples), TypedSamples::Numeric(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::Float(old_samples), TypedSamples::Float(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::String(old_samples), TypedSamples::String(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::Boolean(old_samples), TypedSamples::Boolean(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::Location(old_samples), TypedSamples::Location(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::Blob(old_samples), TypedSamples::Blob(new_samples)) => {
                old_samples.append(new_samples);
            }
            (TypedSamples::Json(old_samples), TypedSamples::Json(new_samples)) => {
                old_samples.append(new_samples);
            }
            _ => {
                anyhow::bail!("Cannot append {:?} to {:?}", new_samples, self.samples);
            }
        }
        Ok(())
    }

    pub async fn len(&self) -> usize {
        let samples_guard = self.samples.read().await;
        match &*samples_guard {
            TypedSamples::Integer(samples) => samples.len(),
            TypedSamples::Numeric(samples) => samples.len(),
            TypedSamples::Float(samples) => samples.len(),
            TypedSamples::String(samples) => samples.len(),
            TypedSamples::Boolean(samples) => samples.len(),
            TypedSamples::Location(samples) => samples.len(),
            TypedSamples::Blob(samples) => samples.len(),
            TypedSamples::Json(samples) => samples.len(),
        }
    }

    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    pub async fn take_samples(&mut self) -> TypedSamples {
        let mut samples_guard = self.samples.write().await;
        let samples = &*samples_guard;
        let replacement = samples.clone_empty();
        std::mem::replace(&mut *samples_guard, replacement)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Sample, SensAppDateTime};
    use super::*;
    use crate::{config::load_configuration, datamodel::SensorType};
    use smallvec::smallvec;

    #[tokio::test]
    async fn test_append() {
        load_configuration().unwrap();

        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Integer, None, None)
                    .unwrap(),
            ),
            TypedSamples::Integer(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: 0
            },]),
        );

        assert_eq!(batch.len().await, 1);

        // New integer
        batch
            .append(TypedSamples::Integer(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Incompatible type
        let result = batch
            .append(TypedSamples::Numeric(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: rust_decimal::Decimal::new(1, 0)
            },]))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot append"));

        // Append numeric to numeric
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Numeric, None, None)
                    .unwrap(),
            ),
            TypedSamples::Numeric(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: rust_decimal::Decimal::new(0, 0)
            },]),
        );
        batch
            .append(TypedSamples::Numeric(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: rust_decimal::Decimal::new(1, 0)
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append float to float
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Float, None, None)
                    .unwrap(),
            ),
            TypedSamples::Float(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: 0.0
            },]),
        );
        batch
            .append(TypedSamples::Float(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1.0
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append string to string
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::String, None, None)
                    .unwrap(),
            ),
            TypedSamples::String(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: "0".to_string()
            },]),
        );
        batch
            .append(TypedSamples::String(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: "1".to_string()
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append boolean to boolean
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Boolean, None, None)
                    .unwrap(),
            ),
            TypedSamples::Boolean(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: false
            },]),
        );
        batch
            .append(TypedSamples::Boolean(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: true
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append location to location
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Location, None, None)
                    .unwrap(),
            ),
            TypedSamples::Location(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: geo::Point::new(0.0, 0.0)
            },]),
        );
        batch
            .append(TypedSamples::Location(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: geo::Point::new(1.0, 1.0)
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append blob to blob
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Blob, None, None).unwrap(),
            ),
            TypedSamples::Blob(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: vec![0]
            },]),
        );
        batch
            .append(TypedSamples::Blob(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: vec![1]
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);

        // Append json to json
        let mut batch = SingleSensorBatch::new(
            Arc::new(
                Sensor::new_without_uuid("test".to_string(), SensorType::Json, None, None).unwrap(),
            ),
            TypedSamples::Json(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: serde_json::json!({"test": 0})
            },]),
        );
        batch
            .append(TypedSamples::Json(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: serde_json::json!({"test": 1})
            },]))
            .await
            .unwrap();
        assert_eq!(batch.len().await, 2);
    }
}

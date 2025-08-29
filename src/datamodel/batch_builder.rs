use super::{
    Sensor, TypedSamples,
    batch::{Batch, SingleSensorBatch},
};
use crate::{datamodel::SensAppVec, storage::StorageInstance};
use anyhow::{Error, anyhow};
use hybridmap::HybridMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// A batch builder is used to build a batch from a stream of samples.
pub struct BatchBuilder {
    batch_size: usize,
    single_sensor_batches: RwLock<HybridMap<Uuid, SingleSensorBatch>>,
}

impl BatchBuilder {
    pub fn new() -> Result<Self, Error> {
        let batch_size = crate::config::get()?.batch_size;

        if batch_size == 0 {
            return Err(anyhow::anyhow!("Batch size is 0"));
        }

        Ok(Self {
            batch_size,
            single_sensor_batches: RwLock::new(HybridMap::new()),
        })
    }

    pub async fn add(&mut self, sensor: Arc<Sensor>, samples: TypedSamples) -> Result<(), Error> {
        let uuid = sensor.uuid;
        let mut write_guard = self.single_sensor_batches.write().await;
        let single_sensor_batches = &mut *write_guard;
        if let Some(sensor_batch) = single_sensor_batches.get_mut(&uuid) {
            sensor_batch.append(samples).await?;
        } else {
            let sensor_batch = SingleSensorBatch::new(sensor, samples);
            single_sensor_batches.insert(uuid, sensor_batch);
        }
        Ok(())
    }

    async fn build_batch(&mut self) -> Batch {
        let tmp_sensors;
        {
            let mut write_guard = self.single_sensor_batches.write().await;
            tmp_sensors = std::mem::replace(&mut *write_guard, HybridMap::new());
        }
        let sensors_iter = tmp_sensors.into_iter().map(|(_, v)| v);
        let sensors = SensAppVec::from_iter(sensors_iter);
        Batch { sensors }
    }

    // This is a small bin packing problem, as we want to create
    // efficient batches. We are not looking for the optimal solution
    // but a good enough one. A First Fit Decreasing algorithm is
    // used to solve this problem.
    async fn build_batches(&mut self) -> Vec<Batch> {
        let batch_size = self.batch_size;

        let tmp_single_sensor_batches;
        {
            let mut write_guard = self.single_sensor_batches.write().await;
            tmp_single_sensor_batches = std::mem::replace(&mut *write_guard, HybridMap::new());
        }

        let chunked_batch_futures =
            tmp_single_sensor_batches
                .into_iter()
                .map(|(_, mut single_sensor_batch)| async move {
                    let sensor = single_sensor_batch.sensor.clone();
                    let samples = single_sensor_batch.take_samples().await;
                    let chunks = samples.into_chunks(batch_size);
                    chunks.map(move |chunk| {
                        let len = chunk.len();
                        let single_batch = SingleSensorBatch::new(sensor.clone(), chunk);
                        (single_batch, len)
                    })
                });
        let single_sensor_batches = futures::future::join_all(chunked_batch_futures)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // We work on indices to avoid the awful borrow checker
        let mut indices: Vec<usize> = (0..single_sensor_batches.len()).collect();

        // Sort indices by decreasing order of the length of the single sensor batch
        indices.sort_by(|a, b| {
            single_sensor_batches[*b]
                .1
                .cmp(&single_sensor_batches[*a].1)
        });

        // Each bin contains its size
        let mut bins: Vec<usize> = Vec::new();

        // Contain the index of the bin for each single sensor batch
        let mut final_indices: Vec<usize> = vec![0; single_sensor_batches.len()];

        // For each single sensor batch, we try to place it in an existing bin
        for index in indices {
            let batch_len = single_sensor_batches[index].1;
            let mut placed = false;
            // Try to place the single sensor batch in an existing bin
            for (bin_index, total_bin_len) in bins.iter_mut().enumerate() {
                if *total_bin_len + batch_len <= batch_size {
                    // We mark the single sensor batch as placed in the bin
                    final_indices[index] = bin_index;
                    // We update the total length of the bin
                    *total_bin_len += batch_len;
                    placed = true;
                    break;
                }
            }
            // If the single sensor batch doesn't fit in any bin, create a new one
            if !placed {
                bins.push(batch_len);
                final_indices[index] = bins.len() - 1;
            }
        }

        // We create the batches
        let mut batches: Vec<Batch> = bins.iter().map(|_| Batch::default()).collect();

        // We fill the batches with the single sensor batches
        for (index, single_sensor_batch) in single_sensor_batches.into_iter().enumerate() {
            let batch_index = final_indices[index];
            let batch = &mut batches[batch_index];
            batch.sensors.push(single_sensor_batch.0);
        }

        batches
    }

    async fn len(&self) -> usize {
        let read_guard = self.single_sensor_batches.read().await;
        let single_sensor_batches = &*read_guard;
        let sensors_len = single_sensor_batches.len();
        if sensors_len == 0 {
            return 0;
        }
        if sensors_len == 1 {
            return single_sensor_batches.iter().next().unwrap().1.len().await;
        }
        let futures = single_sensor_batches.iter().map(|(_, v)| v.len());
        let results = futures::future::join_all(futures).await;
        results.into_iter().sum()
    }

    async fn send_multiple_batch(
        &mut self,
        storage: Arc<dyn StorageInstance>,
    ) -> Result<(), Error> {
        let batches_iter = self.build_batches().await;

        for batch in batches_iter {
            storage.publish(Arc::new(batch)).await?;
        }

        Ok(())
    }

    async fn send(&mut self, storage: Arc<dyn StorageInstance>, len: usize) -> Result<(), Error> {
        if len == 0 {
            // Shouldn't happen but just in case
            return Ok(());
        }
        if len > self.batch_size {
            return self.send_multiple_batch(storage).await;
        }

        let batch = self.build_batch().await;
        storage.publish(Arc::new(batch)).await?;

        Ok(())
    }

    #[cfg(feature = "test-utils")]
    pub async fn send_if_batch_full(
        &mut self,
        storage: Arc<dyn StorageInstance>,
    ) -> Result<bool, Error> {
        let len = self.len().await;
        if len < self.batch_size {
            return Ok(false);
        }
        self.send(storage, len).await?;
        Ok(true)
    }

    pub async fn send_what_is_left(
        &mut self,
        storage: Arc<dyn StorageInstance>,
    ) -> Result<bool, Error> {
        let len = self.len().await;
        if len == 0 {
            return Ok(false);
        }
        match self.send(storage, len).await {
            Ok(()) => Ok(true),
            Err(err) => Err(anyhow!("Error sending batch: {:?}", err)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        config::load_configuration,
        datamodel::{Sample, SensorType, sensapp_vec::SensAppLabels},
    };

    // Utility function to create a test sensor
    fn create_test_sensor(uuid: Uuid) -> Arc<Sensor> {
        Arc::new(Sensor {
            uuid,
            name: "Test Sensor".to_string(),
            unit: None,
            sensor_type: SensorType::Integer,
            labels: SensAppLabels::new(),
        })
    }

    // Utility function to create test typed samples
    fn create_test_samples(count: usize) -> TypedSamples {
        let samples = (0..count)
            .map(|i| Sample {
                datetime: hifitime::Epoch::from_unix_seconds(i as f64 * 1000_f64),
                value: i as i64,
            })
            .collect();
        TypedSamples::Integer(samples)
    }

    #[tokio::test]
    async fn test_add_samples() {
        _ = load_configuration();

        let mut batch_builder = BatchBuilder::new().unwrap();
        let sensor = create_test_sensor(Uuid::new_v4());
        let samples = create_test_samples(5);

        batch_builder.add(sensor.clone(), samples).await.unwrap();

        let more_samples = create_test_samples(10);
        batch_builder
            .add(sensor.clone(), more_samples)
            .await
            .unwrap();

        let single_sensor_batches = batch_builder.single_sensor_batches.read().await;
        assert_eq!(single_sensor_batches.len(), 1);
        assert_eq!(
            single_sensor_batches.get(&sensor.uuid).unwrap().len().await,
            15
        );
    }

    #[tokio::test]
    async fn test_build_batch() {
        _ = load_configuration();

        let mut batch_builder = BatchBuilder::new().unwrap();
        let sensor1 = create_test_sensor(Uuid::new_v4());
        let sensor2 = create_test_sensor(Uuid::new_v4());
        let samples1 = create_test_samples(3);
        let samples2 = create_test_samples(2);

        batch_builder.add(sensor1.clone(), samples1).await.unwrap();
        batch_builder.add(sensor2.clone(), samples2).await.unwrap();

        let batch = batch_builder.build_batch().await;
        assert_eq!(batch.sensors.len(), 2);
        assert_eq!(batch.sensors[0].len().await, 3);
        assert_eq!(batch.sensors[1].len().await, 2);
    }

    #[tokio::test]
    async fn test_build_batches() {
        _ = load_configuration();

        let mut batch_builder = BatchBuilder::new().unwrap();
        let sensor1 = create_test_sensor(Uuid::new_v4());
        let sensor2 = create_test_sensor(Uuid::new_v4());
        let sensor3 = create_test_sensor(Uuid::new_v4());

        batch_builder.batch_size = 5;

        let samples1 = create_test_samples(3);
        let samples2 = create_test_samples(2);
        let samples3 = create_test_samples(1);
        let samples4 = create_test_samples(1);

        batch_builder.add(sensor1.clone(), samples1).await.unwrap();
        batch_builder.add(sensor1.clone(), samples2).await.unwrap();
        batch_builder.add(sensor2.clone(), samples3).await.unwrap();
        batch_builder.add(sensor3.clone(), samples4).await.unwrap();

        let batches = batch_builder.build_batches().await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len().await, 5);
        assert_eq!(batches[1].len().await, 2);
    }

    #[tokio::test]
    async fn test_len() {
        _ = load_configuration();

        let mut batch_builder = BatchBuilder::new().unwrap();
        assert_eq!(batch_builder.len().await, 0);

        let sensor1 = create_test_sensor(Uuid::new_v4());
        let samples1 = create_test_samples(3);

        batch_builder.add(sensor1.clone(), samples1).await.unwrap();

        assert_eq!(batch_builder.len().await, 3);

        let samples2 = create_test_samples(7);
        batch_builder.add(sensor1.clone(), samples2).await.unwrap();
        assert_eq!(batch_builder.len().await, 10);

        let sensor2 = create_test_sensor(Uuid::new_v4());
        let samples3 = create_test_samples(1);

        batch_builder.add(sensor2.clone(), samples3).await.unwrap();
        assert_eq!(batch_builder.len().await, 11);
    }
}

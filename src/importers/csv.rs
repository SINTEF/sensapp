use crate::{
    bus::{wait_for_all::WaitForAll, EventBus},
    datamodel::{
        batch::{Batch, SingleSensorBatch},
        //batch_builder::BatchBuilder,
        Sample,
        SensAppDateTime,
        Sensor,
        TypedSamples,
    },
};
use anyhow::Result;
use csv_async::AsyncReader;
use futures::{io, StreamExt};
use smallvec::smallvec;
use std::sync::Arc;

pub async fn publish_csv_async<R: io::AsyncRead + Unpin + Send>(
    mut csv_reader: AsyncReader<R>,
    batch_size: usize,
    event_bus: Arc<EventBus>,
) -> Result<()> {
    println!("{:?}", csv_reader.has_headers());
    println!("{:?}", csv_reader.headers().await.unwrap());
    let mut records = csv_reader.records();

    let mut current_samples: Vec<Sample<i64>> = vec![];

    //let mut batch_builder = BatchBuilder::new()?;
    let mut all_batches_waiter = WaitForAll::new();

    let mut i = 0;

    println!("Reading CSV");
    while let Some(record) = records.next().await {
        let record = record.unwrap();
        println!("{:?}", record);

        current_samples.push(Sample {
            datetime: SensAppDateTime::from_unix_seconds(0.0),
            value: i,
        });

        i += 1;

        if current_samples.len() >= batch_size {
            let single_sensor_batch = SingleSensorBatch::new(
                Arc::new(Sensor::new_without_uuid(
                    "test".to_string(),
                    crate::datamodel::SensorType::Integer,
                    None,
                    None,
                )?),
                TypedSamples::Integer(current_samples.into()),
            );
            let batch = Batch {
                sensors: smallvec![single_sensor_batch],
            };
            let sync_receiver = event_bus.publish(batch).await?;
            //sync_receiver.activate().recv().await?;
            current_samples = vec![];
            all_batches_waiter.add(sync_receiver.activate()).await;
        }
    }

    if !current_samples.is_empty() {
        let single_sensor_batch = SingleSensorBatch::new(
            Arc::new(Sensor::new_without_uuid(
                "test".to_string(),
                crate::datamodel::SensorType::Integer,
                None,
                None,
            )?),
            TypedSamples::Integer(current_samples.into()),
        );
        let batch = Batch {
            sensors: smallvec![single_sensor_batch],
        };
        let sync_receiver = event_bus.publish(batch).await?;
        all_batches_waiter.add(sync_receiver.activate()).await;
    }

    // Wait for all batches to sync
    all_batches_waiter.wait().await?;

    println!("Done reading CSV");
    Ok(())
}

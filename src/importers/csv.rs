use crate::{
    bus::{utils::WaitForAll, EventBus},
    datamodel::batch::{Batch, Sample, TypedSamples},
};
use anyhow::Result;
use csv_async::AsyncReader;
use futures::{io, StreamExt};
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

    let mut toto = WaitForAll::new();

    let mut i = 0;

    println!("Reading CSV");
    while let Some(record) = records.next().await {
        let record = record.unwrap();
        //println!("{:?}", record);

        current_samples.push(Sample {
            timestamp_ms: 0,
            value: i,
        });

        i += 1;

        if current_samples.len() >= batch_size {
            let batch = Batch {
                sensor_uuid: uuid::Uuid::from_bytes([0; 16]),
                sensor_name: "test".to_string(),
                samples: Arc::new(TypedSamples::Integer(current_samples)),
            };
            let sync_receiver = event_bus.publish(batch).await?;
            //sync_receiver.activate().recv().await?;
            current_samples = vec![];
            toto.add(sync_receiver.activate()).await;
        }
    }

    if !current_samples.is_empty() {
        let batch = Batch {
            sensor_uuid: uuid::Uuid::from_bytes([0; 16]),
            sensor_name: "test".to_string(),
            samples: Arc::new(TypedSamples::Integer(current_samples)),
        };
        let sync_receiver = event_bus.publish(batch).await?;
        toto.add(sync_receiver.activate()).await;
    }

    // Wololo ??
    toto.wait().await?;

    println!("Done reading CSV");
    Ok(())
}

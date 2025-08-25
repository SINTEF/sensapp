use anyhow::{Result, bail};
use gcp_bigquery_client::{
    google::cloud::bigquery::storage::v1::AppendRowsResponse, storage::TableDescriptor,
};
use tokio_stream::StreamExt;
use tonic::Streaming;

use super::BigQueryStorage;
use tracing::{debug, error};

pub async fn publish_rows(
    bqs: &BigQueryStorage,
    table_name: &'static str,
    table_descriptor: &TableDescriptor,
    rows: Vec<impl prost::Message>,
) -> Result<()> {
    if rows.is_empty() {
        debug!("BigQuery: No {} rows to publish", table_name);
        return Ok(());
    }

    debug!("BigQuery: Publishing {} rows to {}", rows.len(), table_name);

    let stream_name = bqs.new_stream_name(table_name.to_string());
    let trace_id = create_trace_id(table_name);

    let streaming = bqs
        .client()
        .write()
        .await
        .storage_mut()
        .append_rows(&stream_name, table_descriptor, &rows, trace_id)
        .await?;

    check_streaming(streaming).await?;

    Ok(())
}

async fn check_streaming(mut streaming: Streaming<AppendRowsResponse>) -> Result<()> {
    while let Some(response) = streaming.next().await {
        let response = response?;
        if !response.row_errors.is_empty() {
            for error in response.row_errors {
                error!("BigQuery Row Error: {:?}", error);
            }
            bail!("Failed to publish rows");
        }
    }

    Ok(())
}

pub fn create_trace_id(context: &str) -> String {
    format!("sensapp-{}-{}", context, uuid::Uuid::new_v4())
}

// pub fn convert_sensapp_timestamp_to_prost_timestamp(
//     timestamp: SensAppDateTime,
// ) -> Result<prost_types::Timestamp> {
//     let unix_seconds = timestamp.to_unix_seconds();
//     let seconds = unix_seconds
//         .trunc()
//         .to_i64()
//         .ok_or_else(|| anyhow!("Failed to convert seconds"))?;

//     let nanos = (unix_seconds.fract() * 1_000_000_000.0)
//         .to_i32()
//         .ok_or_else(|| anyhow!("Failed to convert nanos"))?;

//     Ok(prost_types::Timestamp { seconds, nanos })
// }

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
    // use prost_types::Timestamp;
    //
    // #[test]
    // fn test_convert_sensapp_timestamp_to_prost_timestamp() {
    //     // Test case 1: Simple whole second
    //     let sensapp_time = SensAppDateTime::from_unix_seconds_i64(1625097600); // 2021-07-01 00:00:00 UTC
    //     let result = convert_sensapp_timestamp_to_prost_timestamp(sensapp_time).unwrap();
    //     assert_eq!(
    //         result,
    //         Timestamp {
    //             seconds: 1625097600,
    //             nanos: 0
    //         }
    //     );

    //     // Test case 2: With fractional seconds
    //     let sensapp_time = SensAppDateTime::from_unix_seconds_i64(1625097600)
    //         + hifitime::Duration::from_milliseconds(500.0);
    //     let result = convert_sensapp_timestamp_to_prost_timestamp(sensapp_time).unwrap();
    //     assert_eq!(
    //         result,
    //         Timestamp {
    //             seconds: 1625097600,
    //             nanos: 500_000_000
    //         }
    //     );

    //     // Test case 3: Current time
    //     let sensapp_time = SensAppDateTime::now().unwrap();
    //     let result = convert_sensapp_timestamp_to_prost_timestamp(sensapp_time).unwrap();
    //     assert!(result.seconds > 0);
    //     assert!(result.nanos >= 0 && result.nanos < 1_000_000_000);

    //     // Test case 4: Edge case - christmas 2124
    //     const CHRISTMAS_UNIX_TIMESTAMP: i64 = 4_890_758_400;
    //     let max_time = SensAppDateTime::from_unix_seconds_i64(CHRISTMAS_UNIX_TIMESTAMP);
    //     let result = convert_sensapp_timestamp_to_prost_timestamp(max_time).unwrap();
    //     assert_eq!(result.seconds, CHRISTMAS_UNIX_TIMESTAMP);
    //     assert_eq!(result.nanos, 0);

    //     // Test case: Time before Unix epoch
    //     let before_epoch = SensAppDateTime::from_unix_seconds_i64(-1);
    //     let result = convert_sensapp_timestamp_to_prost_timestamp(before_epoch).unwrap();
    //     assert_eq!(result.seconds, -1);
    // }
}

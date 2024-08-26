use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::SqlitePool;

use crate::{
    crud::{list_cursor::ListCursor, viewmodel::sensor_viewmodel::SensorViewModel},
    datamodel::matchers::SensorMatcher,
};

pub async fn list_sensors(
    pool: &SqlitePool,
    matcher: SensorMatcher,
    cursor: ListCursor,
    limit: usize,
) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)> {
    // We fetch the limit + 1 to know if there is a next page
    let query_limit = limit as i64 + 1;
    let query = sqlx::query!(
        r#"
        SELECT uuid, name, created_at, type, unit, labels
        FROM sensor_labels_view
        WHERE (created_at, uuid) >= (?, ?)
        ORDER BY created_at ASC, uuid ASC
        LIMIT ?
        "#,
        cursor.next_created_at,
        cursor.next_uuid,
        query_limit,
    );

    let mut connection = pool.acquire().await?;
    let mut records = query.fetch_all(&mut *connection).await?;

    // check if there is a next page
    let next_cursor = if records.len() == limit + 1 {
        let last = records.pop().unwrap();
        let last_created_at = last.created_at.to_string();
        Some(ListCursor::new(last_created_at, last.uuid))
    } else {
        None
    };

    let sensors_views = records
        .into_iter()
        .map(|record| {
            // labels is a json object that need to be parsed and transformed into a HashMap
            let labels: BTreeMap<String, String> = serde_json::from_str(&record.labels)?;
            Ok(SensorViewModel {
                uuid: uuid::Uuid::parse_str(&record.uuid)?,
                name: record.name,
                // TODO: parse created_at
                created_at: Some(record.created_at.to_string()),
                sensor_type: record.r#type,
                unit: record.unit,
                labels,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((sensors_views, next_cursor))
}

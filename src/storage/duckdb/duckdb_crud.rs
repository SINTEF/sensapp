use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use duckdb::{params, CachedStatement, Connection};
use tokio::sync::Mutex;

use crate::{
    crud::{list_cursor::ListCursor, viewmodel::sensor_viewmodel::SensorViewModel},
    datamodel::{matchers::SensorMatcher, sensapp_datetime::SensAppDateTimeExt, SensAppDateTime},
};

pub async fn list_sensors(
    connection: Arc<Mutex<Connection>>,
    matcher: SensorMatcher,
    cursor: ListCursor,
    limit: usize,
) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)> {
    // We fetch the limit + 1 to know if there is a next page
    let query_limit = limit as i64 + 1;

    let connection = connection.lock().await;

    let cursor_next_created_at = cursor.next_created_at.parse::<i64>()?;
    let cursor_next_datetime =
        SensAppDateTime::from_unix_microseconds_i64(cursor_next_created_at).to_rfc3339();

    // cast uuid to varchar, because casting parameter to uuid
    // fails with following error:
    // Assertion failed: (GetType() == other.GetType()), function Copy, file base_statistics.cpp, line 220.
    let mut select_stmt: CachedStatement = connection.prepare_cached(
        r#"
        SELECT uuid, name, created_at, type, unit, labels
        FROM sensor_labels_view
        WHERE (created_at, CAST(uuid AS VARCHAR)) >= (CAST(? AS TIMESTAMP), ?)
        ORDER BY created_at ASC, uuid ASC
        LIMIT ?
        "#,
    )?;

    let mut rows = select_stmt
        .query(params![cursor_next_datetime, cursor.next_uuid, query_limit])
        .map_err(|e| {
            println!("Failed to execute query: {}", e);
            e
        })?;

    let mut sensors_views: Vec<SensorViewModel> = Vec::with_capacity(limit);
    let mut cursor: Option<ListCursor> = None;

    while let Some(row) = rows.next()? {
        let uuid_string: String = row.get(0)?;
        let uuid = uuid::Uuid::parse_str(&uuid_string)?;
        let created_at: i64 = row.get(2)?;

        // If we reached the limit, we use the value as a cursor
        if sensors_views.len() == limit {
            cursor = Some(ListCursor::new(created_at.to_string(), uuid.to_string()));
            break;
        }

        let name: String = row.get(1)?;
        let sensor_type: String = row.get(3)?;
        let unit: Option<String> = row.get(4)?;
        let labels_string: String = row.get(5)?;
        let labels: BTreeMap<String, String> = serde_json::from_str(&labels_string)?;

        let created_at_datetime = SensAppDateTime::from_unix_microseconds_i64(created_at);
        let created_at_rfc3339 = created_at_datetime.to_rfc3339();

        let sensor_view_model = SensorViewModel {
            uuid,
            name,
            created_at: Some(created_at_rfc3339),
            sensor_type,
            unit,
            labels,
        };
        sensors_views.push(sensor_view_model);
    }

    Ok((sensors_views, cursor))
}

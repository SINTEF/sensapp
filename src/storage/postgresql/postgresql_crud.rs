use std::collections::BTreeMap;

use anyhow::anyhow;
use anyhow::Result;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::types::time::PrimitiveDateTime;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;

use crate::crud::{list_cursor::ListCursor, viewmodel::sensor_viewmodel::SensorViewModel};
use crate::datamodel::matchers::SensorMatcher;
use crate::storage::postgresql::postgresql_matchers::append_sensor_matcher_to_query;

pub async fn list_sensors(
    pool: &PgPool,
    matcher: SensorMatcher,
    cursor: ListCursor,
    limit: usize,
) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)> {
    // We fetch the limit + 1 to know if there is a next page
    let query_limit = limit as i64 + 1;

    let cursor_next_created_at_timestamp = cursor.next_created_at.parse::<i128>()?;
    let cursor_next_created_at_offset_datetime =
        ::time::OffsetDateTime::from_unix_timestamp_nanos(cursor_next_created_at_timestamp)?;
    let cursor_uuid = uuid::Uuid::parse_str(&cursor.next_uuid)?;

    let mut query: Query<Postgres, PgArguments>;
    let mut query_string: String;

    if matcher.is_all() {
        query = sqlx::query(
            r#"SELECT uuid, name, created_at, type, unit, labels
FROM sensor_labels_view
WHERE (created_at, uuid) >= ($1, $2)
ORDER BY created_at ASC, uuid ASC
LIMIT $3"#,
        )
        .bind(cursor_next_created_at_offset_datetime)
        .bind(cursor_uuid)
        .bind(query_limit);
    } else {
        query_string = String::from(
            r#"SELECT uuid, name, created_at, type, unit, labels
FROM sensor_labels_view
WHERE (created_at, uuid) >= ($1, $2)
AND sensor_id IN (
"#,
        );

        let mut params: Vec<String> = Vec::new();

        append_sensor_matcher_to_query(&mut query_string, &mut params, &matcher, 2);

        query_string.push_str(
            r#"
)
ORDER BY created_at ASC, uuid ASC
LIMIT $"#,
        );
        query_string.push_str((params.len() + 3).to_string().as_str());

        //println!("query_string: {}", query_string);
        //println!("params: {:?}", params);

        query = sqlx::query(&query_string)
            .bind(cursor_next_created_at_offset_datetime)
            .bind(cursor_uuid);

        for param in params {
            query = query.bind(param);
        }

        query = query.bind(query_limit);
    }

    let mut connection = pool.acquire().await?;
    let mut records = query.fetch_all(&mut *connection).await?;

    // check if there is a next page
    let next_cursor = if records.len() == limit + 1 {
        let last = records.pop().unwrap();
        let last_created_at_datetime: PrimitiveDateTime = last.get("created_at");
        let last_created_at_timestamp: i128 =
            last_created_at_datetime.assume_utc().unix_timestamp_nanos();
        let last_uuid: uuid::Uuid = last.get("uuid");
        Some(ListCursor::new(
            last_created_at_timestamp.to_string(),
            last_uuid.to_string(),
        ))
    } else {
        None
    };

    let sensors_views = records
        .into_iter()
        .map(|record| {
            let uuid: uuid::Uuid = record.get("uuid");
            let labels_json: serde_json::Value = record.get("labels");
            // labels is a json object that need to be parsed and transformed into a Map
            let labels: BTreeMap<String, String> = labels_json
                .as_object()
                .ok_or_else(|| anyhow!("labels_json is not an object"))?
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.as_str().unwrap_or("").to_string()))
                .collect();
            let name: String = record.get("name");
            let created_at_datetime: PrimitiveDateTime = record.get("created_at");

            let created_at_rfc3339: String = created_at_datetime
                .assume_utc()
                .format(&::time::format_description::well_known::Rfc3339)?;

            let sensor_type: String = record.get("type");
            let unit: Option<String> = record.get("unit");

            Ok(SensorViewModel {
                uuid,
                name,
                created_at: Some(created_at_rfc3339),
                sensor_type,
                unit,
                labels,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((sensors_views, next_cursor))
}

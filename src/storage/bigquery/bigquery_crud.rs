use std::collections::BTreeMap;

use crate::{
    crud::{list_cursor::ListCursor, viewmodel::sensor_viewmodel::SensorViewModel},
    datamodel::sensor,
};
use anyhow::{anyhow, Result};
use gcp_bigquery_client::model::{
    query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
    query_parameter_value::QueryParameterValue, query_request::QueryRequest,
};
use time::OffsetDateTime;

use super::BigQueryStorage;

pub async fn list_sensors(
    bqs: &BigQueryStorage,
    cursor: ListCursor,
    limit: usize,
) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)> {
    // We fetch the limit + 1 to know if there is a next page
    let query_limit = limit + 1;

    let mut query_request = QueryRequest::new(
        r#"
        SELECT uuid, name, UNIX_MICROS(created_at) as created_at, type, unit, labels
        FROM `{dataset_id}.sensor_labels_view`
        WHERE created_at > TIMESTAMP_MICROS(@created_at)
        OR (created_at = TIMESTAMP_MICROS(@created_at) AND uuid >= @uuid)
        ORDER BY created_at ASC, uuid ASC
        LIMIT @limit
        "#
        .replace("{dataset_id}", bqs.dataset_id()),
    );
    // BigQuery doesn't support the >= operator on STRUCT:
    // Greater than is not defined for arguments of type STRUCT<TIMESTAMP, STRING>
    // WHERE (created_at, uuid) >= (TIMESTAMP_MILLIS(@created_at), @uuid)

    let limit_query_parameter = QueryParameter {
        name: Some("limit".to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "INT64".to_string(),
            struct_types: None,
            array_type: None,
        }),
        parameter_value: Some(QueryParameterValue {
            value: Some(query_limit.to_string()),
            struct_values: None,
            array_values: None,
        }),
    };
    let created_at_query_parameter = QueryParameter {
        name: Some("created_at".to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "INT64".to_string(),
            struct_types: None,
            array_type: None,
        }),
        parameter_value: Some(QueryParameterValue {
            value: Some(cursor.next_created_at),
            struct_values: None,
            array_values: None,
        }),
    };
    let uuid_query_parameter = QueryParameter {
        name: Some("uuid".to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "STRING".to_string(),
            struct_types: None,
            array_type: None,
        }),
        parameter_value: Some(QueryParameterValue {
            value: Some(cursor.next_uuid),
            struct_values: None,
            array_values: None,
        }),
    };

    query_request.query_parameters = Some(vec![
        limit_query_parameter,
        created_at_query_parameter,
        uuid_query_parameter,
    ]);

    let mut result = bqs
        .client()
        .read()
        .await
        .job()
        .query(bqs.project_id(), query_request)
        .await?;

    let mut sensors_views: Vec<SensorViewModel> = Vec::with_capacity(result.row_count());
    let mut cursor: Option<ListCursor> = None;

    while result.next_row() {
        let uuid_string: String = result
            .get_string(0)?
            .ok_or_else(|| anyhow!("uuid is null"))?;
        let uuid = uuid::Uuid::parse_str(&uuid_string)?;
        let name: String = result
            .get_string(1)?
            .ok_or_else(|| anyhow!("name is null"))?;
        let created_at: i64 = result
            .get_i64(2)?
            .ok_or_else(|| anyhow!("created_at is null"))?;

        let created_at_offset_datetime =
            OffsetDateTime::from_unix_timestamp_nanos((created_at as i128) * 1_000)?;
        let created_at_rfc3339: String =
            created_at_offset_datetime.format(&::time::format_description::well_known::Rfc3339)?;

        // If we reached the limit, we use the value as a cursor
        if sensors_views.len() == limit {
            cursor = Some(ListCursor::new(created_at.to_string(), uuid_string));
            break;
        }

        let sensor_type: String = result
            .get_string(3)?
            .ok_or_else(|| anyhow!("type is null"))?;
        let unit: Option<String> = result.get_string(4)?;
        let labels_json: String = result
            .get_string(5)?
            .ok_or_else(|| anyhow!("labels is null"))?;
        let labels: BTreeMap<String, String> = serde_json::from_str(&labels_json)?;

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

use crate::storage::StorageError;
use anyhow::{Result, anyhow};
use gcp_bigquery_client::model::{
    query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
    query_parameter_value::QueryParameterValue, query_request::QueryRequest,
};
use hybridmap::HybridMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

use super::{
    BigQueryStorage,
    bigquery_labels_utilities::{
        get_or_create_labels_description_ids, get_or_create_labels_name_ids,
    },
    bigquery_prost_structs::{Label as ProstLabel, Sensor as ProstSensor},
    bigquery_table_descriptors::LABELS_DESCRIPTOR,
    bigquery_units_utilities::get_or_create_units_ids,
    bigquery_utilities::publish_rows,
};
use crate::{
    datamodel::{SensAppVec, Sensor, unit::Unit},
    storage::bigquery::bigquery_table_descriptors::SENSORS_DESCRIPTOR,
};

// We assume that the sensor ids are stable and never updated from BigQuery.
static SENSOR_ID_CACHE: Lazy<RwLock<HybridMap<Uuid, i64>>> =
    Lazy::new(|| RwLock::new(HybridMap::new()));

pub async fn get_sensor_ids_or_create_sensors(
    bqs: &BigQueryStorage,
    sensors: &[Arc<Sensor>],
) -> Result<HybridMap<Uuid, i64>> {
    let mut unknown_sensors: SensAppVec<Arc<Sensor>> = SensAppVec::new();

    let mut result = HybridMap::new();

    {
        let sensor_ids_read = SENSOR_ID_CACHE.read().await;
        for sensor in sensors {
            let uuid = sensor.uuid;
            match sensor_ids_read.get(&uuid) {
                Some(id) => {
                    result.insert(uuid, *id);
                }
                None => {
                    unknown_sensors.push(sensor.clone());
                }
            };
        }
    }

    debug!("BigQuery: Found {} known sensors", result.len());
    debug!("BigQuery: Found {} unknown sensors", unknown_sensors.len());

    if unknown_sensors.is_empty() {
        return Ok(result);
    }

    let just_the_uuids = unknown_sensors
        .iter()
        .map(|sensor| sensor.uuid)
        .collect::<Vec<_>>();

    let found_ids = get_existing_sensors_ids_from_uuids(bqs, &just_the_uuids).await?;
    {
        let mut sensor_ids_write = SENSOR_ID_CACHE.write().await;
        for (uuid, id) in found_ids.iter() {
            sensor_ids_write.insert(*uuid, *id);
            result.insert(*uuid, *id);
        }
    }

    let sensors_to_create = unknown_sensors
        .into_iter()
        .filter(|sensor| found_ids.get(&sensor.uuid).is_none())
        .collect::<SensAppVec<_>>();

    if sensors_to_create.is_empty() {
        return Ok(result);
    }

    info!("BigQuery: Creating {} new sensors", sensors_to_create.len());

    let new_ids = create_sensors(bqs, &sensors_to_create).await?;
    {
        let mut sensor_ids_write = SENSOR_ID_CACHE.write().await;
        for (uuid, id) in new_ids.iter() {
            sensor_ids_write.insert(*uuid, *id);
            result.insert(*uuid, *id);
        }
    }

    Ok(result)
}

async fn get_existing_sensors_ids_from_uuids(
    bqs: &BigQueryStorage,
    sensor_uuids: &[Uuid],
) -> Result<HybridMap<Uuid, i64>> {
    let mut query_request = QueryRequest::new(
        r#"
        SELECT uuid, sensor_id
        FROM `{dataset_id}.sensors`
        WHERE uuid IN UNNEST(@sensor_uuids)
    "#
        .replace("{dataset_id}", bqs.dataset_id()),
    );

    let query_parameter = QueryParameter {
        name: Some("sensor_uuids".to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "ARRAY".to_string(),
            struct_types: None,
            array_type: Some(Box::new(QueryParameterType {
                r#type: "STRING".to_string(),
                struct_types: None,
                array_type: None,
            })),
        }),
        parameter_value: Some(QueryParameterValue {
            value: None,
            struct_values: None,
            array_values: Some(
                sensor_uuids
                    .iter()
                    .map(|uuid| QueryParameterValue {
                        value: Some(uuid.to_string()),
                        struct_values: None,
                        array_values: None,
                    })
                    .collect(),
            ),
        }),
    };

    query_request.query_parameters = Some(vec![query_parameter]);

    let mut result = bqs
        .client()
        .read()
        .await
        .job()
        .query(bqs.project_id(), query_request)
        .await?;

    let mut results_map = HybridMap::with_capacity(result.row_count());

    while result.next_row() {
        let uuid = result
            .get_string(0)?
            .ok_or_else(|| anyhow::Error::from(StorageError::missing_field("uuid", None, None)))?;
        let sensor_id = result.get_i64(1)?.ok_or_else(|| {
            let parsed_uuid = Uuid::parse_str(&uuid).ok();
            anyhow::Error::from(StorageError::missing_field("sensor_id", parsed_uuid, None))
        })?;
        debug!(
            "BigQuery: Found existing sensor {} with id: {}",
            uuid, sensor_id
        );
        results_map.insert(Uuid::parse_str(&uuid)?, sensor_id);
    }

    Ok(results_map)
}

async fn create_sensors(
    bqs: &BigQueryStorage,
    sensors: &[Arc<Sensor>],
) -> Result<HybridMap<Uuid, i64>> {
    sinteflake::update_time_async().await?;

    let mut map = HybridMap::with_capacity(sensors.len());
    let mut units: SensAppVec<Unit> = SensAppVec::new();
    let mut labels_names: SensAppVec<String> = SensAppVec::new();
    let mut labels_descriptions: SensAppVec<String> = SensAppVec::new();
    for sensor in sensors {
        let sensor_id = sinteflake::next_id_with_hash_async(sensor.uuid.as_bytes()).await? as i64;
        map.insert(sensor.uuid, sensor_id);
        if let Some(unit) = &sensor.unit {
            units.push(unit.clone());
        }
        for label in sensor.labels.iter() {
            labels_names.push(label.0.clone());
            labels_descriptions.push(label.1.clone());
        }
    }

    let units_map = get_or_create_units_ids(bqs, units).await?;
    let labels_names_map = get_or_create_labels_name_ids(bqs, labels_names).await?;
    let labels_descriptions_map =
        get_or_create_labels_description_ids(bqs, labels_descriptions).await?;

    // create the sensors
    let rows = sensors
        .iter()
        .map(|sensor| {
            let sensor_id = map.get(&sensor.uuid).ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "sensor_id",
                    Some(sensor.uuid),
                    Some(&sensor.name),
                ))
            })?;
            let unit = sensor
                .unit
                .as_ref()
                .and_then(|unit| units_map.get(&unit.name).copied());
            ProstSensor {
                sensor_id: *sensor_id,
                uuid: sensor.uuid.to_string(),
                name: sensor.name.clone(),
                r#type: sensor.sensor_type.to_string(),
                unit,
            }
        })
        .collect::<Vec<_>>();

    publish_rows(bqs, "sensors", &SENSORS_DESCRIPTOR, rows).await?;

    // create the labels
    let mut labels_rows = Vec::new();
    for sensor in sensors {
        let sensor_id = map.get(&sensor.uuid).ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "sensor_id",
                Some(sensor.uuid),
                Some(&sensor.name),
            ))
        })?;
        for (name, description) in sensor.labels.iter() {
            let name_id = labels_names_map.get(name).ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "label_name_id",
                    Some(sensor.uuid),
                    Some(&sensor.name),
                ))
            })?;
            let description_id = labels_descriptions_map.get(description).ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "label_description_id",
                    Some(sensor.uuid),
                    Some(&sensor.name),
                ))
            })?;
            labels_rows.push(ProstLabel {
                sensor_id: *sensor_id,
                name: *name_id,
                description: Some(*description_id),
            });
        }
    }

    publish_rows(bqs, "labels", &LABELS_DESCRIPTOR, labels_rows).await?;

    Ok(map)
}

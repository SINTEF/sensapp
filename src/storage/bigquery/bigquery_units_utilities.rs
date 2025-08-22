use std::{collections::HashMap, num::NonZeroUsize};

use crate::storage::StorageError;
use anyhow::{Result, anyhow};
use clru::CLruCache;
use gcp_bigquery_client::model::{
    query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
    query_parameter_value::QueryParameterValue, query_request::QueryRequest,
};
use hybridmap::HybridMap;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::datamodel::{SensAppVec, unit::Unit};

use super::{
    BigQueryStorage, bigquery_prost_structs::Unit as ProstUnit,
    bigquery_table_descriptors::UNITS_DESCRIPTOR, bigquery_utilities::publish_rows,
};

static UNITS_CACHE: Lazy<Mutex<CLruCache<String, i64>>> =
    Lazy::new(|| Mutex::new(CLruCache::new(NonZeroUsize::new(16384).unwrap())));

pub async fn get_or_create_units_ids(
    bqs: &BigQueryStorage,
    units: SensAppVec<Unit>,
) -> Result<HybridMap<String, i64>> {
    if units.is_empty() {
        return Ok(HybridMap::new());
    }

    let mut unknown_units: HashMap<String, Unit> = HashMap::new();

    let mut result = HybridMap::new();

    {
        let mut cache_guard = UNITS_CACHE.lock().await;
        for unit in units.into_iter() {
            match cache_guard.get(&unit.name) {
                Some(id) => {
                    result.insert(unit.name, *id);
                }
                None => {
                    unknown_units.insert(unit.name.clone(), unit);
                }
            };
        }
    }

    debug!("BigQuery: Found {} known units", result.len());
    debug!("BigQuery: Found {} unknown units", unknown_units.len());

    if unknown_units.is_empty() {
        return Ok(result);
    }

    let just_the_units = unknown_units
        .values()
        .cloned()
        .collect::<SensAppVec<Unit>>();

    let found_ids = get_existing_units_ids(bqs, &just_the_units).await?;
    {
        let mut cache_guard = UNITS_CACHE.lock().await;
        for (unit, id) in found_ids.iter() {
            cache_guard.put(unit.clone(), *id);
            result.insert(unit.clone(), *id);
        }
    }

    let units_to_create = unknown_units
        .into_values()
        .filter(|unit| found_ids.get(&unit.name).is_none())
        .collect::<SensAppVec<Unit>>();

    if units_to_create.is_empty() {
        return Ok(result);
    }

    info!("BigQuery: Creating {} new units", units_to_create.len());

    let new_ids = create_units(bqs, units_to_create).await?;
    {
        let mut cache_guard = UNITS_CACHE.lock().await;
        for (unit, id) in new_ids.iter() {
            cache_guard.put(unit.clone(), *id);
            result.insert(unit.clone(), *id);
        }
    }

    Ok(result)
}

async fn get_existing_units_ids(
    bqs: &BigQueryStorage,
    units: &[Unit],
) -> Result<HybridMap<String, i64>> {
    let mut query_request = QueryRequest::new(
        r#"
        SELECT id, name
        FROM `{dataset_id}.units`
        WHERE name IN UNNEST(@names)
        "#
        .replace("{dataset_id}", bqs.dataset_id()),
    );

    let query_parameter = QueryParameter {
        name: Some("names".to_string()),
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
                units
                    .iter()
                    .map(|unit| QueryParameterValue {
                        value: Some(unit.name.clone()),
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
        let id = result.get_i64(0)?.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("unit_id", None, None))
        })?;
        let name = result.get_string(1)?.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("unit_name", None, None))
        })?;

        results_map.insert(name, id);
    }

    Ok(results_map)
}

async fn create_units(
    bqs: &BigQueryStorage,
    units: SensAppVec<Unit>,
) -> Result<HybridMap<String, i64>> {
    let mut map = HybridMap::with_capacity(units.len());

    for unit in units.as_ref() {
        let id = sinteflake::next_id_with_hash_async(unit.name.as_bytes()).await? as i64;
        map.insert(unit.name.clone(), id);
    }

    let rows = units
        .into_iter()
        .map(|unit| ProstUnit {
            id: *map
                .get(&unit.name)
                .expect("Internal consistency error: Unit missing from cached map"),
            name: unit.name,
            description: unit.description,
        })
        .collect::<Vec<_>>();

    publish_rows(bqs, "units", &UNITS_DESCRIPTOR, rows).await?;

    Ok(map)
}

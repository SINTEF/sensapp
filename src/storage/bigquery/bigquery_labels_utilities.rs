use std::{collections::HashSet, num::NonZeroUsize};

use anyhow::{Result, anyhow};
use clru::CLruCache;
use gcp_bigquery_client::model::{
    query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
    query_parameter_value::QueryParameterValue, query_request::QueryRequest,
};
use hybridmap::HybridMap;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::datamodel::SensAppVec;

use super::{
    BigQueryStorage,
    bigquery_prost_structs::{
        LabelDescriptionDictionary as ProstLabelDescriptionDictionary,
        LabelNameDictionary as ProstLabelNameDictionary,
    },
    bigquery_table_descriptors::{
        LABELS_DESCRIPTION_DICTIONARY_DESCRIPTOR, LABELS_NAME_DICTIONARY_DESCRIPTOR,
    },
    bigquery_utilities::publish_rows,
};

static LABELS_NAME_CACHE: Lazy<Mutex<CLruCache<String, i64>>> =
    Lazy::new(|| Mutex::new(CLruCache::new(NonZeroUsize::new(16384).unwrap())));

pub async fn get_or_create_labels_name_ids(
    bqs: &BigQueryStorage,
    labels: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    if labels.is_empty() {
        return Ok(HybridMap::new());
    }

    let mut unknown_labels: HashSet<String> = HashSet::new();
    let mut result = HybridMap::new();

    {
        let mut cache_guard = LABELS_NAME_CACHE.lock().await;
        for label in labels.into_iter() {
            match cache_guard.get(&label) {
                Some(id) => {
                    result.insert(label, *id);
                }
                None => {
                    unknown_labels.insert(label);
                }
            };
        }
    }

    println!("Found {} known label names", result.len());
    println!("Found {} unknown label names", unknown_labels.len());

    if unknown_labels.is_empty() {
        return Ok(result);
    }

    let just_the_labels = unknown_labels
        .iter()
        .cloned()
        .collect::<SensAppVec<String>>();

    let found_ids = get_existing_labels_name_ids(bqs, &just_the_labels).await?;
    {
        let mut cache_guard = LABELS_NAME_CACHE.lock().await;
        for (label, id) in found_ids.iter() {
            cache_guard.put(label.clone(), *id);
            result.insert(label.clone(), *id);
        }
    }

    let labels_to_create = unknown_labels
        .into_iter()
        .filter(|label| found_ids.get(label).is_none())
        .collect::<SensAppVec<String>>();

    if labels_to_create.is_empty() {
        return Ok(result);
    }

    println!("Found {} label names to create", labels_to_create.len());

    let new_ids = create_labels_name(bqs, labels_to_create).await?;
    {
        let mut cache_guard = LABELS_NAME_CACHE.lock().await;
        for (label, id) in new_ids.iter() {
            cache_guard.put(label.clone(), *id);
            result.insert(label.clone(), *id);
        }
    }

    Ok(result)
}

async fn get_existing_labels_name_ids(
    bqs: &BigQueryStorage,
    labels: &[String],
) -> Result<HybridMap<String, i64>> {
    let mut query_request = QueryRequest::new(
        r#"
        SELECT id, name
        FROM `{dataset_id}.labels_name_dictionary`
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
                labels
                    .iter()
                    .map(|label| QueryParameterValue {
                        value: Some(label.clone()),
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
        let id = result.get_i64(0)?.ok_or_else(|| anyhow!("id is null"))?;
        let name = result
            .get_string(1)?
            .ok_or_else(|| anyhow!("name is null"))?;

        results_map.insert(name, id);
    }

    Ok(results_map)
}

async fn create_labels_name(
    bqs: &BigQueryStorage,
    labels: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    let mut map = HybridMap::with_capacity(labels.len());

    for label in labels.as_ref() {
        let id = sinteflake::next_id_with_hash_async(label.as_bytes()).await? as i64;
        map.insert(label.clone(), id);
    }

    let rows = labels
        .into_iter()
        .map(|label| ProstLabelNameDictionary {
            id: *map
                .get(&label)
                .expect("Label not found in the map, this should not happen"),
            name: label,
        })
        .collect::<Vec<_>>();

    publish_rows(
        bqs,
        "labels_name_dictionary",
        &LABELS_NAME_DICTIONARY_DESCRIPTOR,
        rows,
    )
    .await?;

    Ok(map)
}

static LABELS_DESCRIPTION_CACHE: Lazy<Mutex<CLruCache<String, i64>>> =
    Lazy::new(|| Mutex::new(CLruCache::new(NonZeroUsize::new(16384).unwrap())));

pub async fn get_or_create_labels_description_ids(
    bqs: &BigQueryStorage,
    labels: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    if labels.is_empty() {
        return Ok(HybridMap::new());
    }

    let mut unknown_labels: HashSet<String> = HashSet::new();
    let mut result = HybridMap::new();

    {
        let mut cache_guard = LABELS_DESCRIPTION_CACHE.lock().await;
        for label in labels.into_iter() {
            match cache_guard.get(&label) {
                Some(id) => {
                    result.insert(label, *id);
                }
                None => {
                    unknown_labels.insert(label);
                }
            };
        }
    }

    println!("Found {} known label descriptions", result.len());
    println!("Found {} unknown label descriptions", unknown_labels.len());

    if unknown_labels.is_empty() {
        return Ok(result);
    }

    let just_the_labels = unknown_labels
        .iter()
        .cloned()
        .collect::<SensAppVec<String>>();

    let found_ids = get_existing_labels_description_ids(bqs, &just_the_labels).await?;
    {
        let mut cache_guard = LABELS_DESCRIPTION_CACHE.lock().await;
        for (label, id) in found_ids.iter() {
            cache_guard.put(label.clone(), *id);
            result.insert(label.clone(), *id);
        }
    }

    let labels_to_create = unknown_labels
        .into_iter()
        .filter(|label| found_ids.get(label).is_none())
        .collect::<SensAppVec<String>>();

    if labels_to_create.is_empty() {
        return Ok(result);
    }

    println!(
        "Found {} label descriptions to create",
        labels_to_create.len()
    );

    let new_ids = create_labels_description(bqs, labels_to_create).await?;
    {
        let mut cache_guard = LABELS_DESCRIPTION_CACHE.lock().await;
        for (label, id) in new_ids.iter() {
            cache_guard.put(label.clone(), *id);
            result.insert(label.clone(), *id);
        }
    }

    Ok(result)
}

async fn get_existing_labels_description_ids(
    bqs: &BigQueryStorage,
    labels: &[String],
) -> Result<HybridMap<String, i64>> {
    let mut query_request = QueryRequest::new(
        r#"
        SELECT id, description
        FROM `{dataset_id}.labels_description_dictionary`
        WHERE description IN UNNEST(@descriptions)
        "#
        .replace("{dataset_id}", bqs.dataset_id()),
    );

    let query_parameter = QueryParameter {
        name: Some("descriptions".to_string()),
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
                labels
                    .iter()
                    .map(|label| QueryParameterValue {
                        value: Some(label.clone()),
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
        let id = result.get_i64(0)?.ok_or_else(|| anyhow!("id is null"))?;
        let description = result
            .get_string(1)?
            .ok_or_else(|| anyhow!("description is null"))?;

        results_map.insert(description, id);
    }

    Ok(results_map)
}

async fn create_labels_description(
    bqs: &BigQueryStorage,
    labels: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    let mut map = HybridMap::with_capacity(labels.len());

    for label in labels.as_ref() {
        let id = sinteflake::next_id_with_hash_async(label.as_bytes()).await? as i64;
        map.insert(label.clone(), id);
    }

    let rows = labels
        .into_iter()
        .map(|label| ProstLabelDescriptionDictionary {
            id: *map
                .get(&label)
                .expect("Label not found in the map, this should not happen"),
            description: label,
        })
        .collect::<Vec<_>>();

    publish_rows(
        bqs,
        "labels_description_dictionary",
        &LABELS_DESCRIPTION_DICTIONARY_DESCRIPTOR,
        rows,
    )
    .await?;

    Ok(map)
}

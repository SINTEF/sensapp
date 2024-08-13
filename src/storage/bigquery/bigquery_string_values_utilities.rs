use std::{collections::HashSet, num::NonZeroUsize};

use anyhow::{anyhow, Result};
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
    bigquery_prost_structs::StringValueDictionary,
    bigquery_table_descriptors::STRINGS_VALUES_DICTIONARY_DESCRIPTOR,
    bigquery_utilities::publish_rows, BigQueryStorage,
};

static STRING_VALUES_CACHE: Lazy<Mutex<CLruCache<String, i64>>> =
    Lazy::new(|| Mutex::new(CLruCache::new(NonZeroUsize::new(32768).unwrap())));

pub async fn get_or_create_string_values_ids(
    bqs: &BigQueryStorage,
    strings: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    let mut unknown_string_values: HashSet<String> = HashSet::new();

    let mut result = HybridMap::new();

    {
        let mut cache_guard = STRING_VALUES_CACHE.lock().await;
        for string_value in strings.into_iter() {
            match cache_guard.get(&string_value) {
                Some(id) => {
                    result.insert(string_value, *id);
                }
                None => {
                    unknown_string_values.insert(string_value);
                }
            };
        }
    }

    println!("Found {} known string values", result.len());
    println!(
        "Found {} unknown string values",
        unknown_string_values.len()
    );

    if unknown_string_values.is_empty() {
        return Ok(result);
    }

    let just_the_values = unknown_string_values
        .iter()
        .cloned()
        .collect::<SensAppVec<String>>();

    let found_ids = get_existing_string_values_ids(bqs, &just_the_values).await?;
    {
        let mut cache_guard = STRING_VALUES_CACHE.lock().await;
        for (value, id) in found_ids.iter() {
            cache_guard.put(value.clone(), *id);
            result.insert(value.clone(), *id);
        }
    }

    let values_to_create = unknown_string_values
        .into_iter()
        .filter(|value| found_ids.get(value).is_none())
        .collect::<SensAppVec<String>>();

    if values_to_create.is_empty() {
        return Ok(result);
    }

    println!("Found {} string values to create", values_to_create.len());

    let new_ids = create_string_values(bqs, values_to_create).await?;
    {
        let mut cache_guard = STRING_VALUES_CACHE.lock().await;
        for (value, id) in new_ids.into_iter() {
            cache_guard.put(value.clone(), id);
            result.insert(value, id);
        }
    }

    Ok(result)
}

async fn get_existing_string_values_ids(
    bqs: &BigQueryStorage,
    string_values: &[String],
) -> Result<HybridMap<String, i64>> {
    let mut query_request = QueryRequest::new(
        r#"
        SELECT id, value
        FROM `{dataset_id}.strings_values_dictionary`
        WHERE value IN UNNEST(@values)
        "#
        .replace("{dataset_id}", bqs.dataset_id()),
    );

    let query_parameter = QueryParameter {
        name: Some("values".to_string()),
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
                string_values
                    .iter()
                    .map(|string_value| QueryParameterValue {
                        value: Some(string_value.clone()),
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
        let value = result
            .get_string(1)?
            .ok_or_else(|| anyhow!("value is null"))?;

        results_map.insert(value, id);
    }

    Ok(results_map)
}

async fn create_string_values(
    bqs: &BigQueryStorage,
    string_values: SensAppVec<String>,
) -> Result<HybridMap<String, i64>> {
    let mut map = HybridMap::with_capacity(string_values.len());

    for string_value in string_values.as_ref() {
        let id = sinteflake::next_id_with_hash_async(string_value.as_bytes()).await? as i64;
        map.insert(string_value.clone(), id);
    }

    let rows = string_values
        .into_iter()
        .map(|string_value| StringValueDictionary {
            id: *map
                .get(&string_value)
                .expect("String value not found in the map, this should not happen"),
            value: string_value,
        })
        .collect::<Vec<_>>();

    publish_rows(
        bqs,
        "strings_values_dictionary",
        &STRINGS_VALUES_DICTIONARY_DESCRIPTOR,
        rows,
    )
    .await?;

    Ok(map)
}

use anyhow::Result;
use sqlx::PgPool;
use sqlx::Row;

use crate::datamodel::{
    matchers::{LabelMatcher, SensorMatcher, StringMatcher},
    SensAppVec,
};

fn append_string_matcher(
    query: &mut String,
    params: &mut Vec<String>,
    matcher: &StringMatcher,
    offset: usize,
) {
    match matcher {
        StringMatcher::All => {
            query.push_str(" IS NOT NULL");
            return;
        }
        StringMatcher::Equal(value) => {
            params.push(value.clone());
            query.push_str(" = $");
        }
        StringMatcher::NotEqual(value) => {
            params.push(value.clone());
            query.push_str(" <> $");
        }
        StringMatcher::Match(value) => {
            params.push(value.clone());
            query.push_str(" ~ $");
        }
        StringMatcher::NotMatch(value) => {
            params.push(value.clone());
            query.push_str(" !~ $");
        }
    }
    query.push_str((params.len() + offset).to_string().as_str());
}

fn append_name_string_matcher_to_query(
    query: &mut String,
    params: &mut Vec<String>,
    matcher: &StringMatcher,
    offset: usize,
) {
    if let StringMatcher::All = matcher {
        return;
    }

    const QUERY_PREFIX: &str = "SELECT sensor_id FROM sensors WHERE name";
    query.push_str(QUERY_PREFIX);
    append_string_matcher(query, params, matcher, offset);
}

fn append_label_matcher_to_query(
    query: &mut String,
    params: &mut Vec<String>,
    matcher: &LabelMatcher,
    offset: usize,
) {
    const QUERY_PART_A: &str = r#"SELECT sensor_id
FROM labels
INNER JOIN labels_name_dictionary ON labels.name = labels_name_dictionary.id
INNER JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
WHERE labels_name_dictionary.name = $"#;
    const QUERY_PART_B: &str = r#" AND labels_description_dictionary.description"#;

    query.push_str(QUERY_PART_A);
    params.push(matcher.name().clone());
    query.push_str((params.len() + offset).to_string().as_str());
    query.push_str(QUERY_PART_B);
    append_string_matcher(query, params, matcher.matcher(), offset);
}

pub fn append_sensor_matcher_to_query(
    query: &mut String,
    params: &mut Vec<String>,
    matcher: &SensorMatcher,
    offset: usize,
) {
    // We are working on set theory, and we need to know
    // if we are working on a subset of the data or if we
    // need to select all the data first.
    let mut need_select_all = true;

    let name_matcher = matcher.name_matcher();
    if name_matcher != &StringMatcher::All {
        append_name_string_matcher_to_query(query, params, name_matcher, offset);
        need_select_all = false;
    }

    // We need to know if we have at least a negative selector
    // if so, we need to apply them last.
    let mut has_negative_selector = false;
    if let Some(label_matchers) = matcher.label_matchers() {
        for label_matcher in label_matchers {
            if label_matcher.matcher().is_negative() {
                has_negative_selector = true;
                if !need_select_all {
                    break;
                }
            } else {
                need_select_all = false;
                if has_negative_selector {
                    break;
                }
            }
        }
    }

    if need_select_all {
        query.push_str("SELECT sensor_id FROM sensors");
    }

    if let Some(label_matchers) = matcher.label_matchers() {
        let mut positive_selectors: Vec<LabelMatcher>;
        let mut negative_selectors: Vec<LabelMatcher>;
        if has_negative_selector {
            positive_selectors = Vec::new();
            negative_selectors = Vec::new();
            for label_matcher in label_matchers {
                if label_matcher.matcher().is_negative() {
                    negative_selectors.push(label_matcher.clone());
                } else {
                    positive_selectors.push(label_matcher.clone());
                }
            }
        } else {
            positive_selectors = label_matchers.clone();
            negative_selectors = Vec::with_capacity(0);
        }

        for label_matcher in positive_selectors {
            query.push_str("\nINTERSECT\n");
            append_label_matcher_to_query(query, params, &label_matcher, offset);
        }

        if !negative_selectors.is_empty() {
            query.push_str("\nEXCEPT (\n");
            for (index, label_matcher) in negative_selectors.iter().enumerate() {
                if index > 0 {
                    query.push_str("\nUNION\n");
                }
                append_label_matcher_to_query(query, params, &label_matcher.negate(), offset);
            }
            query.push_str("\n)\n");
        }
    }
}

pub async fn get_sensor_ids_from_matcher(
    pool: &PgPool,
    matcher: SensorMatcher,
) -> Result<SensAppVec<i64>> {
    let mut query = String::new();
    let mut params: Vec<String> = Vec::new();

    println!("aaa: {:?}", matcher);

    append_sensor_matcher_to_query(&mut query, &mut params, &matcher, 0);

    println!("query: {:?}", query);

    let mut sqlx_query = sqlx::query(&query);
    for param in params {
        sqlx_query = sqlx_query.bind(param);
    }

    let mut connection = pool.acquire().await?;
    let records = sqlx_query.fetch_all(&mut *connection).await?;

    let labels = records
        .into_iter()
        .map(|record| record.get("sensor_id"))
        .collect::<SensAppVec<i64>>();

    Ok(labels)
}

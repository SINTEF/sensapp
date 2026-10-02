//! The dictionary of string values of the PostgreSQL family of backends (PostgreSQL and
//! TimescaleDB share the table): the ids of all the distinct strings of a batch with two
//! statements, instead of one lookup per sample.

use anyhow::Result;
use sqlx::PgConnection;
use std::collections::{BTreeSet, HashMap};

/// The dictionary ids of the given strings, adding the ones that are not in it yet.
///
/// The strings are inserted sorted, so that concurrent transactions that add overlapping sets
/// take their locks in the same order and cannot deadlock. A string another transaction added at
/// the same time is read again by the second statement.
pub async fn ensure_string_ids(
    connection: &mut PgConnection,
    strings: BTreeSet<&str>,
) -> Result<HashMap<String, i64>> {
    if strings.is_empty() {
        return Ok(HashMap::new());
    }
    let values: Vec<&str> = strings.into_iter().collect();
    sqlx::query(
        r#"
        INSERT INTO strings_values_dictionary (value)
        SELECT unnest($1::text[])
        ON CONFLICT (value) DO NOTHING
        "#,
    )
    .bind(&values)
    .execute(&mut *connection)
    .await?;
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, value FROM strings_values_dictionary WHERE value = ANY($1)")
            .bind(&values)
            .fetch_all(connection)
            .await?;
    Ok(rows.into_iter().map(|(id, value)| (value, id)).collect())
}

use anyhow::Result;
use cached::macros::cached;
use sqlx::{Executor, Postgres, Row, Transaction};

/**
 * Pure copy of the postgresql file, but duplicated because of the cached macros.
 *
 * Reusing the same file could lead to conflicts when an instance uses both postgresql and timescaledb
 * storages at the same time.
 */
#[cached(
    ttl_secs = 120,
    sync_writes = "default",
    key = "String",
    convert = { string_value.to_string() }
)]
pub async fn get_string_value_id_or_create(
    transaction: &mut Transaction<'_, Postgres>,
    string_value: &str,
) -> Result<i64> {
    let query = sqlx::query(
        r#"
        WITH inserted AS (
            INSERT INTO strings_values_dictionary (value)
            VALUES ($1)
            ON CONFLICT (value) DO NOTHING
            RETURNING id
        )
        SELECT id FROM inserted
        UNION ALL
        SELECT id FROM strings_values_dictionary WHERE value = $1
        LIMIT 1
    "#,
    )
    .bind(string_value);

    let string_value_id = transaction.fetch_one(query).await?.get("id");
    Ok(string_value_id)
}

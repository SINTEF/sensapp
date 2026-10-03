//! The `INSERT` of the samples of the PostgreSQL family (PostgreSQL and TimescaleDB), with or
//! without deduplication at ingestion.
//!
//! Without it the statement is `INSERT INTO table (columns) source`. With it, the rows of `source`
//! that are already stored are left out, and so are the repeated rows of `source` itself: a sample
//! is a duplicate when the series, the time and the value (the coordinates for a location) are the
//! same. The probe is bounded to the time window of the batch, so that the planner reads the
//! window once (with the BRIN index) and joins it with the batch, instead of probing the index for
//! every row of the batch: the second costs 0.36 ms a row on a table of a million rows.

/// Namespace of the advisory locks of the series, the first key of the two-integer form.
const SERIES_LOCK_NAMESPACE: i32 = 0x5345_4E53; // "SENS"

/// Takes, until the end of the transaction, a lock per series of the batch. Needed to deduplicate
/// with several writers (several instances, a retry that reaches another one while the first
/// request is still running): the check for a stored sample cannot see the rows another
/// transaction has not committed, so two writers of the same sample would both write it. With the
/// lock the second one waits for the first to commit, and its check (a new snapshot for every
/// statement in READ COMMITTED) then sees the sample. Other series are not blocked.
///
/// The locks are taken in the order of the ids, so that two writers of overlapping sets of series
/// cannot wait for each other. Take them before anything else that can wait for another
/// transaction (the dictionary of strings): a writer that waits on a lock then holds nothing the
/// others need. Two series whose ids are equal modulo 2^31 share a lock, which only serializes them.
pub async fn lock_series(
    connection: &mut sqlx::PgConnection,
    sensor_ids: &[i64],
) -> anyhow::Result<()> {
    if sensor_ids.is_empty() {
        return Ok(());
    }
    let mut keys: Vec<i32> = sensor_ids
        .iter()
        .map(|id| (*id & 0x7FFF_FFFF) as i32)
        .collect();
    keys.sort_unstable();
    keys.dedup();
    sqlx::query(
        "SELECT pg_advisory_xact_lock($1, key) \
         FROM (SELECT key FROM unnest($2::INT[]) AS key ORDER BY key) ordered",
    )
    .bind(SERIES_LOCK_NAMESPACE)
    .bind(keys)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Makes the planner join the batch with the window of stored rows with a hash join, whatever its
/// statistics say. For the rest of the transaction. Without it the plan depends on them:
///   - the new samples of an append are newer than anything the statistics know, so the window is
///     estimated at one row and the planner chooses a nested loop, which compares every row of the
///     batch with every row of the window (seconds for 20 000 samples), or probes the index once per
///     row (0.36 ms each);
///   - on a table that was never analyzed (right after a bulk load) it chooses a sequential scan.
/// The hash join builds its table from the batch, so its memory does not grow with the window.
/// Call it after the statements that need the usual plans (the registration of the series).
pub async fn prefer_hash_join(connection: &mut sqlx::PgConnection) -> anyhow::Result<()> {
    sqlx::query("SELECT set_config('enable_nestloop', 'off', true), set_config('enable_seqscan', 'off', true)")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// The window of a batch as bind parameters: `first_param` is the number of the parameter that
/// holds the lowest time, the next one holds the highest. `sql_type` is the type of the time column.
#[derive(Clone, Copy)]
pub struct Window {
    pub first_param: usize,
    pub sql_type: &'static str,
}

/// `columns` are the columns of the table that `source` returns, in order, the series first and
/// the time second. `source` is a `SELECT`; its own parameters are numbered before the window's.
pub fn insert_samples(
    table: &str,
    time_column: &str,
    columns: &[&str],
    source: &str,
    deduplicate: Option<Window>,
) -> String {
    let column_list = columns.join(", ");
    let Some(window) = deduplicate else {
        return format!("INSERT INTO {table} ({column_list}) {source}");
    };
    let same_sample = columns
        .iter()
        .map(|column| format!("e.{column} = u.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let (low, high) = (window.first_param, window.first_param + 1);
    let sql_type = window.sql_type;
    format!(
        "WITH u ({column_list}) AS ({source}) \
         INSERT INTO {table} ({column_list}) \
         SELECT DISTINCT {column_list} FROM u \
         WHERE NOT EXISTS (\
           SELECT 1 FROM {table} e \
           WHERE e.{time_column} BETWEEN ${low}::{sql_type} AND ${high}::{sql_type} \
             AND {same_sample})"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_deduplication_it_is_a_plain_insert() {
        assert_eq!(
            insert_samples(
                "float_values",
                "timestamp_us",
                &["sensor_id", "timestamp_us", "value"],
                "SELECT * FROM unnest($1::BIGINT[], $2::BIGINT[], $3::FLOAT8[])",
                None
            ),
            "INSERT INTO float_values (sensor_id, timestamp_us, value) \
             SELECT * FROM unnest($1::BIGINT[], $2::BIGINT[], $3::FLOAT8[])"
        );
    }

    #[test]
    fn with_deduplication_the_known_rows_are_left_out_inside_the_window() {
        let sql = insert_samples(
            "float_values",
            "time",
            &["sensor_id", "time", "value"],
            "SELECT * FROM unnest($1::BIGINT[], $2::TIMESTAMPTZ[], $3::FLOAT8[])",
            Some(Window {
                first_param: 4,
                sql_type: "TIMESTAMPTZ",
            }),
        );
        assert!(sql.starts_with("WITH u (sensor_id, time, value) AS (SELECT * FROM unnest("));
        assert!(sql.contains("SELECT DISTINCT sensor_id, time, value FROM u"));
        assert!(sql.contains("e.time BETWEEN $4::TIMESTAMPTZ AND $5::TIMESTAMPTZ"));
        assert!(
            sql.contains("e.sensor_id = u.sensor_id AND e.time = u.time AND e.value = u.value")
        );
    }

    #[test]
    fn a_location_is_the_same_sample_with_the_same_coordinates() {
        let sql = insert_samples(
            "location_values",
            "timestamp_us",
            &["sensor_id", "timestamp_us", "latitude", "longitude"],
            "SELECT $1::BIGINT, t, lat, lon FROM unnest($2::BIGINT[], $3::FLOAT8[], $4::FLOAT8[]) AS x(t, lat, lon)",
            Some(Window {
                first_param: 5,
                sql_type: "BIGINT",
            }),
        );
        assert!(sql.contains("e.latitude = u.latitude AND e.longitude = u.longitude"));
        assert!(sql.contains("BETWEEN $5::BIGINT AND $6::BIGINT"));
    }
}

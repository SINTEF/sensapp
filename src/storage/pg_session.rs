//! The session settings of the PostgreSQL and TimescaleDB connections.
//!
//! When a request times out, its future is dropped, but its statement keeps running on the server:
//! sqlx does not cancel it. A few slow statements then hold the connections of the pool, and the
//! writers wait for one until they get a 503. Every connection therefore has a `statement_timeout`
//! (`SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS`, a little more than the request timeout) and a
//! `lock_timeout` (`SENSAPP_PG_LOCK_TIMEOUT_SECONDS`). The maintenance (vacuum and deduplication)
//! runs on a connection of its own with the maintenance timeout instead.

use anyhow::{Context, Result};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool};

/// Defaults of `SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS` and `SENSAPP_PG_LOCK_TIMEOUT_SECONDS`
pub const DEFAULT_STATEMENT_TIMEOUT_SECONDS: u64 = 130;
pub const DEFAULT_LOCK_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_MAINTENANCE_TIMEOUT_SECONDS: u64 = 3600;

/// The timeouts of the configuration, or their defaults when it is not loaded.
fn configured_timeouts() -> (u64, u64, u64) {
    match crate::config::get() {
        Ok(config) => (
            config.pg_statement_timeout_seconds,
            config.pg_lock_timeout_seconds,
            config.http_maintenance_timeout_seconds,
        ),
        Err(_) => (
            DEFAULT_STATEMENT_TIMEOUT_SECONDS,
            DEFAULT_LOCK_TIMEOUT_SECONDS,
            DEFAULT_MAINTENANCE_TIMEOUT_SECONDS,
        ),
    }
}

/// The connection options with the configured timeouts, see [`session_options`].
pub fn with_timeouts(options: PgConnectOptions, connection_string: &str) -> PgConnectOptions {
    let (statement, lock, _) = configured_timeouts();
    session_options(options, connection_string, statement, lock)
}

/// The connection options with a `statement_timeout` and a `lock_timeout`, in seconds (0 means
/// none, as in PostgreSQL). A setting the connection string gives itself (in its `options`) is
/// kept.
pub fn session_options(
    options: PgConnectOptions,
    connection_string: &str,
    statement_timeout_seconds: u64,
    lock_timeout_seconds: u64,
) -> PgConnectOptions {
    let mut settings = Vec::new();
    for (name, seconds) in [
        ("statement_timeout", statement_timeout_seconds),
        ("lock_timeout", lock_timeout_seconds),
    ] {
        if !connection_string.contains(name) {
            settings.push((name, format!("{seconds}s")));
        }
    }
    options.options(settings)
}

/// A connection for the maintenance, out of the pool: its statements may take up to the maintenance
/// timeout. It is closed when dropped, so its setting never reaches the connections of requests.
pub async fn maintenance_connection(pool: &PgPool) -> Result<PgConnection> {
    let (_, _, maintenance) = configured_timeouts();
    let mut connection = pool
        .acquire()
        .await
        .context("Failed to get a connection for the maintenance")?
        .detach();
    // A number of seconds, from the configuration
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET statement_timeout = '{maintenance}s'"
    )))
    .execute(&mut connection)
    .await
    .context("Failed to set the timeout of the maintenance")?;
    connection
        .ping()
        .await
        .context("Failed to reach the database for the maintenance")?;
    Ok(connection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    /// The PostgreSQL server of the tests, when the tests run on PostgreSQL or TimescaleDB.
    async fn test_server() -> Option<String> {
        let url = crate::test_utils::get_test_database_url();
        if !(url.starts_with("postgres") || url.starts_with("timescaledb")) {
            return None;
        }
        crate::test_utils::ensure_test_database_exists(&url)
            .await
            .unwrap();
        Some(url.replace("timescaledb://", "postgres://"))
    }

    async fn pool(url: &str, statement: u64, lock: u64) -> PgPool {
        let options = PgConnectOptions::from_str(url).unwrap();
        PgPoolOptions::new()
            .max_connections(2)
            .connect_with(session_options(options, url, statement, lock))
            .await
            .unwrap()
    }

    async fn show(pool: &PgPool, setting: &str) -> String {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SHOW {setting}")))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_statement_longer_than_the_timeout_is_cancelled_by_the_server() {
        let Some(url) = test_server().await else {
            return;
        };
        let pool = pool(&url, 1, 1).await;
        let started = Instant::now();
        let error = sqlx::query("SELECT pg_sleep(5)")
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        let code = error
            .as_database_error()
            .and_then(|error| error.code())
            .map(|code| code.to_string());
        // query_canceled
        assert_eq!(code.as_deref(), Some("57014"), "{error}");
        // The connection is still good
        assert_eq!(show(&pool, "statement_timeout").await, "1s");
    }

    #[tokio::test]
    async fn every_connection_has_the_timeouts_unless_the_connection_string_sets_them() {
        let Some(url) = test_server().await else {
            return;
        };
        let pool = pool(&url, DEFAULT_STATEMENT_TIMEOUT_SECONDS, 60).await;
        assert_eq!(show(&pool, "statement_timeout").await, "130s");
        assert_eq!(show(&pool, "lock_timeout").await, "1min");

        let separator = if url.contains('?') { '&' } else { '?' };
        let url = format!("{url}{separator}options=-c%20statement_timeout%3D7s");
        let pool = pool_with_url(&url).await;
        assert_eq!(show(&pool, "statement_timeout").await, "7s");
        assert_eq!(show(&pool, "lock_timeout").await, "1min");

        // The maintenance has its own, and does not change the pool
        let mut connection = maintenance_connection(&pool).await.unwrap();
        let maintenance: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(maintenance, "1h");
        drop(connection);
        assert_eq!(show(&pool, "statement_timeout").await, "7s");
    }

    async fn pool_with_url(url: &str) -> PgPool {
        pool(url, DEFAULT_STATEMENT_TIMEOUT_SECONDS, 60).await
    }
}

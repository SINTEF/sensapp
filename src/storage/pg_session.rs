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

/// The connection options with a `statement_timeout` and a `lock_timeout`, in seconds (0 sends
/// nothing: no limit, unless the server sets one). A setting the connection string gives itself (in its `options`) is
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
        // 0 is no limit: the server's own setting stays, and with both at 0 no startup option is
        // sent, which a PgBouncer that does not list `options` in `ignore_startup_parameters`
        // refuses
        if seconds > 0 && !connection_string.contains(name) {
            settings.push((name, format!("{seconds}s")));
        }
    }
    if settings.is_empty() {
        options
    } else {
        options.options(settings)
    }
}

/// Apply the migrations on a connection of their own, out of the pool: the pool has the
/// `statement_timeout` of the requests, and a migration that builds an index over a large table
/// outlasts it, rolls back, and fails every start the same way. This connection has none, and the
/// maintenance timeout as its `lock_timeout`. It is closed when the migrations are done.
pub async fn run_migrations(pool: &PgPool, migrator: &sqlx::migrate::Migrator) -> Result<()> {
    let (_, _, maintenance) = configured_timeouts();
    let mut connection = pool
        .acquire()
        .await
        .context("Failed to get a connection for the migrations")?
        .detach();
    // A number of seconds, from the configuration
    for setting in [
        "SET statement_timeout = 0".to_string(),
        format!("SET lock_timeout = '{maintenance}s'"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(setting))
            .execute(&mut connection)
            .await
            .context("Failed to set the timeouts of the migrations")?;
    }
    // `run` does not compile within an `async_trait` method ("Acquire is not general enough")
    migrator
        .run_direct(None, &mut connection, false)
        .await
        .context("Failed to migrate database")?;
    // The connection is not the pool's anymore: close it instead of leaving it to the server
    let _ = connection.close().await;
    Ok(())
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
    use sqlx::SqlSafeStr;
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

    /// A migration that outlasts the statement timeout of the pool (the index builds of a large
    /// database do) used to roll back, and every start failed the same way.
    #[tokio::test]
    async fn a_migration_longer_than_the_statement_timeout_succeeds() {
        use sqlx::migrate::{Migration, MigrationType, Migrator};
        let Some(url) = test_server().await else {
            return;
        };
        // The migration table of this test is in a schema of its own
        let schema = format!("migration_test_{}", uuid::Uuid::new_v4().simple());
        let admin = pool(&url, 0, 0).await;
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .options([("search_path", schema.clone())]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(session_options(options, &url, 1, 1))
            .await
            .unwrap();
        let migrator = Migrator {
            migrations: std::borrow::Cow::Owned(vec![Migration::new(
                1,
                "slow".into(),
                MigrationType::Simple,
                sqlx::AssertSqlSafe("SELECT pg_sleep(3)").into_sql_str(),
                false,
            )]),
            ..Migrator::DEFAULT
        };
        // The pool refuses it
        assert!(
            sqlx::query("SELECT pg_sleep(3)")
                .execute(&pool)
                .await
                .is_err()
        );
        run_migrations(&pool, &migrator).await.unwrap();
        let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(applied, 1);
        // The pool keeps its timeouts
        assert_eq!(show(&pool, "statement_timeout").await, "1s");
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
    }

    /// 0 sends nothing, for a PgBouncer that refuses startup parameters it does not know.
    #[test]
    fn zero_timeouts_send_no_startup_options() {
        let url = "postgres://u:p@localhost/db";
        let options = PgConnectOptions::from_str(url).unwrap();
        let none = session_options(options.clone(), url, 0, 0);
        assert!(format!("{none:?}").contains("options: None"), "{none:?}");
        let both = session_options(options.clone(), url, 5, 7);
        assert!(
            format!("{both:?}").contains("statement_timeout=5s"),
            "{both:?}"
        );
        let only_lock = session_options(options, url, 0, 7);
        let only_lock = format!("{only_lock:?}");
        assert!(!only_lock.contains("statement_timeout"), "{only_lock}");
        assert!(only_lock.contains("lock_timeout=7s"), "{only_lock}");
    }
}

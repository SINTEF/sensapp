//! Test utilities for SensApp tests
//!
//! This module provides centralized test configuration and utilities,
//! particularly for database connection management.

use anyhow::{Result, anyhow};
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;
use std::path::{Path, PathBuf};
use url::Url;

/// Default PostgreSQL connection string for tests
const DEFAULT_POSTGRES_CONNECTION_STRING: &str =
    "postgres://postgres:postgres@localhost:5432/sensapp-test";

/// Default SQLite connection string for tests
const DEFAULT_SQLITE_CONNECTION_STRING: &str = "sqlite://test.db";

/// Default DuckDB connection string for tests
const DEFAULT_DUCKDB_CONNECTION_STRING: &str = "duckdb://test.duckdb";

/// Get the test database connection string from environment or use the default
/// connection string for an enabled storage backend.
///
/// Checks the `TEST_DATABASE_URL` environment variable first. If it is not set,
/// the fallback is selected from the enabled cargo features so sqlite-only test
/// runs do not accidentally try to boot a postgres backend.
///
/// # Example
///
/// ```
/// use sensapp::test_utils::get_test_database_url;
///
/// let connection_string = get_test_database_url();
/// // Use connection_string for testing
/// ```
pub fn get_test_database_url() -> String {
    let connection_string = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| default_test_database_url().to_string());

    isolate_file_backed_test_database(&connection_string)
}

pub async fn ensure_test_database_exists(connection_string: &str) -> Result<()> {
    #[cfg(feature = "postgres")]
    if connection_string.starts_with("postgres://")
        || connection_string.starts_with("postgresql://")
    {
        return ensure_postgres_test_database(connection_string).await;
    }

    Ok(())
}

fn default_test_database_url() -> &'static str {
    if cfg!(feature = "postgres") {
        DEFAULT_POSTGRES_CONNECTION_STRING
    } else if cfg!(feature = "sqlite") {
        DEFAULT_SQLITE_CONNECTION_STRING
    } else if cfg!(feature = "duckdb") {
        DEFAULT_DUCKDB_CONNECTION_STRING
    } else {
        DEFAULT_POSTGRES_CONNECTION_STRING
    }
}

fn isolate_file_backed_test_database(connection_string: &str) -> String {
    if connection_string.starts_with("postgres://")
        || connection_string.starts_with("postgresql://")
    {
        return isolate_postgres_test_database(connection_string);
    }

    if let Some(path) = connection_string.strip_prefix("sqlite://") {
        return format!("sqlite://{}", isolate_test_database_path(path));
    }

    if let Some(path) = connection_string.strip_prefix("duckdb://") {
        return format!("duckdb://{}", isolate_test_database_path(path));
    }

    connection_string.to_string()
}

fn isolate_postgres_test_database(connection_string: &str) -> String {
    let Ok(mut url) = Url::parse(connection_string) else {
        return connection_string.to_string();
    };

    let Some(database_name) = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty())
    else {
        return connection_string.to_string();
    };

    let isolated_name = format!("{}-{}", database_name, std::process::id());
    url.set_path(&format!("/{}", isolated_name));
    url.to_string()
}

#[cfg(feature = "postgres")]
async fn ensure_postgres_test_database(connection_string: &str) -> Result<()> {
    let url = Url::parse(connection_string).map_err(|e| {
        anyhow!(
            "Failed to parse PostgreSQL test URL '{}': {}",
            connection_string,
            e
        )
    })?;

    let database_name = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "PostgreSQL test URL '{}' is missing a database name",
                connection_string
            )
        })?
        .to_string();

    let mut admin_url = url.clone();
    admin_url.set_path("/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url.as_str())
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to connect to PostgreSQL admin database '{}': {}",
                admin_url,
                e
            )
        })?;

    let create_database_sql = format!("CREATE DATABASE \"{}\"", database_name.replace('"', "\"\""));

    match sqlx::query(&create_database_sql).execute(&pool).await {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => Ok(()),
        Err(e) => Err(anyhow!(
            "Failed to create PostgreSQL test database '{}': {}",
            database_name,
            e
        )),
    }
}

fn isolate_test_database_path(path: &str) -> String {
    if path.is_empty() || path == ":memory:" || path.starts_with("file:") {
        return path.to_string();
    }

    let process_id = std::process::id();
    let path = Path::new(path);

    let Some(file_name) = path.file_name() else {
        return path.to_string_lossy().into_owned();
    };

    let file_name = file_name.to_string_lossy();
    let stem = path
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_else(|| file_name.into_owned());
    let extension = path
        .extension()
        .map(|ext| ext.to_string_lossy().into_owned());

    let isolated_name = match extension {
        Some(extension) => format!("{}-{}.{}", stem, process_id, extension),
        None => format!("{}-{}", stem, process_id),
    };

    let mut isolated_path = path.parent().map(PathBuf::from).unwrap_or_default();
    isolated_path.push(isolated_name);
    isolated_path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn respects_test_database_url_override() {
        temp_env::with_var("TEST_DATABASE_URL", Some("sqlite://override.db"), || {
            let process_id = std::process::id();
            assert_eq!(
                get_test_database_url(),
                format!("sqlite://override-{}.db", process_id)
            );
        });
    }

    #[test]
    fn feature_aware_default_matches_enabled_backends() {
        let expected = if cfg!(feature = "postgres") {
            DEFAULT_POSTGRES_CONNECTION_STRING
        } else if cfg!(feature = "sqlite") {
            DEFAULT_SQLITE_CONNECTION_STRING
        } else if cfg!(feature = "duckdb") {
            DEFAULT_DUCKDB_CONNECTION_STRING
        } else {
            DEFAULT_POSTGRES_CONNECTION_STRING
        };

        assert_eq!(default_test_database_url(), expected);
    }

    #[test]
    fn isolates_postgres_database_name_per_process() {
        temp_env::with_var(
            "TEST_DATABASE_URL",
            Some("postgres://postgres:postgres@localhost:5432/sensapp-test"),
            || {
                let process_id = std::process::id();
                assert_eq!(
                    get_test_database_url(),
                    format!(
                        "postgres://postgres:postgres@localhost:5432/sensapp-test-{}",
                        process_id
                    )
                );
            },
        );
    }
}

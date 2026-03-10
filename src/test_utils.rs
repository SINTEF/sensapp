//! Test utilities for SensApp tests
//!
//! This module provides centralized test configuration and utilities,
//! particularly for database connection management.

use std::path::{Path, PathBuf};

/// Default PostgreSQL connection string for tests
const DEFAULT_POSTGRES_CONNECTION_STRING: &str =
    "postgres://postgres:postgres@localhost:5432/sensapp-test";

/// Get the test database connection string from environment or use default PostgreSQL
///
/// Checks the `TEST_DATABASE_URL` environment variable first, falling back to
/// the default PostgreSQL connection string if not set.
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
        .unwrap_or_else(|_| DEFAULT_POSTGRES_CONNECTION_STRING.to_string());

    isolate_file_backed_test_database(&connection_string)
}

fn isolate_file_backed_test_database(connection_string: &str) -> String {
    if let Some(path) = connection_string.strip_prefix("sqlite://") {
        return format!("sqlite://{}", isolate_test_database_path(path));
    }

    if let Some(path) = connection_string.strip_prefix("duckdb://") {
        return format!("duckdb://{}", isolate_test_database_path(path));
    }

    connection_string.to_string()
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
    let extension = path.extension().map(|ext| ext.to_string_lossy().into_owned());

    let isolated_name = match extension {
        Some(extension) => format!("{}-{}.{}", stem, process_id, extension),
        None => format!("{}-{}", stem, process_id),
    };

    let mut isolated_path = path.parent().map(PathBuf::from).unwrap_or_default();
    isolated_path.push(isolated_name);
    isolated_path.to_string_lossy().into_owned()
}

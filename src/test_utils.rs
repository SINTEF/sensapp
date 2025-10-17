//! Test utilities for SensApp tests
//!
//! This module provides centralized test configuration and utilities,
//! particularly for database connection management.

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
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| DEFAULT_POSTGRES_CONNECTION_STRING.to_string())
}

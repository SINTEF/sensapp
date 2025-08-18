use std::sync::Arc;

use anyhow::{Result, bail};

use super::StorageInstance;

#[cfg(feature = "postgres")]
use super::postgresql::PostgresStorage;

#[cfg(feature = "sqlite")]
use super::sqlite::SqliteStorage;

#[cfg(feature = "timescaledb")]
use super::timescaledb::TimeScaleDBStorage;

#[cfg(feature = "duckdb")]
use super::duckdb::DuckDBStorage;

#[cfg(feature = "bigquery")]
use super::bigquery::BigQueryStorage;

#[cfg(feature = "rrdcached")]
use super::rrdcached::RrdCachedStorage;

/*#[enum_delegate::implement(StorageInstance)]
pub enum StorageDelegate {
    Sqlite(SqliteStorage),
    Postgres(PostgresStorage),
}

pub async fn create_storage_from_connection_string(
    connection_string: &str,
) -> Result<StorageDelegate> {
    Ok(match connection_string {
        s if s.starts_with("sqlite:") => StorageDelegate::Sqlite(SqliteStorage::connect(s).await?),
        s if s.starts_with("postgres:") => {
            StorageDelegate::Postgres(PostgresStorage::connect(s).await?)
        }
        _ => bail!("Unsupported storage type: {}", connection_string),
    })
}*/

pub async fn create_storage_from_connection_string(
    connection_string: &str,
) -> Result<Arc<dyn StorageInstance>> {
    Ok(match connection_string {
        #[cfg(feature = "bigquery")]
        s if s.starts_with("bigquery:") => Arc::new(BigQueryStorage::connect(s).await?),

        #[cfg(feature = "duckdb")]
        s if s.starts_with("duckdb:") => Arc::new(DuckDBStorage::connect(s).await?),

        #[cfg(feature = "postgres")]
        s if s.starts_with("postgres:") => Arc::new(PostgresStorage::connect(s).await?),

        #[cfg(feature = "sqlite")]
        s if s.starts_with("sqlite:") => Arc::new(SqliteStorage::connect(s).await?),

        #[cfg(feature = "timescaledb")]
        s if s.starts_with("timescaledb:") => Arc::new(TimeScaleDBStorage::connect(s).await?),

        #[cfg(feature = "rrdcached")]
        s if s.starts_with("rrdcached:") => Arc::new(RrdCachedStorage::connect(s).await?),

        // Provide helpful error messages for disabled backends
        #[cfg(not(feature = "bigquery"))]
        s if s.starts_with("bigquery:") => {
            bail!("BigQuery storage backend is not enabled. Enable with --features bigquery")
        }

        #[cfg(not(feature = "duckdb"))]
        s if s.starts_with("duckdb:") => {
            bail!("DuckDB storage backend is not enabled. Enable with --features duckdb")
        }

        #[cfg(not(feature = "postgres"))]
        s if s.starts_with("postgres:") => {
            bail!("PostgreSQL storage backend is not enabled. Enable with --features postgres")
        }

        #[cfg(not(feature = "sqlite"))]
        s if s.starts_with("sqlite:") => {
            bail!("SQLite storage backend is not enabled. Enable with --features sqlite")
        }

        #[cfg(not(feature = "timescaledb"))]
        s if s.starts_with("timescaledb:") => {
            bail!("TimescaleDB storage backend is not enabled. Enable with --features timescaledb")
        }

        #[cfg(not(feature = "rrdcached"))]
        s if s.starts_with("rrdcached:") => {
            bail!("RRDCached storage backend is not enabled. Enable with --features rrdcached")
        }

        _ => bail!("Unsupported storage type: {}", connection_string),
    })
}

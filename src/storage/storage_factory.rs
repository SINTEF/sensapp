use std::sync::Arc;

use anyhow::{bail, Result};

use super::{
    bigquery::BigQueryStorage, duckdb::DuckDBStorage, postgresql::PostgresStorage,
    sqlite::SqliteStorage, storage::StorageInstance, timescaledb::TimeScaleDBStorage,
};

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
        // Ascending order, no favoritisim
        s if s.starts_with("bigquery:") => Arc::new(BigQueryStorage::connect(s).await?),
        s if s.starts_with("duckdb:") => Arc::new(DuckDBStorage::connect(s).await?),
        s if s.starts_with("postgres:") => Arc::new(PostgresStorage::connect(s).await?),
        s if s.starts_with("sqlite:") => Arc::new(SqliteStorage::connect(s).await?),
        s if s.starts_with("timescaledb:") => Arc::new(TimeScaleDBStorage::connect(s).await?),
        _ => bail!("Unsupported storage type: {}", connection_string),
    })
}

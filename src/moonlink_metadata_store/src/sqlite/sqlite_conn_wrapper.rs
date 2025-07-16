use sqlx::sqlite::SqlitePoolOptions;

use crate::error::Result;

/// A wrapper around [`sqlx`] connections.
pub(super) struct SqliteConnWrapper {
    /// Sqlite connection.
    pub(super) pool: sqlx::SqlitePool,
}

impl SqliteConnWrapper {
    pub(super) async fn new(location: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new().connect(location).await?;
        Ok(Self { pool })
    }
}

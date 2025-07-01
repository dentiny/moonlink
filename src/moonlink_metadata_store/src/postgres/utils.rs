/// Util functions for metadata storage based on postgres.
use tokio_postgres::Client;

use crate::error::Result;

/// Return whether the given [`schema_name`] exists in the current database.
pub async fn schema_exists(postgres_client: &Client, schema_name: &str) -> Result<bool> {
    let row = postgres_client
        .query_opt(
            "SELECT 1 FROM pg_namespace WHERE nspname = $1;",
            &[&schema_name],
        )
        .await?;

    Ok(row.is_some())
}

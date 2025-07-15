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

/// Return whether the given <schema>.<table> exists in the current database.
pub async fn table_exists(
    postgres_client: &Client,
    schema_name: &str,
    table_name: &str,
) -> Result<bool> {
    let row = postgres_client
        .query_opt(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2;",
            &[&schema_name, &table_name],
        )
        .await?;

    Ok(row.is_some())
}

/// Create metadata storage table, which fails if the table already exists.
pub async fn create_table(postgres_client: &Client, statements: &str) -> Result<()> {
    postgres_client.simple_query(statements).await?;
    Ok(())
}

/// Create table if not exist.
pub async fn create_table_if_non_existent(
    postgres_client: &Client,
    schema_name: &str,
    table_name: &str,
    statements: &str,
) -> Result<()> {
    if table_exists(postgres_client, schema_name, table_name).await? {
        return Ok(());
    }
    create_table(postgres_client, statements).await
}

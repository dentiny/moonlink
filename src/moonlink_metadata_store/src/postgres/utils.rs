/// Util functions for metadata storage based on postgres.
use tokio_postgres::Client;

use crate::error::Result;

/// Return whether the given <table> exists in the current database.
pub async fn table_exists(postgres_client: &Client, table_name: &str) -> Result<bool> {
    let row = postgres_client
        .query_opt(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2;",
            &[&"public", &table_name], // Query under default schema.
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
    table_name: &str,
    statements: &str,
) -> Result<()> {
    if table_exists(postgres_client, table_name).await? {
        return Ok(());
    }
    create_table(postgres_client, statements).await
}

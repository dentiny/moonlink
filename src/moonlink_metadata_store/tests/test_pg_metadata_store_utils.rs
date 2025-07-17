use tokio_postgres::{connect, NoTls};

/// Test connection string.
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    use moonlink_metadata_store::PgUtils;

    #[tokio::test]
    #[serial]
    async fn test_table_exists() {
        const EXISTENT_TABLE: &str = "existent_table";

        let (postgres_client, connection) = connect(URI, NoTls).await.unwrap();

        // Spawn connection driver in background to keep eventloop alive.
        let _pg_connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });

        // Re-create mooncake schema.
        postgres_client
            .simple_query(&format!("DROP SCHEMA IF EXISTS {EXISTENT_TABLE} CASCADE;"))
            .await
            .unwrap();

        // Check table existence.
        //
        // Case-1: schema existent, but table non-existent.
        assert!(!PgUtils::table_exists(&postgres_client, EXISTENT_TABLE)
            .await
            .unwrap());
        // Case-2: schema non-existent.
        assert!(!PgUtils::table_exists(&postgres_client, EXISTENT_TABLE)
            .await
            .unwrap());
        // Case-3: schema existent and table existent.
        postgres_client
            .simple_query(&format!("CREATE TABLE {EXISTENT_TABLE} (id INT);",))
            .await
            .unwrap();
        assert!(PgUtils::table_exists(&postgres_client, EXISTENT_TABLE)
            .await
            .unwrap());
    }
}

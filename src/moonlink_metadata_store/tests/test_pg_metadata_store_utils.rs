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
    async fn test_schema_exists() {
        const EXISTENT_SCHEMA: &str = "existent_schema";
        const NON_EXISTENT_SCHEMA: &str = "non_existent_schema";

        let (postgres_client, connection) = connect(URI, NoTls).await.unwrap();

        // Spawn connection driver in background to keep eventloop alive.
        let _pg_connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        // Re-create mooncake schema.
        postgres_client
            .simple_query(&format!(
                "DROP SCHEMA IF EXISTS {0}; CREATE SCHEMA {0};",
                EXISTENT_SCHEMA
            ))
            .await
            .unwrap();
        postgres_client
            .simple_query(&format!("DROP SCHEMA IF EXISTS {0};", NON_EXISTENT_SCHEMA))
            .await
            .unwrap();

        // Check schema existence.
        assert!(PgUtils::schema_exists(&postgres_client, EXISTENT_SCHEMA)
            .await
            .unwrap());
        assert!(
            !PgUtils::schema_exists(&postgres_client, NON_EXISTENT_SCHEMA)
                .await
                .unwrap()
        );
    }
}

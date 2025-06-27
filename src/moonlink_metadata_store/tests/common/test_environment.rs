/// Test environment to setup and cleanup a test case.
use tokio_postgres::{connect, NoTls};

pub(crate) struct TestEnvironment {
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl TestEnvironment {
    /// Delete test moonlink metadata table.
    pub(crate) async fn new(uri: &str) -> Self {
        let (postgres_client, connection) = connect(uri, NoTls).await.unwrap();
        let _connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });
        postgres_client
            .simple_query("DROP TABLE IF EXISTS mooncake.tables")
            .await
            .unwrap();
        postgres_client
            .simple_query("CREATE SCHEMA IF NOT EXISTS mooncake;")
            .await
            .unwrap();
        Self { _connection_handle }
    }
}

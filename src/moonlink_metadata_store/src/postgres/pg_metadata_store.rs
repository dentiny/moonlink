use crate::error::Result;
use crate::postgres::config_utils;
use crate::{base_metadata_store::MetadataStoreTrait, error::Error};
use moonlink::{IcebergTableConfig, MoonlinkTableConfig};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{connect, Client, Config, Connection, NoTls, Socket};

use std::sync::Arc;

// TODO(hjiang): Hard-code uri for dev.
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

pub struct PgMetadataStore {
    /// Postgres client.
    postgres_client: Arc<Mutex<Client>>,
}

#[async_trait]
impl MetadataStoreTrait for PgMetadataStore {
    async fn load_table_config(&self, table_id: u32) -> Result<MoonlinkTableConfig> {
        let rows = {
            let guard = self.postgres_client.lock().await;
            let rows = guard
                .query("SELECT * FROM moonlink_tables WHERE oid = $1", &[&table_id])
                .await
                .expect("Failed to query moonlink_tables");
            rows
        };

        if rows.is_empty() {
            return Err(Error::TableIdNotFound(table_id));
        }

        let row = &rows[0];
        let config_str: &str = row.get("config");
        let config_json: serde_json::Value = serde_json::from_str(config_str)?;
        let moonlink_config = config_utils::deserialze_moonlink_table_config(config_json)?;

        Ok(moonlink_config)
    }
}

impl PgMetadataStore {
    pub async fn new() -> Result<Self> {
        let (postgres_client, connection) = connect(URI, NoTls).await.unwrap();
        // Spawn connection driver in background to keep it alive
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        postgres_client
            .simple_query(
                "CREATE TABLE IF NOT EXISTS moonlink_tables (
                oid oid PRIMARY KEY,             -- column store table OID
                table_name text NOT NULL,     -- source table name
                uri text,            -- source URI
                config json,         -- mooncake and persistence configurations
                cardinality bigint   -- estimated row count or similar
            );",
            )
            .await?;

        Ok(Self {
            postgres_client: Arc::new(Mutex::new(postgres_client)),
        })
    }
}

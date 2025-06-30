use crate::error::Result;
use crate::postgres::config_utils;
use crate::{base_metadata_store::MetadataStoreTrait, error::Error};
use moonlink::MoonlinkTableConfig;

use async_trait::async_trait;
use postgres_types::Json as PgJson;
use tokio::sync::Mutex;
use tokio_postgres::{connect, Client, NoTls};

use std::sync::Arc;

/// SQL statements for moonlink metadata table schema.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");

#[allow(dead_code)]
pub struct PgMetadataStore {
    /// Postgres client.
    postgres_client: Arc<Mutex<Client>>,
    /// Pg connection join handle.
    _pg_connection: tokio::task::JoinHandle<()>,
}

#[async_trait]
impl MetadataStoreTrait for PgMetadataStore {
    async fn load_table_config(&self, table_id: u32) -> Result<MoonlinkTableConfig> {
        let rows = {
            let guard = self.postgres_client.lock().await;

            guard
                .query("SELECT * FROM mooncake.tables WHERE oid = $1", &[&table_id])
                .await
                .expect("Failed to query tables")
        };

        if rows.is_empty() {
            return Err(Error::TableIdNotFound(table_id));
        }

        let row = &rows[0];
        let config_json = row.get("config");
        let moonlink_config = config_utils::deserialze_moonlink_table_config(config_json)?;

        Ok(moonlink_config)
    }

    async fn store_table_config(
        &self,
        table_id: u32,
        table_name: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let serialized_config =
            config_utils::serialize_moonlink_table_config(moonlink_table_config)?;

        let guard = self.postgres_client.lock().await;
        // TODO(hjiang): Fill in other fields as well.
        guard
            .execute(
                "INSERT INTO mooncake.tables (oid, table_name, config)
                VALUES ($1, $2, $3)",
                &[&table_id, &table_name, &PgJson(&serialized_config)],
            )
            .await?;

        Ok(())
    }

    async fn delete_table_config(&self, table_id: u32) -> Result<()> {
        let guard = self.postgres_client.lock().await;
        guard
            .execute("DELETE FROM mooncake.tables WHERE oid = $1", &[&table_id])
            .await?;

        Ok(())
    }
}

impl PgMetadataStore {
    /// Precondition: [`mooncake`] schema has been created in the current database.
    pub async fn new(uri: &str) -> Result<Self> {
        let (postgres_client, connection) = connect(uri, NoTls).await?;

        // Spawn connection driver in background to keep eventloop alive.
        let _pg_connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        postgres_client
            .simple_query(CREATE_TABLE_SCHEMA_SQL)
            .await?;

        Ok(Self {
            postgres_client: Arc::new(Mutex::new(postgres_client)),
            _pg_connection,
        })
    }
}

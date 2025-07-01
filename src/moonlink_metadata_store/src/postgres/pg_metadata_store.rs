use crate::base_metadata_store::TableMetadataEntry;
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
    async fn get_database_id(&self) -> Result<u32> {
        let row = {
            let guard = self.postgres_client.lock().await;
            guard
                .query_one(
                    "SELECT oid FROM pg_database WHERE datname = current_database()",
                    &[],
                )
                .await?
        };
        let oid = row.get("oid");
        Ok(oid)
    }

    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>> {
        let rows = {
            let guard = self.postgres_client.lock().await;
            guard
                .query("SELECT oid, table_name, config FROM mooncake.tables", &[])
                .await?
        };

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for cur_row in rows.into_iter() {
            assert_eq!(cur_row.len(), 3);
            let table_id = cur_row.get("oid");
            let src_table_name = cur_row.get("table_name");
            let serialized_config = cur_row.get("config");
            let moonlink_table_config =
                config_utils::deserialze_moonlink_table_config(serialized_config)?;
            metadata_entries.push(TableMetadataEntry {
                table_id,
                src_table_name,
                moonlink_table_config,
            });
        }
        Ok(metadata_entries)
    }

    async fn schema_exists(&self, schema_name: &str) -> Result<bool> {
        let row = {
            let guard = self.postgres_client.lock().await;
            guard
                .query_opt(
                    "SELECT 1 FROM pg_namespace WHERE nspname = $1;",
                    &[&schema_name],
                )
                .await?
        };

        Ok(row.is_some())
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
        let rows_affected = guard
            .execute(
                "INSERT INTO mooncake.tables (oid, table_name, config)
                VALUES ($1, $2, $3)",
                &[&table_id, &table_name, &PgJson(&serialized_config)],
            )
            .await?;

        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }

        Ok(())
    }

    async fn delete_table_config(&self, table_id: u32) -> Result<()> {
        let guard = self.postgres_client.lock().await;
        let rows_affected = guard
            .execute("DELETE FROM mooncake.tables WHERE oid = $1", &[&table_id])
            .await?;

        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }
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

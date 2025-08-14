use crate::base_metadata_store::MetadataStoreTrait;
use crate::base_metadata_store::TableMetadataEntry;
use crate::base_metadata_store::MOONLINK_METADATA_TABLE;
use crate::base_metadata_store::MOONLINK_SECRET_TABLE;
use crate::config_utils;
use crate::error::{Error, Result};
use crate::postgres::pg_client_wrapper::PgClientWrapper;
use crate::postgres::utils;
use moonlink::MoonlinkTableConfig;
use moonlink::MoonlinkTableSecret;

use async_trait::async_trait;
use postgres_types::Json as PgJson;

/// SQL statements for moonlink metadata table schema.
const CREATE_TABLE_SCHEMA_SQL: &str = include_str!("sql/create_tables.sql");
/// SQL statements for moonlink secret table schema.
const CREATE_SECRET_SCHEMA_SQL: &str = include_str!("sql/create_secrets.sql");

pub struct PgMetadataStore {
    /// Database connection string.
    uri: String,
}

#[async_trait]
impl MetadataStoreTrait for PgMetadataStore {
    async fn metadata_table_exists(&self) -> Result<bool> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        utils::table_exists(&pg_client.postgres_client, MOONLINK_METADATA_TABLE).await
    }

    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let rows = pg_client
            .postgres_client
            .query(
                r#"
                SELECT 
                    t."schema",
                    t."table",
                    t.src_table_name,
                    t.src_table_uri,
                    t.config,
                    s.secret_type,
                    s.key_id,
                    s.secret,
                    s.endpoint,
                    s.region,
                    s.project
                FROM tables t
                LEFT JOIN secrets s
                    ON t."schema" = s."schema"
                    AND t."table" = s."table"
                "#,
                &[],
            )
            .await?;

        let mut metadata_entries = Vec::with_capacity(rows.len());
        for row in rows {
            let schema: String = row.get("schema");
            let table: String = row.get("table");
            let src_table_name: String = row.get("src_table_name");
            let src_table_uri: String = row.get("src_table_uri");
            let serialized_config: serde_json::Value = row.get("config");
            let secret_type: Option<String> = row.get("secret_type");
            let secret_entry: Option<MoonlinkTableSecret> = {
                secret_type.map(|secret_type| MoonlinkTableSecret {
                    secret_type: MoonlinkTableSecret::convert_secret_type(&secret_type),
                    key_id: row.get("key_id"),
                    secret: row.get("secret"),
                    endpoint: row.get("endpoint"),
                    region: row.get("region"),
                    project: row.get("project"),
                })
            };
            let moonlink_table_config =
                config_utils::deserialize_moonlink_table_config(serialized_config, secret_entry)?;

            let metadata_entry = TableMetadataEntry {
                schema,
                table,
                src_table_name,
                src_table_uri,
                moonlink_table_config,
            };
            metadata_entries.push(metadata_entry);
        }

        Ok(metadata_entries)
    }

    async fn store_table_metadata(
        &self,
        schema: &str,
        table: &str,
        src_table_name: &str,
        src_table_uri: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;
        let (serialized_config, moonlink_table_secret) =
            config_utils::parse_moonlink_table_config(moonlink_table_config)?;

        // Create metadata table if not exist.
        utils::create_table_if_non_existent(
            &pg_client.postgres_client,
            MOONLINK_METADATA_TABLE,
            CREATE_TABLE_SCHEMA_SQL,
        )
        .await?;

        // Create secret table if not exist.
        utils::create_table_if_non_existent(
            &pg_client.postgres_client,
            MOONLINK_SECRET_TABLE,
            CREATE_SECRET_SCHEMA_SQL,
        )
        .await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Persist table metadata.
        // TODO(hjiang): Fill in other fields as well.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                r#"INSERT INTO tables ("schema", "table", src_table_name, src_table_uri, config)
                VALUES ($1, $2, $3, $4, $5)"#,
                &[
                    &schema,
                    &table,
                    &src_table_name,
                    &src_table_uri,
                    &PgJson(&serialized_config),
                ],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }

        // Persist table secrets.
        if let Some(table_secret) = moonlink_table_secret {
            let rows_affected = pg_client
                .postgres_client
                .execute(
                    r#"INSERT INTO secrets ("schema", "table", secret_type, key_id, secret, endpoint, region, project)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
                    &[
                        &schema,
                        &table,
                        &table_secret.get_secret_type(),
                        &table_secret.key_id,
                        &table_secret.secret,
                        &table_secret.endpoint.as_deref(),
                        &table_secret.region.as_deref(),
                        &table_secret.project.as_deref(),
                    ],
                )
                .await?;
            if rows_affected != 1 {
                return Err(Error::PostgresRowCountError(1, rows_affected as u32));
            }
        }

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }

    async fn delete_table_metadata(&self, schema: &str, table: &str) -> Result<()> {
        let pg_client = PgClientWrapper::new(&self.uri).await?;

        // Start a transaction to insert rows into metadata table and secret table.
        pg_client.postgres_client.execute("BEGIN", &[]).await?;

        // Delete rows for metadata table.
        let rows_affected = pg_client
            .postgres_client
            .execute(
                r#"DELETE FROM tables WHERE "schema" = $1 AND "table" = $2"#,
                &[&schema, &table],
            )
            .await?;
        if rows_affected != 1 {
            return Err(Error::PostgresRowCountError(1, rows_affected as u32));
        }

        // Delete rows for secret table, intentionally no check affected row counts.
        pg_client
            .postgres_client
            .execute(
                r#"DELETE FROM secrets WHERE "schema" = $1 AND "table" = $2"#,
                &[&schema, &table],
            )
            .await?;

        // Commit the transaction.
        pg_client.postgres_client.execute("COMMIT", &[]).await?;

        Ok(())
    }
}

impl PgMetadataStore {
    /// Attempt to create a metadata storage; if [`mooncake`] schema doesn't exist, current database is not managed by moonlink, return None.
    pub fn new(uri: String) -> Result<Self> {
        Ok(Self { uri })
    }
}

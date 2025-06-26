use crate::base_metadata_store::MetadataStoreTrait;
use crate::error::Result;

use tokio_postgres::{connect, Client, Config, NoTls};

// TODO(hjiang): Hard-code uri for dev.
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

pub struct PgMetadataStore {}

impl MetadataStoreTrait for PgMetadataStore {}

impl PgMetadataStore {
    pub async fn new() -> Result<()> {
        let (postgres_client, connection) = connect(URI, NoTls).await.unwrap();
        postgres_client
            .simple_query(
                "CREATE TABLE IF NOT EXISTS moonlink_tables (
                oid oid,             -- column store table OID
                table_name text,     -- source table name
                uri text,            -- source URI
                config json,         -- mooncake and persistence configurations
                cardinality bigint   -- estimated row count or similar
            );",
            )
            .await?;

        Ok(())
    }
}

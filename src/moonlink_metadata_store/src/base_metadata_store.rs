/// This trait provides the interface for moonlink table metadata storage.
use async_trait::async_trait;

use crate::error::Result;
use moonlink::MoonlinkTableConfig;

/// Metadata entry for each table.
#[derive(Clone, Debug)]
pub struct TableMetadataEntry {
    /// Table id.
    pub table_id: u32,
    /// Src table name.
    pub src_table_name: String,
    /// Moonlink table config.
    pub moonlink_table_config: MoonlinkTableConfig,
}

#[async_trait]
pub trait MetadataStoreTrait: Send {
    /// Get database id.
    #[allow(async_fn_in_trait)]
    async fn get_database_id(&self) -> Result<u32>;

    /// Return whether the given schema exists.
    /// Context: database concept hierarchy is database -> schema -> table.
    #[allow(async_fn_in_trait)]
    async fn schema_exists(&self, schema_name: &str) -> Result<bool>;

    /// Get all mooncake table metadata entries in the metadata storage table.
    /// Notice, schema existence should be checked beforehand, otherwise empty entries will be returned.
    #[allow(async_fn_in_trait)]
    async fn get_all_table_metadata_entries(&self) -> Result<Vec<TableMetadataEntry>>;

    /// Store table config for the given table.
    /// Precondition: the requested table id hasn't been recorded in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn store_table_config(
        &self,
        table_id: u32,
        table_name: &str,
        moonlink_table_config: MoonlinkTableConfig,
    ) -> Result<()>;

    /// Delete table config for the given table.
    /// Precondition: the requested table id has been record in the metadata storage.
    #[allow(async_fn_in_trait)]
    async fn delete_table_config(&self, table_id: u32) -> Result<()>;
}

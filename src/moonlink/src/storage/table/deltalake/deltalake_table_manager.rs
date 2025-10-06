use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use deltalake::DeltaTable;

use crate::error::Result;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::{
    PersistenceSnapshotPayload, TableMetadata as MooncakeTableMetadata,
};
use crate::storage::storage_utils::FileId;
use crate::storage::table::common::table_manager::PersistenceFileParams;
use crate::storage::table::common::table_manager::PersistenceResult;
use crate::storage::table::common::table_manager::TableManager;
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::storage::table::deltalake::utils;
use crate::{BaseFileSystemAccess, CacheTrait};

#[allow(unused)]
#[derive(Clone, Debug)]
pub(crate) struct DataFileEntry {
    /// Remote filepath.
    pub(crate) remote_filepath: String,
}

#[allow(unused)]
#[derive(Debug)]
pub struct DeltalakeTableManager {
    /// Mooncake table metadata.
    pub(crate) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Deltalake table configuration.
    pub(crate) config: DeltalakeTableConfig,

    /// Deltalake table.
    pub(crate) table: Option<DeltaTable>,

    /// Snapshot should be loaded for at most once.
    pub(crate) snapshot_loaded: bool,

    /// Object storage cache.
    pub(crate) object_storage_cache: Arc<dyn CacheTrait>,

    /// Filesystem accessor.
    pub(crate) filesystem_accessor: Arc<dyn BaseFileSystemAccess>,

    /// Maps from file id to file entry.
    pub(crate) persisted_data_files: HashMap<FileId, DataFileEntry>,
}

impl DeltalakeTableManager {
    #[allow(unused)]
    pub async fn new(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: Arc<dyn CacheTrait>,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        config: DeltalakeTableConfig,
    ) -> Result<DeltalakeTableManager> {
        Ok(Self {
            mooncake_table_metadata,
            config,
            table: None,
            snapshot_loaded: false,
            object_storage_cache,
            filesystem_accessor,
            persisted_data_files: HashMap::new(),
        })
    }

    #[allow(unused)]
    pub(crate) async fn initialize_table_if_exists(&mut self) -> Result<()> {
        assert!(self.table.is_none());
        self.table = utils::get_deltalake_table_if_exists(&self.config).await?;
        Ok(())
    }
}

#[async_trait]
impl TableManager for DeltalakeTableManager {
    #[allow(unused)]
    fn get_warehouse_location(&self) -> String {
        self.config.location.clone()
    }

    #[allow(unused)]
    async fn sync_snapshot(
        &mut self,
        snapshot_payload: PersistenceSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> Result<PersistenceResult> {
        let persistence_result = self
            .sync_snapshot_impl(snapshot_payload, file_params)
            .await?;
        Ok(persistence_result)
    }

    #[allow(unused)]
    async fn load_snapshot_from_table(&mut self) -> Result<(u32, MooncakeSnapshot)> {
        let snapshot = self.load_snapshot_from_table_impl().await?;
        Ok(snapshot)
    }

    #[allow(unused)]
    async fn drop_table(&mut self) -> Result<()> {
        let warehouse = self.get_warehouse_location();
        self.filesystem_accessor
            .remove_directory(&warehouse)
            .await?;

        // Unset all data members.
        self.table = None;
        self.snapshot_loaded = false;
        self.persisted_data_files.clear();

        Ok(())
    }
}

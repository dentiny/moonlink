use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::operations::create::CreateBuilder;
use deltalake::DeltaTable;

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::storage::deltalake::utils;
use crate::storage::iceberg::table_manager::PersistenceFileParams;
use crate::storage::iceberg::table_manager::PersistenceResult;
use crate::storage::mooncake_table::{
    PersistenceSnapshotPayload, TableMetadata as MooncakeTableMetadata,
};
use crate::storage::storage_utils::FileId;
use crate::{BaseFileSystemAccess, CacheTrait};

#[derive(Clone, Debug)]
pub(crate) struct DataFileEntry {
    /// Remote filepath.
    pub(crate) remote_filepath: String,
}

// TODO(hjiang): Use the same table manager interface as iceberg one.
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

    pub(crate) async fn initialize_table_if_exists(&mut self) -> Result<()> {
        assert!(self.table.is_none());
        self.table = utils::get_deltalake_table_if_exists(&self.config).await?;
        Ok(())
    }

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

    async fn load_snapshot_from_table(&mut self) -> Result<(u32, MooncakeSnapshot)> {
        let snapshot = self.load_snapshot_from_table_impl().await?;
        Ok(snapshot)
    }
}

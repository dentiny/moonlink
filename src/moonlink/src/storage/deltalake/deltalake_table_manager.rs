use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::operations::create::CreateBuilder;
use deltalake::DeltaTable;

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::storage::deltalake::deltalake_table_config::DeltalakeTableConfig;
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
    /// Deltalake table configuration.
    pub(crate) config: DeltalakeTableConfig,

    /// Deltalake table.
    pub(crate) table: DeltaTable,

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
        let arrow_schema = mooncake_table_metadata.schema.as_ref();
        let delta_schema_struct = deltalake::kernel::Schema::try_from_arrow(arrow_schema).unwrap();
        let delta_schema_fields = delta_schema_struct
            .fields
            .iter()
            .map(|(_, cur_field)| cur_field.clone())
            .collect::<Vec<_>>();

        let table = CreateBuilder::new()
            .with_location(config.location.clone())
            .with_columns(delta_schema_fields)
            .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
            .await?;
        Ok(Self {
            config,
            table,
            object_storage_cache,
            filesystem_accessor,
            persisted_data_files: HashMap::new(),
        })
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
}

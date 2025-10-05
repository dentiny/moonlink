use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::{open_table, operations::create::CreateBuilder, DeltaTable};
use std::sync::Arc;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::CacheTrait;
use crate::Result;

/// Get or create a Delta table at the given location.
///
/// - If the table doesn't exist → create a new one using the Arrow schema.
/// - If it already exists → load and return.
/// - This mirrors the Iceberg `get_or_create_iceberg_table` pattern.
#[allow(unused)]
pub(crate) async fn get_or_create_deltalake_table(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    _object_storage_cache: Arc<dyn CacheTrait>,
    _filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    config: DeltalakeTableConfig,
) -> Result<DeltaTable> {
    match open_table(config.location.clone()).await {
        Ok(existing_table) => Ok(existing_table),
        Err(_) => {
            let arrow_schema = mooncake_table_metadata.schema.as_ref();
            let delta_schema_struct = deltalake::kernel::Schema::try_from_arrow(arrow_schema)?;
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
            Ok(table)
        }
    }
}

#[allow(unused)]
pub(crate) async fn get_deltalake_table_if_exists(
    config: &DeltalakeTableConfig,
) -> Result<Option<DeltaTable>> {
    match open_table(config.location.clone()).await {
        Ok(table) => Ok(Some(table)),
        Err(_) => Ok(None),
    }
}

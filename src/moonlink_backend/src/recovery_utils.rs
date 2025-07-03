use crate::error::Result;
use crate::mooncake_table_id::MooncakeTableId;
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};
use moonlink_metadata_store::metadata_store_utils;

use std::collections::HashMap;
use std::hash::Hash;

/// Recovery the given table.
async fn recover_table<D, T>(
    database_id: u32,
    metadata_entry: TableMetadataEntry,
    replication_manager: &mut ReplicationManager<MooncakeTableId<D, T>>,
) -> Result<()>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    let mooncake_table_id = MooncakeTableId {
        database_id: D::from(database_id),
        table_id: T::from(metadata_entry.table_id),
    };
    replication_manager
        .add_table(
            &metadata_entry.src_table_uri,
            mooncake_table_id,
            metadata_entry.table_id,
            &metadata_entry.src_table_name,
            /*override_table_base_path=*/
            Some(
                &metadata_entry
                    .moonlink_table_config
                    .iceberg_table_config
                    .warehouse_uri,
            ),
        )
        .await?;
    Ok(())
}

/// Recovery all databases indicated by the connection strings.
/// Return recovered metadata storage clients.
///
/// Recovery process for each database:
/// - if schema not exist, skip
/// - if metadata table not exist, skip
/// - load metadata table, and perform recovery on all mooncake tables
///
/// TODO(hjiang): Parallelize all IO operations.
pub(super) async fn recover_all_tables<D, T>(
    metadata_store_uris: Vec<String>,
    replication_manager: &mut ReplicationManager<MooncakeTableId<D, T>>,
) -> Result<HashMap<D, Box<dyn MetadataStoreTrait>>>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    let mut recovered_metadata_stores: HashMap<D, Box<dyn MetadataStoreTrait>> = HashMap::new();

    for cur_metadata_store_uri in metadata_store_uris.into_iter() {
        let metadata_store_accessor =
            metadata_store_utils::create_metadata_store_accessor(cur_metadata_store_uri)?;

        // Step-1: check schema existence, skip if not.
        if !metadata_store_accessor.schema_exists().await? {
            continue;
        }

        // Skep-2: check metadata store table existence, skip if not.
        if !metadata_store_accessor.metadata_table_exists().await? {
            continue;
        }

        // Step-3: load persisted metadata from storage, perform recovery for each managed tables.
        //
        // Get database id.
        let database_id = metadata_store_accessor.get_database_id().await?;

        // Get all mooncake tables to recovery.
        let table_metadata_entries = metadata_store_accessor
            .get_all_table_metadata_entries()
            .await?;

        // Perform recovery on all managed tables.
        for cur_metadata_entry in table_metadata_entries.into_iter() {
            recover_table(database_id, cur_metadata_entry, replication_manager).await?;
        }

        // Place into metadata store clients map.
        recovered_metadata_stores.insert(D::from(database_id), metadata_store_accessor);
    }

    Ok(recovered_metadata_stores)
}

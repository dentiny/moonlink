mod error;
pub mod file_utils;
mod logging;
pub mod mooncake_table_id;
mod recovery_utils;

pub use error::{Error, Result};
use mooncake_table_id::MooncakeTableId;
pub use moonlink::ReadState;
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use moonlink_metadata_store::metadata_store_utils;
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MoonlinkBackend<
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
> {
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<MooncakeTableId<D, T>>>,
    // Maps from metadata store connection string to metadata store client.
    metadata_store_accessors: RwLock<HashMap<D, Box<dyn MetadataStoreTrait>>>,
}

impl<D, T> MoonlinkBackend<D, T>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    // # Arguments
    //
    // * metadata_store_uris: connection strings for metadata storage database.
    pub async fn new(base_path: String, metadata_store_uris: Vec<String>) -> Result<Self> {
        logging::init_logging();

        // Re-create directory for temporary files directory and read cache files directory under base directory.
        let temp_files_dir = file_utils::get_temp_file_directory_under_base(&base_path);
        let read_cache_files_dir = file_utils::get_cache_directory_under_base(&base_path);
        file_utils::recreate_directory(temp_files_dir.to_str().unwrap()).unwrap();
        file_utils::recreate_directory(read_cache_files_dir.to_str().unwrap()).unwrap();

        let mut replication_manager = ReplicationManager::new(
            base_path,
            temp_files_dir.to_str().unwrap().to_string(),
            file_utils::create_default_object_storage_cache(read_cache_files_dir),
        );
        let metadata_store_accessors =
            recovery_utils::recover_all_tables(metadata_store_uris, &mut replication_manager)
                .await?;

        Ok(Self {
            replication_manager: RwLock::new(replication_manager),
            metadata_store_accessors: RwLock::new(metadata_store_accessors),
        })
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_snapshot(&self, database_id: D, table_id: T, lsn: u64) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&mooncake_table_id);
            writer.initiate_snapshot(lsn).await
        };
        rx.recv().await.unwrap()?;
        Ok(())
    }

    /// Perform an index merge operation, return when it completes.
    /// Notice: the function will be returned right after index merge results buffered to mooncake snapshot, instead of being persisted into iceberg.
    pub async fn perform_index_merge(&self, database_id: D, table_id: T) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&mooncake_table_id);
            writer.initiate_index_merge().await
        };
        rx.recv().await.unwrap();
        Ok(())
    }

    /// Perform a data compaction operation, return when it completes.
    /// Notice: the function will be returned right after data compaction results buffered to mooncake snapshot, instead of being persisted into iceberg.
    pub async fn perform_data_compaction(&self, database_id: D, table_id: T) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&mooncake_table_id);
            writer.initiate_data_compaction().await
        };
        rx.recv().await.unwrap()?;
        Ok(())
    }

    /// # Arguments
    ///
    /// * src_uri: connection string for source database (row storage database).
    pub async fn create_table(
        &self,
        database_id: D,
        table_id: T,
        metadata_store_uri: String,
        src_table_name: String,
        src_uri: String,
    ) -> Result<()> {
        let mooncake_table_id = MooncakeTableId {
            database_id: database_id.clone(),
            table_id,
        };
        let table_id = mooncake_table_id.get_table_id_value();

        // Add mooncake table to replication, and create corresponding mooncake table.
        let moonlink_table_config = {
            let mut manager = self.replication_manager.write().await;
            // TODO(hjiang): Should pass real secrets into the mooncake table.
            let table_config = manager
                .add_table(
                    &src_uri,
                    mooncake_table_id,
                    table_id,
                    &src_table_name,
                    /*override_iceberg_filesystem_config=*/ None,
                    /*is_recovery=*/ false,
                )
                .await?;
            manager.start_replication(&src_uri).await?;
            table_config
        };

        // Create metadata store entry.
        {
            let mut guard = self.metadata_store_accessors.write().await;
            let cur_metadata_store_accessor = match guard.entry(database_id.clone()) {
                HashMapEntry::Occupied(entry) => entry.into_mut(),
                HashMapEntry::Vacant(entry) => {
                    let new_metadata_store =
                        metadata_store_utils::create_metadata_store_accessor(metadata_store_uri)?;
                    entry.insert(new_metadata_store)
                }
            };
            cur_metadata_store_accessor
                .store_table_metadata(table_id, &src_table_name, &src_uri, moonlink_table_config)
                .await?;
        }

        Ok(())
    }

    pub async fn drop_table(&self, database_id: D, table_id: T) {
        let mooncake_table_id = MooncakeTableId {
            database_id: database_id.clone(),
            table_id,
        };
        let table_id = mooncake_table_id.get_table_id_value();

        let table_exists = {
            let mut manager = self.replication_manager.write().await;

            manager.drop_table(mooncake_table_id).await.unwrap()
        };
        if !table_exists {
            return;
        }

        let metadata_store = {
            let mut guard = self.metadata_store_accessors.write().await;
            guard.remove(&database_id).unwrap()
        };
        metadata_store
            .delete_table_metadata(table_id)
            .await
            .unwrap()
    }

    pub async fn scan_table(
        &self,
        database_id: D,
        table_id: T,
        lsn: Option<u64>,
    ) -> Result<Arc<ReadState>> {
        let read_state = {
            let manager = self.replication_manager.read().await;
            let mooncake_table_id = MooncakeTableId {
                database_id,
                table_id,
            };
            let table_reader = manager.get_table_reader(&mooncake_table_id);
            table_reader.try_read(lsn).await?
        };

        Ok(read_state.clone())
    }

    /// Gracefully shutdown a replication connection identified by its URI.
    pub async fn shutdown_connection(&self, uri: &str) {
        let mut manager = self.replication_manager.write().await;
        manager.shutdown_connection(uri);
    }
}

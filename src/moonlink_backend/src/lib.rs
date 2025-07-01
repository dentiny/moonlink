mod error;
mod logging;
pub mod mooncake_table_id;

pub use error::{Error, Result};
use mooncake_table_id::MooncakeTableId;
pub use moonlink::ReadState;
use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::{MetadataStoreTrait, TableMetadataEntry};
use moonlink_metadata_store::PgMetadataStore;
use more_asserts as ma;
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Default local filesystem directory under the above base directory (which defaults to `PGDATA/pg_mooncake`) where all temporary files (used for union read) will be stored under.
/// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_TEMP_FILE_PATH: &str = "./temp/";
/// Default object storage read-through cache directory under the above mooncake directory (which defaults to `PGDATA/pg_mooncake`).
/// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH: &str = "./read_through_cache/";
/// Min left disk space for on-disk cache of the filesystem which cache directory is mounted on.
const MIN_DISK_SPACE_FOR_CACHE: u64 = 1 << 30; // 1GiB
/// Database schema for moonlink.
const MOONLINK_SCHEMA: &str = "mooncake";

/// Get temporary directory under base path.
fn get_temp_file_directory_under_base(base_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(base_path).join(DEFAULT_MOONLINK_TEMP_FILE_PATH)
}
/// Get cache directory under base path.
fn get_cache_directory_under_base(base_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(base_path).join(DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH)
}

/// Util function to delete and re-create the given directory.
pub fn recreate_directory(dir: &str) -> Result<()> {
    // Clean up directory to place moonlink temporary files.
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                return Err(error::Error::Io(e));
            }
        }
    }
    std::fs::create_dir_all(dir)?;

    Ok(())
}

pub struct MoonlinkBackend<
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
> {
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<MooncakeTableId<D, T>>>,
    // Maps from metadata store connection string to metadata store client.
    //
    // TODO(hjiang): Store trait instead of concrete metadata store client.
    metadata_store_clients: RwLock<HashMap<D, PgMetadataStore>>,
}

/// Util function to get filesystem size for cache directory
fn get_cache_filesystem_size(path: &str) -> u64 {
    let vfs_stat = nix::sys::statvfs::statvfs(path).unwrap();
    let block_size = vfs_stat.block_size();
    let avai_blocks = vfs_stat.files_available();

    (block_size as u64).checked_mul(avai_blocks as u64).unwrap()
}

/// Create default object storage cache.
/// Precondition: cache directory has been created beforehand.
fn create_default_object_storage_cache(
    cache_directory_pathbuf: std::path::PathBuf,
) -> ObjectStorageCache {
    let cache_directory = cache_directory_pathbuf.to_str().unwrap().to_string();
    let filesystem_size = get_cache_filesystem_size(&cache_directory);
    ma::assert_ge!(filesystem_size, MIN_DISK_SPACE_FOR_CACHE);

    let cache_config = ObjectStorageCacheConfig {
        max_bytes: filesystem_size - MIN_DISK_SPACE_FOR_CACHE,
        cache_directory,
        optimize_local_filesystem: true,
    };
    ObjectStorageCache::new(cache_config)
}

impl<D, T> MoonlinkBackend<D, T>
where
    D: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
    T: std::convert::From<u32> + Eq + Hash + Clone + std::fmt::Display,
{
    pub fn new(base_path: String) -> Self {
        logging::init_logging();

        // Re-create directory for temporary files directory and cache files directory under base directory.
        let temp_files_dir = get_temp_file_directory_under_base(&base_path);
        let cache_files_dir = get_cache_directory_under_base(&base_path);
        recreate_directory(temp_files_dir.to_str().unwrap()).unwrap();
        recreate_directory(cache_files_dir.to_str().unwrap()).unwrap();

        Self {
            replication_manager: RwLock::new(ReplicationManager::new(
                base_path,
                temp_files_dir.to_str().unwrap().to_string(),
                create_default_object_storage_cache(cache_files_dir),
            )),
            metadata_store_clients: RwLock::new(HashMap::new()),
        }
    }

    /// Recovery the given table.
    async fn recover_table(
        &mut self,
        src_uri: &str,
        database_id: u32,
        metadata_entry: TableMetadataEntry,
    ) -> Result<()> {
        let mooncake_table_id = MooncakeTableId {
            database_id: D::from(database_id),
            table_id: T::from(metadata_entry.table_id),
        };

        let mut manager = self.replication_manager.write().await;
        manager
            .add_table(
                src_uri,
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
    ///
    /// TODO(hjiang): Parallelize all IO operations.
    async fn recover_all_tables(&mut self, uris: HashMap<String, String>) -> Result<()> {
        for (src_uri, metadata_store_uri) in uris.into_iter() {
            // If "mooncake" schema doesn't exist for the given database, it means no table under the current database is managed by moonlink.
            let pg_metadata_store = PgMetadataStore::new(&metadata_store_uri).await?;
            let schema_exists = pg_metadata_store.schema_exists(MOONLINK_SCHEMA).await?;
            if !schema_exists {
                continue;
            }

            // Get database id.
            let database_id = pg_metadata_store.get_database_id().await?;
            // Get all mooncake tables to recovery.
            let table_metadata_entries = pg_metadata_store.get_all_table_metadata_entries().await?;

            // Load config and try recovery.
            for cur_metadata_entry in table_metadata_entries.into_iter() {
                self.recover_table(&src_uri, database_id, cur_metadata_entry)
                    .await?;
            }
        }

        Ok(())
    }

    /// TODO(hjiang): Add this new initialization construct for dev purpose, should merge with [`new`] at the end of the day.
    ///
    /// # Arguments
    ///
    /// * uris: maps from source uris to metadata store uris.
    pub async fn new_with_recovery(
        base_path: String,
        uris: HashMap<String, String>,
    ) -> Result<Self> {
        let mut backend = Self::new(base_path);
        backend.recover_all_tables(uris).await?;
        Ok(backend)
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
            manager
                .add_table(
                    &src_uri,
                    mooncake_table_id,
                    table_id,
                    &src_table_name,
                    /*override_table_base_path=*/ None,
                )
                .await?
        };

        // Create metadata store entry.
        {
            let mut guard = self.metadata_store_clients.write().await;
            let cur_metadata_store_client = match guard.entry(database_id.clone()) {
                HashMapEntry::Occupied(entry) => entry.into_mut(),
                HashMapEntry::Vacant(entry) => {
                    let new_metadata_store = PgMetadataStore::new(&metadata_store_uri).await?;
                    entry.insert(new_metadata_store)
                }
            };
            cur_metadata_store_client
                .store_table_config(table_id, &src_table_name, moonlink_table_config)
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
            let mut guard = self.metadata_store_clients.write().await;
            guard.remove(&database_id).unwrap()
        };
        metadata_store.delete_table_config(table_id).await.unwrap()
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

pub mod columnstore_table_id;
mod error;
mod logging;

use columnstore_table_id::DbColumnstoreTableId;
pub use error::{Error, Result};
pub use moonlink::ReadState;
use moonlink::{ObjectStorageCache, ObjectStorageCacheConfig};
use moonlink_connectors::ReplicationManager;
use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use moonlink_metadata_store::PgMetadataStore;
use more_asserts as ma;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::RwLock;

// Default local filesystem directory under the above base directory (which defaults to `PGDATA/pg_mooncake`) where all temporary files (used for union read) will be stored under.
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_TEMP_FILE_PATH: &str = "./temp/";
// Default object storage read-through cache directory under the above mooncake directory (which defaults to `PGDATA/pg_mooncake`).
// The whole directory is cleaned up at moonlink backend start, to prevent file leak.
pub const DEFAULT_MOONLINK_OBJECT_STORAGE_CACHE_PATH: &str = "./read_through_cache/";
// Min left disk space for on-disk cache of the filesystem which cache directory is mounted on.
const MIN_DISK_SPACE_FOR_CACHE: u64 = 1 << 30; // 1GiB

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
    D: Eq + Hash + Clone + std::fmt::Display,
    T: Eq + Hash + Clone + std::fmt::Display,
> {
    // Could be either relative or absolute path.
    replication_manager: RwLock<ReplicationManager<DbColumnstoreTableId<D, T>>>,
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
    D: Eq + Hash + Clone + std::fmt::Display,
    T: Eq + Hash + Clone + std::fmt::Display,
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

    /// Get column store table id from its generic type.
    fn get_columnstore_table_id(table_id: &T) -> Result<u32> {
        let columnstore_table_id = table_id.to_string().parse::<u32>()?;
        Ok(columnstore_table_id)
    }

    /// Create an iceberg snapshot with the given LSN, return when the a snapshot is successfully created.
    pub async fn create_snapshot(&self, database_id: D, table_id: T, lsn: u64) -> Result<()> {
        let mut rx = {
            let mut manager = self.replication_manager.write().await;
            let db_columnstore_table_id = DbColumnstoreTableId {
                database_id,
                table_id,
            };
            let writer = manager.get_table_event_manager(&db_columnstore_table_id);
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
        let columnstore_table_id = Self::get_columnstore_table_id(&table_id)?;

        // Add column store table to replication, and create corresponding mooncake table.
        let moonlink_table_config = {
            let mut manager = self.replication_manager.write().await;
            let db_columnstore_table_id = DbColumnstoreTableId {
                database_id: database_id.clone(),
                table_id: table_id.clone(),
            };

            manager
                .add_table(&src_uri, db_columnstore_table_id, &src_table_name)
                .await?
        };

        // Create metadata store entry.
        {
            let mut guard = self.metadata_store_clients.write().await;
            let cur_metadata_store_client = if guard.contains_key(&database_id) {
                guard.get(&database_id).unwrap()
            } else {
                let new_metadata_store = PgMetadataStore::new(&metadata_store_uri).await?;
                assert!(guard
                    .insert(database_id.clone(), new_metadata_store)
                    .is_none());
                guard.get(&database_id).unwrap()
            };
            cur_metadata_store_client
                .store_table_config(columnstore_table_id, &src_table_name, moonlink_table_config)
                .await?;
        }

        Ok(())
    }

    pub async fn drop_table(&self, database_id: D, table_id: T) {
        let columnstore_table_id = Self::get_columnstore_table_id(&table_id).unwrap();
        let table_exists = {
            let mut manager = self.replication_manager.write().await;
            let db_columnstore_table_id = DbColumnstoreTableId {
                database_id: database_id.clone(),
                table_id,
            };
            manager.drop_table(db_columnstore_table_id).await.unwrap()
        };
        if !table_exists {
            return;
        }

        let metadata_store = {
            let mut guard = self.metadata_store_clients.write().await;
            guard.remove(&database_id).unwrap()
        };
        metadata_store
            .delete_table_config(columnstore_table_id)
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
            let db_columnstore_table_id = DbColumnstoreTableId {
                database_id,
                table_id,
            };
            let table_reader = manager.get_table_reader(&db_columnstore_table_id);
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

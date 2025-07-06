use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, FileMetadata};
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::table_notify::TableEvent;
use crate::{
    MooncakeTable, NonEvictableHandle, ObjectStorageCache, ObjectStorageCacheConfig, ReadState,
};

/// Test util function to import a second object storage cache entry.
pub(crate) async fn import_fake_cache_entry(
    temp_dir: &TempDir,
    cache: &mut ObjectStorageCache,
) -> NonEvictableHandle {
    // Create a physical fake file, so later evicted files deletion won't fail.
    let fake_filepath = get_fake_file_path(temp_dir);
    let filepath = std::path::PathBuf::from(&fake_filepath);
    tokio::fs::File::create(&filepath).await.unwrap();

    let cache_entry = CacheEntry {
        cache_filepath: fake_filepath,
        file_metadata: FileMetadata {
            file_size: FAKE_FILE_SIZE,
        },
    };
    cache.import_cache_entry(FAKE_FILE_ID, cache_entry).await.0
}

/// ===================================
/// Cache utils
/// ===================================
///
/// Test util function to create an infinitely large object storage cache.
pub(crate) fn create_infinite_object_storage_cache(
    temp_dir: &TempDir,
    optimize_local_filesystem: bool,
) -> ObjectStorageCache {
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    );
    ObjectStorageCache::new(cache_config)
}

/// Test util function to create an object storage cache, with size of only one file.
pub(crate) fn create_object_storage_cache_with_one_file_size(
    temp_dir: &TempDir,
    optimize_local_filesystem: bool,
) -> ObjectStorageCache {
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    );
    ObjectStorageCache::new(cache_config)
}

/// ===================================
/// Request read
/// ===================================
///
/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(crate) async fn drop_read_states(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
        sync_read_request_for_test(table, receiver).await;
    }
}

/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(crate) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> Vec<String> {
    drop_read_states(read_states, table, receiver).await;
    let (_, _, _, _, files_to_delete) = create_mooncake_snapshot_for_test(table, receiver).await;
    files_to_delete
}

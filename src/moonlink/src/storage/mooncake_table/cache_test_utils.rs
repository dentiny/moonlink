use tempfile::TempDir;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, FileMetadata};
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::{NonEvictableHandle, ObjectStorageCache, ObjectStorageCacheConfig};

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

use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::cache_handle::{DataCacheHandle, NonEvictableHandle};
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::storage_utils::FileId;

/// Content for test files.
pub(crate) const CONTENT: &[u8; 10] = b"0123456789";
/// File path for two test files.
pub(crate) const TEST_FILENAME_1: &str = "remote-1.parquet";
pub(crate) const TEST_FILENAME_2: &str = "remote-2.parquet";

/// Util function to prepare a test file.
pub(crate) async fn create_test_file(
    tmp_dir: &std::path::Path,
    filename: &str,
) -> std::path::PathBuf {
    let filepath = tmp_dir.join(filename);
    let mut file = tokio::fs::File::create(&filepath).await.unwrap();
    file.write_all(CONTENT).await.unwrap();
    file.flush().await.unwrap();
    filepath
}

/// Test util function to assert file content.
pub(crate) async fn check_file_content(filepath: &str) {
    let mut file = tokio::fs::File::open(filepath).await.unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await.unwrap();
    assert_eq!(contents, CONTENT);
}

/// Test util function to create config for cache.
pub(crate) fn get_test_cache_config(tmp_dir: &TempDir) -> ObjectStorageCacheConfig {
    ObjectStorageCacheConfig {
        // Set max bytes larger than one file, but less than two files.
        max_bytes: 15,
        cache_directory: tmp_dir.path().to_str().unwrap().to_string(),
    }
}

/// Test util function to create object storage cache.
pub(crate) fn get_test_object_storage_cache(tmp_dir: &TempDir) -> ObjectStorageCache {
    let config = get_test_cache_config(tmp_dir);
    ObjectStorageCache::new(config)
}

/// Test util function to get cache file number.
pub(crate) async fn check_cache_file_count(tmp_dir: &TempDir, expected_count: usize) {
    let mut actual_count = 0;
    let mut entries = tokio::fs::read_dir(tmp_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let metadata = entry.metadata().await.unwrap();
        if metadata.is_file() {
            actual_count += 1;
        }
    }
    assert_eq!(actual_count, expected_count);
}

/// Test util function to assert returned cache handle is non-evictable.
pub(crate) async fn assert_non_evictable_cache_handle(data_cache_handle: &DataCacheHandle) {
    let non_evictable_handle = match data_cache_handle {
        DataCacheHandle::NonEvictable(non_evictable_handle) => non_evictable_handle,
        _ => {
            panic!("Expects to get non-evictable cache handle, but get unimported or evictable one")
        }
    };
    // Check cache file existence.
    assert!(
        tokio::fs::try_exists(&non_evictable_handle.cache_entry.cache_filepath)
            .await
            .unwrap()
    );
}

/// Test util function to assert and get non-evictable cache handle.
pub(crate) fn get_non_evictable_cache_handle(
    data_cache_handle: &DataCacheHandle,
) -> &NonEvictableHandle {
    match data_cache_handle {
        DataCacheHandle::NonEvictable(handle) => handle,
        _ => {
            panic!("Expects to get non-evictable cache handle, but get unimported or evictable one")
        }
    }
}

/// Test util function to check evictable cache size.
pub(crate) async fn assert_evictable_cache_size(
    cache: &mut ObjectStorageCache,
    expected_count: usize,
) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.evictable_cache.len(), expected_count);
}

/// Test util function to check non-evictable cache size.
pub(crate) async fn assert_non_evictable_cache_size(
    cache: &mut ObjectStorageCache,
    expected_count: usize,
) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.non_evictable_cache.len(), expected_count);
}

/// Test util function to check non-evictable cache handle reference count.
pub(crate) async fn assert_non_evictable_cache_handle_ref_count(
    cache: &mut ObjectStorageCache,
    file_id: FileId,
    expected_ref_count: u32,
) {
    let guard = cache.cache.read().await;
    let non_evictable = &guard.non_evictable_cache;
    assert_eq!(
        non_evictable
            .get(&file_id)
            .as_ref()
            .unwrap()
            .reference_count,
        expected_ref_count
    );
}

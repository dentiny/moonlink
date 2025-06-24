use tempfile::tempdir;
use tempfile::TempDir;

use crate::storage::cache::object_storage::base_cache::CacheEntry;
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::FileMetadata;
use crate::storage::cache::object_storage::test_utils::*;
use crate::{ObjectStorageCache, ObjectStorageCacheConfig};

/// This module check state machine when local filesystem optimization enabled.
/// The state transfer is the same as usual, but different at eviction / deletion logic.
/// This test suite only check different parts.
///
/// Test state transfer (which displays different behavior as normal one):
/// (3) + persist + still reference count => (3)
/// (3) + persist + no reference count => (2)
///
/// For more details, please refer to
/// - remote object storage state tests: https://github.com/Mooncake-Labs/moonlink/blob/main/src/moonlink/src/storage/cache/object_storage/state_tests.rs
/// - state machine: https://docs.google.com/document/d/1kwXIl4VPzhgzV4KP8yT42M35PfvMJW9PdjNTF7VNEfA/edit?usp=sharing
///
/// Test util function to create object storage cache, with local filesystem optimization enabled.
fn create_object_storage_cache_with_local_optimization(tmp_dir: &TempDir) -> ObjectStorageCache {
    let config = ObjectStorageCacheConfig {
        // Set max bytes larger than one file, but less than two files.
        max_bytes: 15,
        cache_directory: tmp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem: true,
    };
    ObjectStorageCache::new(config)
}

#[tokio::test]
async fn test_cache_state_3_persist_and_unreferenced_2() {
    let cache_file_directory = tempdir().unwrap();
    let test_cache_file =
        create_test_file(cache_file_directory.path(), TEST_CACHE_FILENAME_1).await;
    let test_remote_file =
        create_test_file(cache_file_directory.path(), TEST_REMOTE_FILENAME_1).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_cache_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let mut cache = create_object_storage_cache_with_local_optimization(&cache_file_directory);
    let file_id = get_table_unique_file_id(0);
    let (mut cache_handle, evicted_files_to_delete) =
        cache.import_cache_entry(file_id, cache_entry.clone()).await;
    assert!(evicted_files_to_delete.is_empty());

    let evicted_files_to_delete = cache_handle
        .unreference_and_replace_with_remote(test_remote_file.to_str().unwrap())
        .await;
    assert_eq!(
        evicted_files_to_delete,
        vec![test_cache_file.to_str().unwrap().to_string()]
    );

    assert_cache_bytes_size(&mut cache, /*expected_bytes=*/ CONTENT.len() as u64).await;
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
}

#[tokio::test]
async fn test_cache_state_3_persist_and_referenced_3() {
    let cache_file_directory = tempdir().unwrap();
    let test_cache_file =
        create_test_file(cache_file_directory.path(), TEST_CACHE_FILENAME_1).await;
    let test_remote_file =
        create_test_file(cache_file_directory.path(), TEST_REMOTE_FILENAME_1).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_cache_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let mut cache = create_object_storage_cache_with_local_optimization(&cache_file_directory);
    let file_id = get_table_unique_file_id(0);
    let (mut cache_handle_1, evicted_files_to_delete) =
        cache.import_cache_entry(file_id, cache_entry.clone()).await;
    assert!(evicted_files_to_delete.is_empty());

    // Get the second reference.
    let (cache_handle_2, evicted_files_to_delete) = cache
        .get_cache_entry(file_id, test_remote_file.to_str().unwrap())
        .await
        .unwrap();
    assert!(evicted_files_to_delete.is_empty());

    // Unreference and try import with remote doesn't work, since there're other references.
    let evicted_files_to_delete = cache_handle_1
        .unreference_and_replace_with_remote(test_remote_file.to_str().unwrap())
        .await;
    assert!(evicted_files_to_delete.is_empty());

    // Unreference the second cache handle places it back to evictale entries.
    let evicted_files_to_delete = cache_handle_2.unwrap().unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    assert_cache_bytes_size(&mut cache, /*expected_bytes=*/ CONTENT.len() as u64).await;
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
}

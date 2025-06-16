/// Possible states for data file cache entries:
/// (1) Not managed by cache
/// (2) Imported into cache, no reference count => can be evicted
/// (3) Imported into cache, has reference count => cannot be evicted
///
/// State inputs related to cache:
/// - When mooncake snapshot, disk slice is record at current snapshot, thus readable
/// - When requested to read, return local file cache to pg_mooncake
/// - When new cache entries imported
/// - Query finishes, thus release pinned cache files
///
/// State transfer to data file cache entries:
/// (1) + create mooncake snapshot => (2)
/// (1) + requested to read + sufficient space => (3)
/// (2) + requested to read + sufficient space => (3)
/// (2) + requested to read + insufficient space => (2)
/// (3) + requested to read => (3)
/// (2) + new entry + sufficient space => (2)
/// (2) + new entry + insufficient space => (1)
/// (3) + query finishes + still reference count => (3)
/// (3) + query finishes + no reference count => (2)
///
/// For more details, please refer to https://docs.google.com/document/d/1kwXIl4VPzhgzV4KP8yT42M35PfvMJW9PdjNTF7VNEfA/edit?usp=sharing
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::cache::object_storage::test_utils::*;
use crate::storage::storage_utils::{FileId, TableId, TableUniqueFileId};

use tempfile::tempdir;

/// Test util function to return table unique id, with table id hard-coded to 0.
fn get_table_unique_file_id(file_id: u64) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TableId(0),
        file_id: FileId(file_id),
    }
}

/// Test util function to return table unique id, with file id 1.

// (1) + create mooncake snapshot => (2)
#[tokio::test]
async fn test_cache_state_1_create_snashot() {
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(cache_file_directory.path(), TEST_FILENAME_1).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Check cache handle status.
    let (_, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (1) + requested to read => (3)
#[tokio::test]
async fn test_cache_1_requested_to_read() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Check cache handle status.
    let (_, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (2) + requested to read + sufficient space => (3)
#[tokio::test]
async fn test_cache_2_requested_to_read_with_sufficient_space() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file_1 = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes: CONTENT.len() as u64,
        cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
    });

    // Import into cache first.
    let cache_entry = CacheEntry {
        cache_filepath: test_file_1.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (_, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Request to read, but failed to pin due to insufficient disk space.
    let test_file_2 = create_test_file(remote_file_directory.path(), TEST_FILENAME_2).await;
    let (cache_handle, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(1),
            test_file_2.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert!(cache_handle.is_none());
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (2) + requested to read + insufficient space => (2)
#[tokio::test]
async fn test_cache_2_requested_to_read_with_insufficient_space() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Import into cache first.
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (mut cache_handle, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle.unreference().await;

    // Request to read, thus pinning the cache entry.
    let (_, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (3) + requested to read => (3)
#[tokio::test]
async fn test_cache_3_requested_to_read() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Import into cache first.
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (_, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Request to read, thus pinning the cache entry.
    let (_, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 2,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (2) + new entry + sufficient space => (2)
#[tokio::test]
async fn test_cache_2_new_entry_with_sufficient_space() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes: (CONTENT.len() * 2) as u64,
        cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
    });

    // Import the first cache file.
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (mut cache_handle, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle.unreference().await;

    // Import the second cache file.
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_2).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (_, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(1), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(1),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
}

// (2) + new entry + insufficient space => (1)
#[tokio::test]
async fn test_cache_2_new_entry_with_insufficient_space() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes: CONTENT.len() as u64,
        cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
    });

    // Import the first cache file.
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (mut cache_handle_1, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle_1.unreference().await;

    // Import the second cache file.
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_2).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (_, files_to_evict) = cache
        .import_cache_entry(/*file_id=*/ get_table_unique_file_id(1), cache_entry)
        .await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(1),
        /*expected_ref_count=*/ 1,
    )
    .await;
    let cache_file_1 = cache_handle_1.cache_entry.cache_filepath.clone();
    assert_eq!(files_to_evict, vec![cache_file_1]);

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (3) + query finishes + still reference count => (3)
#[tokio::test]
async fn test_cache_3_unpin_still_referenced() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Check cache handle status.
    let (_, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Get the same cache entry again to increase its reference count.
    let (cache_handle, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 2,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference one of the cache handles.
    cache_handle.unwrap().unreference().await;

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (3) + query finishes + no reference count => (2)
#[tokio::test]
async fn test_cache_3_unpin_not_referenced() {
    let remote_file_directory = tempdir().unwrap();
    let cache_file_directory = tempdir().unwrap();
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_1).await;
    let mut cache = get_test_object_storage_cache(&cache_file_directory);

    // Check cache handle status.
    let (cache_handle_1, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Get the same cache entry again to increase its reference count.
    let (cache_handle_2, files_to_evict) = cache
        .get_cache_entry(
            /*file_id=*/ get_table_unique_file_id(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ get_table_unique_file_id(0),
        /*expected_ref_count=*/ 2,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference all cache handles.
    cache_handle_1.unwrap().unreference().await;
    cache_handle_2.unwrap().unreference().await;

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
}

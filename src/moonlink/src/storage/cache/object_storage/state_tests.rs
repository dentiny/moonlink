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
/// (1) + requested to read => (3)
/// (2) + requested to read => (3)
/// (3) + requested to read => (3)
/// (2) + new entry + sufficient space => (2)
/// (2) + new entry + insufficient space => (1)
/// (3) + query finishes + still reference count => (3)
/// (3) + query finishes + no reference count => (2)
///
/// For more details, please refer to https://docs.google.com/document/d/1kwXIl4VPzhgzV4KP8yT42M35PfvMJW9PdjNTF7VNEfA/edit?usp=sharing
use crate::create_data_file;
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, FileMetadata, ObjectStorageCacheConfig,
};
use crate::storage::cache::object_storage::cache_handle::DataCacheHandle;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::cache::object_storage::test_utils::*;
use crate::storage::storage_utils::FileId;

use tempfile::tempdir;

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
    let (cache_handle, files_to_evict) = cache
        ._import_cache_entry(/*file_id=*/ FileId(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
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
    let (cache_handle, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

// (2) + requested to read => (3)
#[tokio::test]
async fn test_cache_2_requested_to_read() {
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
        ._import_cache_entry(/*file_id=*/ FileId(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle._unreference().await;

    // Request to read, thus pinning the cache entry.
    let (cache_handle, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
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
    let (cache_handle, files_to_evict) = cache
        ._import_cache_entry(/*file_id=*/ FileId(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Request to read, thus pinning the cache entry.
    let (cache_handle, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
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
    let mut cache = ObjectStorageCache::_new(ObjectStorageCacheConfig {
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
        ._import_cache_entry(/*file_id=*/ FileId(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle._unreference().await;

    // Import the second cache file.
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_2).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (cache_handle, files_to_evict) = cache
        ._import_cache_entry(/*file_id=*/ FileId(1), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(1),
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
    let mut cache = ObjectStorageCache::_new(ObjectStorageCacheConfig {
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
        ._import_cache_entry(/*file_id=*/ FileId(0), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle_1).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference to make cache entry evictable.
    cache_handle_1._unreference().await;

    // Import the second cache file.
    let test_file = create_test_file(remote_file_directory.path(), TEST_FILENAME_2).await;
    let cache_entry = CacheEntry {
        cache_filepath: test_file.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    };
    let (cache_handle_2, files_to_evict) = cache
        ._import_cache_entry(/*file_id=*/ FileId(1), cache_entry)
        .await;
    assert_non_evictable_cache_handle(&cache_handle_2).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(1),
        /*expected_ref_count=*/ 1,
    )
    .await;
    let cache_file_1 = get_non_evictable_cache_handle(&cache_handle_1)
        .cache_entry
        .cache_filepath
        .clone();
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
    let (cache_handle, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Get the same cache entry again to increase its reference count.
    let (mut cache_handle, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 2,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference one of the cache handles.
    cache_handle._unreference().await;

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
    let (mut cache_handle_1, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle_1).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 1,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Get the same cache entry again to increase its reference count.
    let (mut cache_handle_2, files_to_evict) = cache
        ._get_cache_entry(
            /*file_id=*/ FileId(0),
            test_file.as_path().to_str().unwrap(),
        )
        .await
        .unwrap();
    assert_non_evictable_cache_handle(&cache_handle_2).await;
    assert_non_evictable_cache_handle_ref_count(
        &mut cache,
        /*file_id=*/ FileId(0),
        /*expected_ref_count=*/ 2,
    )
    .await;
    assert!(files_to_evict.is_empty());

    // Unreference all cache handles.
    cache_handle_1._unreference().await;
    cache_handle_2._unreference().await;

    // Check cache status.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
}

#[tokio::test]
async fn test_concurrent_data_file_cache() {
    const PARALLEL_TASK_NUM: usize = 10;
    let mut handle_futures = Vec::with_capacity(PARALLEL_TASK_NUM);

    let cache_file_directory = tempdir().unwrap();
    let remote_file_directory = tempdir().unwrap();

    let config = ObjectStorageCacheConfig {
        // Set max bytes larger than one file, but less than two files.
        max_bytes: (CONTENT.len() * PARALLEL_TASK_NUM) as u64,
        cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
    };
    let cache = ObjectStorageCache::_new(config);

    for idx in 0..PARALLEL_TASK_NUM {
        let mut temp_cache = cache.clone();
        let remote_file_directory = remote_file_directory.path().to_path_buf();

        let handle = tokio::task::spawn_blocking(async move || -> DataCacheHandle {
            let filename = format!("{}.parquet", idx);
            let test_file = create_test_file(remote_file_directory.as_path(), &filename).await;
            let data_file = create_data_file(
                /*file_id=*/ idx as u64,
                test_file.to_str().unwrap().to_string(),
            );
            let (cache_handle, cache_to_delete) = temp_cache
                ._get_cache_entry(data_file.file_id(), data_file.file_path())
                .await
                .unwrap();
            assert!(cache_to_delete.is_empty());
            cache_handle
        });
        handle_futures.push(handle);
    }

    let results = futures::future::join_all(handle_futures).await;
    for cur_handle_future in results.into_iter() {
        let cur_cache_handle = cur_handle_future.unwrap().await;
        let non_evictable_handle = get_non_evictable_cache_handle(&cur_cache_handle);
        check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
        assert_eq!(
            non_evictable_handle.cache_entry.file_metadata.file_size as usize,
            CONTENT.len()
        );
    }
    assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
    assert_eq!(
        cache.cache.read().await.non_evictable_cache.len(),
        PARALLEL_TASK_NUM
    );
    check_cache_file_count(&cache_file_directory, PARALLEL_TASK_NUM).await;
}

use std::collections::HashMap;
use std::sync::Arc;

/// Object storage cache, which caches data file in file granularity at local filesystem.
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, FileMetadata, ObjectStorageCacheConfig,
};
use crate::storage::cache::object_storage::cache_handle::{DataCacheHandle, NonEvictableHandle};
use crate::storage::storage_utils::FileId;
use crate::Result;

use lru::LruCache;
use more_asserts as ma;
use tokio::sync::RwLock;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheEntryWrapper {
    /// Cache entry.
    cache_entry: CacheEntry,
    /// Reference count.
    reference_count: u32,
}

/// A cache entry could be either evictable or non-evictable.
/// A general lifecycle of a cache entry is to
/// (1) fetch and mark as non-evictable on access
/// (2) dereference after usage, down-level to evictable when it's _unreferenced
#[allow(dead_code)]
pub(super) struct ObjectStorageCacheInternal {
    /// Current number of bytes of all cache entries.
    cur_bytes: u64,
    /// Evictable data file cache entries.
    evictable_cache: LruCache<FileId, CacheEntryWrapper>,
    /// Non-evictable data file cache entries.
    non_evictable_cache: HashMap<FileId, CacheEntryWrapper>,
}

impl ObjectStorageCacheInternal {
    /// Util function to remove entries from evictable cache, until overall file size drops down below max size.
    /// Return data files which get evicted from LRU cache, and will be deleted locally.
    fn _evict_cache_entries(&mut self, max_bytes: u64) -> Vec<String> {
        let mut evicted_data_files = vec![];
        while self.cur_bytes > max_bytes {
            assert!(
                !self.evictable_cache.is_empty(),
                "Cannot reduce disk usage by evicting entries."
            );
            let (_, mut cache_entry_wrapper) = self.evictable_cache.pop_lru().unwrap();
            assert_eq!(cache_entry_wrapper.reference_count, 0);
            self.cur_bytes -= cache_entry_wrapper.cache_entry.file_metadata.file_size;
            let cache_filepath =
                std::mem::take(&mut cache_entry_wrapper.cache_entry.cache_filepath);
            evicted_data_files.push(cache_filepath);
        }
        evicted_data_files
    }

    /// Util function to insert into non-evictable cache.
    /// NOTICE: cache current bytes won't be updated.
    fn _insert_non_evictable(
        &mut self,
        file_id: FileId,
        cache_entry_wrapper: CacheEntryWrapper,
        max_bytes: u64,
    ) -> Vec<String> {
        assert!(self.evictable_cache.get(&file_id).is_none());
        let old_entry = self
            .non_evictable_cache
            .insert(file_id, cache_entry_wrapper);
        assert!(old_entry.is_none());
        self._evict_cache_entries(max_bytes)
    }

    /// Unreference the given cache entry.
    pub(super) fn _unreference(&mut self, file_id: FileId) {
        let cache_entry_wrapper = self.non_evictable_cache.get_mut(&file_id);
        assert!(cache_entry_wrapper.is_some());
        let cache_entry_wrapper = cache_entry_wrapper.unwrap();
        cache_entry_wrapper.reference_count -= 1;

        // Down-level to evictable if reference count goes away.
        if cache_entry_wrapper.reference_count == 0 {
            let cache_entry_wrapper = self.non_evictable_cache.remove(&file_id).unwrap();
            self.evictable_cache.push(file_id, cache_entry_wrapper);
        }
    }
}

// TODO(hjiang): Add stats for cache, like cache hit/miss rate, cache size, etc.
#[allow(dead_code)]
struct ObjectStorageCache {
    /// Cache configs.
    config: ObjectStorageCacheConfig,
    /// Data file caches.
    cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

impl ObjectStorageCache {
    pub fn _new(config: ObjectStorageCacheConfig) -> Self {
        let evictable_cache = LruCache::unbounded();
        let non_evictable_cache = HashMap::new();
        Self {
            config,
            cache: Arc::new(RwLock::new(ObjectStorageCacheInternal {
                cur_bytes: 0,
                evictable_cache,
                non_evictable_cache,
            })),
        }
    }

    /// Read from remote [`src`] and write to local cache file, return cache entries.
    async fn _load_from_remote(&self, src: &str) -> Result<CacheEntry> {
        let src_pathbuf = std::path::PathBuf::from(src);
        let suffix = src_pathbuf.extension().unwrap().to_str().unwrap();
        let mut dst_pathbuf = std::path::PathBuf::from(&self.config.cache_directory);
        dst_pathbuf.push(format!("{}.{}", Uuid::now_v7(), suffix));
        let dst_filepath = dst_pathbuf.to_str().unwrap().to_string();

        let mut src_file = tokio::fs::File::open(src).await?;
        let mut dst_file = tokio::fs::File::create(&dst_filepath).await?;
        tokio::io::copy(&mut src_file, &mut dst_file).await?;

        let file_size = src_file.metadata().await?.len();
        Ok(CacheEntry {
            cache_filepath: dst_filepath,
            file_metadata: FileMetadata { file_size },
        })
    }
}

#[async_trait::async_trait]
impl CacheTrait for ObjectStorageCache {
    async fn _add_new_cache_entry(
        &mut self,
        file_id: FileId,
        cache_entry: CacheEntry,
        evictable: bool,
    ) -> (DataCacheHandle, Vec<String>) {
        let mut cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };

        let mut guard = self.cache.write().await;
        guard.cur_bytes += cache_entry.file_metadata.file_size;

        // Emplace evictable cache.
        if evictable {
            assert!(!guard.non_evictable_cache.contains_key(&file_id));
            cache_entry_wrapper.reference_count = 0;
            let old_entry = guard.evictable_cache.push(file_id, cache_entry_wrapper);
            assert!(old_entry.is_none());
            let cache_files_to_delete = guard._evict_cache_entries(self.config.max_bytes);
            return (DataCacheHandle::Evictable, cache_files_to_delete);
        }

        // Emplace non-evictable cache.
        let cache_files_to_delete =
            guard._insert_non_evictable(file_id, cache_entry_wrapper, self.config.max_bytes);
        let non_evictable_handle =
            NonEvictableHandle::_new(file_id, cache_entry, self.cache.clone());
        (
            DataCacheHandle::NonEvictable(non_evictable_handle),
            cache_files_to_delete,
        )
    }

    async fn _get_cache_entry(
        &mut self,
        file_id: FileId,
        remote_filepath: &str,
    ) -> Result<(DataCacheHandle, Vec<String>)> {
        {
            let mut guard = self.cache.write().await;

            // Check non-evictable cache.
            let mut value = guard.non_evictable_cache.get_mut(&file_id);
            if value.is_some() {
                ma::assert_gt!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                let cache_entry = value.as_ref().unwrap().cache_entry.clone();
                let non_evictable_handle =
                    NonEvictableHandle::_new(file_id, cache_entry, self.cache.clone());
                return Ok((DataCacheHandle::NonEvictable(non_evictable_handle), vec![]));
            }

            // Check evictable cache.
            let mut value = guard.evictable_cache.pop(&file_id);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                let cache_entry = value.as_ref().unwrap().cache_entry.clone();
                let files_to_delete =
                    guard._insert_non_evictable(file_id, value.unwrap(), self.config.max_bytes);
                let non_evictable_handle =
                    NonEvictableHandle::_new(file_id, cache_entry, self.cache.clone());
                return Ok((
                    DataCacheHandle::NonEvictable(non_evictable_handle),
                    files_to_delete,
                ));
            }
        }

        // The requested item doesn't exist, perform IO operations to load.
        //
        // TODO(hjiang):
        // 1. IO operations should leverage opendal, use tokio::fs as of now to validate cache functionality.
        // 2. We could have different requests for the same remote filepath, should able to avoid wasted repeated IO.
        let cache_entry = self._load_from_remote(remote_filepath).await?;
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };
        let non_evictable_handle =
            NonEvictableHandle::_new(file_id, cache_entry.clone(), self.cache.clone());

        {
            let mut guard = self.cache.write().await;
            guard.cur_bytes += cache_entry.file_metadata.file_size;
            let evicted_entries =
                guard._insert_non_evictable(file_id, cache_entry_wrapper, self.config.max_bytes);
            Ok((
                DataCacheHandle::NonEvictable(non_evictable_handle),
                evicted_entries,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::create_data_file;

    use super::*;

    use tempfile::{tempdir, TempDir};
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    /// Content for test files.
    const CONTENT: &[u8; 10] = b"0123456789";
    /// File path for two test files.
    const TEST_FILENAME_1: &str = "remote-1.parquet";
    const TEST_FILENAME_2: &str = "remote-2.parquet";
    // Fake cache file.
    const FAKE_FILENAME: &str = "fake-cache.parquet";

    /// Util function to prepare a local file.
    async fn create_test_file_1(tmp_dir: &TempDir) -> PathBuf {
        let filepath = tmp_dir.path().join(TEST_FILENAME_1);
        let mut file = tokio::fs::File::create(&filepath).await.unwrap();
        file.write_all(CONTENT).await.unwrap();
        file.flush().await.unwrap();
        filepath
    }
    async fn create_test_file_2(tmp_dir: &TempDir) -> PathBuf {
        let filepath = tmp_dir.path().join(TEST_FILENAME_2);
        let mut file = tokio::fs::File::create(&filepath).await.unwrap();
        file.write_all(CONTENT).await.unwrap();
        file.flush().await.unwrap();
        filepath
    }

    /// Test util function to assert file content.
    async fn check_file_content(filepath: &str) {
        let mut file = tokio::fs::File::open(filepath).await.unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, CONTENT);
    }

    /// Test util function to create config for cache.
    fn get_test_cache_config(tmp_dir: &TempDir) -> ObjectStorageCacheConfig {
        ObjectStorageCacheConfig {
            // Set max bytes larger than one file, but less than two files.
            max_bytes: 15,
            cache_directory: tmp_dir.path().to_str().unwrap().to_string(),
        }
    }

    /// Test util function to create object storage cache.
    fn get_test_object_storage_cache(tmp_dir: &TempDir) -> ObjectStorageCache {
        let config = get_test_cache_config(tmp_dir);
        ObjectStorageCache::_new(config)
    }

    /// Test util function to get cache file number.
    async fn check_cache_file_count(tmp_dir: &TempDir, expected_count: usize) {
        let mut actual_count = 0;
        let mut entries = tokio::fs::read_dir(tmp_dir.path()).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            // Skip test file, we only check cache file count.
            if entry.file_name().to_str().unwrap() == TEST_FILENAME_1
                || entry.file_name().to_str().unwrap() == TEST_FILENAME_2
            {
                continue;
            }

            let metadata = entry.metadata().await.unwrap();
            if metadata.is_file() {
                actual_count += 1;
            }
        }
        assert_eq!(actual_count, expected_count);
    }

    /// Test util function to assert returned cache handle is evictable.
    fn assert_evictable_cache_handle(data_cache_handle: &DataCacheHandle) {
        match data_cache_handle {
            DataCacheHandle::Evictable => {}
            _ => {
                panic!("Expects to get evictable cache handle, but get unimported or non evictable one")
            }
        }
    }

    /// Test util function to assert returned cache handle is non-evictable.
    fn assert_non_evictable_cache_handle(data_cache_handle: &DataCacheHandle) {
        match data_cache_handle {
            DataCacheHandle::NonEvictable(_) => {}
            _ => panic!(
                "Expects to get non-evictable cache handle, but get unimported or evictable one"
            ),
        }
    }

    /// Test util function to assert and get non-evictable cache handle.
    fn get_non_evictable_cache_handle(data_cache_handle: &DataCacheHandle) -> &NonEvictableHandle {
        match data_cache_handle {
            DataCacheHandle::NonEvictable(handle) => handle,
            _ => {
                panic!("Expects to get non-evictable cache handle, but get unimported or evictable one")
            }
        }
    }

    #[tokio::test]
    async fn test_cache_operation() {
        let tmp_dir = tempdir().unwrap();
        let test_data_file_1 = create_test_file_1(&tmp_dir).await;
        let data_file_1 = create_data_file(
            /*file_id=*/ 0,
            test_data_file_1.to_str().unwrap().to_string(),
        );
        let mut cache = get_test_object_storage_cache(&tmp_dir);

        // Operation-1: download and pin with reference count 1.
        let (mut cache_handle_1, cache_to_delete) = cache
            ._get_cache_entry(data_file_1.file_id(), data_file_1.file_path())
            .await
            .unwrap();
        let non_evictable_handle = get_non_evictable_cache_handle(&cache_handle_1);
        check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
        assert_eq!(
            non_evictable_handle.cache_entry.file_metadata.file_size as usize,
            CONTENT.len()
        );
        assert!(cache_to_delete.is_empty());
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-2: get the cached file, and increment one additional reference count.
        let (mut cache_handle_2, cache_to_delete) = cache
            ._get_cache_entry(data_file_1.file_id(), data_file_1.file_path())
            .await
            .unwrap();
        let non_evictable_handle = get_non_evictable_cache_handle(&cache_handle_2);
        check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
        assert_eq!(
            non_evictable_handle.cache_entry.file_metadata.file_size as usize,
            CONTENT.len()
        );
        assert!(cache_to_delete.is_empty());
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-3: explicitly decrement reference count to 0, so later we could cache other files.
        cache_handle_1._unreference().await;
        cache_handle_2._unreference().await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);

        // Operation-4: now cache file 1 is not referenced any more, and we could add import new data entries.
        let test_data_file_2 = create_test_file_2(&tmp_dir).await;
        let data_file_2 = create_data_file(
            /*file_id=*/ 1,
            test_data_file_2.to_str().unwrap().to_string(),
        );
        let cache_entry_2 = CacheEntry {
            cache_filepath: FAKE_FILENAME.to_string(),
            file_metadata: FileMetadata {
                file_size: CONTENT.len() as u64,
            },
        };
        let (cache_handle, cache_to_delete) = cache
            ._add_new_cache_entry(
                data_file_2.file_id(),
                cache_entry_2.clone(),
                /*evictable=*/ true,
            )
            .await;
        assert_evictable_cache_handle(&cache_handle);
        assert_eq!(cache_to_delete.len(), 1);
        tokio::fs::remove_file(&cache_to_delete[0]).await.unwrap();
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);

        // Operation-5: newly imported file is not pinned, so we're able to evict it out.
        let (mut cache_handle, cache_to_delete) = cache
            ._get_cache_entry(data_file_1.file_id(), data_file_1.file_path())
            .await
            .unwrap();
        let non_evictable_handle = get_non_evictable_cache_handle(&cache_handle);
        check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
        assert_eq!(
            non_evictable_handle.cache_entry.file_metadata.file_size as usize,
            CONTENT.len()
        );
        assert_eq!(cache_to_delete.len(), 1);
        assert!(cache_to_delete[0].ends_with(FAKE_FILENAME));
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-6: drop and unreference, so we could import new cache files.
        cache_handle._unreference().await;
        let (mut cache_handle, cache_to_delete) = cache
            ._add_new_cache_entry(
                data_file_2.file_id(),
                cache_entry_2.clone(),
                /*evictable=*/ false,
            )
            .await;
        assert_non_evictable_cache_handle(&cache_handle);
        assert_eq!(cache_to_delete.len(), 1);
        tokio::fs::remove_file(&cache_to_delete[0]).await.unwrap();
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-7: unreference all cache entries.
        cache_handle._unreference().await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);
    }
}

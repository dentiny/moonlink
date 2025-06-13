use std::collections::HashMap;
use std::sync::Arc;

/// Object storage cache, which caches data file in file granularity at local filesystem.
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::cache_handle::{DataCacheHandle, NonEvictableHandle};
use crate::storage::storage_utils::FileId;
use crate::Result;

use lru::LruCache;
use more_asserts as ma;
use tokio::sync::RwLock;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheEntryWrapper {
    /// Cache entry.
    pub(crate) cache_entry: CacheEntry,
    /// Reference count.
    pub(crate) reference_count: u32,
}

/// A cache entry could be either evictable or non-evictable.
/// A general lifecycle of a cache entry is to
/// (1) fetch and mark as non-evictable on access
/// (2) dereference after usage, down-level to evictable when it's _unreferenced
#[allow(dead_code)]
pub(crate) struct ObjectStorageCacheInternal {
    /// Current number of bytes of all cache entries.
    pub(crate) cur_bytes: u64,
    /// Evictable data file cache entries.
    pub(crate) evictable_cache: LruCache<FileId, CacheEntryWrapper>,
    /// Non-evictable data file cache entries.
    pub(crate) non_evictable_cache: HashMap<FileId, CacheEntryWrapper>,
}

impl ObjectStorageCacheInternal {
    /// Util function to remove entries from evictable cache, until overall file size drops down below max size.
    /// Return data files which get evicted from LRU cache, and will be deleted locally.
    fn evict_cache_entries(&mut self, max_bytes: u64) -> Vec<String> {
        let mut evicted_data_files = vec![];
        while self.cur_bytes > max_bytes {
            // TODO(hjiang): In certain case, we could tolerate disk space insufficiency (i.e. when pg_mooncake request for data files, we could fallback to return remote files).
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
    fn insert_non_evictable(
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
        self.evict_cache_entries(max_bytes)
    }

    /// Unreference the given cache entry.
    pub(super) fn unreference(&mut self, file_id: FileId) {
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
#[derive(Clone)]
pub struct ObjectStorageCache {
    /// Cache configs.
    config: ObjectStorageCacheConfig,
    /// Data file caches.
    pub(crate) cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

impl ObjectStorageCache {
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        let config = ObjectStorageCacheConfig::default_for_test();
        Self::new(config)
    }

    pub fn new(config: ObjectStorageCacheConfig) -> Self {
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
    async fn import_cache_entry(
        &mut self,
        file_id: FileId,
        cache_entry: CacheEntry,
    ) -> (DataCacheHandle, Vec<String>) {
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };

        let mut guard = self.cache.write().await;
        guard.cur_bytes += cache_entry.file_metadata.file_size;

        let cache_files_to_delete =
            guard.insert_non_evictable(file_id, cache_entry_wrapper, self.config.max_bytes);
        let non_evictable_handle =
            NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
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
                    NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
                return Ok((DataCacheHandle::NonEvictable(non_evictable_handle), vec![]));
            }

            // Check evictable cache.
            let mut value = guard.evictable_cache.pop(&file_id);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                let cache_entry = value.as_ref().unwrap().cache_entry.clone();
                let files_to_delete =
                    guard.insert_non_evictable(file_id, value.unwrap(), self.config.max_bytes);
                let non_evictable_handle =
                    NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
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
            NonEvictableHandle::new(file_id, cache_entry.clone(), self.cache.clone());

        {
            let mut guard = self.cache.write().await;
            guard.cur_bytes += cache_entry.file_metadata.file_size;
            let evicted_entries =
                guard.insert_non_evictable(file_id, cache_entry_wrapper, self.config.max_bytes);
            Ok((
                DataCacheHandle::NonEvictable(non_evictable_handle),
                evicted_entries,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::create_data_file;
    use crate::storage::cache::object_storage::test_utils::*;

    use super::*;

    use tempfile::tempdir;

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
        let cache = ObjectStorageCache::new(config);

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
}

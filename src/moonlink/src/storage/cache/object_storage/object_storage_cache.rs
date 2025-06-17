use std::collections::HashMap;
use std::sync::Arc;

/// Object storage cache, which caches data file in file granularity at local filesystem.
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::cache_handle::NonEvictableHandle;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::Result;

use lru::LruCache;
use more_asserts as ma;
#[cfg(test)]
use tempfile::TempDir;
use tokio::sync::RwLock;
use uuid::Uuid;

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
/// (2) dereference after usage, down-level to evictable when it's unreferenced
pub(crate) struct ObjectStorageCacheInternal {
    /// Current number of bytes of all cache entries.
    pub(crate) cur_bytes: u64,
    /// Evictable data file cache entries.
    pub(crate) evictable_cache: LruCache<TableUniqueFileId, CacheEntryWrapper>,
    /// Non-evictable data file cache entries.
    pub(crate) non_evictable_cache: HashMap<TableUniqueFileId, CacheEntryWrapper>,
}

impl ObjectStorageCacheInternal {
    /// Util function to remove entries from evictable cache, until overall file size drops down below max size.
    ///
    /// # Arguments
    ///
    /// * tolerate_insufficiency: if true, tolerate disk space insufficiency by returning `false` in such case; otherwise panic if nothing to evict when insufficient disk space.
    ///
    /// Return
    /// - whether cache entries eviction succeeds or not.
    /// - data files which get evicted from LRU cache, and will be deleted locally.
    fn evict_cache_entries(
        &mut self,
        max_bytes: u64,
        tolerate_insufficiency: bool,
    ) -> (bool, Vec<String>) {
        let mut evicted_data_files = vec![];
        while self.cur_bytes > max_bytes {
            if self.evictable_cache.is_empty() {
                assert!(
                    tolerate_insufficiency,
                    "Cannot reduce disk usage by evicting entries."
                );
                return (false, evicted_data_files);
            }
            let (_, mut cache_entry_wrapper) = self.evictable_cache.pop_lru().unwrap();
            assert_eq!(cache_entry_wrapper.reference_count, 0);
            self.cur_bytes -= cache_entry_wrapper.cache_entry.file_metadata.file_size;
            let cache_filepath =
                std::mem::take(&mut cache_entry_wrapper.cache_entry.cache_filepath);
            evicted_data_files.push(cache_filepath);
        }
        (true, evicted_data_files)
    }

    /// Util function to insert into non-evictable cache.
    ///
    /// Return
    /// - whether cache entries eviction succeeds or not.
    /// - data files which get evicted from LRU cache, and will be deleted locally.
    ///
    /// NOTICE:
    /// - cache current bytes won't be updated.
    /// - If insertion fails due to insufficiency, the input cache entry won't be inserted into cache.
    fn insert_non_evictable(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry_wrapper: CacheEntryWrapper,
        max_bytes: u64,
        tolerate_insufficiency: bool,
    ) -> (bool, Vec<String>) {
        assert!(self.evictable_cache.get(&file_id).is_none());
        assert!(self
            .non_evictable_cache
            .insert(file_id, cache_entry_wrapper)
            .is_none());
        let (evict_succ, files_to_delete) =
            self.evict_cache_entries(max_bytes, tolerate_insufficiency);
        if !evict_succ {
            assert!(self.non_evictable_cache.remove(&file_id).is_some());
        }
        (evict_succ, files_to_delete)
    }

    /// Unreference the given cache entry.
    pub(super) fn unreference(&mut self, file_id: TableUniqueFileId) {
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

    /// ================================
    /// Test util functions
    /// ================================
    ///
    /// Test util function to get reference count for the given file id, return 0 if doesn't exist.
    #[cfg(test)]
    pub(crate) fn get_non_evictable_entry_ref_count(&self, file_id: &TableUniqueFileId) -> u32 {
        let cache_entry = self.non_evictable_cache.get(file_id);
        if let Some(cache_entry) = cache_entry {
            return cache_entry.reference_count;
        }
        0
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

// A dummy [`Debug`] trait implementation.
impl std::fmt::Debug for ObjectStorageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStorageCache")
            .field("config", &self.config)
            .finish()
    }
}

impl ObjectStorageCache {
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
    async fn load_from_remote(&self, src: &str) -> Result<CacheEntry> {
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

    /// ================================
    /// Test/bench util functions
    /// ================================
    ///
    #[cfg(test)]
    pub fn default_for_test(temp_dir: &TempDir) -> Self {
        let config = ObjectStorageCacheConfig::default_for_test(temp_dir);
        Self::new(config)
    }

    #[cfg(feature = "bench")]
    pub fn default_for_bench() -> Self {
        let config = ObjectStorageCacheConfig::default_for_bench();
        Self::new(config)
    }

    /// Test util function to get reference count for reference count.
    #[cfg(test)]
    pub(crate) async fn get_non_evictable_entry_ref_count(
        &self,
        file_id: &TableUniqueFileId,
    ) -> u32 {
        let guard = self.cache.read().await;
        guard.get_non_evictable_entry_ref_count(file_id)
    }

    /// Test util function to get non-evictable filenames.
    #[cfg(test)]
    pub(crate) async fn get_non_evictable_filenames(&self) -> Vec<TableUniqueFileId> {
        let guard = self.cache.read().await;
        guard
            .non_evictable_cache
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    }
}

#[async_trait::async_trait]
impl CacheTrait for ObjectStorageCache {
    async fn import_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, Vec<String>) {
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };

        let mut guard = self.cache.write().await;
        guard.cur_bytes += cache_entry.file_metadata.file_size;

        let cache_files_to_delete = guard
            .insert_non_evictable(
                file_id,
                cache_entry_wrapper,
                self.config.max_bytes,
                /*tolerate_insufficiency=*/ false,
            )
            .1;
        let non_evictable_handle =
            NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
        (non_evictable_handle, cache_files_to_delete)
    }

    async fn get_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
    ) -> Result<(
        Option<NonEvictableHandle>,
        Vec<String>, /*files_to_delete*/
    )> {
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
                return Ok((Some(non_evictable_handle), /*files_to_delete=*/ vec![]));
            }

            // Check evictable cache.
            let mut value = guard.evictable_cache.pop(&file_id);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                let cache_entry = value.as_ref().unwrap().cache_entry.clone();
                let files_to_delete = guard
                    .insert_non_evictable(
                        file_id,
                        value.unwrap(),
                        self.config.max_bytes,
                        /*tolerate_insufficiency=*/ true,
                    )
                    .1;
                assert!(files_to_delete.is_empty());
                let non_evictable_handle =
                    NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
                return Ok((Some(non_evictable_handle), /*files_to_delete=*/ vec![]));
            }
        }

        // The requested item doesn't exist, perform IO operations to load.
        //
        // TODO(hjiang):
        // 1. IO operations should leverage opendal, use tokio::fs as of now to validate cache functionality.
        // 2. We could have different requests for the same remote filepath, should able to avoid wasted repeated IO.
        let cache_entry = self.load_from_remote(remote_filepath).await?;
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };
        let non_evictable_handle =
            NonEvictableHandle::new(file_id, cache_entry.clone(), self.cache.clone());

        {
            let mut guard = self.cache.write().await;
            guard.cur_bytes += cache_entry.file_metadata.file_size;
            let (cache_succ, files_to_delete) = guard.insert_non_evictable(
                file_id,
                cache_entry_wrapper,
                self.config.max_bytes,
                /*tolerate_insufficiency=*/ true,
            );
            if cache_succ {
                return Ok((Some(non_evictable_handle), files_to_delete));
            }
            Ok((None, files_to_delete))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::create_data_file;
    use crate::storage::cache::object_storage::test_utils::*;
    use crate::storage::storage_utils::TableId;

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

            let handle = tokio::task::spawn_blocking(async move || -> NonEvictableHandle {
                let filename = format!("{}.parquet", idx);
                let test_file = create_test_file(remote_file_directory.as_path(), &filename).await;
                let data_file = create_data_file(
                    /*file_id=*/ idx as u64,
                    test_file.to_str().unwrap().to_string(),
                );
                let unique_file_id = TableUniqueFileId {
                    table_id: TableId(0),
                    file_id: data_file.file_id(),
                };
                let (cache_handle, cache_to_delete) = temp_cache
                    .get_cache_entry(unique_file_id, data_file.file_path())
                    .await
                    .unwrap();
                assert!(cache_to_delete.is_empty());
                cache_handle.unwrap()
            });
            handle_futures.push(handle);
        }

        let results = futures::future::join_all(handle_futures).await;
        for cur_handle_future in results.into_iter() {
            let non_evictable_handle = cur_handle_future.unwrap().await;
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

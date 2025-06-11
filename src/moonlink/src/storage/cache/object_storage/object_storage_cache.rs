use std::collections::HashMap;
use std::sync::Arc;

/// Object storage cache, which caches data file in file granularity at local filesystem.
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, FileMetadata, ObjectStorageCacheConfig,
};
use crate::storage::storage_utils::{MooncakeDataFile, MooncakeDataFileRef};
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
struct ObjectStorageCacheInternal {
    /// Current number of bytes of all cache entries.
    cur_bytes: u64,
    /// Evictable data file cache entries.
    evictable_cache: LruCache<MooncakeDataFileRef, CacheEntryWrapper>,
    /// Non-evictable data file cache entries.
    non_evictable_cache: HashMap<MooncakeDataFileRef, CacheEntryWrapper>,
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
        data_file: MooncakeDataFileRef,
        cache_entry_wrapper: CacheEntryWrapper,
        max_bytes: u64,
    ) -> Vec<String> {
        assert!(self.evictable_cache.get(&data_file).is_none());
        let old_entry = self
            .non_evictable_cache
            .insert(data_file, cache_entry_wrapper);
        assert!(old_entry.is_none());
        self._evict_cache_entries(max_bytes)
    }
}

// TODO(hjiang): Add stats for cache, like cache hit/miss rate, cache size, etc.
#[allow(dead_code)]
struct ObjectStorageCache {
    /// Cache configs.
    config: ObjectStorageCacheConfig,
    /// Data file caches.
    cache: RwLock<ObjectStorageCacheInternal>,
}

impl ObjectStorageCache {
    pub fn _new(config: ObjectStorageCacheConfig) -> Self {
        let evictable_cache = LruCache::unbounded();
        let non_evictable_cache = HashMap::new();
        Self {
            config,
            cache: RwLock::new(ObjectStorageCacheInternal {
                cur_bytes: 0,
                evictable_cache,
                non_evictable_cache,
            }),
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
        data_file: MooncakeDataFileRef,
        file_metadata: FileMetadata,
        evictable: bool,
    ) -> Vec<String> {
        assert!(data_file.cache_file_path().is_some());
        let file_size = file_metadata.file_size;
        let cache_entry = CacheEntry {
            cache_filepath: data_file.cache_file_path().as_ref().unwrap().clone(),
            file_metadata,
        };
        let mut cache_entry_wrapper = CacheEntryWrapper {
            cache_entry,
            reference_count: 1,
        };

        let mut guard = self.cache.write().await;
        guard.cur_bytes += file_size;

        // Emplace evictable cache.
        if evictable {
            assert!(!guard.non_evictable_cache.contains_key(&data_file));
            cache_entry_wrapper.reference_count = 0;
            let old_entry = guard.evictable_cache.push(data_file, cache_entry_wrapper);
            assert!(old_entry.is_none());
            return guard._evict_cache_entries(self.config.max_bytes);
        }

        // Emplace non-evictable cache.
        guard._insert_non_evictable(data_file, cache_entry_wrapper, self.config.max_bytes)
    }

    async fn _get_cache_entry(
        &mut self,
        data_file: &MooncakeDataFileRef,
    ) -> Result<(CacheEntry, Vec<String>)> {
        {
            let mut guard = self.cache.write().await;

            // Check non-evictable cache.
            let mut value = guard.non_evictable_cache.get_mut(data_file);
            if value.is_some() {
                ma::assert_gt!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                return Ok((value.as_ref().unwrap().cache_entry.clone(), vec![]));
            }

            // Check evictable cache.
            let mut value = guard.evictable_cache.pop(data_file);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().reference_count, 0);
                value.as_mut().unwrap().reference_count += 1;
                let cache_entry = value.as_ref().unwrap().cache_entry.clone();
                let files_to_delete = guard._insert_non_evictable(
                    data_file.clone(),
                    value.unwrap(),
                    self.config.max_bytes,
                );
                return Ok((cache_entry, files_to_delete));
            }
        }

        // The requested item doesn't exist, perform IO operations to load.
        //
        // TODO(hjiang):
        // 1. IO operations should leverage opendal, use tokio::fs as of now to validate cache functionality.
        // 2. We could have different requests for the same remote filepath, should able to avoid wasted repeated IO.
        let cache_entry = self._load_from_remote(data_file.file_path()).await?;
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
        };

        let result_data_file = Arc::new(MooncakeDataFile {
            file_id: data_file.file_id,
            file_path: data_file.file_path.clone(),
            cache_file_path: Some(cache_entry.cache_filepath.clone()),
        });

        {
            let mut guard = self.cache.write().await;
            guard.cur_bytes += cache_entry.file_metadata.file_size;
            let evicted_entries = guard._insert_non_evictable(
                result_data_file,
                cache_entry_wrapper,
                self.config.max_bytes,
            );
            Ok((cache_entry, evicted_entries))
        }
    }

    async fn _unreference(&mut self, data_file: &MooncakeDataFileRef) {
        let mut guard = self.cache.write().await;
        let cache_entry_wrapper = guard.non_evictable_cache.get_mut(data_file);
        assert!(cache_entry_wrapper.is_some());
        let cache_entry_wrapper = cache_entry_wrapper.unwrap();
        cache_entry_wrapper.reference_count -= 1;

        // Down-level to evictable if reference count goes away.
        if cache_entry_wrapper.reference_count == 0 {
            let cache_entry_wrapper = guard.non_evictable_cache.remove(data_file).unwrap();
            guard
                .evictable_cache
                .push(data_file.clone(), cache_entry_wrapper);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::create_data_file;
    use crate::storage::storage_utils::FileId;

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
        let (cache_entry, cache_to_delete) = cache._get_cache_entry(&data_file_1).await.unwrap();
        check_file_content(&cache_entry.cache_filepath).await;
        assert_eq!(cache_entry.file_metadata.file_size as usize, CONTENT.len());
        assert!(cache_to_delete.is_empty());
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-2: get the cached file, and increment one additional reference count.
        let (cache_entry, cache_to_delete) = cache._get_cache_entry(&data_file_1).await.unwrap();
        check_file_content(&cache_entry.cache_filepath).await;
        assert_eq!(cache_entry.file_metadata.file_size as usize, CONTENT.len());
        assert!(cache_to_delete.is_empty());
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-3: decrement reference count to 0, so later we could cache other files.
        cache._unreference(&data_file_1).await;
        cache._unreference(&data_file_1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);

        // Operation-4: now cache file 1 is not referenced any more, and we could add import new data entries.
        let test_data_file_2 = create_test_file_2(&tmp_dir).await;
        let data_file_2 = Arc::new(MooncakeDataFile {
            file_id: FileId(1),
            file_path: test_data_file_2.to_str().unwrap().to_string(),
            cache_file_path: Some(FAKE_FILENAME.to_string()),
        });
        let cache_to_delete = cache
            ._add_new_cache_entry(
                data_file_2.clone(),
                FileMetadata {
                    file_size: CONTENT.len() as u64,
                },
                /*evictable=*/ true,
            )
            .await;
        assert_eq!(cache_to_delete.len(), 1);
        tokio::fs::remove_file(&cache_to_delete[0]).await.unwrap();
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);

        // Operation-5: newly imported file is not pinned, so we're able to evict it out.
        let (cache_entry, cache_to_delete) = cache._get_cache_entry(&data_file_1).await.unwrap();
        check_file_content(&cache_entry.cache_filepath).await;
        assert_eq!(cache_entry.file_metadata.file_size as usize, CONTENT.len());
        assert_eq!(cache_to_delete.len(), 1);
        assert!(cache_to_delete[0].ends_with(FAKE_FILENAME));
        check_cache_file_count(&tmp_dir, 1).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-6: _unreference, so we could import new cache files.
        cache._unreference(&data_file_1).await;
        let cache_to_delete = cache
            ._add_new_cache_entry(
                data_file_2.clone(),
                FileMetadata {
                    file_size: CONTENT.len() as u64,
                },
                /*evictable=*/ false,
            )
            .await;
        assert_eq!(cache_to_delete.len(), 1);
        tokio::fs::remove_file(&cache_to_delete[0]).await.unwrap();
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 1);

        // Operation-7: reference all cache entries.
        cache._unreference(&data_file_2).await;
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 1);
        assert_eq!(cache.cache.read().await.non_evictable_cache.len(), 0);
    }
}

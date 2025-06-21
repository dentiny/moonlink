use std::sync::Arc;

use crate::storage::cache::object_storage::base_cache::CacheEntry;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCacheInternal;
use crate::storage::storage_utils::TableUniqueFileId;

use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NonEvictableHandle {
    /// File id for the mooncake table data file.
    pub(crate) file_id: TableUniqueFileId,
    /// Non-evictable cache entry.
    pub(crate) cache_entry: CacheEntry,
    /// Access to cache, used to unreference at drop.
    cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

impl std::fmt::Debug for NonEvictableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonEvictableHandle")
            .field("file_id", &self.file_id)
            .field("cache_entry", &self.cache_entry)
            .finish()
    }
}

impl NonEvictableHandle {
    pub(super) fn new(
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
        cache: Arc<RwLock<ObjectStorageCacheInternal>>,
    ) -> Self {
        Self {
            file_id,
            cache,
            cache_entry,
        }
    }

    /// Get cache file path.
    pub(crate) fn get_cache_filepath(&self) -> &str {
        &self.cache_entry.cache_filepath
    }

    /// Unreference the pinned cache file.
    #[must_use]
    pub(crate) async fn unreference(&mut self) -> Vec<String> {
        let mut guard = self.cache.write().await;
        guard.unreference(self.file_id)
    }

    /// Unreference and pinned cache file and mark it as deleted.
    #[must_use]
    pub(crate) async fn unreference_and_delete(&mut self) -> Vec<String> {
        let mut guard = self.cache.write().await;

        // Total bytes within cache doesn't change, so current cache entry not evicted.
        let cur_evicted_files = guard.unreference(self.file_id);
        assert!(cur_evicted_files.is_empty());

        // The cache entry could be held elsewhere.
        guard.delete_cache_entry(self.file_id, /*panic_if_non_existent=*/ true)
    }
}

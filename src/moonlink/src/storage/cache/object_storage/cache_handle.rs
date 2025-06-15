use std::sync::Arc;

use crate::storage::cache::object_storage::base_cache::CacheEntry;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCacheInternal;
use crate::storage::storage_utils::FileId;

use tokio::sync::RwLock;

#[allow(dead_code)]
#[derive(Clone)]
pub struct NonEvictableHandle {
    /// File id for the mooncake table data file.
    pub(crate) file_id: FileId,
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
        file_id: FileId,
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
    pub(crate) async fn unreference(&mut self) {
        let mut guard = self.cache.write().await;
        guard.unreference(self.file_id);
    }
}

use std::sync::Arc;

use crate::storage::cache::object_storage::base_cache::CacheEntry;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCacheInternal;
use crate::storage::storage_utils::{FileId, MooncakeDataFileRef};

use tokio::sync::RwLock;

pub struct NonEvictableHandle {
    /// File id for the mooncake table data file.
    pub(crate) data_file: MooncakeDataFileRef,
    /// Non-evictable cache entry.
    pub(crate) cache_entry: CacheEntry,
    /// Access to cache, used to unreference at drop.
    cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

impl std::fmt::Debug for NonEvictableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonEvictableHandle")
            .field("data_file", &self.data_file)
            .field("cache_entry", &self.cache_entry)
            .finish()
    }
}

impl NonEvictableHandle {
    pub(super) fn new(
        data_file: MooncakeDataFileRef,
        cache_entry: CacheEntry,
        cache: Arc<RwLock<ObjectStorageCacheInternal>>,
    ) -> Self {
        Self {
            data_file,
            cache,
            cache_entry,
        }
    }

    /// Unreference the pinned cache file.
    pub(super) async fn _unreference(&mut self) {
        let mut guard = self.cache.write().await;
        guard._unreference(&self.data_file);
    }
}

/// A unified handle for data file cache entries, which represents different states for a data file cache resource.
#[derive(Debug)]
pub enum DataCacheHandle {
    /// Cache file is not managed by data file cache yet.
    UnimportedHandle(String),
    /// Cache file is managed by data file already and at evictable state; should pin before use.
    EvictableHandle,
    /// Cache file is managed by data file already and pinned, could use at any time.
    NonEvictableHandle(NonEvictableHandle),
}

impl DataCacheHandle {
    /// Get cache file path.
    pub fn get_cache_filepath(&self) -> String {
        match self {
            DataCacheHandle::UnimportedHandle(path) => path.clone(),
            DataCacheHandle::NonEvictableHandle(handle) => {
                handle.cache_entry.cache_filepath.clone()
            }
            DataCacheHandle::EvictableHandle => {
                panic!("Cannot get filepath from evictable cache handle")
            }
        }
    }

    /// Get unimported cache file path.
    pub fn get_unimported_cache_path(&self) -> String {
        match self {
            DataCacheHandle::UnimportedHandle(path) => path.clone(),
            _ => panic!("Cannot get filepath from already imported cache handle"),
        }
    }

    /// Unreferenced the pinned cache file.
    pub async fn unreference(&mut self) {
        match self {
            DataCacheHandle::NonEvictableHandle(handle) => {
                handle._unreference().await;
            }
            _ => panic!("Cannot unreference for an unpinned cache handle"),
        }
    }
}

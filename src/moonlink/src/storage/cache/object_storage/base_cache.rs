/// This trait provides the interface for object storage cache, which is used to provides write-through and read-through caching feature for data files.
///
/// At moonlink, object storage cache
/// - leverage local filesystem for on-disk cache
/// - evictable if a cache entry is unreferenced
use async_trait::async_trait;

use crate::storage::cache::object_storage::cache_handle::NonEvictableHandle;
use crate::storage::storage_utils::FileId;
use crate::Result;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    /// Size of the current file.
    pub(crate) file_size: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectStorageCacheConfig {
    /// Max number of bytes for cache entries at local filesystem.
    pub(crate) max_bytes: u64,
    /// Directory to store local cache files.
    pub(crate) cache_directory: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// Cache data file at local filesystem.
    pub(crate) cache_filepath: String,
    /// File metadata.
    pub(crate) file_metadata: FileMetadata,
}

#[allow(dead_code)]
#[async_trait]
pub trait CacheTrait {
    /// Import cache entry to the cache. If there's no enough disk space, panic directly.
    /// Precondition: the file is not managed by cache.
    #[allow(async_fn_in_trait)]
    async fn _import_cache_entry(
        &mut self,
        file_id: FileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, Vec<String>);

    /// Get file entry.
    /// If the requested file entry doesn't exist in cache, an IO operation is performed.
    /// If there's no sufficient disk space, return [`None`].
    #[allow(async_fn_in_trait)]
    async fn _get_cache_entry(
        &mut self,
        file_id: FileId,
        remote_filepath: &str,
    ) -> Result<(
        Option<NonEvictableHandle>,
        Vec<String>, /*files_to_delete*/
    )>;
}

/// This trait provides the interface for object storage cache, which is used to provides write-through and read-through caching feature for data files.
///
/// At moonlink, object storage cache
/// - leverage local filesystem for on-disk cache
/// - evictable if a cache entry is unreferenced
use async_trait::async_trait;

use crate::storage::cache::object_storage::cache_handle::NonEvictableHandle;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    /// Size of the current file.
    pub(crate) file_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// Cache data file at local filesystem.
    pub(crate) cache_filepath: String,
    /// File metadata.
    pub(crate) file_metadata: FileMetadata,
}

#[async_trait]
pub trait CacheTrait {
    /// Import cache entry to the cache. If there's no enough disk space, panic directly.
    /// Precondition: the file is not managed by cache.
    #[must_use]
    #[allow(async_fn_in_trait)]
    async fn import_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, Vec<String>);

    /// Delete cache entry from the cache, which will be evicted immediately at next cache access.
    /// It's required requested file id exists in cache, otherwise panic.
    /// Return evicted files to delete.
    #[must_use]
    #[allow(async_fn_in_trait)]
    async fn delete_cache_entry(&mut self, file_id: TableUniqueFileId) -> Vec<String>;

    /// Similar to [`delete_cache_entry`], but doesn't panic if requested entry doesn't exist.
    #[must_use]
    #[allow(async_fn_in_trait)]
    async fn try_delete_cache_entry(&mut self, file_id: TableUniqueFileId) -> Vec<String>;

    /// Attempt to get a pinned cache file entry.
    ///
    /// If the requested file is already pinned, cache handle will returned immediately without any IO operations.
    /// Otherwise, an IO operation might be performed, depending on whether the corresponding cache entry happens to be alive.
    /// If there's no sufficient disk space, return [`None`].
    #[deny(unused_must_use)]
    #[must_use]
    async fn get_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
    ) -> Result<(
        Option<NonEvictableHandle>,
        Vec<String>, /*files_to_delete*/
    )>;
}

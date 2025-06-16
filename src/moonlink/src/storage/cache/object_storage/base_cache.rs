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
    #[allow(async_fn_in_trait)]
    async fn import_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, Vec<String>);

    /// Attempt to get a pinned cache file entry.
    ///
    /// If the requested file is already pinned, cache handle will returned immediately without any IO operations.
    /// Otherwise, an IO operation might be performed, depending on whether the corresponding cache entry happens to be alive.
    /// If there's no sufficient disk space, return [`None`].
    #[allow(async_fn_in_trait)]
    async fn get_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
    ) -> Result<(
        Option<NonEvictableHandle>,
        Vec<String>, /*files_to_delete*/
    )>;
}

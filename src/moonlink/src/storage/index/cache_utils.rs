use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::storage_utils::{TableId, TableUniqueFileId};
use crate::ObjectStorageCache;

/// Util functions for index integration with cache.
///
/// Import the given file index into cache, and return evicted files to delete.
pub async fn import_file_index_to_cache(
    file_index: &mut GlobalIndex,
    mut object_storage_cache: ObjectStorageCache,
    table_id: TableId,
) -> Vec<String> {
    // Aggregate evicted files to delete.
    let mut evicted_files_to_delete = vec![];

    for cur_index_block in file_index.index_blocks.iter_mut() {
        let table_unique_file_id = TableUniqueFileId {
            table_id,
            file_id: cur_index_block.index_file.file_id(),
        };
        let cache_entry = CacheEntry {
            cache_filepath: cur_index_block.index_file.file_path().clone(),
            file_metadata: FileMetadata {
                file_size: cur_index_block.file_size,
            },
        };
        let (cache_handle, cur_evicted_files) = object_storage_cache
            .import_cache_entry(table_unique_file_id, cache_entry)
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);
        cur_index_block.cache_handle = Some(cache_handle);
    }

    evicted_files_to_delete
}

/// Import the given file indices into cache, and return evicted files to delete.
pub async fn import_file_indices_to_cache(
    file_indices: &mut [GlobalIndex],
    object_storage_cache: ObjectStorageCache,
    table_id: TableId,
) -> Vec<String> {
    // Aggregate evicted files to delete.
    let mut evicted_files_to_delete = vec![];

    for cur_file_index in file_indices.iter_mut() {
        let cur_evicted_files =
            import_file_index_to_cache(cur_file_index, object_storage_cache.clone(), table_id)
                .await;
        evicted_files_to_delete.extend(cur_evicted_files);
    }

    evicted_files_to_delete
}

#[cfg(test)]
mod tests {
    use crate::create_data_file;
    use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;

    use super::*;

    #[tokio::test]
    async fn test_import_index_to_cache() {
        let temp_dir = tempfile::tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);

        // Create first file index.
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(vec![create_data_file(
                /*file_id=*/ 0,
                "a.parquet".to_string(),
            )])
            .set_directory(tempfile::tempdir().unwrap().keep());
        let file_index_1 = builder
            .build_from_flush(/*hash_entries=*/ vec![(1, 0, 0)], /*file_id=*/ 1)
            .await;

        // Create second file index.
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(vec![create_data_file(
                /*file_id=*/ 2,
                "b.parquet".to_string(),
            )])
            .set_directory(tempfile::tempdir().unwrap().keep());
        let file_index_2 = builder
            .build_from_flush(/*hash_entries=*/ vec![(2, 0, 0)], /*file_id=*/ 3)
            .await;

        let mut file_indices = vec![file_index_1, file_index_2];
        import_file_indices_to_cache(&mut file_indices, object_storage_cache.clone(), TableId(0))
            .await;

        // Check both file indices are pinned in cache.
        assert_eq!(
            object_storage_cache
                .cache
                .read()
                .await
                .non_evictable_cache
                .len(),
            2
        );
        assert_eq!(
            object_storage_cache
                .cache
                .read()
                .await
                .evictable_cache
                .len(),
            0
        );
        assert_eq!(
            object_storage_cache
                .cache
                .read()
                .await
                .evicted_entries
                .len(),
            0
        );

        // Check cache handle is assigned to the file indice.
        assert!(file_indices[0].index_blocks[0].cache_handle.is_some());
        assert!(file_indices[1].index_blocks[0].cache_handle.is_some());
    }
}

use crate::storage::mooncake_table::DataCompactionPayload;

/// This file implements util function on compaction and object storage cache.
///
/// Compaction protocol with object storage cache works as follows:
/// - To prevent files used for compaction gets deleted, already referenced files should be pinned again before compaction in the eventloop; no IO operation is involved.
/// - For unreferenced files, they'll be downloaded from remote and pinned at object storage cache at best effort; this happens in the process of compaction within a background thread.
/// - After compaction, all referenced cache handles shall be unpinned.
///
/// Pin all existing pinnned files before compaction, so they're guaranteed to be valid during compaction.
pub(crate) async fn pin_referenced_compaction_payload(
    data_compaction_payload: &DataCompactionPayload,
) {
    for cur_compaction_payload in &data_compaction_payload.disk_files {
        // Pin data files, which have already been pinned.
        if let Some(cache_handle) = &cur_compaction_payload.data_file_cache_handle {
            data_compaction_payload
                .object_storage_cache
                .increment_reference_count(cache_handle)
                .await;
        }

        // Pin puffin blobs, which have already been pinned.
        if let Some(puffin_blob_ref) = &cur_compaction_payload.deletion_vector {
            data_compaction_payload
                .object_storage_cache
                .increment_reference_count(&puffin_blob_ref.puffin_file_cache_handle)
                .await;
        }
    }

    // Pin index blocks, which have already been pinned.
    for cur_file_index in &data_compaction_payload.file_indices {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            // Index block files always live in cache.
            let cache_handle = cur_index_block.cache_handle.as_ref().unwrap();
            data_compaction_payload
                .object_storage_cache
                .increment_reference_count(cache_handle)
                .await;
        }
    }
}

/// Unpin all referenced files after compaction, so they could be evicted and deleted.
/// Return evicted files to delete.
pub(crate) async fn unpin_referenced_compaction_payload(
    data_compaction_payload: &DataCompactionPayload,
) -> Vec<String> {
    let mut evicted_files_to_delete = vec![];

    for cur_compaction_payload in &data_compaction_payload.disk_files {
        // Unpin data files, if already pinnned.
        if let Some(cache_handle) = &cur_compaction_payload.data_file_cache_handle {
            let cur_evicted_files = cache_handle.unreference().await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }

        // Unpin puffin blobs, which have already been pinned.
        if let Some(puffin_blob_ref) = &cur_compaction_payload.deletion_vector {
            let cur_evicted_files = puffin_blob_ref.puffin_file_cache_handle.unreference().await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }
    }

    // Unpin index blocks, which have already been pinned.
    for cur_file_index in &data_compaction_payload.file_indices {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            // Index block files always live in cache.
            let cache_handle = cur_index_block.cache_handle.as_ref().unwrap();
            let cur_evicted_files = cache_handle.unreference().await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }
    }

    evicted_files_to_delete
}

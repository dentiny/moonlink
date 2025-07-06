/// This module contains testing utils for validation.
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::mooncake_table::Snapshot;

use iceberg::io::FileIOBuilder;
use std::collections::HashSet;

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
async fn check_deletion_vector_consistency(disk_file_entry: &DiskFileEntry) {
    if disk_file_entry.puffin_deletion_blob.is_none() {
        assert!(disk_file_entry
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = puffin_utils::load_blob_from_puffin_file(
        local_fileio,
        disk_file_entry
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath(),
    )
    .await
    .unwrap();
    let iceberg_deletion_vector = DeletionVector::deserialize(blob).unwrap();
    let batch_deletion_vector = iceberg_deletion_vector.take_as_batch_delete_vector();
    assert_eq!(batch_deletion_vector, disk_file_entry.batch_deletion_vector);
}

/// Test util function to check deletion vector consistency for the given snapshot.
pub(crate) async fn check_deletion_vector_consistency_for_snapshot(snapshot: &Snapshot) {
    for disk_deletion_vector in snapshot.disk_files.values() {
        check_deletion_vector_consistency(disk_deletion_vector).await;
    }
}

/// Test util functions to check recovered snapshot only contains remote filepaths and they do exist.
pub(crate) async fn validate_recovered_snapshot(snapshot: &Snapshot, warehouse_uri: &str) {
    let warehouse_directory = std::path::PathBuf::from(warehouse_uri);
    let mut data_filepaths: HashSet<String> = HashSet::new();

    // Check data files and their puffin blobs.
    for (cur_disk_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_disk_pathbuf = std::path::PathBuf::from(cur_disk_file.file_path());
        assert!(cur_disk_pathbuf.starts_with(&warehouse_directory));
        assert!(tokio::fs::try_exists(cur_disk_pathbuf).await.unwrap());
        assert!(data_filepaths.insert(cur_disk_file.file_path().clone()));

        if cur_deletion_vector.puffin_deletion_blob.is_none() {
            continue;
        }
        let puffin_filepath = cur_deletion_vector
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath();
        assert!(tokio::fs::try_exists(puffin_filepath).await.unwrap());
    }

    // Check file indices.
    let mut index_referenced_data_filepaths: HashSet<String> = HashSet::new();
    for cur_file_index in snapshot.indices.file_indices.iter() {
        // Check index blocks are imported into the iceberg table.
        // But index blocks are always cached on-disk, so not under warehouse uri.
        for cur_index_block in cur_file_index.index_blocks.iter() {
            let index_pathbuf = std::path::PathBuf::from(&cur_index_block.index_file.file_path());
            assert!(tokio::fs::try_exists(&index_pathbuf).await.unwrap());
        }

        // Check data files referenced by index blocks are imported into iceberg table.
        for cur_data_filepath in cur_file_index.files.iter() {
            let data_file_pathbuf = std::path::PathBuf::from(cur_data_filepath.file_path());
            assert!(data_file_pathbuf.starts_with(&warehouse_directory));
            assert!(tokio::fs::try_exists(&data_file_pathbuf).await.unwrap());
            index_referenced_data_filepaths.insert(cur_data_filepath.file_path().clone());
        }
    }

    assert_eq!(index_referenced_data_filepaths, data_filepaths);
}

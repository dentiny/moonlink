use itertools::Itertools;
/// This module contains test utils for mooncake table access.
use std::collections::HashMap;
use tempfile::TempDir;

use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::{DiskFileEntry, Snapshot};
use crate::storage::storage_utils::{FileId, MooncakeDataFileRef, ProcessedDeletionRecord};
use crate::storage::{MooncakeTable, PuffinBlobRef};

/// Test util function to get disk files for the given mooncake table.
pub(crate) async fn get_disk_files_for_snapshot(
    table: &MooncakeTable,
) -> HashMap<MooncakeDataFileRef, DiskFileEntry> {
    let guard = table.snapshot.read().await;
    guard.current_snapshot.disk_files.clone()
}

/// Test util function to get sorted data file filepaths for the given mooncake table, and assert on expected disk file number.
pub(crate) async fn get_disk_files_for_snapshot_and_assert(
    table: &MooncakeTable,
    expected_file_num: usize,
) -> Vec<String> {
    let guard = table.snapshot.read().await;
    assert_eq!(guard.current_snapshot.disk_files.len(), expected_file_num);
    let data_files = guard
        .current_snapshot
        .disk_files
        .keys()
        .map(|f| f.file_path().to_string())
        .sorted()
        .collect::<Vec<_>>();
    data_files
}

/// Test util to get all index block file ids for the table, and assert there's only one file.
pub(crate) async fn get_only_index_block_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
    is_local: bool,
) -> FileId {
    let mut file_ids = vec![];

    let guard = table.snapshot.read().await;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            assert!(cur_index_block.cache_handle.is_some());
            file_ids.push(cur_index_block.index_file.file_id());
            if is_local {
                assert!(is_local_file(&cur_index_block.index_file, temp_dir));
            } else {
                assert!(is_remote_file(&cur_index_block.index_file, temp_dir));
            }
        }
    }

    assert_eq!(file_ids.len(), 1);
    file_ids[0]
}

/// Test util to get all index block file ids for the table, and assert there's only one file.
pub(crate) async fn get_only_index_block_filepath(table: &MooncakeTable) -> String {
    let mut index_block_filepaths = vec![];

    let guard = table.snapshot.read().await;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            assert!(cur_index_block.cache_handle.is_some());
            index_block_filepaths.push(cur_index_block.index_file.file_path().to_string());
        }
    }

    assert_eq!(index_block_filepaths.len(), 1);
    index_block_filepaths[0].clone()
}

/// Test util to get the only puffin blob ref for the given mooncake snapshot.
pub(crate) fn get_only_puffin_blob_ref_from_snapshot(snapshot: &Snapshot) -> PuffinBlobRef {
    let disk_files = snapshot.disk_files.clone();
    assert_eq!(disk_files.len(), 1);
    disk_files
        .iter()
        .next()
        .unwrap()
        .1
        .puffin_deletion_blob
        .as_ref()
        .unwrap()
        .clone()
}

/// Test util to get the only puffin blob ref for the given mooncake table.
pub(crate) async fn get_only_puffin_blob_ref_from_table(table: &MooncakeTable) -> PuffinBlobRef {
    let guard = table.snapshot.read().await;
    let disk_files = guard.current_snapshot.disk_files.clone();
    assert_eq!(disk_files.len(), 1);
    disk_files
        .iter()
        .next()
        .unwrap()
        .1
        .puffin_deletion_blob
        .as_ref()
        .unwrap()
        .clone()
}

/// Test util to get data files and index block filepaths for the given mooncake table, filepaths returned in alphabetical order.
pub(crate) async fn get_data_files_and_index_block_files(table: &MooncakeTable) -> Vec<String> {
    let mut files = vec![];

    let guard = table.snapshot.read().await;

    // Check and get data files.
    let disk_files = guard.current_snapshot.disk_files.clone();
    for (cur_file, _) in disk_files.iter() {
        files.push(cur_file.file_path().to_string());
    }

    // Check and get index block files.
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            files.push(cur_index_block.index_file.file_path().to_string());
        }
    }

    files.sort();
    files
}

///Test util function to assert there's only one data file in table snapshot, and it indicates remote file.
pub(crate) async fn get_only_remote_data_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> FileId {
    let guard = table.snapshot.read().await;
    let disk_files = &guard.current_snapshot.disk_files;
    assert_eq!(disk_files.len(), 1);
    let data_file = disk_files.iter().next().unwrap().0;
    assert!(is_remote_file(data_file, temp_dir));
    data_file.file_id()
}

/// Test util function to assert there's only one data file in table snapshot, and it indicates local file.
pub(crate) async fn get_only_local_data_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> FileId {
    let guard = table.snapshot.read().await;
    let disk_files = &guard.current_snapshot.disk_files;
    assert_eq!(disk_files.len(), 1);
    let (data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());

    assert_eq!(
        data_file.file_path(),
        &disk_file_entry
            .cache_handle
            .as_ref()
            .unwrap()
            .cache_entry
            .cache_filepath
    );
    assert!(is_local_file(data_file, temp_dir));

    data_file.file_id()
}

/// Test util function to get committed and uncommitted deletion logs states.
pub(crate) async fn get_deletion_logs_for_snapshot(
    table: &MooncakeTable,
) -> (
    Vec<ProcessedDeletionRecord>,
    Vec<Option<ProcessedDeletionRecord>>,
) {
    let guard = table.snapshot.read().await;
    (
        guard.committed_deletion_log.clone(),
        guard.uncommitted_deletion_log.clone(),
    )
}

/// Test util function to get all index block filepaths from the given mooncake table, returned in alphabetic order.
pub(crate) async fn get_index_block_filepaths(table: &MooncakeTable) -> Vec<String> {
    let guard = table.snapshot.read().await;
    let mut index_block_files = vec![];
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.file_path().clone());
        }
    }

    index_block_files.sort();
    index_block_files
}

/// Test util function to get overall file size for all index block files from the given mooncake table.
pub(crate) async fn get_index_block_files_size(table: &MooncakeTable) -> u64 {
    let guard = table.snapshot.read().await;
    let mut index_blocks_file_size = 0;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_blocks_file_size += cur_index_block.file_size;
        }
    }
    index_blocks_file_size
}

/// Test util function to get index block file ids for the given mooncake table.
pub(crate) async fn get_index_block_file_ids(table: &MooncakeTable) -> Vec<FileId> {
    let guard = table.snapshot.read().await;
    let mut index_block_files = vec![];
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.file_id());
        }
    }
    index_block_files
}

/// Test util to get new compacted data file size and file id for the given mooncake table.
/// Assert the file is of local filepath, and there's only one new compacted data file.
pub(crate) async fn get_new_compacted_local_file_size_and_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> (usize, FileId) {
    let disk_files = get_disk_files_for_snapshot(table).await;
    assert_eq!(disk_files.len(), 1);
    let (new_compacted_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(new_compacted_file, temp_dir));
    let new_compacted_data_file_size = disk_file_entry.file_size;
    (new_compacted_data_file_size, new_compacted_file.file_id())
}

/// Test util function to get index block files, and the overall file size.
pub(crate) fn get_index_block_files(
    file_indices: Vec<GlobalIndex>,
) -> (Vec<MooncakeDataFileRef>, u64) {
    let mut index_block_files = vec![];
    let mut overall_file_size = 0;
    for cur_file_index in file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.clone());
            overall_file_size += cur_index_block.file_size;
        }
    }
    (index_block_files, overall_file_size)
}

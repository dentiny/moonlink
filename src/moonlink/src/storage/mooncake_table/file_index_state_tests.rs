/// ====================================
/// State machine for file indices
/// ====================================
///
/// Possible states:
/// (1) No file index
/// (2) No remote, local
/// (3) Remote, local
///
/// Constraint:
/// Only perform index merge when has remote path
///
/// Difference with data files:
/// - File index always sits on-disk
/// - Data file has an extra state: not referenced but not requested to deleted
/// - Current usage include only compaction and index merge; after all usage for file indices, they are requested to delete
/// - File indices won’t be used by both compaction and index merge, so no need to pin before usage
///
/// State transition input:
/// - Import into mooncake snapshot
/// - Persist into iceberg table
/// - Recover from iceberg table
/// - Use file index (i.e. index merge, compaction)
/// - Usage finishes + request to delete
///
/// State machine transfer:
/// Initial state: no file index
/// - No file index + import => no remote, local
/// - No file index + recover => no remote, local
///
/// Initial state: no remote, local
/// - No remote, local + persist => remote, local
///
/// Initial state: Remote, local
/// - Remote, local + use => remote, local
/// - Remote, local + use over + request delete => no file index
///
/// For more details, please refer to https://docs.google.com/document/d/1Q8zJqxwM9Gc5foX2ela8aAbW4bmWV8wBRkDSh86vvAY/edit?usp=sharing
///
/// Most of the state transitions are shared with data files, with only two differences:
/// - no file index + recover => no remote, local
/// - index merge related states
use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

use crate::row::{MoonlinkRow, RowValue};
use crate::storage::mooncake_table::state_test_utils::*;
use crate::table_notify::TableNotify;
use crate::{
    IcebergTableManager, MooncakeTable, ObjectStorageCache, ObjectStorageCacheConfig, TableManager,
};

async fn prepare_test_disk_file_for_recovery(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, object_storage_cache).await;

    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();

    (table, table_notify)
}

/// Test scenario: no file index + recover => remote, local
#[tokio::test]
async fn test_1_recover_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_recovery(&temp_dir, ObjectStorageCache::new(cache_config)).await;
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Now the disk file and deletion vector has been persist into iceberg.
    let object_storage_cache_for_recovery = ObjectStorageCache::default_for_test(&temp_dir);
    let mut iceberg_table_manager_to_recover = IcebergTableManager::new(
        table.metadata.clone(),
        object_storage_cache_for_recovery.clone(),
        get_iceberg_table_config(&temp_dir),
    )
    .unwrap();
    let mooncake_snapshot = iceberg_table_manager_to_recover
        .load_snapshot_from_table()
        .await
        .unwrap();

    // Check data file has been pinned in mooncake table.
    let file_indices = mooncake_snapshot.indices.file_indices.clone();
    assert_eq!(file_indices.len(), 1);
    let (index_block_files, overall_file_size) = get_index_block_files(file_indices);
    assert_eq!(index_block_files.len(), 1);

    // Check cache state.
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                index_block_files[0].file_id()
            ))
            .await,
        1,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .cur_bytes,
        overall_file_size
    );
}

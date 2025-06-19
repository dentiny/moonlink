use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, FileMetadata};
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::test_utils::*;
use crate::storage::mooncake_table::TableConfig as MooncakeTableConfig;
use crate::storage::storage_utils::{FileId, MooncakeDataFileRef, TableId, TableUniqueFileId};
use crate::table_notify::TableNotify;
use crate::{IcebergTableConfig, MooncakeTable, NonEvictableHandle, ObjectStorageCache, ReadState};

/// This module contains util functions for state-based tests.
///
/// Test constant to mimic an infinitely large object storage cache.
pub(super) const INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE: u64 = u64::MAX;
/// Test constant to allow only one file in object storage cache.
pub(super) const ONE_FILE_CACHE_SIZE: u64 = 1 << 20;
/// Iceberg test namespace and table name.
pub(super) const ICEBERG_TEST_NAMESPACE: &str = "namespace";
pub(super) const ICEBERG_TEST_TABLE: &str = "test_table";
/// Test constant for table id.
pub(super) const TEST_TABLE_ID: TableId = TableId(0);
/// File attributes for a fake file.
///
/// File id for the fake file.
pub(super) const FAKE_FILE_ID: TableUniqueFileId = TableUniqueFileId {
    table_id: TEST_TABLE_ID,
    file_id: FileId(100),
};
/// Fake file size.
pub(super) const FAKE_FILE_SIZE: u64 = ONE_FILE_CACHE_SIZE;
/// Fake filename.
pub(super) const FAKE_FILE_NAME: &str = "fake-file-name";

/// Test util function to get unique table file id.
pub(super) fn get_unique_table_file_id(file_id: FileId) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TEST_TABLE_ID,
        file_id,
    }
}

/// Test util function to decide whether a given file is remote file.
pub(super) fn is_remote_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    // Local filesystem directory for iceberg warehouse.
    let mut temp_pathbuf = temp_dir.path().to_path_buf();
    temp_pathbuf.push(ICEBERG_TEST_NAMESPACE); // iceberg namespace
    temp_pathbuf.push(ICEBERG_TEST_TABLE); // iceberg table name

    file.file_path().starts_with(temp_pathbuf.to_str().unwrap())
}

/// Test util function to decide whether a given file is local file.
pub(super) fn is_local_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    !is_remote_file(file, temp_dir)
}

/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(super) async fn drop_read_states(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableNotify>,
) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
        table.sync_read_request(receiver).await;
    }
}
/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(super) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableNotify>,
) -> Vec<String> {
    drop_read_states(read_states, table, receiver).await;
    let (_, _, _, files_to_delete) = table.create_mooncake_snapshot_for_test(receiver).await;
    files_to_delete
}

/// Test util function to get fake file path.
pub(super) fn get_fake_file_path(temp_dir: &TempDir) -> String {
    temp_dir
        .path()
        .join(FAKE_FILE_NAME)
        .to_str()
        .unwrap()
        .to_string()
}

/// Test util function to import a second object storage cache entry.
pub(super) async fn import_fake_cache_entry(
    temp_dir: &TempDir,
    cache: &mut ObjectStorageCache,
) -> NonEvictableHandle {
    // Create a physical fake file, so later evicted files deletion won't fail.
    let fake_filepath = get_fake_file_path(temp_dir);
    let filepath = std::path::PathBuf::from(&fake_filepath);
    tokio::fs::File::create(&filepath).await.unwrap();

    let cache_entry = CacheEntry {
        cache_filepath: fake_filepath,
        file_metadata: FileMetadata {
            file_size: FAKE_FILE_SIZE,
        },
    };
    cache.import_cache_entry(FAKE_FILE_ID, cache_entry).await.0
}

/// Test util function to check only fake file in object storage cache.
pub(super) async fn check_only_fake_file_in_cache(object_storage_cache: &ObjectStorageCache) {
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
            .non_evictable_cache
            .len(),
        1,
    );
    assert_eq!(
        object_storage_cache.get_non_evictable_filenames().await,
        vec![FAKE_FILE_ID]
    );
}

/// Test util function to create mooncake table and table notify for read test.
pub(super) async fn create_mooncake_table_and_notify_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        iceberg_snapshot_new_data_file_count: 0,
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to create mooncake table and table notify for compaction test.
pub(super) async fn create_mooncake_table_and_notify_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        iceberg_snapshot_new_data_file_count: 0,
        // Trigger compaction as long as there're two data files.
        data_compaction_config: DataCompactionConfig {
            data_file_final_size: u64::MAX,
            data_file_to_compact: 2,
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::mooncake_table::table_creation_test_utils::{
    create_delta_table_config, create_test_table_metadata, get_delta_table_config
};
use crate::storage::mooncake_table::table_operation_test_utils::create_local_parquet_file;
use crate::storage::mooncake_table::{
    PersistenceSnapshotDataCompactionPayload, PersistenceSnapshotImportPayload,
    PersistenceSnapshotIndexMergePayload, PersistenceSnapshotPayload,
};
use crate::storage::table::common::table_manager::TableManager;
use crate::storage::table::common::table_manager::{PersistenceFileParams, PersistenceResult};
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::storage::table::deltalake::deltalake_table_manager::DeltalakeTableManager;
use crate::{create_data_file, FileSystemAccessor, ObjectStorageCache};

async fn test_basic_store_and_load_impl(delta_table_config: DeltalakeTableConfig) {
    let temp_dir = TempDir::new().unwrap();
    let table_path = delta_table_config.location.clone();
    let mooncake_table_metadata = create_test_table_metadata(table_path.clone());
    let filesystem_accessor =
        create_filesystem_accessor(delta_table_config.data_accessor_config.clone());
    let mut delta_table_manager = DeltalakeTableManager::new(
        mooncake_table_metadata.clone(),
        Arc::new(ObjectStorageCache::default_for_test(&temp_dir)), // Use independent object storage cache.
        filesystem_accessor.clone(),
        delta_table_config.clone(),
    )
    .await
    .unwrap();

    // ==============================
    // Operation-1: simply sync
    // ==============================
    //
    // Perform persistence operation.
    let flush_lsn = 10;
    let filepath_1 = create_local_parquet_file(&temp_dir).await;
    let filepath_2 = create_local_parquet_file(&temp_dir).await;
    let persistence_payload = PersistenceSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn,
        committed_deletion_logs: HashSet::new(),
        new_table_schema: None,
        import_payload: PersistenceSnapshotImportPayload {
            data_files: vec![
                create_data_file(/*file_id=*/ 0, filepath_1.clone()),
                create_data_file(/*file_id=*/ 1, filepath_2.clone()),
            ],
            new_deletion_vector: HashMap::new(),
            file_indices: Vec::new(),
        },
        index_merge_payload: PersistenceSnapshotIndexMergePayload::default(),
        data_compaction_payload: PersistenceSnapshotDataCompactionPayload::default(),
    };

    let persist_result: PersistenceResult = delta_table_manager
        .sync_snapshot(
            persistence_payload,
            PersistenceFileParams {
                table_auto_incr_ids: 0..2,
            },
        )
        .await
        .unwrap();

    // Check persistence result.
    assert_eq!(persist_result.remote_data_files.len(), 2);

    // Load latest snapshot from delta table.
    let mut reload_mgr = DeltalakeTableManager::new(
        mooncake_table_metadata.clone(),
        Arc::new(ObjectStorageCache::default_for_test(&temp_dir)), // Use independent object storage cache.
        filesystem_accessor.clone(),
        delta_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = reload_mgr.load_snapshot_from_table().await.unwrap();

    // Validate loaded mooncake snapshot.
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.flush_lsn.unwrap(), flush_lsn);

    // ==============================
    // Operation-2: simply remove
    // ==============================
    //
    let flush_lsn = 20;
    let data_file_to_remove = snapshot.disk_files.keys().next().cloned().unwrap();
    let persistence_payload = PersistenceSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn,
        committed_deletion_logs: HashSet::new(),
        new_table_schema: None,
        import_payload: PersistenceSnapshotImportPayload::default(),
        index_merge_payload: PersistenceSnapshotIndexMergePayload::default(),
        data_compaction_payload: PersistenceSnapshotDataCompactionPayload {
            new_data_files_to_import: Vec::new(),
            old_data_files_to_remove: vec![data_file_to_remove],
            new_file_indices_to_import: Vec::new(),
            old_file_indices_to_remove: Vec::new(),
            data_file_records_remap: HashMap::new(),
        },
    };

    let persist_result: PersistenceResult = delta_table_manager
        .sync_snapshot(
            persistence_payload,
            PersistenceFileParams {
                table_auto_incr_ids: 2..4,
            },
        )
        .await
        .unwrap();

    // Check persistence result.
    assert_eq!(persist_result.remote_data_files.len(), 0);

    // Load latest snapshot from delta table.
    let mut reload_mgr = DeltalakeTableManager::new(
        mooncake_table_metadata.clone(),
        Arc::new(ObjectStorageCache::default_for_test(&temp_dir)), // Use independent object storage cache.
        filesystem_accessor.clone(),
        delta_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = reload_mgr.load_snapshot_from_table().await.unwrap();

    // Validate loaded mooncake snapshot.
    assert_eq!(next_file_id, 1);
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), flush_lsn);

    // Drop table and check.
    delta_table_manager.drop_table().await.unwrap();
    // Explicitly drop the file handle to release the reference count within the unix filesystem.
    drop(temp_dir);

    let dir_exists = tokio::fs::try_exists(table_path).await.unwrap();
    assert!(!dir_exists);
}

#[tokio::test]
async fn test_basic_store_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let delta_table_config = get_delta_table_config(&temp_dir);

    test_basic_store_and_load_impl(delta_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_basic_store_and_load_with_s3() {
    let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let delta_table_config = create_delta_table_config(warehouse_uri);

    test_basic_store_and_load_impl(delta_table_config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_basic_store_and_load_with_gcs() {
    let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let delta_table_config = create_delta_table_config(warehouse_uri);

    // Common testing logic.
    test_basic_store_and_load_impl(delta_table_config.clone()).await;
}


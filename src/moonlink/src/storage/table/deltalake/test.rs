use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;

use crate::storage::mooncake_table::table_creation_test_utils::{
    create_test_table_metadata, get_delta_table_config,
};
use crate::storage::mooncake_table::table_operation_test_utils::create_local_parquet_file;
use crate::storage::mooncake_table::{
    PersistenceSnapshotDataCompactionPayload, PersistenceSnapshotImportPayload,
    PersistenceSnapshotIndexMergePayload, PersistenceSnapshotPayload,
};
use crate::storage::table::common::table_manager::{PersistenceFileParams, PersistenceResult};
use crate::storage::table::deltalake::deltalake_table_manager::DeltalakeTableManager;
use crate::{create_data_file, FileSystemAccessor, ObjectStorageCache};

#[tokio::test]
async fn test_basic_store_and_load() {
    const TEST_FLUSH_LSN: u64 = 10;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap().to_string();
    let mooncake_table_metadata = create_test_table_metadata(table_path.clone());
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let delta_table_config = get_delta_table_config(&temp_dir);

    let mut delta_table_manager = DeltalakeTableManager::new(
        mooncake_table_metadata.clone(),
        Arc::new(ObjectStorageCache::default_for_test(&temp_dir)), // Use independent object storage cache.
        filesystem_accessor.clone(),
        delta_table_config.clone(),
    )
    .await
    .unwrap();

    // Perform persistence operation.
    let filepath = create_local_parquet_file(&temp_dir).await;
    let persistence_payload = PersistenceSnapshotPayload {
        uuid: uuid::Uuid::new_v4(),
        flush_lsn: TEST_FLUSH_LSN,
        committed_deletion_logs: HashSet::new(),
        new_table_schema: None,
        import_payload: PersistenceSnapshotImportPayload {
            data_files: vec![create_data_file(0, filepath)],
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
                table_auto_incr_ids: 0..1,
            },
        )
        .await
        .unwrap();

    // Check persistence result.
    assert_eq!(persist_result.remote_data_files.len(), 1);

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
    assert_eq!(snapshot.flush_lsn.unwrap(), TEST_FLUSH_LSN);
}

use super::test_utils::*;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::MooncakeTableConfig;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::ObjectStorageCache;
use crate::TableEventManager;

use iceberg::{Error as IcebergError, ErrorKind};
use tempfile::tempdir;

use std::sync::Arc;

#[tokio::test]
async fn test_iceberg_snapshot_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
            Box::pin(async move {
                Ok((
                    /*next_file_id=*/ 0,
                    MooncakeSnapshot::new(table_metadata_copy),
                ))
            })
        });
    mock_table_manager
        .expect_sync_snapshot()
        .times(1)
        .returning(|_, _| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append rows to trigger mooncake and iceberg snapshot.
    env.append_row(
        /*id=*/ 1, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 10).await;

    // Initiate snapshot and block wait its completion, check whether error status is correctly propagated.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 10).await;
    let res = TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_iceberg_drop_table_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
            Box::pin(async move {
                Ok((
                    /*next_file_id=*/ 0,
                    MooncakeSnapshot::new(table_metadata_copy),
                ))
            })
        });
    mock_table_manager
        .expect_drop_table()
        .times(1)
        .returning(|| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Drop table and block wait its completion, check whether error status is correctly propagated.
    let res = env.drop_table().await;
    assert!(res.is_err());
}

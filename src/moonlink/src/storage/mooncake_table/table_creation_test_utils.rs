/// This module contains table creation tests utils.
use crate::row::IdentityProp as RowIdentity;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::IcebergPersistenceConfig;
use crate::storage::mooncake_table::{MooncakeTableConfig, TableMetadata as MooncakeTableMetadata};
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
use crate::ObjectStorageCache;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

/// Test util function to get iceberg table config.
pub(crate) fn get_iceberg_table_config(temp_dir: &TempDir) -> IcebergTableConfig {
    IcebergTableConfig {
        warehouse_uri: temp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        ..Default::default()
    }
}

/// Test util function to create arrow schema.
pub(crate) fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
    ]))
}

/// Test util function to create mooncake table metadata.
pub(crate) fn create_test_table_metadata(
    local_table_directory: String,
) -> Arc<MooncakeTableMetadata> {
    let config = MooncakeTableConfig::new(local_table_directory.clone());
    create_test_table_metadata_with_config(local_table_directory, config)
}

/// Test util function to create mooncake table metadata with mooncake table config.
pub(crate) fn create_test_table_metadata_with_config(
    local_table_directory: String,
    mooncake_table_config: MooncakeTableConfig,
) -> Arc<MooncakeTableMetadata> {
    Arc::new(MooncakeTableMetadata {
        name: "test_table".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config,
        path: std::path::PathBuf::from(local_table_directory),
        identity: RowIdentity::FullRow,
    })
}

/// Util function to create mooncake table and iceberg table manager; object storage cache will be created internally.
///
/// Iceberg snapshot will be created whenever `create_snapshot` is called.
pub(crate) async fn create_table_and_iceberg_manager(
    temp_dir: &TempDir,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableEvent>) {
    let default_data_compaction_config = DataCompactionConfig::default();
    create_table_and_iceberg_manager_with_data_compaction_config(
        temp_dir,
        default_data_compaction_config,
    )
    .await
}

/// Similar to [`create_table_and_iceberg_manager`], but it takes data compaction config.
pub(crate) async fn create_table_and_iceberg_manager_with_data_compaction_config(
    temp_dir: &TempDir,
    data_compaction_config: DataCompactionConfig,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let object_storage_cache = ObjectStorageCache::default_for_test(temp_dir);
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        ..Default::default()
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        data_compaction_config,
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache.clone(),
    )
    .await
    .unwrap();

    let iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, iceberg_table_manager, notify_rx)
}

/// Test util function to create mooncake table and table notify for compaction test.
pub(crate) async fn create_mooncake_table_and_notify_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        ..Default::default()
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
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

/// Test util function to create mooncake table and table notify for read test.
pub(crate) async fn create_mooncake_table_and_notify_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
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

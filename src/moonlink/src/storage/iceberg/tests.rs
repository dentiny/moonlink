use crate::row::IdentityProp as RowIdentity;
use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::table_manager::PersistenceFileParams;
use crate::storage::iceberg::table_manager::TableManager;
use crate::storage::iceberg::test_utils::*;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::index::MooncakeIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::validation_test_utils::*;
use crate::storage::mooncake_table::IcebergPersistenceConfig;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::{
    IcebergSnapshotDataCompactionPayload, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload,
};
use crate::storage::mooncake_table::{MooncakeTableConfig, TableMetadata as MooncakeTableMetadata};
use crate::storage::storage_utils;
use crate::storage::storage_utils::create_data_file;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RawDeletionRecord;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::MooncakeTable;
use crate::FileSystemAccessor;
use crate::ObjectStorageCache;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIOBuilder;
use parquet::arrow::AsyncArrowWriter;
use tempfile::tempdir;
use tokio::sync::mpsc;

/// Create test batch deletion vector.
fn test_committed_deletion_log_1(
    data_filepath: MooncakeDataFileRef,
) -> HashMap<MooncakeDataFileRef, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::DEFAULT_BATCH_SIZE);
    deletion_vector.delete_row(0);

    HashMap::<MooncakeDataFileRef, BatchDeletionVector>::from([(
        data_filepath.clone(),
        deletion_vector,
    )])
}
fn test_committed_deletion_log_2(
    data_filepath: MooncakeDataFileRef,
) -> HashMap<MooncakeDataFileRef, BatchDeletionVector> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::DEFAULT_BATCH_SIZE);
    deletion_vector.delete_row(1);
    deletion_vector.delete_row(2);

    HashMap::<MooncakeDataFileRef, BatchDeletionVector>::from([(
        data_filepath.clone(),
        deletion_vector,
    )])
}

/// Test util function to create file indices.
/// NOTICE: The util function does write index block file.
fn create_file_index(data_files: Vec<MooncakeDataFileRef>) -> GlobalIndex {
    GlobalIndex {
        files: data_files,
        num_rows: 0,
        hash_bits: 0,
        hash_upper_bits: 0,
        hash_lower_bits: 0,
        seg_id_bits: 0,
        row_id_bits: 0,
        bucket_bits: 0,
        index_blocks: vec![],
    }
}

/// Test util functions to create moonlink rows.
fn test_row_1() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(10),
    ])
}
fn test_row_2() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ])
}
fn test_row_3() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ])
}

/// Test util function to create iceberg table config.
fn create_iceberg_table_config(warehouse_uri: String) -> IcebergTableConfig {
    let filesystem_config = if warehouse_uri.starts_with("s3://") {
        #[cfg(feature = "storage-s3")]
        {
            s3_test_utils::create_s3_filesystem_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-s3"))]
        {
            panic!("S3 support not enabled. Enable `storage-s3` feature.");
        }
    } else if warehouse_uri.starts_with("gs://") {
        #[cfg(feature = "storage-gcs")]
        {
            gcs_test_utils::create_gcs_filesystem_config(&warehouse_uri)
        }
        #[cfg(not(feature = "storage-gcs"))]
        {
            panic!("GCS support not enabled. Enable `storage-gcs` feature.");
        }
    } else {
        FileSystemConfig::FileSystem {
            root_directory: warehouse_uri.clone(),
        }
    };

    IcebergTableConfig {
        warehouse_uri,
        filesystem_config,
        ..Default::default()
    }
}

/// Test util function to write arrow record batch into local file.
async fn write_arrow_record_batch_to_local<P: AsRef<std::path::Path>>(
    path: P,
    schema: Arc<ArrowSchema>,
    batch: &RecordBatch,
) {
    let file = tokio::fs::File::create(&path).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(batch).await.unwrap();
    writer.close().await.unwrap();
}

/// Test util to get file indices filepaths and their corresponding data filepaths.
fn get_file_indices_filepath_and_data_filepaths(
    mooncake_index: &MooncakeIndex,
) -> (
    Vec<String>, /*file indices filepath*/
    Vec<String>, /*data filepaths*/
) {
    let file_indices = &mooncake_index.file_indices;

    let mut data_files: Vec<String> = vec![];
    let mut index_files: Vec<String> = vec![];
    for cur_file_index in file_indices.iter() {
        data_files.extend(
            cur_file_index
                .files
                .iter()
                .map(|cur_file| cur_file.file_path().clone())
                .collect::<Vec<_>>(),
        );
        index_files.extend(
            cur_file_index
                .index_blocks
                .iter()
                .map(|cur_index_block| cur_index_block.index_file.file_path().to_string())
                .collect::<Vec<_>>(),
        );
    }

    (data_files, index_files)
}

/// Test snapshot store and load for different types of catalogs based on the given warehouse.
async fn test_store_and_load_snapshot_impl(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
) {
    let tmp_dir = tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);

    // ==============
    // Step 1
    // ==============
    //
    // At the beginning of the test, there's nothing in table.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    assert!(iceberg_table_manager.persisted_data_files.is_empty());

    // Create arrow schema and table.
    let arrow_schema = create_test_arrow_schema();

    // Write first snapshot to iceberg table (with deletion vector).
    let data_filename_1 = "data-1.parquet";
    let batch_1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap();
    let parquet_path = tmp_dir.path().join(data_filename_1);
    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch_1).await;
    let file_index_1 = create_file_index(vec![data_file_1.clone()]);

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 0,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file_1.clone()],
            new_deletion_vector: test_committed_deletion_log_1(data_file_1.clone()),
            file_indices: vec![file_index_1.clone()],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
    };

    let peristence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 1..2,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, peristence_file_params)
        .await
        .unwrap();

    // ==============
    // Step 2
    // ==============
    //
    // Write second snapshot to iceberg table, with updated deletion vector and new data file.
    let data_filename_2 = "data-2.parquet";
    let batch_2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])), // id column
            Arc::new(StringArray::from(vec!["d", "e", "f"])), // name column
            Arc::new(Int32Array::from(vec![40, 50, 60])), // age column
        ],
    )
    .unwrap();
    let parquet_path = tmp_dir.path().join(data_filename_2);
    let data_file_2 = create_data_file(
        /*file_id=*/ 2,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch_2).await;
    let file_index_2 = create_file_index(vec![data_file_2.clone()]);

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 1,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![data_file_2.clone()],
            new_deletion_vector: test_committed_deletion_log_2(data_file_2.clone()),
            file_indices: vec![file_index_2.clone()],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
    };

    let peristence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 3..4,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, peristence_file_params)
        .await
        .unwrap();

    // Check persisted items in the iceberg table.
    assert_eq!(
        iceberg_table_manager.persisted_data_files.len(),
        2,
        "Persisted items for table manager is {:?}",
        iceberg_table_manager.persisted_data_files
    );
    assert_eq!(iceberg_table_manager.persisted_file_indices.len(), 2);

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io()
        .clone();

    let mut remote_data_files = vec![];
    for (file_id, data_entry) in iceberg_table_manager.persisted_data_files.iter() {
        let file_path = data_entry.data_file.file_path();
        let loaded_arrow_batch = load_arrow_batch(&file_io, file_path).await.unwrap();
        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        assert_eq!(*loaded_arrow_batch.schema_ref(), arrow_schema);
        remote_data_files.push(create_data_file(
            file_id.0,
            data_entry.data_file.file_path().to_string(),
        ));

        // Check second data file and its deletion vector.
        if file_path.ends_with(data_filename_2) {
            assert_eq!(loaded_arrow_batch, batch_2,);
            assert_eq!(deleted_rows, vec![1, 2],);
            continue;
        }

        // Check first data file and its deletion vector.
        assert!(file_path.ends_with(data_filename_1));
        assert_eq!(loaded_arrow_batch, batch_1,);
        assert_eq!(deleted_rows, vec![0],);
    }

    // ==============
    // Step 3
    // ==============
    //
    // Write third snapshot to iceberg table, with file indices to add and remove.
    let merged_file_index = create_file_index(remote_data_files.clone());
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 2,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![merged_file_index.clone()],
            old_file_indices_to_remove: vec![file_index_1.clone(), file_index_2.clone()],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
    };
    let peristence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 4..5,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, peristence_file_params)
        .await
        .unwrap();
    assert_eq!(iceberg_table_manager.persisted_file_indices.len(), 1);

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 2);
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config.warehouse_uri,
        filesystem_accessor.as_ref(),
    )
    .await;

    // Prepare a compacted data file for data file 1 and 2.
    let compacted_data_filename = "compacted-data.parquet";
    let compacted_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 4])), // id column
            Arc::new(StringArray::from(vec!["b", "c", "d"])), // name column
            Arc::new(Int32Array::from(vec![20, 30, 40])), // age column
        ],
    )
    .unwrap();
    let parquet_path = tmp_dir.path().join(compacted_data_filename);
    let compacted_data_file = create_data_file(
        /*file_id=*/ 5,
        parquet_path.to_str().unwrap().to_string(),
    );
    write_arrow_record_batch_to_local(
        parquet_path.as_path(),
        arrow_schema.clone(),
        &compacted_batch,
    )
    .await;
    let compacted_file_index = create_file_index(vec![compacted_data_file.clone()]);

    // ==============
    // Step 4
    // ==============
    //
    // Attempt a fourth snapshot persistence, which goes after data file compaction.
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 3,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![compacted_data_file.clone()],
            old_data_files_to_remove: vec![data_file_1.clone(), data_file_2.clone()],
            new_file_indices_to_import: vec![compacted_file_index.clone()],
            old_file_indices_to_remove: vec![merged_file_index.clone()],
        },
    };
    let peristence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 6..7,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, peristence_file_params)
        .await
        .unwrap();

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 3);
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, batch_deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    // No deletion vector is expected.
    assert!(batch_deletion_vector.puffin_deletion_blob.is_none());
    assert!(batch_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    // Check data file.
    let loaded_arrow_batch = load_arrow_batch(&file_io, data_file.file_path())
        .await
        .unwrap();
    assert_eq!(loaded_arrow_batch, compacted_batch);
    // Check file indices.
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config.warehouse_uri,
        filesystem_accessor.as_ref(),
    )
    .await;

    // ==============
    // Step 5
    // ==============
    //
    // Remove all existing data files and file indices.
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 4,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![compacted_data_file.clone()],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![compacted_file_index.clone()],
        },
    };
    let peristence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 7..8,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, peristence_file_params)
        .await
        .unwrap();

    // Create a new iceberg table manager and check persisted content.
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 4);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_config.warehouse_uri,
        filesystem_accessor.as_ref(),
    )
    .await;
}

/// Basic iceberg snapshot sync and load test via iceberg table manager.
#[tokio::test]
async fn test_sync_snapshots() {
    // Create arrow schema and table.
    let tmp_dir = tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let iceberg_table_config = get_iceberg_table_config(&tmp_dir);
    test_store_and_load_snapshot_impl(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    )
    .await;
}

/// Test iceberg table manager drop table.
#[tokio::test]
async fn test_drop_table() {
    let tmp_dir = tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = IcebergTableConfig {
        warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .unwrap();
    iceberg_table_manager
        .initialize_iceberg_table_for_once()
        .await
        .unwrap();

    // Perform whitebox testing, which assume the table directory `<warehouse>/<namespace>/<table>` on local filesystem , to check whether table are correctly created or dropped.
    let mut table_directory = PathBuf::from(tmp_dir.path());
    table_directory.push(config.namespace.first().unwrap());
    table_directory.push(config.table_name);
    let directory_exists = tokio::fs::try_exists(&table_directory).await.unwrap();
    assert!(directory_exists);

    // Drop table and check directory existence.
    iceberg_table_manager.drop_table().await.unwrap();
    let directory_exists = tokio::fs::try_exists(&table_directory).await.unwrap();
    assert!(!directory_exists);
}

/// Testing scenario: attempt an iceberg snapshot load with no preceding store.
#[tokio::test]
async fn test_empty_snapshot_load() {
    let tmp_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&tmp_dir);
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = create_iceberg_table_config(tmp_dir.path().to_str().unwrap().to_string());

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .unwrap();
    let (next_table_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_table_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    validate_recovered_snapshot(
        &snapshot,
        &config.warehouse_uri,
        filesystem_accessor.as_ref(),
    )
    .await;
}

/// Testing scenario: iceberg snapshot should be loaded only once at recovery, otherwise it panics.
#[tokio::test]
async fn test_snapshot_load_for_multiple_times() {
    let tmp_dir = tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = create_iceberg_table_config(tmp_dir.path().to_str().unwrap().to_string());
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .unwrap();

    iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::runtime::Handle::current().block_on(async {
            iceberg_table_manager
                .load_snapshot_from_table()
                .await
                .unwrap();
        });
    }));
    assert!(result.is_err());
}

/// Testing scenario: create iceberg snapshot for index merge.
#[tokio::test]
async fn test_index_merge_and_create_snapshot() {
    let tmp_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&tmp_dir);
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);

    // File indices merge is triggered as long as there's not only one file indice.
    let file_index_config = FileIndexMergeConfig {
        file_indices_to_merge: 2,
        index_block_final_size: 1000,
    };

    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        // Flush on every commit.
        mem_slice_size: 1,
        snapshot_deletion_record_count: 1000,
        temp_files_directory: tmp_dir.path().to_str().unwrap().to_string(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config,
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 1000,
            new_committed_deletion_log: 1000,
        },
    };

    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "test_table".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: std::path::PathBuf::from(tmp_dir.path().to_str().unwrap().to_string()),
        identity: RowIdentity::FullRow,
    });

    let iceberg_table_config = get_iceberg_table_config(&tmp_dir);
    let (mut mooncake_table, _, mut notify_rx) = create_table_and_iceberg_manager_with_config(
        &tmp_dir,
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    )
    .await;

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_1 = test_row_1();
    mooncake_table.append(row_1.clone()).unwrap();
    mooncake_table.commit(/*lsn=*/ 1);
    mooncake_table.flush(/*lsn=*/ 1).await.unwrap();

    // Append one row and commit/flush, so we have one file indice persisted.
    let row_2 = test_row_2();
    mooncake_table.append(row_2.clone()).unwrap();
    mooncake_table.commit(/*lsn=*/ 2);
    mooncake_table.flush(/*lsn=*/ 2).await.unwrap();

    // Attempt index merge and flush to iceberg table.
    create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(
        &mut mooncake_table,
        &mut notify_rx,
    )
    .await;

    // Create a new iceberg table manager and check states.
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // two data files, one index block file
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 2);
    validate_recovered_snapshot(
        &snapshot,
        tmp_dir.path().to_str().unwrap(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Delete rows after merge, to make sure file indices are serving correctly.
    mooncake_table.delete(row_1.clone(), /*lsn=*/ 3).await;
    mooncake_table.delete(row_2.clone(), /*lsn=*/ 4).await;
    mooncake_table.commit(/*lsn=*/ 5);
    mooncake_table.flush(/*lsn=*/ 5).await.unwrap();
    create_mooncake_and_persist_for_test(&mut mooncake_table, &mut notify_rx).await;
}

/// Testing scenario: attempt an iceberg snapshot when no data file, deletion vector or index files generated.
#[tokio::test]
async fn test_empty_content_snapshot_creation() {
    let tmp_dir = tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&tmp_dir);
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = create_iceberg_table_config(tmp_dir.path().to_str().unwrap().to_string());
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .unwrap();
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 0,
        import_payload: IcebergSnapshotImportPayload {
            data_files: vec![],
            new_deletion_vector: HashMap::new(),
            file_indices: vec![],
        },
        index_merge_payload: IcebergSnapshotIndexMergePayload {
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
        data_compaction_payload: IcebergSnapshotDataCompactionPayload {
            new_data_files_to_import: vec![],
            old_data_files_to_remove: vec![],
            new_file_indices_to_import: vec![],
            old_file_indices_to_remove: vec![],
        },
    };

    let persistence_file_params = PersistenceFileParams {
        table_auto_incr_ids: 0..1,
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload, persistence_file_params)
        .await
        .unwrap();

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&config),
        config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 0);
}

/// Testing scenario: when mooncake snapshot is created periodically, there're no committed deletion logs later than flush LSN.
/// In the test case we shouldn't create iceberg snapshot.
#[tokio::test]
async fn test_create_snapshot_when_no_committed_deletion_log_to_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);
    let schema = create_test_arrow_schema();
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        MooncakeTableConfig::default(),
        ObjectStorageCache::default_for_test(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    table.flush(/*lsn=*/ 10).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Second time snapshot check, committed deletion logs haven't reached flush LSN.
    table.delete(row.clone(), /*lsn=*/ 20).await;
    table.commit(/*lsn=*/ 30);

    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;
    assert!(iceberg_snapshot_payload.is_none());
}

/// Test scenario: iceberg snapshot is requested to skip when creating mooncake snapshot.
#[tokio::test]
async fn test_skip_iceberg_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri);
    let schema = create_test_arrow_schema();
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        MooncakeTableConfig::default(),
        ObjectStorageCache::default_for_test(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Persist data file to local filesystem, so iceberg snapshot should be created, if skip iceberg not specified.
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    table.flush(/*lsn=*/ 10).await.unwrap();

    // Create mooncake snapshot.
    assert!(table.create_snapshot(SnapshotOption {
        force_create: false,
        skip_iceberg_snapshot: true,
        skip_file_indices_merge: false,
        skip_data_file_compaction: false,
    }));
    let (_, iceberg_snapshot_payload, _, _, _) =
        sync_mooncake_snapshot(&mut table, &mut notify_rx).await;
    assert!(iceberg_snapshot_payload.is_none());
}

/// Test scenario: small batch size and large parquet file, which means:
/// 1. all rows live within their own record batch, and potentially their own batch deletion vector.
/// 2. when flushed to on-disk parquet files, they're grouped into one file but different arrow batch records.
/// Fixed issue: https://github.com/Mooncake-Labs/moonlink/issues/343
#[tokio::test]
async fn test_small_batch_size_and_large_parquet_size() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri.clone());
    let schema = create_test_arrow_schema();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: 1,
        disk_slice_parquet_file_size: 1000,
        // Trigger iceberg snapshot as long as there're any commit deletion log.
        persistence_config: IcebergPersistenceConfig {
            new_committed_deletion_log: 1,
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
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Append first row.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();

    // Append second row.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();

    // Commit, flush and create snapshots.
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Delete the second record.
    table.delete(/*row=*/ row_2.clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    table.flush(/*lsn=*/ 3).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    let deletion_vector = snapshot.disk_files.iter().next().unwrap().1.clone();
    assert_eq!(
        deletion_vector.batch_deletion_vector.collect_deleted_rows(),
        vec![1]
    );
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(&snapshot, &warehouse_uri, filesystem_accessor.as_ref()).await;
}

/// Testing scenario: mooncake snapshot and iceberg snapshot doesn't correspond to each other 1-1.
/// In the test case we perform one iceberg snapshot after three mooncake snapshots.
#[tokio::test]
async fn test_async_iceberg_snapshot() {
    let expected_arrow_batch_1 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John"])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_2 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob"])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_3 = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["Cat"])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_for_recovery, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Operation group 1: Append new rows and create mooncake snapshot.
    let row_1 = test_row_1();
    table.append(row_1.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    table.flush(/*lsn=*/ 10).await.unwrap();
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Operation group 2: Append new rows and create mooncake snapshot.
    let row_2 = test_row_2();
    table.append(row_2.clone()).unwrap();
    table.delete(row_1.clone(), /*lsn=*/ 20).await;
    table.commit(/*lsn=*/ 30);
    table.flush(/*lsn=*/ 30).await.unwrap();
    let (_, _, _, _, _) = create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Create iceberg snapshot for the first mooncake snapshot.
    let iceberg_snapshot_result =
        create_iceberg_snapshot(&mut table, iceberg_snapshot_payload, &mut notify_rx).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

    // Load and check iceberg snapshot.
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 10);

    let (data_file_1, deletion_vector_1) = snapshot.disk_files.iter().next().unwrap();
    let actual_arrow_batch = load_arrow_batch(&file_io, data_file_1.file_path())
        .await
        .unwrap();
    assert_eq!(actual_arrow_batch, expected_arrow_batch_1);
    assert!(deletion_vector_1
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    assert!(deletion_vector_1.puffin_deletion_blob.is_none());
    validate_recovered_snapshot(
        &snapshot,
        temp_dir.path().to_str().unwrap(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Operation group 3: Append new rows and create mooncake snapshot.
    let row_3 = test_row_3();
    table.append(row_3.clone()).unwrap();
    table.commit(/*lsn=*/ 40);
    table.flush(/*lsn=*/ 40).await.unwrap();
    let (_, iceberg_snapshot_payload, _, _, _) =
        create_mooncake_snapshot_for_test(&mut table, &mut notify_rx).await;

    // Create iceberg snapshot for the mooncake snapshot.
    let iceberg_snapshot_result =
        create_iceberg_snapshot(&mut table, iceberg_snapshot_payload, &mut notify_rx).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

    // Load and check iceberg snapshot.
    let (_, mut iceberg_table_manager, _) = create_table_and_iceberg_manager(&temp_dir).await;
    let (next_file_id, mut snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 7); // three data files, three index block files, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 3);
    assert_eq!(snapshot.indices.file_indices.len(), 3);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 40);

    validate_recovered_snapshot(
        &snapshot,
        temp_dir.path().to_str().unwrap(),
        filesystem_accessor.as_ref(),
    )
    .await;
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Find the key-value pair, which correspond to old snapshot's only key.
    let mut old_data_file: Option<MooncakeDataFileRef> = None;
    for (cur_data_file, _) in snapshot.disk_files.iter() {
        if cur_data_file.file_path() == data_file_1.file_path() {
            old_data_file = Some(cur_data_file.clone());
            break;
        }
    }
    let old_data_file = old_data_file.unwrap();

    // Left arrow record 2 and 3, both don't have deletion vector.
    let deletion_entry = snapshot.disk_files.remove(&old_data_file).unwrap();
    assert_eq!(
        deletion_entry.batch_deletion_vector.collect_deleted_rows(),
        vec![0]
    );
    let mut arrow_batch_2_persisted = false;
    let mut arrow_batch_3_persisted = false;
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
        assert!(cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());

        let actual_arrow_batch = load_arrow_batch(&file_io, cur_data_file.file_path())
            .await
            .unwrap();
        if actual_arrow_batch == expected_arrow_batch_2 {
            arrow_batch_2_persisted = true;
        } else if actual_arrow_batch == expected_arrow_batch_3 {
            arrow_batch_3_persisted = true;
        }
    }
    assert!(arrow_batch_2_persisted, "Arrow batch 2 is not persisted!");
    assert!(arrow_batch_3_persisted, "Arrow batch 3 is not persisted!");
}

/// Test util function to check the given row doesn't exist in the snapshot indices.
async fn check_row_index_nonexistent(snapshot: &Snapshot, row: &MoonlinkRow) {
    let key = snapshot.metadata.identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot.metadata.identity.extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
        })
        .await;
    assert!(
        locs.is_empty(),
        "Deletion record {:?} exists for row {:?}",
        locs,
        row
    );
}

/// Test util function to check the given row exists in snapshot, and it's on-disk.
async fn check_row_index_on_disk(
    snapshot: &Snapshot,
    row: &MoonlinkRow,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let key = snapshot.metadata.identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot.metadata.identity.extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
        })
        .await;
    assert_eq!(
        locs.len(),
        1,
        "Actual location for row {:?} is {:?}",
        row,
        locs
    );
    match &locs[0] {
        RecordLocation::DiskFile(file_id, _) => {
            let filepath = snapshot
                .disk_files
                .get_key_value(&FileId(file_id.0))
                .as_ref()
                .unwrap()
                .0
                .file_path();
            let exists = filesystem_accessor.object_exists(filepath).await.unwrap();
            assert!(exists, "Data file {:?} doesn't exist", filepath);
        }
        _ => {
            panic!("Unexpected location {:?}", locs[0]);
        }
    }
}

async fn mooncake_table_snapshot_persist_impl(warehouse_uri: String) {
    // For the ease of testing, we use different directories for mooncake table and iceberg warehouse uri.
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();

    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = create_iceberg_table_config(warehouse_uri.clone());
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let schema = create_test_arrow_schema();
    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        identity_property.clone(),
        iceberg_table_config.clone(),
        mooncake_table_config,
        ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor.clone(),
    )
    .await
    .unwrap();
    table.register_table_notify(notify_tx).await;

    // Perform a few table write operations.
    //
    // Operation series 1: append three rows, delete one of them, flush, commit and create snapshot.
    // Expects to see one data file with no deletion vector, because mooncake table handle deletion inline before persistence, and all record batches are dumped into one single data file.
    // The three rows are deleted in three operations series respectively.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row1.clone()).unwrap();
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row2.clone()).unwrap();
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row3.clone()).unwrap();
    // First deletion of row1, which happens in MemSlice.
    table.delete(row1.clone(), /*flush_lsn=*/ 100).await;
    table.commit(/*flush_lsn=*/ 200);
    table.flush(/*flush_lsn=*/ 200).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        ObjectStorageCache::default_for_test(&temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(&snapshot, &warehouse_uri, filesystem_accessor.as_ref()).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        schema.clone(),
        // row2 and row3
        vec![
            Arc::new(Int32Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![10, 50])),
        ],
    )
    .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );
    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "There should be no deletion vector in iceberg table."
    );

    // --------------------------------------
    // Operation series 2: no more additional rows appended, only to delete the first row in the table.
    // Expects to see a new deletion vector, because its corresponding data file has been persisted.
    table.delete(row2.clone(), /*flush_lsn=*/ 200).await;
    table.commit(/*flush_lsn=*/ 300);
    table.flush(/*flush_lsn=*/ 300).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        ObjectStorageCache::default_for_test(&temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    // row1 is deleted in-memory, so file index doesn't track it
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 is deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(&snapshot, &warehouse_uri, filesystem_accessor.as_ref()).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64];
    assert_eq!(deleted_rows, expected_deleted_rows);

    // --------------------------------------
    // Operation series 3: no more additional rows appended, only to delete the last row in the table.
    // Expects to see the existing deletion vector updated, because its corresponding data file has been persisted.
    table.delete(row3.clone(), /*flush_lsn=*/ 300).await;
    table.flush(/*flush_lsn=*/ 400).await.unwrap();
    table.commit(/*flush_lsn=*/ 400);
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        ObjectStorageCache::default_for_test(&temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 and row3 are deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(&snapshot, &warehouse_uri, filesystem_accessor.as_ref()).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64, 1_u64];
    assert_eq!(deleted_rows, expected_deleted_rows);

    // --------------------------------------
    // Operation series 4: append a new row, and don't delete any rows.
    // Expects to see the existing deletion vector unchanged and new data file created.
    let row4 = MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("Tom".as_bytes().to_vec()),
        RowValue::Int32(40),
    ]);
    table.append(row4.clone()).unwrap();
    table.commit(/*flush_lsn=*/ 500);
    table.flush(/*flush_lsn=*/ 500).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        ObjectStorageCache::default_for_test(&temp_dir), // Use separate and fresh new cache for each recovery.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, mut snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 5); // two data file, two index block file, one deletion vector puffin
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(
        snapshot.indices.file_indices.len(),
        2,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row3, filesystem_accessor.as_ref()).await;
    check_row_index_on_disk(&snapshot, &row4, filesystem_accessor.as_ref()).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);

    let (file_in_new_snapshot, _) = snapshot
        .disk_files
        .iter()
        .find(|(path, _)| path.file_path() == loaded_path.file_path())
        .unwrap();
    snapshot.disk_files.remove(&file_in_new_snapshot.file_id());

    // Check new data file is correctly managed by iceberg table with no deletion vector.
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_path().as_str())
        .await
        .unwrap();

    let expected_arrow_batch = RecordBatch::try_new(
        schema.clone(),
        // row4
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["Tom"])),
            Arc::new(Int32Array::from(vec![40])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "The new appended data file should have no deletion vector aside, but actually it contains deletion vector {:?}",
        deleted_rows
    );
}

#[tokio::test]
async fn test_filesystem_sync_snapshots() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    mooncake_table_snapshot_persist_impl(path).await
}

#[tokio::test]
#[cfg(feature = "storage-s3")]
async fn test_object_storage_sync_snapshots_with_s3() {
    let (bucket_name, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    s3_test_utils::create_test_s3_bucket(bucket_name.clone())
        .await
        .unwrap();
    mooncake_table_snapshot_persist_impl(warehouse_uri).await;
    s3_test_utils::delete_test_s3_bucket(bucket_name.clone())
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "storage-gcs")]
async fn test_object_storage_sync_snapshots_with_gcs() {
    let (bucket_name, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    gcs_test_utils::create_test_gcs_bucket(bucket_name.clone())
        .await
        .unwrap();
    mooncake_table_snapshot_persist_impl(warehouse_uri).await;
    gcs_test_utils::delete_test_gcs_bucket(bucket_name.clone())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_drop_table_at_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);

    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let iceberg_table_config = create_iceberg_table_config(path.clone());

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut table = MooncakeTable::new(
        create_test_arrow_schema().as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        PathBuf::from(&path),
        mooncake_table_metadata.identity.clone(),
        iceberg_table_config.clone(),
        mooncake_table_config,
        ObjectStorageCache::default_for_test(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();

    // Create a new iceberg table manager, recovery gets a fresh state.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config.clone(),
    )
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
}

/// Testing scenario: a large number of deletion records are requested to persist to iceberg, thus multiple table auto increment ids are needed.
/// For more details, please refer to https://github.com/Mooncake-Labs/moonlink/issues/640
#[tokio::test]
async fn test_multiple_table_ids_for_deletion_vector() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_config = MooncakeTableConfig {
        // Flush as long as there's new rows appended at commit.
        mem_slice_size: 1,
        // At flush, place each row in a separate parquet file.
        disk_slice_parquet_file_size: 1,
        ..Default::default()
    };
    let mooncake_table_metadata = create_test_table_metadata_with_config(
        temp_dir.path().to_str().unwrap().to_string(),
        mooncake_table_config,
    );
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = get_iceberg_table_config(&temp_dir);
    let schema = create_test_arrow_schema();
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*table_id=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        MooncakeTableConfig::default(),
        ObjectStorageCache::default_for_test(&temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (notify_tx, mut notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    // Create a large number of data files, which is larger than [`NUM_FILES_PER_FLUSH`].
    let target_data_files_num = storage_utils::NUM_FILES_PER_FLUSH + 1;
    let mut all_rows = Vec::with_capacity(target_data_files_num as usize);
    for idx in 0..target_data_files_num {
        let cur_row = MoonlinkRow::new(vec![
            RowValue::Int32(idx as i32),
            RowValue::ByteArray(idx.to_be_bytes().to_vec()),
            RowValue::Int32(idx as i32),
        ]);
        all_rows.push(cur_row.clone());
        table.append(cur_row).unwrap();
        table.commit(/*lsn=*/ idx);
        table.flush(/*lsn=*/ idx).await.unwrap();
    }

    // Create the first mooncake and iceberg snapshot, which include [`target_data_files_num`] number of files.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Delete all rows, which corresponds to large number of deletion vector puffin files.
    for cur_row in all_rows.into_iter() {
        table.delete(cur_row, /*lsn=*/ target_data_files_num).await;
    }
    table.commit(/*lsn=*/ target_data_files_num + 1);
    table
        .flush(/*lsn=*/ target_data_files_num + 1)
        .await
        .unwrap();

    // Create the second mooncake and iceberg snapshot, which include [`target_data_files_num`] number of deletion vector puffin files.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Load snaphot from iceberg table to validate.
    let (_, mut iceberg_table_manager_for_recovery, _) =
        create_table_and_iceberg_manager(&temp_dir).await;
    let (next_file_id, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id as u64, target_data_files_num * 3); // one for data file, one for deletion vector puffin, one for file indices.

    // Check deletion vector puffin files' file id.
    assert_eq!(snapshot.disk_files.len() as u64, target_data_files_num);
    for (_, cur_disk_file_entry) in snapshot.disk_files.iter() {
        assert!(cur_disk_file_entry.puffin_deletion_blob.is_some());
    }
}

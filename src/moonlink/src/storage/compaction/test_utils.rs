use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::puffin::CompressionCodec;
use parquet::arrow::AsyncArrowWriter;

use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
    MOONCAKE_DELETION_VECTOR_NUM_ROWS,
};
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_writer_proxy;
use crate::storage::iceberg::test_utils as iceberg_test_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::PuffinBlobRef;

use std::collections::HashMap;
use std::sync::Arc;

/// Test util function to dump parquet files to local filesystem.
pub(crate) fn create_test_batch_1() -> RecordBatch {
    let arrow_schema = iceberg_test_utils::create_test_arrow_schema();
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap()
}
pub(crate) fn create_test_batch_2() -> RecordBatch {
    let arrow_schema = iceberg_test_utils::create_test_arrow_schema();
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])), // id column
            Arc::new(StringArray::from(vec!["d", "e", "f"])), // name column
            Arc::new(Int32Array::from(vec![40, 50, 60])), // age column
        ],
    )
    .unwrap()
}

/// Test util function to dump arrow record batches to local filesystem.
pub(crate) async fn dump_arrow_record_batches(
    record_batches: Vec<RecordBatch>,
    data_file: MooncakeDataFileRef,
) {
    let write_file = tokio::fs::File::create(data_file.file_path())
        .await
        .unwrap();
    let mut writer = AsyncArrowWriter::try_new(
        write_file,
        iceberg_test_utils::create_test_arrow_schema(),
        /*props=*/ None,
    )
    .unwrap();
    for cur_record_batch in record_batches.iter() {
        writer.write(cur_record_batch).await.unwrap();
    }
    writer.close().await.unwrap();
}

/// Test util functions to dump deletion vector puffin file to local filesystem.
/// Precondition: rows to delete are sorted in ascending order.
pub(crate) async fn dump_deletion_vector_puffin(
    data_file: String,
    puffin_filepath: String,
    batch_deletion_vector: BatchDeletionVector,
) -> PuffinBlobRef {
    let deleted_rows = batch_deletion_vector.collect_deleted_rows();
    let deleted_rows_num = deleted_rows.len();

    let mut iceberg_deletion_vector = DeletionVector::new();
    iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
    let blob_properties = HashMap::from([
        (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
        (
            DELETION_VECTOR_CADINALITY.to_string(),
            deleted_rows_num.to_string(),
        ),
        (
            MOONCAKE_DELETION_VECTOR_NUM_ROWS.to_string(),
            batch_deletion_vector.get_max_rows().to_string(),
        ),
    ]);
    let blob = iceberg_deletion_vector.serialize(blob_properties);
    let blob_size = blob.data().len();
    let mut puffin_writer = puffin_utils::create_puffin_writer(
        &FileIOBuilder::new_fs_io().build().unwrap(),
        &puffin_filepath,
    )
    .await
    .unwrap();
    puffin_writer
        .add(blob, CompressionCodec::None)
        .await
        .unwrap();
    puffin_writer_proxy::get_puffin_metadata_and_close(puffin_writer)
        .await
        .unwrap();

    PuffinBlobRef {
        puffin_filepath,
        start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
        blob_size: blob_size as u32,
    }
}

use crate::storage::mooncake_table::DataCompactionPayload;
use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
use crate::Result;

use arrow_array::RecordBatch;
use iceberg::io::FileIO;
use iceberg::io::FileRead;
use iceberg::Result as IcebergResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc::Receiver;

/// Test util function to load the first arrow batch from the given parquet file.
/// Precondition: caller unit tests persist rows in one arrow record batch and one parquet file.
pub(crate) async fn load_arrow_batch(
    file_io: &FileIO,
    filepath: &str,
) -> IcebergResult<RecordBatch> {
    let input_file = file_io.new_input(filepath)?;
    let input_file_metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let bytes = reader.read(0..input_file_metadata.size).await?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;
    let batch = reader.next().transpose()?.expect("Should have one batch");
    Ok(batch)
}

/// Test util function to block wait a mooncake snapshot and get its result.
pub(crate) async fn get_mooncake_snapshot_result(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableEvent>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    let notification = notify_rx.recv().await.unwrap();
    table.mark_mooncake_snapshot_completed();
    match notification {
        TableEvent::MooncakeTableSnapshotResult {
            lsn,
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
        } => (
            lsn,
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
        ),
        _ => {
            panic!("Expects to receive mooncake snapshot completion notification, but receives others.");
        }
    }
}

/// Test util function to perform a mooncake snapshot, block wait its completion and get its result.
pub(crate) async fn create_mooncake_snapshot(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableEvent>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    assert!(table.create_snapshot(SnapshotOption::default()));
    get_mooncake_snapshot_result(table, notify_rx).await
}

/// Test util function to perform an iceberg snapshot, block wait its completion and gets its result.
pub(crate) async fn create_iceberg_snapshot(
    table: &mut MooncakeTable,
    iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    notify_rx: &mut Receiver<TableEvent>,
) -> Result<IcebergSnapshotResult> {
    table.persist_iceberg_snapshot(iceberg_snapshot_payload.unwrap());
    let notification = notify_rx.recv().await.unwrap();
    match notification {
        TableEvent::IcebergSnapshot {
            iceberg_snapshot_result,
        } => iceberg_snapshot_result,
        _ => {
            panic!(
                "Expects to receive iceberg snapshot completion notification, but receives others."
            )
        }
    }
}

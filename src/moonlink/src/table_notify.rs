use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::FileIndiceMergeResult;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::Result;

/// Completion notifications for mooncake table, including snapshot creation and compaction, etc.
pub(crate) enum TableNotify {
    /// Mooncake snapshot completes.
    MooncakeTableSnapshot {
        /// Mooncake snapshot LSN.
        lsn: u64,
        /// Payload used to create an iceberg snapshot.
        iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
        /// Payload used to trigger an index merge.
        file_indice_merge_payload: Option<FileIndiceMergePayload>,  
    },
    /// Iceberg snapshot completes.
    IcebergSnapshot {
        /// Result for iceberg snapshot.
        iceberg_snapshot_result: Result<IcebergSnapshotResult>,
    },
    /// Index merge completes.
    IndexMerge {
        /// Result for index merge.
        index_merge_result: FileIndiceMergeResult,
    },
}

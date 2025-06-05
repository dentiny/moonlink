use crate::storage::{iceberg::puffin_utils::PuffinBlobRef, mooncake_table::DiskFileDeletionVector};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
/// Items needed for iceberg snapshot.
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::{MooncakeDataFile, MooncakeDataFileRef};
use crate::storage::TableManager;

use std::collections::{HashMap, HashSet};

////////////////////////////
/// Iceberg snapshot payload
////////////////////////////
///
/// Iceberg snapshot payload by write operations.
#[derive(Debug)]
pub struct IcebergSnapshotImportPayload {
    /// New data files to introduce to the iceberg table.
    pub(crate) data_files: Vec<MooncakeDataFileRef>,
    /// Maps from data filepath to its latest deletion vector.
    pub(crate) new_deletion_vector: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    /// New file indices to import.
    pub(crate) file_indices: Vec<MooncakeFileIndex>,
}

/// Iceberg snapshot payload by index merge operations.
#[derive(Debug)]
pub struct IcebergSnapshotIndexMergePayload {
    /// New file indices to import to the iceberg table.
    pub(crate) new_file_indices_to_import: Vec<MooncakeFileIndex>,
    /// Merged file indices to remove from the iceberg table.
    pub(crate) old_file_indices_to_remove: Vec<MooncakeFileIndex>,
}

#[derive(Debug)]
pub struct IcebergSnapshotPayload {
    /// Flush LSN.
    pub(crate) flush_lsn: u64,
    /// Payload by import operations.
    pub(crate) import_payload: IcebergSnapshotImportPayload,
    /// Payload by index merge operations.
    pub(crate) index_merge_payload: IcebergSnapshotIndexMergePayload,
}

////////////////////////////
/// Iceberg snapshot result
////////////////////////////
///
/// Iceberg snapshot import result.
pub struct IcebergSnapshotImportResult {
    /// Persisted data files.
    pub(crate) new_data_files: Vec<MooncakeDataFileRef>,
    /// Persisted puffin blob reference.
    pub(crate) puffin_blob_ref: HashMap<MooncakeDataFileRef, PuffinBlobRef>,
    /// Imported file indices.
    pub(crate) imported_file_indices: Vec<MooncakeFileIndex>,
}

/// Iceberg snapshot index merge result.
pub struct IcebergSnapshotIndexMergeResult {
    /// New file indices to import to the iceberg table.
    pub(crate) new_file_indices_to_import: Vec<MooncakeFileIndex>,
    /// Merged file indices to remove from the iceberg table.
    pub(crate) old_file_indices_to_remove: Vec<MooncakeFileIndex>,
}

pub(crate) struct IcebergSnapshotResult {
    /// Table manager is (1) not `Sync` safe; (2) only used at iceberg snapshot creation, so we `move` it around every snapshot.
    pub(crate) table_manager: Box<dyn TableManager>,
    /// Iceberg flush LSN.
    pub(crate) flush_lsn: u64,
    /// Iceberg import result.
    pub(crate) import_result: IcebergSnapshotImportResult,
    /// Iceberg index merge result.
    #[allow(dead_code)]
    pub(crate) index_merge_result: IcebergSnapshotIndexMergeResult,
}

////////////////////////////
/// Index merge
////////////////////////////
///
#[derive(Clone, Debug)]
pub struct FileIndiceMergePayload {
    /// File indices to merge.
    pub(crate) file_indices: HashSet<GlobalIndex>,
}

#[derive(Clone, Debug)]
pub struct FileIndiceMergeResult {
    /// File indices merged.
    pub(crate) old_file_indices: HashSet<GlobalIndex>,
    /// Merged file indices.
    pub(crate) merged_file_indices: GlobalIndex,
}

////////////////////////////
/// Data file compaction
////////////////////////////
///
#[derive(Clone, Debug)]
pub struct DataFileCompactionPayload {
    /// Data filepath and their deletion vector.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, DiskFileDeletionVector>,
}

#[derive(Clone, Debug)]
pub struct DataFileCompactionResult {
    /// Old data files and their deletion vector, which gets merged and should not appear in later snapshots.
    pub(crate) old_disk_files: HashMap<MooncakeDataFileRef, DiskFileDeletionVector>,
    /// New data files and their deletion vector after compaction.
    pub(crate) new_disk_files: HashMap<MooncakeDataFileRef, DiskFileDeletionVector>,
}

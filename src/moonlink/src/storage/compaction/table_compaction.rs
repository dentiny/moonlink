use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RecordLocation;

use std::collections::HashMap;
use std::collections::HashSet;

/// Payload to trigger a compaction operation.
#[derive(Clone, Debug)]
pub(crate) struct DataCompactionPayload {
    /// Maps from data file to their deletion records.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, Option<PuffinBlobRef>>,
    /// File indices to compact and rewrite.
    pub(crate) file_indices: Vec<FileIndex>,
}

/// Entry for compacted data files.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CompactedDataEntry {
    /// Number of rows for the compacted data file.
    pub(crate) num_rows: usize,
    /// Compacted file size.
    pub(crate) file_size: usize,
}

/// Result for a compaction operation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DataCompactionResult {
    /// Data files which get compacted, maps from old record location to new one.
    pub(crate) remapped_data_files: HashMap<RecordLocation, RecordLocation>,
    /// Old compacted data files, which maps to their corresponding compacted data file.
    pub(crate) old_data_files: HashMap<MooncakeDataFileRef, MooncakeDataFileRef>,
    /// New compacted data files.
    pub(crate) new_data_files: HashMap<MooncakeDataFileRef, CompactedDataEntry>,
    /// Old compacted file indices.
    pub(crate) old_file_indices: HashSet<FileIndex>,
    /// New compacted file indices.
    pub(crate) new_file_indices: Vec<FileIndex>,
}

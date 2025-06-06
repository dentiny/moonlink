use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RecordLocation;

use std::collections::HashMap;

/// Payload to trigger a compaction operation.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct CompactionPayload {
    /// Maps from data file to their deletion records.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, Option<PuffinBlobRef>>,
    /// File indices to compact and rewrite.
    pub(crate) file_indices: Vec<FileIndex>,
}

/// Result for a compaction operation.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CompactionResult {
    /// Data files which get compacted, maps from old record location to new one.
    pub(crate) remapped_data_files: HashMap<RecordLocation, RecordLocation>,
    /// Compacted data files.
    pub(crate) data_files: Vec<MooncakeDataFileRef>,
    /// Compacted file indices.
    pub(crate) file_indices: Vec<FileIndex>,
}

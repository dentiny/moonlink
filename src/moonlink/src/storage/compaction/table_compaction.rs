use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::storage_utils::RecordLocation;

use std::collections::HashMap;

/// Payload to trigger a compaction operation.
#[allow(dead_code)]
pub(crate) struct CompactionPayload {
    /// Maps from data file to their deletion records.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, Option<PuffinBlobRef>>,
}

/// Result for a compaction operation.
#[allow(dead_code)]
pub(crate) struct CompactionResult {
    /// Data files which get compacted, maps from old record location to new one.
    pub(crate) remapped_data_files: HashMap<RecordLocation, RecordLocation>,
}

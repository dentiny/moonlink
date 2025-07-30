use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// Configurations for data compaction.
///
/// TODO(hjiang): To reduce code change before preview release, disable data compaction by default until we do further testing to make sure moonlink fine.
#[derive(Clone, Debug, PartialEq, TypedBuilder, Deserialize, Serialize)]
pub struct DataCompactionConfig {
    /// Number of existing data files with deletion vector and under final size to trigger a compaction operation.
    pub data_file_to_compact: u32,
    /// Number of bytes for a block index to consider it finalized and won't be merged again.
    pub data_file_final_size: u64,
}

impl DataCompactionConfig {
    #[cfg(debug_assertions)]
    pub const DEFAULT_DATA_FILE_TO_COMPACT: u32 = u32::MAX;
    #[cfg(debug_assertions)]
    pub const DEFAULT_DATA_FILE_FINAL_SIZE: u64 = u64::MAX;

    #[cfg(not(debug_assertions))]
    pub const DEFAULT_DATA_FILE_TO_COMPACT: u32 = u32::MAX;
    #[cfg(not(debug_assertions))]
    pub const DEFAULT_DATA_FILE_FINAL_SIZE: u64 = u64::MAX;
}

impl Default for DataCompactionConfig {
    fn default() -> Self {
        Self {
            data_file_to_compact: Self::DEFAULT_DATA_FILE_TO_COMPACT,
            data_file_final_size: Self::DEFAULT_DATA_FILE_FINAL_SIZE,
        }
    }
}

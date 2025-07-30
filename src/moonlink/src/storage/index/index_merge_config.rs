use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use more_asserts as ma;

/// Configuration for index merge.
///
/// TODO(hjiang): To reduce code change before preview release, disable data compaction by default until we do further testing to make sure moonlink fine.
#[derive(Clone, Debug, PartialEq, TypedBuilder, Deserialize, Serialize)]
pub struct FileIndexMergeConfig {
    /// Number of existing index blocks under final size to trigger a merge operation.
    pub min_file_indices_to_merge: u32,
    /// Max number of existing index blocks in one merge operation.
    pub max_file_indices_to_merge: u32,
    /// Number of bytes for a block index to consider it finalized and won't be merged again.
    pub index_block_final_size: u64,
}

impl FileIndexMergeConfig {
    #[cfg(test)]
    pub const DEFAULT_MIN_FILE_INDICES_TO_MERGE: u32 = u32::MAX;
    #[cfg(test)]
    pub const DEFAULT_MAX_FILE_INDICES_TO_MERGE: u32 = u32::MAX;
    #[cfg(test)]
    pub const DEFAULT_INDEX_BLOCK_FINAL_SIZE: u64 = u64::MAX;

    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_MIN_FILE_INDICES_TO_MERGE: u32 = 4;
    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_MAX_FILE_INDICES_TO_MERGE: u32 = 8;
    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_INDEX_BLOCK_FINAL_SIZE: u64 = 1 << 10; // 1KiB

    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_MIN_FILE_INDICES_TO_MERGE: u32 = 16;
    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_MAX_FILE_INDICES_TO_MERGE: u32 = 32;
    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_INDEX_BLOCK_FINAL_SIZE: u64 = 1 << 29; // 512MiB

    pub fn validate(&self) {
        ma::assert_le!(self.min_file_indices_to_merge, self.max_file_indices_to_merge);
    }
}

impl Default for FileIndexMergeConfig {
    fn default() -> Self {
        Self {
            min_file_indices_to_merge: Self::DEFAULT_MIN_FILE_INDICES_TO_MERGE,
            max_file_indices_to_merge: Self::DEFAULT_MIN_FILE_INDICES_TO_MERGE,
            index_block_final_size: Self::DEFAULT_INDEX_BLOCK_FINAL_SIZE,
        }
    }
}

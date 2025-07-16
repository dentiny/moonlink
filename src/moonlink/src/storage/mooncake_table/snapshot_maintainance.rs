use std::collections::HashSet;

/// This file contains maintaince related features for mooncake snapshot.
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, MaintainanceOption,
};
use crate::storage::storage_utils::{TableId, TableUniqueFileId};

impl SnapshotTableState {
    /// Util function to decide whether and what to compact data files.
    /// To simplify states (aka, avoid data compaction already in iceberg with those not), only merge those already persisted.
    pub(super) fn get_payload_to_compact(
        &self,
        data_compaction_option: &MaintainanceOption,
    ) -> Option<DataCompactionPayload> {
        let data_compaction_file_num_threshold = match data_compaction_option {
            MaintainanceOption::Skip => usize::MAX,
            MaintainanceOption::ForceRegular => 2,
            MaintainanceOption::ForceFull => 2,
            MaintainanceOption::BestEffort => {
                self.mooncake_table_metadata
                    .config
                    .data_compaction_config
                    .data_file_to_compact as usize
            }
        };
        let default_final_file_size = self
            .mooncake_table_metadata
            .config
            .data_compaction_config
            .data_file_final_size as usize;
        let data_compaction_file_size_threshold = match data_compaction_option {
            MaintainanceOption::Skip => usize::MAX,
            MaintainanceOption::ForceRegular => default_final_file_size,
            MaintainanceOption::ForceFull => 0,
            MaintainanceOption::BestEffort => default_final_file_size,
        };

        // Fast-path: not enough data files to trigger compaction.
        let all_disk_files = &self.current_snapshot.disk_files;
        if all_disk_files.len() < data_compaction_file_num_threshold {
            return None;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_data_files = self.unpersisted_records.get_unpersisted_data_files_set();
        let mut tentative_data_files_to_compact = vec![];

        // TODO(hjiang): We should be able to early exit, if left items are not enough to reach the compaction threshold.
        for (cur_data_file, disk_file_entry) in all_disk_files.iter() {
            // Doesn't compact those unpersisted files.
            if unpersisted_data_files.contains(cur_data_file) {
                continue;
            }

            // Skip compaction if the file size exceeds threshold, AND it has no persisted deletion vectors.
            if disk_file_entry.file_size >= data_compaction_file_size_threshold
                && disk_file_entry.batch_deletion_vector.is_empty()
            {
                continue;
            }

            // Tentatively decide data file to compact.
            let single_file_to_compact = SingleFileToCompact {
                file_id: TableUniqueFileId {
                    table_id: TableId(self.mooncake_table_metadata.table_id),
                    file_id: cur_data_file.file_id(),
                },
                filepath: cur_data_file.file_path().to_string(),
                deletion_vector: disk_file_entry.puffin_deletion_blob.clone(),
            };
            tentative_data_files_to_compact.push(single_file_to_compact);
        }

        if tentative_data_files_to_compact.len() < data_compaction_file_num_threshold {
            return None;
        }

        // Calculate related file indices to compact.
        let mut file_indices_to_compact = vec![];
        let file_ids_to_compact = tentative_data_files_to_compact
            .iter()
            .map(|single_file_to_compact| single_file_to_compact.file_id.file_id)
            .collect::<HashSet<_>>();
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_file in cur_file_index.files.iter() {
                if file_ids_to_compact.contains(&cur_file.file_id()) {
                    file_indices_to_compact.push(cur_file_index.clone());
                    break;
                }
            }
        }

        Some(DataCompactionPayload {
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            disk_files: tentative_data_files_to_compact,
            file_indices: file_indices_to_compact,
        })
    }

    /// Util function to decide whether and what to merge index.
    /// To simplify states (aka, avoid merging file indices already in iceberg with those not), only merge those already persisted.
    #[allow(clippy::mutable_key_type)]
    pub(super) fn get_file_indices_to_merge(
        &self,
        index_merge_option: &MaintainanceOption,
    ) -> Option<FileIndiceMergePayload> {
        let index_merge_file_num_threshold = match index_merge_option {
            MaintainanceOption::Skip => usize::MAX,
            MaintainanceOption::ForceRegular => 2,
            MaintainanceOption::ForceFull => 2,
            MaintainanceOption::BestEffort => {
                self.mooncake_table_metadata
                    .config
                    .file_index_config
                    .file_indices_to_merge as usize
            }
        };
        let default_final_file_size = self
            .mooncake_table_metadata
            .config
            .file_index_config
            .index_block_final_size;
        let index_merge_file_size_threshold = match index_merge_option {
            MaintainanceOption::Skip => u64::MAX,
            MaintainanceOption::ForceRegular => default_final_file_size,
            MaintainanceOption::ForceFull => 0,
            MaintainanceOption::BestEffort => default_final_file_size,
        };

        // Fast-path: not enough file indices to trigger index merge.
        let mut file_indices_to_merge = HashSet::new();
        let all_file_indices = &self.current_snapshot.indices.file_indices;
        if all_file_indices.len() < index_merge_file_num_threshold {
            return None;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_file_indices = self.unpersisted_records.get_unpersisted_file_indices_set();

        for cur_file_index in all_file_indices.iter() {
            // Don't merge unpersisted file indices.
            if unpersisted_file_indices.contains(cur_file_index) {
                continue;
            }

            if cur_file_index.get_index_blocks_size() >= index_merge_file_size_threshold {
                continue;
            }
            assert!(file_indices_to_merge.insert(cur_file_index.clone()));
        }
        // To avoid too many small IO operations, only attempt an index merge when accumulated small indices exceeds the threshold.
        if file_indices_to_merge.len() >= index_merge_file_num_threshold {
            return Some(FileIndiceMergePayload {
                file_indices: file_indices_to_merge,
            });
        }
        None
    }
}

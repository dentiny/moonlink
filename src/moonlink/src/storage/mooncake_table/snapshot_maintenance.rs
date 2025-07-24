use std::collections::HashSet;

/// This file contains maintaince related features for mooncake snapshot.
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, MaintenanceOption, SnapshotTask,
};
use crate::storage::storage_utils::{ProcessedDeletionRecord, TableId, TableUniqueFileId};
use crate::table_notify::{
    DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus, TableMainenanceStatus,
};

/// Remap single record location after compaction.
/// Return if remap succeeds.
fn remap_record_location_after_compaction(
    deletion_log: &mut ProcessedDeletionRecord,
    task: &mut SnapshotTask,
) -> bool {
    if task.data_compaction_result.is_empty() {
        return false;
    }

    let old_record_location = &deletion_log.pos;
    let remapped_data_files_after_compaction = &mut task.data_compaction_result;
    let new_record_location = remapped_data_files_after_compaction
        .remapped_data_files
        .remove(old_record_location);
    if new_record_location.is_none() {
        return false;
    }
    deletion_log.pos = new_record_location.unwrap().record_location;
    true
}

impl SnapshotTableState {
    /// ===============================
    /// Get maintence payload
    /// ===============================
    ///
    /// Util function to decide whether and what to compact data files.
    /// To simplify states (aka, avoid data compaction already in iceberg with those not), only merge those already persisted.
    pub(super) fn get_payload_to_compact(
        &self,
        data_compaction_option: &MaintenanceOption,
    ) -> DataCompactionMaintenanceStatus {
        if *data_compaction_option == MaintenanceOption::Skip {
            return DataCompactionMaintenanceStatus::Unknown;
        }

        let data_compaction_file_num_threshold = match data_compaction_option {
            MaintenanceOption::Skip => usize::MAX,
            MaintenanceOption::ForceRegular => 2,
            MaintenanceOption::ForceFull => 2,
            MaintenanceOption::BestEffort => {
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
            MaintenanceOption::Skip => 0,
            MaintenanceOption::ForceRegular => default_final_file_size,
            MaintenanceOption::ForceFull => usize::MAX,
            MaintenanceOption::BestEffort => default_final_file_size,
        };

        // Fast-path: not enough data files to trigger compaction.
        let all_disk_files = &self.current_snapshot.disk_files;
        if all_disk_files.len() < data_compaction_file_num_threshold {
            return DataCompactionMaintenanceStatus::Nothing;
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
            return DataCompactionMaintenanceStatus::Nothing;
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

        let payload = DataCompactionPayload {
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            disk_files: tentative_data_files_to_compact,
            file_indices: file_indices_to_compact,
        };
        DataCompactionMaintenanceStatus::Payload(payload)
    }

    /// Util function to decide whether and what to merge index.
    /// To simplify states (aka, avoid merging file indices already in iceberg with those not), only merge those already persisted.
    #[allow(clippy::mutable_key_type)]
    pub(super) fn get_file_indices_to_merge(
        &self,
        index_merge_option: &MaintenanceOption,
    ) -> IndexMergeMaintenanceStatus {
        if *index_merge_option == MaintenanceOption::Skip {
            return IndexMergeMaintenanceStatus::Unknown;
        }
        let index_merge_file_num_threshold = match index_merge_option {
            MaintenanceOption::Skip => usize::MAX,
            MaintenanceOption::ForceRegular => 2,
            MaintenanceOption::ForceFull => 2,
            MaintenanceOption::BestEffort => {
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
            MaintenanceOption::Skip => u64::MAX,
            MaintenanceOption::ForceRegular => default_final_file_size,
            MaintenanceOption::ForceFull => 0,
            MaintenanceOption::BestEffort => default_final_file_size,
        };

        // Fast-path: not enough file indices to trigger index merge.
        let mut file_indices_to_merge = HashSet::new();
        let all_file_indices = &self.current_snapshot.indices.file_indices;
        if all_file_indices.len() < index_merge_file_num_threshold {
            return IndexMergeMaintenanceStatus::Nothing;
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
        if file_indices_to_merge.len() < index_merge_file_num_threshold {
            return IndexMergeMaintenanceStatus::Nothing;
        }
        let payload = FileIndiceMergePayload {
            file_indices: file_indices_to_merge,
        };
        IndexMergeMaintenanceStatus::Payload(payload)
    }

    /// ===============================
    /// Reflect maintenance result
    /// ===============================
    ///
    /// Reflect data compaction results to mooncake snapshot.
    /// Return evicted data files to delete due to data compaction.
    pub(super) async fn update_data_compaction_to_mooncake_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        if task.data_compaction_result.is_empty() {
            return vec![];
        }

        // NOTICE: Update data files before file indices, so when update file indices, data files for new file indices already exist in disk files map.
        let data_compaction_res = task.data_compaction_result.clone();
        let cur_evicted_files = self
            .update_data_files_to_mooncake_snapshot_impl(
                data_compaction_res.old_data_files,
                data_compaction_res.new_data_files,
                data_compaction_res.remapped_data_files,
            )
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        let cur_evicted_files = self
            .update_file_indices_to_mooncake_snapshot_impl(
                data_compaction_res.old_file_indices,
                data_compaction_res.new_file_indices,
            )
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Apply evicted data files to delete within data compaction process.
        evicted_files_to_delete.extend(
            task.data_compaction_result
                .evicted_files_to_delete
                .iter()
                .cloned()
                .to_owned(),
        );

        evicted_files_to_delete
    }

    /// Return evicted files to delete.
    pub(super) async fn update_file_indices_merge_to_mooncake_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
        self.update_file_indices_to_mooncake_snapshot_impl(
            task.index_merge_result.old_file_indices.clone(),
            task.index_merge_result.new_file_indices.clone(),
        )
        .await
    }

    /// Data compaction might delete existing persisted files, which invalidates record locations and requires a record location remap.
    pub(super) fn remap_and_prune_deletion_logs_after_compaction(
        &mut self,
        task: &mut SnapshotTask,
    ) {
        // No need to prune and remap if no compaction happening.
        if task.data_compaction_result.is_empty() {
            return;
        }

        // Remap and prune committed deletion log.
        let mut new_committed_deletion_log = vec![];
        let old_committed_deletion_log = std::mem::take(&mut self.committed_deletion_log);
        for mut cur_deletion_log in old_committed_deletion_log.into_iter() {
            if let Some(file_id) = cur_deletion_log.get_file_id() {
                // Case-1: the deletion log doesn't indicate a compacted data file.
                if !task
                    .data_compaction_result
                    .old_data_files
                    .contains(&file_id)
                {
                    new_committed_deletion_log.push(cur_deletion_log);
                    continue;
                }
                // Case-2: the deletion log exists in the compacted new data file, perform a remap.
                let remap_succ =
                    remap_record_location_after_compaction(&mut cur_deletion_log, task);
                if remap_succ {
                    new_committed_deletion_log.push(cur_deletion_log);
                    continue;
                }
                // Case-3: the deletion log doesn't exist in the compacted new file, directly remove it.
            } else {
                new_committed_deletion_log.push(cur_deletion_log);
            }
        }
        self.committed_deletion_log = new_committed_deletion_log;

        // Remap uncommitted deletion log.
        for cur_deletion_log in &mut self.uncommitted_deletion_log {
            if cur_deletion_log.is_none() {
                continue;
            }
            remap_record_location_after_compaction(cur_deletion_log.as_mut().unwrap(), task);
        }
    }
}

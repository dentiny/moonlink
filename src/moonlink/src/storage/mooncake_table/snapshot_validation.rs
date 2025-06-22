use crate::storage::SnapshotTableState;
#[cfg(test)]
use std::collections::HashSet;

/// Validate snapshot invariants.

impl SnapshotTableState {
    /// Test util functions to assert current snapshot is at a consistent state.
    #[cfg(test)]
    pub(super) fn assert_current_snapshot_consistent(&self) {
        // Check one data file is only pointed by one file index.
        self.assert_file_indices_no_duplicate();
        // Check file ids don't have duplicate.
        self.assert_file_ids_no_duplicate();
        // Check persistence buffer.
        self.unpersisted_records.validate_invariants();
    }

    /// Test util function to validate one data file is referenced by exactly one file index.
    #[cfg(test)]
    fn assert_file_indices_no_duplicate(&self) {
        // Get referenced data files by file indices.
        let mut referenced_data_files = HashSet::new();
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_data_file in cur_file_index.files.iter() {
                assert!(referenced_data_files.insert(cur_data_file.file_id()));
            }
        }

        // Get all data files, and assert they're equal.
        let data_files = self
            .current_snapshot
            .disk_files
            .keys()
            .map(|f| f.file_id())
            .collect::<HashSet<_>>();
        assert_eq!(data_files, referenced_data_files);
    }

    /// Test util function to validate file ids don't have duplicates.
    #[cfg(test)]
    fn assert_file_ids_no_duplicate(&self) {
        let mut file_ids = HashSet::new();
        for (cur_data_file, cur_disk_file_entry) in self.current_snapshot.disk_files.iter() {
            assert!(file_ids.insert(cur_data_file.file_id()));
            if let Some(puffin_blob_file) = &cur_disk_file_entry.puffin_deletion_blob {
                assert!(file_ids.insert(puffin_blob_file.puffin_file_cache_handle.file_id.file_id));
            }
        }
        for cur_file_index in &self.current_snapshot.indices.file_indices {
            for cur_index_block in &cur_file_index.index_blocks {
                assert!(file_ids.insert(cur_index_block.index_file.file_id()));
            }
        }
    }
}

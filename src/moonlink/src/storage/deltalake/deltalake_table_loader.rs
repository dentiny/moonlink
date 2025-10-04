use std::collections::{HashMap, HashSet};

use deltalake::kernel::Add;
use deltalake::DeltaTable;

use crate::storage::deltalake::deltalake_table_manager::DeltalakeTableManager;
use crate::storage::index::MooncakeIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::{DiskFileEntry, Snapshot as MooncakeSnapshot};
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::{create_data_file, Result};

impl DeltalakeTableManager {
    fn load_data_files(
        adds: Vec<Add>,
        next_file_id: &mut i32,
    ) -> HashMap<MooncakeDataFileRef, DiskFileEntry> {
        let mut disk_files = HashMap::with_capacity(adds.len());
        for cur_add in adds.into_iter() {
            let cur_file_id = *next_file_id;
            *next_file_id += 1;
            let data_file = create_data_file(cur_file_id as u64, cur_add.path.clone());
            let disk_file_entry = DiskFileEntry {
                cache_handle: None,
                num_rows: 0, // TODO(hjiang): Record and recover.
                file_size: cur_add.size as usize,
                committed_deletion_vector: BatchDeletionVector::new(/*max_rows=*/ 0),
                puffin_deletion_blob: None,
            };
            assert!(disk_files.insert(data_file, disk_file_entry).is_none());
        }
        disk_files
    }

    async fn get_flush_lsn(table: &DeltaTable) -> Result<u64> {
        let commit_infos = table.history(/*limit=*/ Some(1)).await?;
        let latest_commit = commit_infos.last().unwrap();
        println!("lastest commit = {:?}", latest_commit);
        Ok(1)
    }

    pub(crate) async fn load_snapshot_from_table_impl(
        &mut self,
    ) -> Result<(u32, MooncakeSnapshot)> {
        assert!(!self.snapshot_loaded);
        self.snapshot_loaded = true;

        // Unique file id to assign to every data file.
        let mut next_file_id = 0;

        // Handle cases where delta table doesn't exist.
        self.initialize_table_if_exists().await?;
        if self.table.is_none() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }
        // TODO(hjiang): Validate schema before operation.

        // Handle cases where no snapshot.
        let table = self.table.as_ref().unwrap();
        let snapshot = table.snapshot();
        if snapshot.is_err() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        let snapshot = snapshot.unwrap();
        let flush_lsn = Self::get_flush_lsn(table).await?;

        let log_store = table.log_store();
        let adds = snapshot.file_actions(&*log_store).await?;
        let disk_files = Self::load_data_files(adds, &mut next_file_id);

        let mooncake_snapshot = MooncakeSnapshot {
            metadata: self.mooncake_table_metadata.clone(),
            disk_files,
            snapshot_version: flush_lsn,
            flush_lsn: Some(flush_lsn),
            largest_flush_lsn: Some(flush_lsn),
            indices: MooncakeIndex {
                in_memory_index: HashSet::new(),
                file_indices: Vec::new(),
            },
        };
        Ok((next_file_id as u32, mooncake_snapshot))
    }
}

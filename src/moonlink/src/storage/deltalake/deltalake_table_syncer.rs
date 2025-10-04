use std::collections::HashMap;

use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Add};
use deltalake::protocol::DeltaOperation;
use serde_json::Value;

use crate::create_data_file;
use crate::error::Result;
use crate::storage::deltalake::deltalake_table_manager::*;
use crate::storage::deltalake::io_utils::upload_data_file_to_delta;
use crate::storage::iceberg::deletion_vector::MOONCAKE_DELETION_VECTOR_NUM_ROWS;
use crate::storage::iceberg::iceberg_table_manager::MOONCAKE_TABLE_FLUSH_LSN;
use crate::storage::iceberg::parquet_utils;
use crate::storage::iceberg::table_manager::PersistenceFileParams;
use crate::storage::iceberg::table_manager::PersistenceResult;
use crate::storage::mooncake_table::{take_data_files_to_import, PersistenceSnapshotPayload};

impl DeltalakeTableManager {
    pub(crate) async fn sync_snapshot_impl(
        &mut self,
        mut snapshot_payload: PersistenceSnapshotPayload,
        _file_params: PersistenceFileParams,
    ) -> Result<PersistenceResult> {
        let new_data_files = take_data_files_to_import(&mut snapshot_payload);
        let mut new_remote_data_files = Vec::new();
        let mut delta_actions = Vec::new();

        // Upload new data files under the given location.
        for cur_local_data_file in new_data_files.into_iter() {
            let (_parquet_metadata, file_size) =
                parquet_utils::get_parquet_metadata(&cur_local_data_file.file_path()).await?;
            let remote_filepath = upload_data_file_to_delta(
                &self.table,
                &cur_local_data_file.file_path,
                &*self.filesystem_accessor,
            )
            .await?;
            new_remote_data_files.push(create_data_file(
                cur_local_data_file.file_id().0,
                remote_filepath.clone(),
            ));
            let data_file_entry = DataFileEntry {
                remote_filepath: remote_filepath.clone(),
            };
            assert!(self
                .persisted_data_files
                .insert(cur_local_data_file.file_id(), data_file_entry)
                .is_none());
            let add_action = Add {
                path: remote_filepath.clone(),
                size: file_size as i64,
                data_change: true,
                ..Default::default() // TODO(hjiang): Add additional stats, like min/max, row count, etc.
            };
            delta_actions.push(Action::Add(add_action));
        }

        // Record remote filepath to delta table.
        let write_op = DeltaOperation::Write {
            mode: deltalake::protocol::SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        // TODO(hjiang): Add retry attempts.
        let app_metadata = HashMap::<String, Value>::from([(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            serde_json::from_str(&snapshot_payload.flush_lsn.to_string()).unwrap(),
        )]);
        CommitBuilder::default()
            .with_actions(delta_actions)
            .with_app_metadata(app_metadata)
            .build(
                Some(self.table.snapshot()?),
                self.table.log_store().clone(),
                write_op,
            )
            .await?;

        let persistence_result = PersistenceResult {
            remote_data_files: new_remote_data_files,
            remote_file_indices: Vec::new(),
            puffin_blob_ref: HashMap::new(),
            evicted_files_to_delete: Vec::new(),
        };
        return Ok(persistence_result);
    }
}

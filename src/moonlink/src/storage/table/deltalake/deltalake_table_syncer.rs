use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Add, Remove};
use deltalake::protocol::DeltaOperation;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::Value;

use crate::create_data_file;
use crate::error::{Error, Result};
use crate::storage::mooncake_table::{
    take_data_files_to_import, take_data_files_to_remove, PersistenceSnapshotPayload,
};
use crate::storage::storage_utils::MooncakeDataFileRef;
use crate::storage::table::common::table_manager::PersistenceFileParams;
use crate::storage::table::common::table_manager::PersistenceResult;
use crate::storage::table::common::MOONCAKE_TABLE_FLUSH_LSN;
use crate::storage::table::deltalake::io_utils::upload_data_file_to_delta;
use crate::storage::table::deltalake::parquet_utils::collect_parquet_stats;
use crate::storage::table::deltalake::{deltalake_table_manager::*, utils};
use crate::storage::table::iceberg::parquet_utils;

/// Max retry attempts count.
const DEFAULT_MAX_RETRY_COUNT: usize = 5;
/// Default data file upload concurrency.
const DEFAULT_DATA_FILE_UPLOAD_CONCURRENCY: usize = 128;

struct UploadedDataFile {
    remote_data_file: MooncakeDataFileRef,
    add_action: Action,
}

impl DeltalakeTableManager {
    // Validate data files to add don't belong to iceberg snapshot.
    fn validate_new_data_files(&self, new_data_files: &[MooncakeDataFileRef]) -> Result<()> {
        for cur_data_file in new_data_files.iter() {
            if self
                .persisted_data_files
                .contains_key(&cur_data_file.file_id())
            {
                return Err(Error::delta_generic_error(format!(
                    "Data file to add {cur_data_file:?} already persisted in iceberg."
                )));
            }
        }
        Ok(())
    }

    // Validate data files to remove don't belong to iceberg snapshot.
    fn validate_old_data_files(&self, old_data_files: &[MooncakeDataFileRef]) -> Result<()> {
        for cur_data_file in old_data_files.iter() {
            if !self
                .persisted_data_files
                .contains_key(&cur_data_file.file_id())
            {
                return Err(Error::delta_generic_error(format!(
                    "Data file to remove {cur_data_file:?} is not persisted in iceberg."
                )));
            }
        }
        Ok(())
    }

    #[allow(unused)]
    pub(crate) async fn sync_snapshot_impl(
        &mut self,
        mut snapshot_payload: PersistenceSnapshotPayload,
        _file_params: PersistenceFileParams,
    ) -> Result<PersistenceResult> {
        let table = utils::get_or_create_deltalake_table(
            self.mooncake_table_metadata.clone(),
            self.object_storage_cache.clone(),
            self.filesystem_accessor.clone(),
            self.config.clone(),
        )
        .await?;
        self.table = Some(table.clone());
        let filesystem_accessor = self.filesystem_accessor.clone();

        let new_data_files = take_data_files_to_import(&mut snapshot_payload);
        let old_data_files = take_data_files_to_remove(&mut snapshot_payload);

        // Validate data files to add and remove are valid.
        self.validate_new_data_files(&new_data_files)?;
        self.validate_old_data_files(&old_data_files)?;

        let uploaded_files = stream::iter(new_data_files.into_iter())
            .map(|cur_local_data_file| {
                let table = table.clone();
                let fs_accessor = filesystem_accessor.clone();

                async move {
                    let (parquet_metadata, file_size) =
                        parquet_utils::get_parquet_metadata(cur_local_data_file.file_path())
                            .await?;
                    let file_stats = collect_parquet_stats(&parquet_metadata)?;

                    let remote_filepath = upload_data_file_to_delta(
                        &table,
                        &cur_local_data_file.file_path,
                        &*fs_accessor,
                    )
                    .await?;

                    let data_file =
                        create_data_file(cur_local_data_file.file_id().0, remote_filepath.clone());

                    let add_action = Add {
                        path: remote_filepath.clone(),
                        size: file_size as i64,
                        data_change: true,
                        stats: Some(serde_json::to_string(&file_stats).unwrap()),
                        ..Default::default()
                    };

                    Ok::<UploadedDataFile, Error>(UploadedDataFile {
                        remote_data_file: create_data_file(
                            cur_local_data_file.file_id().0,
                            remote_filepath,
                        ),
                        add_action: Action::Add(add_action),
                    })
                }
            })
            .buffer_unordered(DEFAULT_DATA_FILE_UPLOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        // Aggregate results.
        let mut new_remote_data_files = Vec::new();
        let mut delta_actions = Vec::new();

        // Reflect add actions.
        for cur_file in uploaded_files {
            assert!(self
                .persisted_data_files
                .insert(
                    cur_file.remote_data_file.file_id,
                    DataFileEntry {
                        remote_filepath: cur_file.remote_data_file.file_path.clone(),
                    }
                )
                .is_none());
            new_remote_data_files.push(cur_file.remote_data_file.clone());
            delta_actions.push(cur_file.add_action);
        }

        // Reflect remove actions.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for cur_old_data_file in old_data_files.into_iter() {
            let cur_remove_action = Remove {
                path: cur_old_data_file.file_path().clone(),
                data_change: false,
                deletion_timestamp: Some(now_ms),
                ..Default::default()
            };
            delta_actions.push(Action::Remove(cur_remove_action));
        }

        // Record remote filepath to delta table.
        let write_op = DeltaOperation::Write {
            mode: deltalake::protocol::SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        let app_metadata = HashMap::<String, Value>::from([(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            serde_json::from_str(&snapshot_payload.flush_lsn.to_string()).unwrap(),
        )]);

        CommitBuilder::default()
            .with_actions(delta_actions)
            .with_app_metadata(app_metadata)
            .with_max_retries(DEFAULT_MAX_RETRY_COUNT)
            .build(
                Some(self.table.as_ref().unwrap().snapshot()?),
                self.table.as_ref().unwrap().log_store().clone(),
                write_op,
            )
            .await?;

        Ok(PersistenceResult {
            remote_data_files: new_remote_data_files,
            remote_file_indices: Vec::new(),
            puffin_blob_ref: HashMap::new(),
            evicted_files_to_delete: Vec::new(),
        })
    }
}

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
#[cfg(any(test, debug_assertions))]
use crate::storage::iceberg::schema_utils;
use crate::storage::iceberg::snapshot_utils;
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::io_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::storage_utils::{create_data_file, FileId, TableId, TableUniqueFileId};
use crate::storage::wal::wal_persistence_metadata::WalPersistenceMetadata;

use std::collections::{HashMap, HashSet};
use std::vec;

use iceberg::io::FileIO;
use iceberg::spec::{DataFileFormat, ManifestEntry};
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;

/// Results for recovering file indices from iceberg table.
struct FileIndicesRecoveryResult {
    /// All file indices recovered from iceberg table.
    file_indices: Vec<MooncakeFileIndex>,
}

impl IcebergTableManager {
    /// Validate schema consistency at load operation.
    fn validate_schema_consistency_at_load(&self) {
        // Validate is expensive, only enable at tests.
        #[cfg(any(test, debug_assertions))]
        {
            // Assert table schema matches iceberg table metadata.
            schema_utils::assert_table_schema_consistent(
                self.iceberg_table.as_ref().unwrap(),
                &self.mooncake_table_metadata,
            );
        }
    }

    /// Load index file into table manager from the current manifest entry.
    async fn load_file_indices_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
        next_file_id: &mut u64,
    ) -> IcebergResult<FileIndicesRecoveryResult> {
        if !utils::is_file_index(entry) {
            return Ok(FileIndicesRecoveryResult {
                file_indices: Vec::new(),
            });
        }

        // Load mooncake file indices from iceberg file index blobs.
        let file_index_blob =
            FileIndexBlob::load_from_index_blob(file_io.clone(), entry.data_file()).await?;
        let new_file_indices_count = file_index_blob.file_indices.len();
        let expected_file_indices_count =
            self.persisted_file_indices.len() + new_file_indices_count;
        self.persisted_file_indices
            .reserve(expected_file_indices_count);
        let mut file_indices = Vec::with_capacity(new_file_indices_count);
        for mut cur_iceberg_file_indice in file_index_blob.file_indices.into_iter() {
            let table_id = TableId(self.mooncake_table_metadata.table_id);
            let cur_mooncake_file_indice = cur_iceberg_file_indice
                .as_mooncake_file_index(
                    &self.remote_data_file_to_file_id,
                    self.object_storage_cache.clone(),
                    self.filesystem_accessor.as_ref(),
                    table_id,
                    next_file_id,
                )
                .await?;
            file_indices.push(cur_mooncake_file_indice.clone());

            self.persisted_file_indices.insert(
                cur_mooncake_file_indice,
                entry.data_file().file_path().to_string(),
            );
        }

        Ok(FileIndicesRecoveryResult { file_indices })
    }

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        next_file_id: &mut u64,
    ) -> IcebergResult<()> {
        if !utils::is_data_file_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        assert_eq!(data_file.file_format(), DataFileFormat::Parquet);
        let new_data_file_entry = DataFileEntry {
            data_file: data_file.clone(),
            deletion_vector: BatchDeletionVector::new(UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW),
            persisted_deletion_vector: None,
        };

        self.persisted_data_files
            .insert(FileId(*next_file_id), new_data_file_entry);
        self.remote_data_file_to_file_id
            .insert(data_file.file_path().to_string(), FileId(*next_file_id));
        *next_file_id += 1;

        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
        next_file_id: &mut u64,
    ) -> IcebergResult<()> {
        // Skip data files and file indices.
        if !utils::is_deletion_vector_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        let referenced_data_file = data_file.referenced_data_file().unwrap();
        let file_id = self
            .remote_data_file_to_file_id
            .get(&referenced_data_file)
            .unwrap();
        let data_file_entry = self.persisted_data_files.get_mut(file_id).unwrap();

        IcebergValidation::validate_puffin_manifest_entry(entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
        data_file_entry.deletion_vector = batch_deletion_vector;

        // Load remote puffin file to local cache and pin.
        let cur_file_id = *next_file_id;
        *next_file_id += 1;
        let unique_file_id = TableUniqueFileId {
            table_id: TableId(self.mooncake_table_metadata.table_id),
            file_id: FileId(cur_file_id),
        };
        let (cache_handle, evicted_files_to_delete) = self
            .object_storage_cache
            .get_cache_entry(
                unique_file_id,
                data_file.file_path(),
                self.filesystem_accessor.as_ref(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to get cache entry for {}: {:?}",
                        data_file.file_path(),
                        e
                    ),
                )
                .with_retryable(true)
            })?;
        io_utils::delete_local_files(&evicted_files_to_delete)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to delete files for {evicted_files_to_delete:?}: {e:?}"),
                )
                .with_retryable(true)
            })?;

        data_file_entry.persisted_deletion_vector = Some(PuffinBlobRef {
            // Deletion vector should be pinned on cache.
            puffin_file_cache_handle: cache_handle.unwrap(),
            start_offset: data_file.content_offset().unwrap() as u32,
            blob_size: data_file.content_size_in_bytes().unwrap() as u32,
        });

        Ok(())
    }

    /// -------- Transformation util functions ---------
    ///
    /// Util function to transform iceberg table status to mooncake table snapshot, assign file id uniquely to all data files.
    fn transform_to_mooncake_snapshot(
        &self,
        persisted_file_indices: Vec<MooncakeFileIndex>,
        flush_lsn: Option<u64>,
        wal_metadata: Option<WalPersistenceMetadata>,
    ) -> MooncakeSnapshot {
        let mut mooncake_snapshot = MooncakeSnapshot::new(self.mooncake_table_metadata.clone());

        // Assign snapshot version.
        let iceberg_table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        mooncake_snapshot.snapshot_version =
            if let Some(ver) = iceberg_table_metadata.current_snapshot_id() {
                ver as u64
            } else {
                0
            };

        // Fill in disk files.
        mooncake_snapshot.disk_files = HashMap::with_capacity(self.persisted_data_files.len());
        for (file_id, data_file_entry) in self.persisted_data_files.iter() {
            let data_file =
                create_data_file(file_id.0, data_file_entry.data_file.file_path().to_string());

            mooncake_snapshot.disk_files.insert(
                data_file,
                DiskFileEntry {
                    file_size: data_file_entry.data_file.file_size_in_bytes() as usize,
                    cache_handle: None,
                    puffin_deletion_blob: data_file_entry.persisted_deletion_vector.clone(),
                    batch_deletion_vector: data_file_entry.deletion_vector.clone(),
                },
            );
        }
        // UNDONE:
        // 1. Update file id in persisted_file_indices.

        // Fill in indices.
        mooncake_snapshot.indices = MooncakeIndex {
            in_memory_index: HashSet::new(),
            file_indices: persisted_file_indices,
        };

        // Fill in flush LSN.
        mooncake_snapshot.data_file_flush_lsn = flush_lsn;
        // Fill in wal persistence metadata.
        mooncake_snapshot.wal_persistence_metadata = wal_metadata;

        mooncake_snapshot
    }

    pub(crate) async fn load_snapshot_from_table_impl(
        &mut self,
    ) -> IcebergResult<(u32, MooncakeSnapshot)> {
        assert!(!self.snapshot_loaded);
        self.snapshot_loaded = true;

        // Unique file id to assign to every data file.
        let mut next_file_id = 0;

        // Handle cases which iceberg table doesn't exist.
        self.initialize_iceberg_table_if_exists().await?;
        if self.iceberg_table.is_none() {
            let empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        // Perform validation before load operation.
        self.validate_schema_consistency_at_load();

        // Load moonlink related metadata.
        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let snapshot_property = snapshot_utils::get_snapshot_properties(table_metadata)?;

        // There's nothing stored in iceberg table.
        if table_metadata.current_snapshot().is_none() {
            let mut empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            empty_mooncake_snapshot.data_file_flush_lsn = snapshot_property.flush_lsn;
            return Ok((next_file_id as u32, empty_mooncake_snapshot));
        }

        // Load table state into iceberg table manager.
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let manifest_list = snapshot_meta
            .load_manifest_list(
                self.iceberg_table.as_ref().unwrap().file_io(),
                table_metadata,
            )
            .await?;

        let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
        let mut loaded_file_indices = vec![];

        // On load, we do two passes on all entries.
        // Data files are loaded first, because we need to get <data file, file id> mapping, which is used for later deletion vector and file indices recovery.
        // Deletion vector puffin and file indices have no dependency, and could be loaded in parallel.
        //
        // Cache manifest file by manifest filepath to avoid repeated IO.
        let mut manifest_file_cache = HashMap::new();

        // Attempt to load data files first.
        for manifest_file in manifest_list.entries().iter() {
            let manifest = manifest_file.load_manifest(&file_io).await?;
            assert!(manifest_file_cache
                .insert(manifest_file.manifest_path.clone(), manifest.clone())
                .is_none());
            let (manifest_entries, _) = manifest.into_parts();
            assert!(!manifest_entries.is_empty());

            // One manifest file only store one type of entities (i.e. data file, deletion vector, file indices).
            if !utils::is_data_file_entry(&manifest_entries[0]) {
                continue;
            }
            for entry in manifest_entries.iter() {
                self.load_data_file_from_manifest_entry(entry.as_ref(), &mut next_file_id)
                    .await?;
            }
        }

        // Attempt to load file indices and deletion vector.
        for manifest_file in manifest_list.entries().iter() {
            let manifest = manifest_file_cache
                .remove(&manifest_file.manifest_path)
                .unwrap();
            let (manifest_entries, _) = manifest.into_parts();
            assert!(!manifest_entries.is_empty());
            if utils::is_data_file_entry(&manifest_entries[0]) {
                continue;
            }

            for entry in manifest_entries.iter() {
                // Load file indices.
                let recovered_file_indices = self
                    .load_file_indices_from_manifest_entry(
                        entry.as_ref(),
                        &file_io,
                        &mut next_file_id,
                    )
                    .await?;
                loaded_file_indices.extend(recovered_file_indices.file_indices);

                // Load deletion vector puffin.
                self.load_deletion_vector_from_manifest_entry(
                    entry.as_ref(),
                    &file_io,
                    &mut next_file_id,
                )
                .await?;
            }
        }

        let mooncake_snapshot = self.transform_to_mooncake_snapshot(
            loaded_file_indices,
            snapshot_property.flush_lsn,
            snapshot_property.wal_persisted_metadata,
        );
        Ok((next_file_id as u32, mooncake_snapshot))
    }
}

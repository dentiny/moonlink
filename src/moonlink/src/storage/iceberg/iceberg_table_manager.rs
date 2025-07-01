use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
    MOONCAKE_DELETION_VECTOR_NUM_ROWS,
};
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::table_manager::{
    PersistenceFileParams, PersistenceResult, TableManager,
};
use crate::storage::iceberg::table_property::{
    self, TABLE_COMMIT_RETRY_MAX_MS_DEFAULT, TABLE_COMMIT_RETRY_MIN_MS_DEFAULT,
};
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::iceberg::{puffin_utils, tokio_retry_utils};
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::mooncake_table::{
    take_data_files_to_import, take_file_indices_to_import, take_file_indices_to_remove,
};
use crate::storage::mooncake_table::{take_data_files_to_remove, DiskFileEntry};
use crate::storage::storage_utils::{
    create_data_file, get_unique_file_id_for_flush, FileId, MooncakeDataFileRef, TableId,
    TableUniqueFileId,
};
use crate::storage::{io_utils, storage_utils};
use crate::ObjectStorageCache;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::{DataFile, DataFileFormat, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::{Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableIdent};
use tokio_retry2::strategy::{jitter, ExponentialBackoff};
use tokio_retry2::{Retry, RetryError};
use typed_builder::TypedBuilder;
use uuid::Uuid;

/// Key for iceberg table property, to record flush lsn.
const MOONCAKE_TABLE_FLUSH_LSN: &str = "mooncake-table-flush-lsn";
/// Used to represent uninitialized deletion vector.
/// TODO(hjiang): Consider using `Option<>` to represent uninitialized, which is more rust-idiometic.
const UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW: usize = 0;

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
pub struct IcebergTableConfig {
    /// Table warehouse location.
    #[builder(default = "/tmp/moonlink_iceberg".to_string())]
    pub warehouse_uri: String,
    /// Namespace for the iceberg table.
    #[builder(default = vec!["default".to_string()])]
    pub namespace: Vec<String>,
    /// Iceberg table name.
    #[builder(default = "table".to_string())]
    pub table_name: String,
}

impl Default for IcebergTableConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    pub(crate) data_file: DataFile,
    /// In-memory deletion vector.
    pub(crate) deletion_vector: BatchDeletionVector,
    /// Puffin blob for its deletion vector.
    persisted_deletion_vector: Option<PuffinBlobRef>,
}

/// Results for importing data files into iceberg table.
struct DataFileImportResult {
    /// New data files to import into iceberg table.
    new_iceberg_data_files: Vec<DataFile>,
    /// Local data file to remote one mapping.
    local_data_files_to_remote: HashMap<String, String>,
    /// New mooncake data files, represented in remote file paths.
    new_remote_data_files: Vec<MooncakeDataFileRef>,
}
/// Results for recovering file indices from iceberg table.
struct FileIndicesRecoveryResult {
    /// All file indices recovered from iceberg table.
    file_indices: Vec<MooncakeFileIndex>,
}

#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg snapshot should be loaded only once at recovery, this boolean records whether recovery has attempted.
    snapshot_loaded: bool,

    /// Iceberg table configuration.
    pub(crate) config: IcebergTableConfig,

    /// Mooncake table metadata.
    pub(crate) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Iceberg catalog, which interacts with the iceberg table.
    pub(crate) catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    pub(crate) iceberg_table: Option<IcebergTable>,

    /// Object storage cache.
    pub(crate) object_storage_cache: ObjectStorageCache,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    pub(crate) persisted_data_files: HashMap<FileId, DataFileEntry>,

    /// Maps from mooncake file index to remote puffin filepath.
    pub(crate) persisted_file_indices: HashMap<MooncakeFileIndex, String>,

    /// Maps from remote data file path to its file id.
    pub(crate) remote_data_file_to_file_id: HashMap<String, FileId>,
}

impl IcebergTableManager {
    pub fn new(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: ObjectStorageCache,
        config: IcebergTableConfig,
    ) -> IcebergResult<IcebergTableManager> {
        let catalog = utils::create_catalog(&config.warehouse_uri)?;
        Ok(Self {
            snapshot_loaded: false,
            config,
            mooncake_table_metadata,
            catalog,
            iceberg_table: None,
            object_storage_cache,
            persisted_data_files: HashMap::new(),
            persisted_file_indices: HashMap::new(),
            remote_data_file_to_file_id: HashMap::new(),
        })
    }

    /// ---------- Util functions ----------
    ///
    /// Get a unique puffin filepath under table warehouse uri.
    fn get_unique_deletion_vector_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-deletion-vector-v1-puffin.bin", Uuid::now_v7()))
    }
    fn get_unique_hash_index_v1_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-hash-index-v1-puffin.bin", Uuid::now_v7()))
    }

    /// Get or create an iceberg table based on the iceberg manager config.
    ///
    /// This function is executed in a lazy style, so no iceberg table will get created if
    /// (1) It doesn't exist before any mooncake table events
    /// (2) Iceberg snapshot is not requested to create
    pub(crate) async fn initialize_iceberg_table_for_once(&mut self) -> IcebergResult<()> {
        if self.iceberg_table.is_none() {
            let table = utils::get_or_create_iceberg_table(
                &*self.catalog,
                &self.config.warehouse_uri,
                &self.config.namespace,
                &self.config.table_name,
                self.mooncake_table_metadata.schema.as_ref(),
            )
            .await?;
            self.iceberg_table = Some(table);
        }
        Ok(())
    }

    /// Initialize table if it exists.
    pub(crate) async fn initialize_iceberg_table_if_exists(&mut self) -> IcebergResult<()> {
        assert!(self.iceberg_table.is_none());
        self.iceberg_table = utils::get_table_if_exists(
            &*self.catalog,
            &self.config.namespace,
            &self.config.table_name,
        )
        .await?;
        Ok(())
    }

    /// ---------- load snapshot ----------
    ///
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
            .get_cache_entry(unique_file_id, data_file.file_path())
            .await
            .map_err(utils::to_iceberg_error)?;
        io_utils::delete_local_files(evicted_files_to_delete)
            .await
            .map_err(utils::to_iceberg_error)?;

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

        mooncake_snapshot
    }

    /// ---------- store snapshot ----------
    ///
    /// Util function to get unique table file id for the deletion vector puffin file.
    ///
    /// Notice: only deletion vector puffin generates new file ids.
    fn get_unique_table_id_for_deletion_vector_puffin(
        &self,
        file_params: &PersistenceFileParams,
        puffin_index: u64,
    ) -> TableUniqueFileId {
        let unique_table_auto_incre_id_offset = puffin_index / storage_utils::NUM_FILES_PER_FLUSH;
        let cur_table_auto_incr_id =
            file_params.table_auto_incr_ids.start as u64 + unique_table_auto_incre_id_offset;
        assert!(file_params
            .table_auto_incr_ids
            .contains(&(cur_table_auto_incr_id as u32)));
        let cur_file_idx =
            puffin_index - storage_utils::NUM_FILES_PER_FLUSH * unique_table_auto_incre_id_offset;
        TableUniqueFileId {
            table_id: TableId(self.mooncake_table_metadata.table_id),
            file_id: FileId(get_unique_file_id_for_flush(
                cur_table_auto_incr_id,
                cur_file_idx,
            )),
        }
    }

    /// Write deletion vector to puffin file.
    /// Precondition: batch deletion vector is not empty.
    ///
    /// Puffin blob write condition:
    /// 1. No compression is performed, otherwise it's hard to get blob size without another read operation.
    /// 2. We put one deletion vector within one puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
        file_params: &PersistenceFileParams,
        puffin_index: u64,
    ) -> IcebergResult<PuffinBlobRef> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(!deleted_rows.is_empty());

        let deleted_row_count = deleted_rows.len();
        let mut iceberg_deletion_vector = DeletionVector::new();
        iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
        let blob_properties = HashMap::from([
            (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
            (
                DELETION_VECTOR_CADINALITY.to_string(),
                deleted_row_count.to_string(),
            ),
            (
                MOONCAKE_DELETION_VECTOR_NUM_ROWS.to_string(),
                deletion_vector.get_max_rows().to_string(),
            ),
        ]);
        let blob = iceberg_deletion_vector.serialize(blob_properties);
        let blob_size = blob.data().len();
        let puffin_filepath = self.get_unique_deletion_vector_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;
        puffin_writer.add(blob, CompressionCodec::None).await?;

        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath.clone(), puffin_writer)
            .await?;

        // Import the puffin file in object storage cache.
        let unique_file_id =
            self.get_unique_table_id_for_deletion_vector_puffin(file_params, puffin_index);
        let (cache_handle, evicted_files_to_delete) = self
            .object_storage_cache
            .get_cache_entry(unique_file_id, &puffin_filepath)
            .await
            .map_err(utils::to_iceberg_error)?;
        io_utils::delete_local_files(evicted_files_to_delete)
            .await
            .map_err(utils::to_iceberg_error)?;

        let puffin_blob_ref = PuffinBlobRef {
            puffin_file_cache_handle: cache_handle.unwrap(),
            start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
            blob_size: blob_size as u32,
        };
        Ok(puffin_blob_ref)
    }

    /// Dump local data files into iceberg table.
    /// Return new iceberg data files for append transaction, and local data filepath to remote data filepath for index block remapping.
    async fn sync_data_files(
        &mut self,
        new_data_files: Vec<MooncakeDataFileRef>,
        old_data_files: Vec<MooncakeDataFileRef>,
        new_deletion_vector: &HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    ) -> IcebergResult<DataFileImportResult> {
        let mut local_data_files_to_remote = HashMap::with_capacity(new_data_files.len());
        let mut new_remote_data_files = Vec::with_capacity(new_data_files.len());

        // Handle imported new data files.
        let mut new_iceberg_data_files = Vec::with_capacity(new_data_files.len());
        for local_data_file in new_data_files.into_iter() {
            let iceberg_data_file = utils::write_record_batch_to_iceberg(
                self.iceberg_table.as_ref().unwrap(),
                local_data_file.file_path(),
                self.iceberg_table.as_ref().unwrap().metadata(),
            )
            .await?;

            // Try get deletion vector batch size.
            let max_rows = new_deletion_vector
                .get(&local_data_file)
                .map(|dv| dv.get_max_rows());

            // Insert new entry into iceberg table manager persisted data files.
            let old_entry = self.persisted_data_files.insert(
                local_data_file.file_id(),
                DataFileEntry {
                    data_file: iceberg_data_file.clone(),
                    // Max number of rows will be initialized when deletion take place.
                    deletion_vector: BatchDeletionVector::new(
                        max_rows.unwrap_or(UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW),
                    ),
                    persisted_deletion_vector: None,
                },
            );
            assert!(old_entry.is_none());

            // Insert into local to remote mapping.
            assert!(local_data_files_to_remote
                .insert(
                    local_data_file.file_path().clone(),
                    iceberg_data_file.file_path().to_string(),
                )
                .is_none());

            // Record all imported iceberg data files, with file id unchanged.
            new_remote_data_files.push(create_data_file(
                local_data_file.file_id().0,
                iceberg_data_file.file_path().to_string(),
            ));

            // Record file path to file id mapping.
            assert!(self
                .remote_data_file_to_file_id
                .insert(
                    iceberg_data_file.file_path().to_string(),
                    local_data_file.file_id(),
                )
                .is_none());

            new_iceberg_data_files.push(iceberg_data_file);
        }

        // Handle removed data files.
        let mut data_files_to_remove_set = HashSet::with_capacity(old_data_files.len());
        for cur_data_file in old_data_files.into_iter() {
            let old_entry = self
                .persisted_data_files
                .remove(&cur_data_file.file_id())
                .unwrap();
            data_files_to_remove_set.insert(old_entry.data_file.file_path().to_string());
            assert!(self
                .remote_data_file_to_file_id
                .remove(old_entry.data_file.file_path())
                .is_some());
        }
        self.catalog
            .set_data_files_to_remove(data_files_to_remove_set);

        Ok(DataFileImportResult {
            new_iceberg_data_files,
            local_data_files_to_remote,
            new_remote_data_files,
        })
    }

    /// Dump committed deletion logs into iceberg table, only the changed part will be persisted.
    async fn sync_deletion_vector(
        &mut self,
        new_deletion_logs: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
        file_params: &PersistenceFileParams,
    ) -> IcebergResult<HashMap<FileId, PuffinBlobRef>> {
        let mut puffin_deletion_blobs = HashMap::with_capacity(new_deletion_logs.len());
        for (puffin_index, (data_file, new_deletion_vector)) in
            new_deletion_logs.into_iter().enumerate()
        {
            let mut entry = self
                .persisted_data_files
                .get(&data_file.file_id())
                .unwrap()
                .clone();

            if entry.deletion_vector.get_max_rows() == UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW {
                entry.deletion_vector =
                    BatchDeletionVector::new(new_deletion_vector.get_max_rows());
            }
            entry.deletion_vector.merge_with(&new_deletion_vector);

            // Data filepath in iceberg table.
            let iceberg_data_file = entry.data_file.file_path();
            let puffin_blob = self
                .write_deletion_vector(
                    iceberg_data_file.to_string(),
                    entry.deletion_vector.clone(),
                    file_params,
                    puffin_index as u64,
                )
                .await?;
            let old_entry = self.persisted_data_files.insert(data_file.file_id(), entry);
            assert!(old_entry.is_some());
            puffin_deletion_blobs.insert(data_file.file_id(), puffin_blob);
        }
        Ok(puffin_deletion_blobs)
    }

    /// Update data file path pointed by file indices, from local filepath to remote, with file id unchanged.
    ///
    /// # Arguments:
    ///
    /// * local_data_file_to_remote: contains mappings from newly imported data files to remote paths.
    /// * local_index_file_to_remote: contains mappings from newly imported data files to remote paths.
    fn get_updated_file_index_at_import(
        old_file_index: &MooncakeFileIndex,
        local_data_file_to_remote: &HashMap<String, String>,
        local_index_file_to_remote: &HashMap<String, String>,
    ) -> MooncakeFileIndex {
        let mut new_file_index = old_file_index.clone();

        // Update data file from local path to remote one.
        for cur_data_file in new_file_index.files.iter_mut() {
            let remote_data_file = local_data_file_to_remote
                .get(cur_data_file.file_path())
                // [`local_data_file_to_remote`] only contains new data files introduced in the previous persistence,
                // but it's possible that the data file was already persisted in the previous iterations.
                .unwrap_or(cur_data_file.file_path())
                .clone();
            *cur_data_file = create_data_file(cur_data_file.file_id().0, remote_data_file);
        }

        // Update index block from local path to remote one.
        for cur_index_block in new_file_index.index_blocks.iter_mut() {
            let remote_index_block_filepath = local_index_file_to_remote
                .get(cur_index_block.index_file.file_path())
                .unwrap()
                .clone();
            cur_index_block.index_file = create_data_file(
                cur_index_block.index_file.file_id().0,
                remote_index_block_filepath,
            );
            // At this point, all index block files are at an inconsistent state, which have their
            // - file path pointing to remote path
            // - cache handle pinned and refers to local cache file path
            // The inconsistency will be fixed when they're imported into mooncake snapshot.
        }

        new_file_index
    }

    /// Process file indices to import.
    /// [`local_data_file_to_remote`] should contain all local data filepath to remote data filepath mapping.
    /// Return the mapping from local index files to remote index files.
    async fn import_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        local_data_file_to_remote: &HashMap<String, String>,
    ) -> IcebergResult<HashMap<String, String>> {
        let puffin_filepath = self.get_unique_hash_index_v1_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;

        // TODO(hjiang): Maps from local filepath to remote filepath.
        // After sync, file index still stores local index file location.
        // After cache design, we should be able to provide a "handle" abstraction, which could be either local or remote.
        // The hash map here is merely a workaround to pass remote path to iceberg file index structure.
        let mut local_index_file_to_remote = HashMap::new();

        let mut new_file_indices: Vec<&MooncakeFileIndex> =
            Vec::with_capacity(file_indices_to_import.len());
        for cur_file_index in file_indices_to_import.iter() {
            new_file_indices.push(cur_file_index);
            // Upload new index file to iceberg table.
            for cur_index_block in cur_file_index.index_blocks.iter() {
                let remote_index_block = utils::upload_index_file(
                    self.iceberg_table.as_ref().unwrap(),
                    cur_index_block.index_file.file_path(),
                )
                .await?;
                local_index_file_to_remote.insert(
                    cur_index_block.index_file.file_path().to_string(),
                    remote_index_block,
                );
            }
            self.persisted_file_indices
                .insert(cur_file_index.clone(), puffin_filepath.clone());
        }

        let file_index_blob = FileIndexBlob::new(
            new_file_indices,
            &local_index_file_to_remote,
            local_data_file_to_remote,
        );
        let puffin_blob = file_index_blob.as_blob()?;
        puffin_writer
            .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
            .await?;
        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
            .await?;

        Ok(local_index_file_to_remote)
    }

    /// Dump file indices into the iceberg table, only new file indices will be persisted into the table.
    /// Return file index ids which should be added into iceberg table.
    ///
    /// # Arguments:
    ///
    /// * local_data_file_to_remote: contains mappings from newly imported data files to remote paths.
    ///
    /// TODO(hjiang): Need to configure (1) the number of blobs in a puffin file; and (2) the number of file index in a puffin blob.
    /// For implementation simpicity, put everything in a single file and a single blob.
    async fn sync_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        file_indices_to_remove: &[MooncakeFileIndex],
        local_data_file_to_remote: HashMap<String, String>,
    ) -> IcebergResult<Vec<MooncakeFileIndex>> {
        if file_indices_to_import.is_empty() && file_indices_to_remove.is_empty() {
            return Ok(vec![]);
        }

        // Import new file indices.
        let local_index_block_to_remote = self
            .import_file_indices(file_indices_to_import, &local_data_file_to_remote)
            .await?;

        // Update local file indices:
        // - Redirect local data file to remote one
        // - Redirect local index block file to remote one
        let remote_file_indices = file_indices_to_import
            .iter()
            .map(|old_file_index| {
                Self::get_updated_file_index_at_import(
                    old_file_index,
                    &local_data_file_to_remote,
                    &local_index_block_to_remote,
                )
            })
            .collect::<Vec<_>>();

        // Process file indices to remove.
        self.catalog.set_puffin_files_to_remove(
            file_indices_to_remove
                .iter()
                .map(|cur_index| self.persisted_file_indices.remove(cur_index).unwrap())
                .collect::<HashSet<String>>(),
        );

        Ok(remote_file_indices)
    }

    /// Commit transaction with retry.
    ///
    /// TODO(hjiang): Add unit test; currently it's hard due to the difficulty to mock opendal or catalog.
    async fn commit_transaction_with_retry(&self, txn: Transaction) -> IcebergResult<IcebergTable> {
        let retry_strategy = ExponentialBackoff::from_millis(TABLE_COMMIT_RETRY_MIN_MS_DEFAULT)
            .factor(table_property::TABLE_COMMIT_RETRY_FACTOR)
            .max_delay(std::time::Duration::from_millis(
                TABLE_COMMIT_RETRY_MAX_MS_DEFAULT,
            ))
            .map(jitter);

        let catalog = &*self.catalog;
        let iceberg_table = Retry::spawn(retry_strategy, || {
            let txn = txn.clone();
            async move {
                let table = txn
                    .commit(catalog)
                    .await
                    .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)?;
                Ok::<IcebergTable, RetryError<IcebergError>>(table)
            }
        })
        .await?;

        Ok(iceberg_table)
    }
}

/// TODO(hjiang): Parallelize all IO operations.
#[async_trait]
impl TableManager for IcebergTableManager {
    async fn sync_snapshot(
        &mut self,
        mut snapshot_payload: IcebergSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> IcebergResult<PersistenceResult> {
        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        let new_data_files = take_data_files_to_import(&mut snapshot_payload);
        let old_data_files = take_data_files_to_remove(&mut snapshot_payload);
        let new_file_indices = take_file_indices_to_import(&mut snapshot_payload);
        let old_file_indices = take_file_indices_to_remove(&mut snapshot_payload);

        // Persist data files.
        let data_file_import_result = self
            .sync_data_files(
                new_data_files,
                old_data_files,
                &snapshot_payload.import_payload.new_deletion_vector,
            )
            .await?;

        // Persist committed deletion logs.
        let new_deletion_vector =
            std::mem::take(&mut snapshot_payload.import_payload.new_deletion_vector);
        let deletion_puffin_blobs = self
            .sync_deletion_vector(new_deletion_vector, &file_params)
            .await?;

        let remote_file_indices = self
            .sync_file_indices(
                &new_file_indices,
                &old_file_indices,
                data_file_import_result.local_data_files_to_remote,
            )
            .await?;

        // Only start append action when there're new data files.
        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        if !data_file_import_result.new_iceberg_data_files.is_empty() {
            let action = txn.fast_append();
            let action = action.add_data_files(data_file_import_result.new_iceberg_data_files);
            txn = action.apply(txn)?;
        }

        // Persist flush lsn at table property
        let action = txn.update_table_properties().set(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            snapshot_payload.flush_lsn.to_string(),
        );
        txn = action.apply(txn)?;
        let updated_iceberg_table = self.commit_transaction_with_retry(txn).await?;
        self.iceberg_table = Some(updated_iceberg_table);

        self.catalog.clear_puffin_metadata();

        // NOTICE: persisted data files and file indices are returned in the order of (1) newly imported ones; (2) index merge ones; (3) data compacted ones.
        Ok(PersistenceResult {
            remote_data_files: data_file_import_result.new_remote_data_files,
            puffin_blob_ref: deletion_puffin_blobs,
            remote_file_indices,
        })
    }

    async fn load_snapshot_from_table(&mut self) -> IcebergResult<(u32, MooncakeSnapshot)> {
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

        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let mut flush_lsn: Option<u64> = None;
        if let Some(lsn) = table_metadata.properties().get(MOONCAKE_TABLE_FLUSH_LSN) {
            flush_lsn = Some(lsn.parse().unwrap());
        }

        // There's nothing stored in iceberg table.
        if table_metadata.current_snapshot().is_none() {
            let mut empty_mooncake_snapshot =
                MooncakeSnapshot::new(self.mooncake_table_metadata.clone());
            empty_mooncake_snapshot.data_file_flush_lsn = flush_lsn;
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

        let mooncake_snapshot = self.transform_to_mooncake_snapshot(loaded_file_indices, flush_lsn);
        Ok((next_file_id as u32, mooncake_snapshot))
    }

    async fn drop_table(&mut self) -> IcebergResult<()> {
        let table_ident = TableIdent::new(
            NamespaceIdent::from_strs(&self.config.namespace).unwrap(),
            self.config.table_name.clone(),
        );
        self.catalog.drop_table(&table_ident).await
    }
}

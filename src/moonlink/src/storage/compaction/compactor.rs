// Compaction struct for data files, which takes a number of data files, compact them into one or more final data files, and one single file indices.
// Deletion vectors, which correspond to data files to compact, will be applied inline.

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use more_asserts as ma;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::storage::compaction::table_compaction::{
    CompactedDataEntry, DataCompactionPayload, DataCompactionResult, RemappedRecordLocation,
};
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::storage_utils::{
    get_file_idx_from_flush_file_id, get_random_file_name_in_dir, get_unique_file_id_for_flush,
    MooncakeDataFileRef,
};
use crate::{create_data_file, Result};

type DataFileRemap = HashMap<RecordLocation, RemappedRecordLocation>;

/// Default ZSTD compression level for parquet data.
const PARQURT_ZSTD_COMPRESSION_LEVEL: i32 = 5;

pub(crate) struct CompactionFileParams {
    /// Local directory to place compacted data files.
    pub(crate) dir_path: std::path::PathBuf,
    /// Used to generate unique file id.
    pub(crate) table_auto_incr_id: u32,
    /// Final size for compacted data files.
    pub(crate) data_file_final_size: u64,
}

pub(crate) struct CompactionBuilder {
    /// Compaction payload.
    compaction_payload: DataCompactionPayload,
    /// Table schema.
    schema: SchemaRef,
    /// File related parameters for compaction usage.
    file_params: CompactionFileParams,
    /// New data files after compaction.
    new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
    /// ===== Current ongoing compaction operation =====
    ///
    /// Current active async arrow writer, which is initialized in a lazy style.
    cur_arrow_writer: Option<AsyncArrowWriter<tokio::fs::File>>,
    /// Current new data file.
    cur_new_data_file: Option<MooncakeDataFileRef>,
    /// Current row number for the new compaction file.
    cur_row_num: usize,
    /// Current compacted data file count.
    compacted_file_count: u64,
}

// TODO(hjiang): CompactionBuilder should take a config to decide how big a compacted file should be, like DiskSliceWriter.
impl CompactionBuilder {
    pub(crate) fn new(
        compaction_payload: DataCompactionPayload,
        schema: SchemaRef,
        file_params: CompactionFileParams,
    ) -> Self {
        Self {
            compaction_payload,
            schema,
            file_params,
            new_data_files: Vec::new(),
            // Current ongoing compaction operation
            cur_arrow_writer: None,
            cur_new_data_file: None,
            cur_row_num: 0,
            compacted_file_count: 0,
        }
    }

    /// Util function to create a new data file.
    fn create_new_data_file(&self) -> MooncakeDataFileRef {
        assert!(self.cur_new_data_file.is_none());
        let file_id = get_unique_file_id_for_flush(
            self.file_params.table_auto_incr_id as u64,
            self.compacted_file_count,
        );
        let file_path = get_random_file_name_in_dir(self.file_params.dir_path.as_path());

        create_data_file(file_id, file_path)
    }

    /// Initialize arrow writer for once.
    async fn initialize_arrow_writer_if_not(&mut self) -> Result<()> {
        // If we create multiple data files during compaction, simply increment file id and recreate a new one.
        if self.cur_arrow_writer.is_some() {
            assert!(self.cur_new_data_file.is_some());
            return Ok(());
        }

        self.cur_new_data_file = Some(self.create_new_data_file());
        let write_file =
            tokio::fs::File::create(self.cur_new_data_file.as_ref().unwrap().file_path()).await?;
        let parquet_properties = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                ZstdLevel::try_new(PARQURT_ZSTD_COMPRESSION_LEVEL).unwrap(),
            ))
            .set_dictionary_enabled(true)
            .set_encoding(Encoding::PLAIN)
            .build();
        let writer: AsyncArrowWriter<tokio::fs::File> =
            AsyncArrowWriter::try_new(write_file, self.schema.clone(), Some(parquet_properties))?;
        self.cur_arrow_writer = Some(writer);

        Ok(())
    }

    /// Util function to flush current arrow write and re-initialize related states.
    async fn flush_arrow_writer(&mut self) -> Result<()> {
        self.cur_arrow_writer.as_mut().unwrap().finish().await?;
        let file_size = self.cur_arrow_writer.as_ref().unwrap().bytes_written();
        ma::assert_gt!(file_size, 0);
        ma::assert_gt!(self.cur_row_num, 0);
        let compacted_data_entry = CompactedDataEntry {
            num_rows: self.cur_row_num,
            file_size,
        };
        // TODO(hjiang): Should be able to save a copy.
        self.new_data_files.push((
            self.cur_new_data_file.as_ref().unwrap().clone(),
            compacted_data_entry,
        ));

        // Reinitialize states related to current new compacted data file.
        self.cur_arrow_writer = None;
        self.cur_new_data_file = None;
        self.cur_row_num = 0;
        self.compacted_file_count += 1;

        Ok(())
    }

    /// Util function to read the given parquet file, apply the corresponding deletion vector, and write it to the given arrow writer.
    /// Return the data file mapping.
    ///
    /// TODO(hjiang): The compacted file won't be closed and will be reused, should set a threshold.
    async fn apply_deletion_vector_and_write(
        &mut self,
        old_data_file: MooncakeDataFileRef,
        puffin_blob_ref: Option<PuffinBlobRef>,
    ) -> Result<DataFileRemap> {
        let file = tokio::fs::File::open(old_data_file.file_path()).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let mut reader = builder.build().unwrap();

        let batch_deletion_vector = if let Some(puffin_blob_ref) = puffin_blob_ref {
            puffin_utils::load_deletion_vector_from_blob(&puffin_blob_ref).await?
        } else {
            BatchDeletionVector::new(/*max_rows=*/ 0)
        };

        let get_filtered_record_batch = |record_batch: RecordBatch, start_row_idx: usize| {
            if batch_deletion_vector.is_empty() {
                return record_batch;
            }
            batch_deletion_vector
                .apply_to_batch_with_slice(&record_batch, start_row_idx)
                .unwrap()
        };

        let mut old_start_row_idx = 0;
        let mut old_to_new_remap = HashMap::new();
        while let Some(cur_record_batch) = reader.try_next().await? {
            // If all rows have been deleted for the old data file, do nothing.
            let cur_num_rows = cur_record_batch.num_rows();
            let filtered_record_batch =
                get_filtered_record_batch(cur_record_batch, old_start_row_idx);
            if filtered_record_batch.num_rows() == 0 {
                continue;
            }

            self.initialize_arrow_writer_if_not().await?;
            self.cur_arrow_writer
                .as_mut()
                .unwrap()
                .write(&filtered_record_batch)
                .await?;

            // Construct old data file to new one mapping on-the-fly.
            old_to_new_remap.reserve(cur_num_rows);
            for old_row_idx in old_start_row_idx..(old_start_row_idx + cur_num_rows) {
                if batch_deletion_vector.is_deleted(old_row_idx) {
                    continue;
                }
                let old_record_location =
                    RecordLocation::DiskFile(old_data_file.file_id(), old_row_idx);
                let new_record_location = RecordLocation::DiskFile(
                    self.cur_new_data_file.as_ref().unwrap().file_id(),
                    self.cur_row_num,
                );
                let remapped_record_location = RemappedRecordLocation {
                    record_location: new_record_location,
                    new_data_file: self.cur_new_data_file.as_ref().unwrap().clone(),
                };
                let old_entry =
                    old_to_new_remap.insert(old_record_location, remapped_record_location);
                assert!(old_entry.is_none());
                self.cur_row_num += 1;
            }

            old_start_row_idx += cur_num_rows;
        }

        // Bytes to write already reached target compacted data file size, flush and close.
        if self.cur_arrow_writer.is_some()
            && self.cur_arrow_writer.as_ref().unwrap().memory_size()
                >= self.file_params.data_file_final_size as usize
        {
            self.flush_arrow_writer().await?;
        }

        Ok(old_to_new_remap)
    }

    /// Util function to compact the given data files, with their corresponding deletion vector applied.
    async fn compact_data_files(&mut self) -> Result<DataFileRemap> {
        let mut old_to_new_remap = HashMap::new();

        let disk_files = std::mem::take(&mut self.compaction_payload.disk_files);
        for (new_data_file, puffin_blob_ref) in disk_files.into_iter() {
            let new_remap = self
                .apply_deletion_vector_and_write(new_data_file.clone(), puffin_blob_ref.clone())
                .await?;
            old_to_new_remap.extend(new_remap);
        }

        Ok(old_to_new_remap)
    }

    /// Util function to get new compacted data files **IN ORDER**.
    fn get_new_compacted_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut prev_file_id: u64 = 0;
        let mut new_data_files = Vec::with_capacity(self.new_data_files.len());
        for (cur_new_data_file, _) in self.new_data_files.iter() {
            ma::assert_lt!(prev_file_id, cur_new_data_file.file_id().0);
            prev_file_id = cur_new_data_file.file_id().0;

            new_data_files.push(cur_new_data_file.clone());
        }
        new_data_files
    }

    /// Util function to merge all given file indices into one.
    async fn compact_file_indices(
        &mut self,
        old_file_indices: Vec<FileIndex>,
        old_to_new_remap: &HashMap<RecordLocation, RemappedRecordLocation>,
    ) -> FileIndex {
        let get_remapped_record_location =
            |old_record_location: RecordLocation| -> Option<RecordLocation> {
                if let Some(remapped_record_location) = old_to_new_remap.get(&old_record_location) {
                    return Some(remapped_record_location.record_location.clone());
                }
                None
            };
        let get_seg_idx = |new_record_location: RecordLocation| -> usize /*seg_idx*/ {
            let file_id = new_record_location.get_file_id().unwrap().0;
            get_file_idx_from_flush_file_id(file_id, self.file_params.table_auto_incr_id as u64) as usize
        };

        let mut global_index_builder = GlobalIndexBuilder::new();
        global_index_builder.set_directory(self.file_params.dir_path.clone());
        global_index_builder
            .build_from_merge_for_compaction(
                /*num_rows=*/ old_to_new_remap.len() as u32,
                old_file_indices,
                /*new_data_files=*/ self.get_new_compacted_data_files(),
                get_remapped_record_location,
                get_seg_idx,
            )
            .await
    }

    /// Perform a compaction operation, and get the result back.
    pub(crate) async fn build(mut self) -> Result<DataCompactionResult> {
        let old_data_files = self
            .compaction_payload
            .disk_files
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let old_file_indices = self
            .compaction_payload
            .file_indices
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let old_to_new_remap = self.compact_data_files().await?;

        // All rows have been deleted.
        if old_to_new_remap.is_empty() {
            return Ok(DataCompactionResult {
                remapped_data_files: old_to_new_remap,
                old_data_files,
                old_file_indices,
                new_data_files: Vec::new(),
                new_file_indices: Vec::new(),
            });
        }

        // Flush and close the compacted data file.
        if self.cur_arrow_writer.is_some() {
            self.flush_arrow_writer().await?;
        }

        // Perform compaction on file indices.
        let new_file_indices = self
            .compact_file_indices(
                self.compaction_payload.file_indices.clone(),
                &old_to_new_remap,
            )
            .await;

        Ok(DataCompactionResult {
            remapped_data_files: old_to_new_remap,
            old_data_files,
            old_file_indices,
            new_data_files: self.new_data_files,
            new_file_indices: vec![new_file_indices],
        })
    }
}

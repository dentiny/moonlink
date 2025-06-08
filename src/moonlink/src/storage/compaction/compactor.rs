use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::AsyncArrowWriter;

use crate::storage::compaction::table_compaction::{
    CompactedDataEntry, DataCompactionPayload, DataCompactionResult,
};
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::storage_utils::{
    get_random_file_name_in_dir, get_unique_file_id_for_flush, MooncakeDataFileRef,
};
use crate::{create_data_file, Result};

type DataFileRemap = HashMap<RecordLocation, RecordLocation>;

pub(crate) struct CompactionFileParams {
    /// Local directory to place compacted data files.
    pub(crate) dir_path: std::path::PathBuf,
    /// Used to generate unique file id.
    pub(crate) table_auto_incr_id: u32,
}

#[allow(dead_code)]
pub(crate) struct CompactionBuilder {
    /// Compaction payload.
    compaction_payload: DataCompactionPayload,
    /// Table schema.
    schema: SchemaRef,
    /// File related parameters for compaction usage.
    file_params: CompactionFileParams,
    /// Async arrow writer, which is initialized in a lazy style.
    arrow_writer: Option<AsyncArrowWriter<tokio::fs::File>>,
    /// New data file after compaction.
    new_data_file: Option<MooncakeDataFileRef>,
    /// Current compacted data file count.
    compacted_file_count: u64,
}

// TODO(hjiang): CompactionBuilder should take a config to decide how big a compacted file should be, like DiskSliceWriter.
#[allow(dead_code)]
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
            new_data_file: None,
            arrow_writer: None,
            compacted_file_count: 0,
        }
    }

    /// Initialize arrow writer for once.
    async fn initialize_arrow_writer_for_once(&mut self) -> Result<()> {
        // If we create multiple data files during compaction, simply increment file id and recreate a new one.
        if self.arrow_writer.is_some() {
            return Ok(());
        }

        let file_id = get_unique_file_id_for_flush(
            self.file_params.table_auto_incr_id as u64,
            self.compacted_file_count,
        );
        let file_path = get_random_file_name_in_dir(self.file_params.dir_path.as_path());
        let data_file = create_data_file(file_id, file_path.clone());
        self.new_data_file = Some(data_file);

        let write_file = tokio::fs::File::create(&file_path).await?;
        let writer: AsyncArrowWriter<tokio::fs::File> =
            AsyncArrowWriter::try_new(write_file, self.schema.clone(), /*props=*/ None)?;
        self.arrow_writer = Some(writer);

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
        mut new_row_idx: usize,
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
            let cur_num_rows = cur_record_batch.num_rows();
            let filtered_record_batch =
                get_filtered_record_batch(cur_record_batch, old_start_row_idx);
            self.initialize_arrow_writer_for_once().await?;
            self.arrow_writer
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
                old_to_new_remap.insert(
                    RecordLocation::DiskFile(old_data_file.file_id(), old_row_idx),
                    RecordLocation::DiskFile(
                        self.new_data_file.as_ref().unwrap().file_id(),
                        new_row_idx,
                    ),
                );
                new_row_idx += 1;
            }

            old_start_row_idx += cur_num_rows;
        }

        Ok(old_to_new_remap)
    }

    /// Util function to compact the given data files, with their corresponding deletion vector applied.
    async fn compact_data_files(&mut self) -> Result<DataFileRemap> {
        let mut new_row_idx = 0;
        let mut old_to_new_remap = HashMap::new();

        let disk_files = std::mem::take(&mut self.compaction_payload.disk_files);
        for (new_data_file, puffin_blob_ref) in disk_files.into_iter() {
            let new_remap = self
                .apply_deletion_vector_and_write(
                    new_data_file.clone(),
                    puffin_blob_ref.clone(),
                    new_row_idx,
                )
                .await?;
            new_row_idx += new_remap.len();
            old_to_new_remap.extend(new_remap);
        }

        Ok(old_to_new_remap)
    }

    /// Util function to merge all given file indices into one.
    async fn compact_file_indices(
        &mut self,
        old_file_indices: Vec<FileIndex>,
        old_to_new_remap: &HashMap<RecordLocation, RecordLocation>,
    ) -> FileIndex {
        let get_remapped_record_location =
            |old_record_location: RecordLocation| -> Option<RecordLocation> {
                old_to_new_remap.get(&old_record_location).cloned()
            };
        let get_seg_idx = |_new_record_location: RecordLocation| -> usize /*seg_idx*/ {
            0 // Now compact all data files into one.
        };

        let mut global_index_builder = GlobalIndexBuilder::new();
        global_index_builder.set_directory(self.file_params.dir_path.clone());
        global_index_builder
            .build_from_merge_for_compaction(
                /*num_rows=*/ old_to_new_remap.len() as u32,
                old_file_indices,
                /*new_data_files=*/ vec![self.new_data_file.as_ref().unwrap().clone()],
                get_remapped_record_location,
                get_seg_idx,
            )
            .await
    }

    /// Perform a compaction operation, and get the result back.
    pub(crate) async fn build(&mut self) -> Result<DataCompactionResult> {
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
                new_data_files: HashMap::new(),
                new_file_indices: Vec::new(),
            });
        }

        // Flush and close the compacted data file.
        assert!(self.arrow_writer.is_some());
        let arrow_writer = std::mem::take(&mut self.arrow_writer);
        arrow_writer.unwrap().close().await?;

        // Perform compaction on file indices.
        let new_file_indices = self
            .compact_file_indices(
                self.compaction_payload.file_indices.clone(),
                &old_to_new_remap,
            )
            .await;

        // TODO(hjiang): Should be able to save a copy for file indices.
        let mut new_data_files = HashMap::new();
        let compacted_data_file_entry = CompactedDataEntry {
            num_rows: old_to_new_remap.len(),
        };
        new_data_files.insert(
            self.new_data_file.clone().unwrap(),
            compacted_data_file_entry,
        );

        Ok(DataCompactionResult {
            remapped_data_files: old_to_new_remap,
            old_data_files,
            old_file_indices,
            new_data_files,
            new_file_indices: vec![new_file_indices],
        })
    }
}

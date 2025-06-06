use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::AsyncArrowWriter;

use crate::storage::compaction::table_compaction::{CompactionPayload, CompactionResult};
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
    compaction_payload: CompactionPayload,
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
        compaction_payload: CompactionPayload,
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
                if let Some(new_record_location) = old_to_new_remap.get(&old_record_location) {
                    Some(new_record_location.clone())
                } else {
                    None
                }
            };
        let get_seg_idx = |_new_record_location: RecordLocation| -> usize /*seg_idx*/ {
            0 // Now compact all data files into one.
        };

        let mut global_index_builder = GlobalIndexBuilder::new();
        global_index_builder.set_files(vec![self.new_data_file.as_ref().unwrap().clone()]);
        global_index_builder
            .build_from_merge_with_predicate(
                old_file_indices,
                get_remapped_record_location,
                get_seg_idx,
            )
            .await
    }

    /// Perform a compaction operation, and get the result back.
    pub(crate) async fn build(&mut self) -> Result<CompactionResult> {
        let old_to_new_remap = self.compact_data_files().await?;
        if !old_to_new_remap.is_empty() {
            assert!(self.arrow_writer.is_some());
            let arrow_writer = std::mem::take(&mut self.arrow_writer);
            arrow_writer.unwrap().close().await?;
        }
        let new_file_indices = self
            .compact_file_indices(
                self.compaction_payload.file_indices.clone(),
                &old_to_new_remap,
            )
            .await;

        let compacted_data_files = if old_to_new_remap.is_empty() {
            vec![]
        } else {
            vec![self.new_data_file.as_mut().unwrap().clone()]
        };

        Ok(CompactionResult {
            remapped_data_files: old_to_new_remap,
            data_files: compacted_data_files,
            file_indices: vec![new_file_indices],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::compaction::test_utils;
    use crate::storage::iceberg::test_utils as iceberg_test_utils;
    use crate::storage::storage_utils::FileId;

    /// Case-1: single file, no deletion vector.
    #[tokio::test]
    async fn test_data_file_compaction_1() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file = temp_dir.path().join("test-1.parquet");
        let data_file =
            create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
        let record_batch = test_utils::create_test_batch_1();
        test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([(
                data_file.clone(),
                None,
            )]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 1;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap results.
        let compacted_file_id = FileId(get_unique_file_id_for_flush(
            table_auto_incr_id,
            /*file_idx=*/ 0,
        ));
        let expected_remap = test_utils::get_expected_remap_for_one_file(
            compacted_file_id,
            /*deletion_vector=*/ vec![],
        );
        assert_eq!(remap.remapped_data_files, expected_remap);
    }

    /// Case-2: single file, with deletion vector, and there're row left after deletion.
    #[tokio::test]
    async fn test_data_file_compaction_2() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file = temp_dir.path().join("test-1.parquet");

        // Create data file.
        let data_file =
            create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
        let record_batch = test_utils::create_test_batch_1();
        test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;

        // Create deletion vector puffin file.
        let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
        let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector.delete_row(1);
        let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
            data_file.file_path().clone(),
            puffin_filepath.to_str().unwrap().to_string(),
            batch_deletion_vector,
        )
        .await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([(
                data_file.clone(),
                Some(puffin_blob_ref),
            )]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 1;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap.
        let compacted_file_id = FileId(get_unique_file_id_for_flush(
            table_auto_incr_id,
            /*file_idx=*/ 0,
        ));
        let expected_remap = test_utils::get_expected_remap_for_one_file(
            compacted_file_id,
            /*deletion_vector=*/ vec![1],
        );
        assert_eq!(remap.remapped_data_files, expected_remap);
    }

    /// Case-3: single file, with deletion vector, and no rows left.
    #[tokio::test]
    async fn test_data_file_compaction_3() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file = temp_dir.path().join("test-1.parquet");

        // Create data file.
        let data_file =
            create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
        let record_batch = test_utils::create_test_batch_1();
        test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;

        // Create deletion vector puffin file.
        let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
        let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector.delete_row(0);
        batch_deletion_vector.delete_row(1);
        batch_deletion_vector.delete_row(2);
        let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
            data_file.file_path().clone(),
            puffin_filepath.to_str().unwrap().to_string(),
            batch_deletion_vector,
        )
        .await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([(
                data_file.clone(),
                Some(puffin_blob_ref),
            )]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 1;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap.
        assert!(remap.remapped_data_files.is_empty());
    }

    /// Case-4: two files, no deletion vector.
    #[tokio::test]
    async fn test_data_file_compaction_4() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file_1 = temp_dir.path().join("test-1.parquet");
        let data_file_2 = temp_dir.path().join("test-2.parquet");

        let data_file_1 = create_data_file(
            /*file_id=*/ 0,
            data_file_1.to_str().unwrap().to_string(),
        );
        let data_file_2 = create_data_file(
            /*file_id=*/ 1,
            data_file_2.to_str().unwrap().to_string(),
        );
        let record_batch_1 = test_utils::create_test_batch_1();
        let record_batch_2 = test_utils::create_test_batch_2();
        test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
        test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([
                (data_file_1.clone(), None),
                (data_file_2.clone(), None),
            ]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 2;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap.
        let compacted_file_id = FileId(get_unique_file_id_for_flush(
            table_auto_incr_id,
            /*file_idx=*/ 0,
        ));
        let possible_remaps =
            test_utils::get_possible_remap_for_two_files(compacted_file_id, vec![vec![], vec![]]);
        assert!(possible_remaps.contains(&remap.remapped_data_files));
    }

    /// Case-5: two files, each with deletion vector and partially deleted.
    #[tokio::test]
    async fn test_data_file_compaction_5() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file_1 = temp_dir.path().join("test-1.parquet");
        let data_file_2 = temp_dir.path().join("test-2.parquet");

        let data_file_1 = create_data_file(
            /*file_id=*/ 0,
            data_file_1.to_str().unwrap().to_string(),
        );
        let data_file_2 = create_data_file(
            /*file_id=*/ 1,
            data_file_2.to_str().unwrap().to_string(),
        );
        let record_batch_1 = test_utils::create_test_batch_1();
        let record_batch_2 = test_utils::create_test_batch_2();
        test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
        test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

        // Create deletion vector puffin file.
        let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
        let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector_1.delete_row(1);
        let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
            data_file_1.file_path().clone(),
            puffin_filepath_1.to_str().unwrap().to_string(),
            batch_deletion_vector_1,
        )
        .await;

        let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
        let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector_2.delete_row(0);
        batch_deletion_vector_2.delete_row(2);
        let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
            data_file_2.file_path().clone(),
            puffin_filepath_2.to_str().unwrap().to_string(),
            batch_deletion_vector_2,
        )
        .await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([
                (data_file_1.clone(), Some(puffin_blob_ref_1)),
                (data_file_2.clone(), Some(puffin_blob_ref_2)),
            ]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 2;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap.
        let compacted_file_id = FileId(get_unique_file_id_for_flush(
            table_auto_incr_id,
            /*file_idx=*/ 0,
        ));
        let possible_remaps = test_utils::get_possible_remap_for_two_files(
            compacted_file_id,
            vec![
                vec![1],    // deletion vector for the first data file
                vec![0, 2], // deletion vector the second data file
            ],
        );
        assert!(possible_remaps.contains(&remap.remapped_data_files));
    }

    /// Case-6: two files, and all rows deleted.
    #[tokio::test]
    async fn test_data_file_compaction_6() {
        // Create data file.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_file_1 = temp_dir.path().join("test-1.parquet");
        let data_file_2 = temp_dir.path().join("test-2.parquet");

        let data_file_1 = create_data_file(
            /*file_id=*/ 0,
            data_file_1.to_str().unwrap().to_string(),
        );
        let data_file_2 = create_data_file(
            /*file_id=*/ 1,
            data_file_2.to_str().unwrap().to_string(),
        );
        let record_batch_1 = test_utils::create_test_batch_1();
        let record_batch_2 = test_utils::create_test_batch_2();
        test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
        test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

        // Create deletion vector puffin file.
        let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
        let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector_1.delete_row(0);
        batch_deletion_vector_1.delete_row(1);
        batch_deletion_vector_1.delete_row(2);
        let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
            data_file_1.file_path().clone(),
            puffin_filepath_1.to_str().unwrap().to_string(),
            batch_deletion_vector_1,
        )
        .await;

        let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
        let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
        batch_deletion_vector_2.delete_row(0);
        batch_deletion_vector_2.delete_row(1);
        batch_deletion_vector_2.delete_row(2);
        let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
            data_file_2.file_path().clone(),
            puffin_filepath_2.to_str().unwrap().to_string(),
            batch_deletion_vector_2,
        )
        .await;

        // Prepare compaction payload.
        let payload = CompactionPayload {
            disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([
                (data_file_1.clone(), Some(puffin_blob_ref_1)),
                (data_file_2.clone(), Some(puffin_blob_ref_2)),
            ]),
            file_indices: vec![],
        };
        let table_auto_incr_id: u64 = 2;
        let file_params = CompactionFileParams {
            dir_path: std::path::PathBuf::from(temp_dir.path()),
            table_auto_incr_id: table_auto_incr_id as u32,
        };

        // Perform compaction.
        let mut builder = CompactionBuilder::new(
            payload,
            iceberg_test_utils::create_test_arrow_schema(),
            file_params,
        );
        let remap = builder.build().await.unwrap();

        // Check remap.
        assert!(remap.remapped_data_files.is_empty());
    }
}

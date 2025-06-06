use crate::create_data_file;
use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
use crate::storage::compaction::table_compaction::CompactionPayload;
use crate::storage::compaction::test_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::test_utils as iceberg_test_utils;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::{get_unique_file_id_for_flush, MooncakeDataFileRef};

use std::collections::HashMap;

/// Case-1: single file, no deletion vector.
#[tokio::test]
async fn test_data_file_compaction_1() {
    // Create data file and corresponding file indices.
    let temp_dir = tempfile::tempdir().unwrap();
    let data_file = temp_dir.path().join("test-1.parquet");
    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_indice =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file.clone()).await;

    // Prepare compaction payload.
    let payload = CompactionPayload {
        disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([(
            data_file.clone(),
            None,
        )]),
        file_indices: vec![file_indice],
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
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_one_file(
        compacted_file_id,
        /*deletion_vector=*/ vec![],
    );
    assert_eq!(compaction_result.remapped_data_files, expected_remap);

    // Check file indice compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 1, 2],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ vec![0, 1, 2],
    )
    .await;
}

/// Case-2: single file, with deletion vector, and there're row left after deletion.
#[tokio::test]
async fn test_data_file_compaction_2() {
    // Create data file and file indices.
    let temp_dir = tempfile::tempdir().unwrap();
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_indice =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file.clone()).await;

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
        file_indices: vec![file_indice.clone()],
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
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_one_file(
        compacted_file_id,
        /*deletion_vector=*/ vec![1],
    );
    assert_eq!(compaction_result.remapped_data_files, expected_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 2],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ vec![0, 2],
    )
    .await;
}

/// Case-3: single file, with deletion vector, and no rows left.
#[tokio::test]
async fn test_data_file_compaction_3() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let data_file = temp_dir.path().join("test-1.parquet");

    // Create data file and file indices.
    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_indice =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file.clone()).await;

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
        file_indices: vec![file_indice.clone()],
    };
    let table_auto_incr_id: u64 = 1;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_id: table_auto_incr_id as u32,
    };

    // Check compaction results.
    //
    // Check remap results.
    let mut builder = CompactionBuilder::new(
        payload,
        iceberg_test_utils::create_test_arrow_schema(),
        file_params,
    );
    let compaction_result = builder.build().await.unwrap();

    // Check remap results.
    assert!(compaction_result.remapped_data_files.is_empty());

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ None,
        /*old_row_indices=*/ vec![],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ vec![],
    )
    .await;
}

/// Case-4: two files, no deletion vector.
#[tokio::test]
async fn test_data_file_compaction_4() {
    // Create data files and file indices.
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

    let file_indice_1 =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file_1.clone()).await;
    let file_indice_2 =
        test_utils::create_file_indices_2(temp_dir.path().to_path_buf(), data_file_2.clone()).await;

    // Prepare compaction payload.
    let payload = CompactionPayload {
        disk_files: HashMap::<MooncakeDataFileRef, Option<PuffinBlobRef>>::from([
            (data_file_1.clone(), None),
            (data_file_2.clone(), None),
        ]),
        file_indices: vec![file_indice_1.clone(), file_indice_2.clone()],
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
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let possible_remaps =
        test_utils::get_possible_remap_for_two_files(compacted_file_id, vec![vec![], vec![]]);
    assert!(possible_remaps.contains(&compaction_result.remapped_data_files));

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ (0..6).collect(),
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ (0..6).collect(),
    )
    .await;
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

    let file_indice_1 =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file_1.clone()).await;
    let file_indice_2 =
        test_utils::create_file_indices_2(temp_dir.path().to_path_buf(), data_file_2.clone()).await;

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
        file_indices: vec![file_indice_1.clone(), file_indice_2.clone()],
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
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
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
    assert!(possible_remaps.contains(&compaction_result.remapped_data_files));

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 2, 4],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ vec![0, 2, 4],
    )
    .await;
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

    let file_indice_1 =
        test_utils::create_file_indices_1(temp_dir.path().to_path_buf(), data_file_1.clone()).await;
    let file_indice_2 =
        test_utils::create_file_indices_2(temp_dir.path().to_path_buf(), data_file_2.clone()).await;

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
        file_indices: vec![file_indice_1.clone(), file_indice_2.clone()],
    };
    let table_auto_incr_id: u64 = 2;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_id: table_auto_incr_id as u32,
    };

    // Check compaction results.
    //
    // Check remap results.
    let mut builder = CompactionBuilder::new(
        payload,
        iceberg_test_utils::create_test_arrow_schema(),
        file_params,
    );
    let compaction_result = builder.build().await.unwrap();

    // Check remap results.
    assert!(compaction_result.remapped_data_files.is_empty());

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.file_indices.as_slice(),
        /*expected_file_id=*/ None,
        /*old_row_indices=*/ vec![],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.data_files.as_slice(),
        /*old_row_indices=*/ vec![],
    )
    .await;
}

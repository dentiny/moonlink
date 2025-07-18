use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor::test_utils::*;
use crate::storage::filesystem::s3::s3_test_utils::*;
use crate::storage::filesystem::s3::test_guard::TestGuard;

use futures::StreamExt;
use rstest::rstest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_stream_read(#[case] file_size: usize) {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let gcs_filesystem_config = create_s3_filesystem_config(&warehouse_uri);

    // Prepare remote file.
    let remote_filepath = format!("{warehouse_uri}/remote");
    let expected_content =
        create_remote_file(&remote_filepath, gcs_filesystem_config.clone(), file_size).await;

    // Stream read from destination path.
    let mut actual_content = vec![];
    let filesystem_accessor = FileSystemAccessor::new(gcs_filesystem_config);
    let mut read_stream = filesystem_accessor
        .stream_read(&remote_filepath)
        .await
        .unwrap();
    while let Some(chunk) = read_stream.next().await {
        let data = chunk.unwrap();
        actual_content.extend_from_slice(&data);
    }

    // Validate destination file content.
    let actual_content = String::from_utf8(actual_content).unwrap();
    assert_eq!(actual_content.len(), expected_content.len());
    assert_eq!(actual_content, expected_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_copy_from_local_to_remote(#[case] file_size: usize) {
    // Prepare src file.
    let temp_dir = tempfile::tempdir().unwrap();
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let src_filepath = format!("{}/src", &root_directory);
    let expected_content = create_local_file(&src_filepath, file_size).await;

    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_filesystem_config = create_s3_filesystem_config(&warehouse_uri);

    // Copy from src to dst.
    let filesystem_accessor = FileSystemAccessor::new(s3_filesystem_config);
    let dst_filepath = format!("{warehouse_uri}/dst");
    filesystem_accessor
        .copy_from_local_to_remote(&src_filepath, &dst_filepath)
        .await
        .unwrap();

    // Validate destination file content.
    let actual_content = filesystem_accessor
        .read_object_as_string(&dst_filepath)
        .await
        .unwrap();
    assert_eq!(actual_content, expected_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[rstest]
#[case(10)]
#[case(18 * 1024 * 1024)]
async fn test_copy_from_remote_to_local(#[case] file_size: usize) {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let dst_filepath = format!("{}/dst", &root_directory);

    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = TestGuard::new(bucket.clone()).await;
    let s3_filesystem_config = create_s3_filesystem_config(&warehouse_uri);

    // Prepare src file.
    let src_filepath = format!("{warehouse_uri}/src");
    let expected_content =
        create_remote_file(&src_filepath, s3_filesystem_config.clone(), file_size).await;

    // Copy from src to dst.
    let filesystem_accessor = FileSystemAccessor::new(s3_filesystem_config);
    filesystem_accessor
        .copy_from_remote_to_local(&src_filepath, &dst_filepath)
        .await
        .unwrap();

    // Validate destination file content.
    let actual_content = tokio::fs::read_to_string(dst_filepath).await.unwrap();
    assert_eq!(actual_content, expected_content);
}

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor::test_utils::*;
use crate::storage::filesystem::gcs::gcs_test_utils::*;

#[tokio::test]
async fn test_copy_from_local_to_remote() {
    // Prepare src file.
    let temp_dir = tempfile::tempdir().unwrap();
    let root_directory = temp_dir.path().to_str().unwrap().to_string();
    let src_filepath = format!("{}/src", &root_directory);
    create_local_file(&src_filepath).await;

    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    create_test_gcs_bucket(bucket.clone()).await.unwrap();
    let gcs_filesystem_config = create_gcs_filesystem_config(&warehouse_uri);

    // Copy from src to dst.
    let filesystem_accessor = FileSystemAccessor::new(gcs_filesystem_config);
    let dst_filepath = format!("{}/dst", root_directory);
    filesystem_accessor
        .copy_from_local_to_remote(&src_filepath, &dst_filepath)
        .await
        .unwrap();

    // Validate destination file content.
    let actual_content = filesystem_accessor
        .read_object(&dst_filepath)
        .await
        .unwrap();
    assert_eq!(actual_content, TEST_CONTEST);

    // Delete all objects within the bucket, otherwise fake GCS server doesn't allow deletion.
    filesystem_accessor.remove_directory("/").await.unwrap();
    // Clean up test bucket.
    delete_test_gcs_bucket(bucket.clone()).await.unwrap();
}

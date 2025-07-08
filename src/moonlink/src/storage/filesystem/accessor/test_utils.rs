use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use tokio::io::AsyncWriteExt;

/// Test content.
pub(crate) const TEST_CONTEST: &str = "helloworld";

/// Test util function to create local file and write [`TEST_CONTEST`] to the destunation file (indicated by absolute path).
pub(crate) async fn create_local_file(filepath: &str) {
    let mut file = tokio::fs::File::create(filepath).await.unwrap();
    let _ = file.write(TEST_CONTEST.as_bytes()).await.unwrap();
    file.flush().await.unwrap();
}

/// Test util function to create remote file and write [`TEST_CONTEST`] to the destunation file (indicated by absolute path).
pub(crate) async fn create_remote_file(filepath: &str, filesystem_config: FileSystemConfig) {
    let accessor = FileSystemAccessor::new(filesystem_config);
    accessor
        .write_object(filepath, TEST_CONTEST.as_bytes().to_vec())
        .await
        .unwrap()
}

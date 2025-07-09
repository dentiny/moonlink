use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;

use rand::Rng;
use tokio::io::AsyncWriteExt;

/// Test content.
pub(crate) const TEST_CONTEST: &str = "helloworld";

/// Test util function to generate random string with the requested size.
fn create_random_string(size: usize) -> String {
    const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let random_string: String = (0..size)
        .map(|_| {
            let idx = rng.random_range(0..ALLOWED_CHARS.len());
            ALLOWED_CHARS[idx] as char
        })
        .collect();
    random_string
}

/// Test util function to create local file and write [`TEST_CONTEST`] to the destunation file (indicated by absolute path).
pub(crate) async fn create_local_file(filepath: &str) {
    let mut file = tokio::fs::File::create(filepath).await.unwrap();
    let _ = file.write(TEST_CONTEST.as_bytes()).await.unwrap();
    file.flush().await.unwrap();
}

/// Test util function to create a remote file with random content of given [`file_size`], and write it to the destination file (indicated by absolute path).
pub(crate) async fn create_remote_file(
    filepath: &str,
    filesystem_config: FileSystemConfig,
    file_size: usize,
) -> String {
    let content = create_random_string(file_size);
    let accessor = FileSystemAccessor::new(filesystem_config);
    accessor
        .write_object(filepath, content.as_bytes().to_vec())
        .await
        .unwrap();
    content
}

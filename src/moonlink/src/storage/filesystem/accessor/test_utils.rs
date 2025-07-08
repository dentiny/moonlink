use tokio::io::AsyncWriteExt;

/// Test content.
pub(crate) const TEST_CONTEST: &str = "helloworld";

/// Test util function to create local file and write [`TEST_CONTEST`] to the destunation file (indicated by absolute path).
pub(crate) async fn create_local_file(filepath: &str) {
    let mut file = tokio::fs::File::create(filepath).await.unwrap();
    file.write(TEST_CONTEST.as_bytes()).await.unwrap();
    file.flush().await.unwrap();
}

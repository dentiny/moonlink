/// A RAII-style test guard, which creates bucket at construction, and deletes at destruction.
use crate::storage::filesystem::gcs::gcs_test_utils;

pub(crate) struct TestGuard {
    /// Bucket name.n
    bucket: String,
}

impl TestGuard {
    pub(crate) async fn new(bucket: String) -> Self {
        gcs_test_utils::create_test_gcs_bucket(bucket.clone())
            .await
            .unwrap();
        Self { bucket }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        let bucket = std::mem::take(&mut self.bucket);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                gcs_test_utils::delete_test_gcs_bucket(bucket).await;
            });
        });
    }
}

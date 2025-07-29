/// A retry layer for filesystem accessor.
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::configs::*;
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::{Error, Result};

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

/// A wrapper that delegates all operations to an inner [`FileSystemAccessor`].
#[derive(Debug)]
pub struct FileSystemRetryWrapper {
    /// Internal filesystem accessor.
    inner: Arc<dyn BaseFileSystemAccess>,
}

impl FileSystemRetryWrapper {
    #[allow(dead_code)]
    pub fn new(inner: Arc<dyn BaseFileSystemAccess>) -> Self {
        Self { inner }
    }

    /// Retry utility for fallible async functions.
    async fn retry_async<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let policy = ExponentialBuilder::default()
            .with_max_times(MAX_RETRY_COUNT)
            .with_jitter()
            .with_factor(RETRY_DELAY_FACTOR)
            .with_min_delay(MIN_RETRY_DELAY)
            .with_max_delay(MAX_RETRY_DELAY);
        f.retry(&policy)
            .sleep(tokio::time::sleep)
            .when(|e: &Error| match e {
                Error::OpenDal { source } => source.is_temporary(),
                _ => false,
            })
            .notify(|e: &Error, _| {
                tracing::warn!("filesystem accessor retrying on error {:?}", e);
            })
            .await
    }
}

#[async_trait]
impl BaseFileSystemAccess for FileSystemRetryWrapper {
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        self.retry_async(|| async { self.inner.list_direct_subdirectories(folder).await })
            .await
    }

    async fn remove_directory(&self, directory: &str) -> Result<()> {
        self.retry_async(|| async { self.inner.remove_directory(directory).await })
            .await
    }

    async fn object_exists(&self, object: &str) -> Result<bool> {
        self.retry_async(|| async { self.inner.object_exists(object).await })
            .await
    }

    async fn get_object_size(&self, object: &str) -> Result<u64> {
        self.retry_async(|| async { self.inner.get_object_size(object).await })
            .await
    }

    async fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        self.retry_async(|| async { self.inner.read_object(object).await })
            .await
    }

    async fn read_object_as_string(&self, object: &str) -> Result<String> {
        self.retry_async(|| async { self.inner.read_object_as_string(object).await })
            .await
    }

    async fn stream_read(
        &self,
        object: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>> {
        self.retry_async(|| async { self.inner.stream_read(object).await })
            .await
    }

    async fn write_object(&self, object: &str, content: Vec<u8>) -> Result<()> {
        self.retry_async(|| {
            let content = content.clone();
            async move { self.inner.write_object(object, content).await }
        })
        .await
    }

    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>> {
        self.retry_async(|| async {
            self.inner
                .create_unbuffered_stream_writer(object_filepath)
                .await
        })
        .await
    }

    async fn delete_object(&self, object_filepath: &str) -> Result<()> {
        self.retry_async(|| async { self.inner.delete_object(object_filepath).await })
            .await
    }

    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.retry_async(|| async { self.inner.copy_from_local_to_remote(src, dst).await })
            .await
    }

    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.retry_async(|| async { self.inner.copy_from_remote_to_local(src, dst).await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::filesystem::accessor::base_filesystem_accessor::MockBaseFileSystemAccess;

    use super::*;

    #[tokio::test]
    async fn test_write_and_read_object_with_retry() {
        const TEST_FILEPATH: &str = "test_object";
        let content = b"hello world".to_vec();

        let mut mock_filesystem_access = MockBaseFileSystemAccess::new();
        mock_filesystem_access
            .expect_write_object()
            .times(1)
            .returning(|_, _| {
                Box::pin(async move {
                    Err(Error::from(
                        opendal::Error::new(opendal::ErrorKind::Unexpected, "Injected error")
                            .set_temporary(),
                    ))
                })
            });
        mock_filesystem_access
            .expect_write_object()
            .times(1)
            .returning(|_, _| Box::pin(async move { Ok(()) }));

        let retry_wrapper = FileSystemRetryWrapper::new(Arc::new(mock_filesystem_access));
        retry_wrapper
            .write_object(TEST_FILEPATH, content)
            .await
            .unwrap();
    }
}

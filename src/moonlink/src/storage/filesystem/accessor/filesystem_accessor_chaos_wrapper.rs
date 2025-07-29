/// A wrapper around filesystem accessor, which provides additional features like injected delay, intended errors, etc.
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor::metadata::ObjectMetadata;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::{Error, Result};

use async_trait::async_trait;
use futures::Stream;
use more_asserts as ma;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct FileSystemChaosOption {
    /// Min and max latency introduced to all operation access, both inclusive.
    pub min_latency: std::time::Duration,
    pub max_latency: std::time::Duration,

    /// Specified error for the given probability, which ranges [0, prob].
    pub injected_error: Option<Error>,
    pub prob: usize,
}

impl PartialEq for FileSystemChaosOption {
    fn eq(&self, other: &Self) -> bool {
        self.min_latency == other.min_latency
            && self.max_latency == other.max_latency
            && self.prob == other.prob
            && match (&self.injected_error, &other.injected_error) {
                (Some(e1), Some(e2)) => e1.to_string() == e2.to_string(),
                (None, None) => true,
                _ => false,
            }
    }
}

impl FileSystemChaosOption {
    /// Validate whether the given option is valid.
    #[allow(dead_code)]
    fn validate(&self) {
        ma::assert_le!(self.min_latency, self.max_latency);
        ma::assert_le!(self.prob, 100);
    }
}

/// A wrapper that delegates all operations to an inner [`FileSystemAccessor`].
#[derive(Debug)]
pub struct FileSystemChaosWrapper {
    /// Randomness.
    rng: Mutex<StdRng>,
    /// Internal filesystem accessor.
    inner: FileSystemAccessor,
    /// Filesystem wrapper option.
    option: FileSystemChaosOption,
}

impl FileSystemChaosWrapper {
    #[allow(dead_code)]
    pub fn new(config: FileSystemConfig, option: FileSystemChaosOption) -> Self {
        option.validate();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let rng = StdRng::seed_from_u64(nanos as u64);
        let accessor = FileSystemAccessor::new(config);
        Self {
            rng: Mutex::new(rng),
            inner: accessor,
            option,
        }
    }

    /// Get random latency.
    async fn get_random_duration(&self) -> std::time::Duration {
        let mut rng = self.rng.lock().await;
        let min_ns = self.option.min_latency.as_nanos();
        let max_ns = self.option.max_latency.as_nanos();
        let sampled_ns = rng.random_range(min_ns..=max_ns);
        std::time::Duration::from_nanos(sampled_ns as u64)
    }

    /// Get random error.
    async fn get_random_error(&self) -> Result<()> {
        if let Some(err) = &self.option.injected_error {
            let mut rng = self.rng.lock().await;
            let rand_val: usize = rng.random_range(0..=100);
            if rand_val <= self.option.prob {
                return Err(err.clone());
            }
        }

        Ok(())
    }

    async fn perform_wrapper_function(&self) -> Result<()> {
        // Introduce latency for IO operations.
        let latency = self.get_random_duration().await;
        tokio::time::sleep(latency).await;

        // Get injected error status.
        self.get_random_error().await?;

        Ok(())
    }
}

#[async_trait]
impl BaseFileSystemAccess for FileSystemChaosWrapper {
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        self.perform_wrapper_function().await?;
        self.inner.list_direct_subdirectories(folder).await
    }

    async fn remove_directory(&self, directory: &str) -> Result<()> {
        self.perform_wrapper_function().await?;
        self.inner.remove_directory(directory).await
    }

    async fn object_exists(&self, object: &str) -> Result<bool> {
        self.perform_wrapper_function().await?;
        self.inner.object_exists(object).await
    }

    async fn stats_object(&self, object: &str) -> Result<opendal::Metadata> {
        self.perform_wrapper_function().await?;
        self.inner.stats_object(object).await
    }

    async fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        self.perform_wrapper_function().await?;
        self.inner.read_object(object).await
    }

    async fn read_object_as_string(&self, object: &str) -> Result<String> {
        self.perform_wrapper_function().await?;
        self.inner.read_object_as_string(object).await
    }

    async fn stream_read(
        &self,
        object: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>> {
        self.perform_wrapper_function().await?;
        self.inner.stream_read(object).await
    }

    async fn write_object(&self, object: &str, content: Vec<u8>) -> Result<()> {
        self.perform_wrapper_function().await?;
        self.inner.write_object(object, content).await
    }

    async fn conditional_write_object(
        &self,
        object_filepath: &str,
        content: Vec<u8>,
        etag: Option<String>,
    ) -> Result<opendal::Metadata> {
        self.perform_wrapper_function().await?;
        self.inner
            .conditional_write_object(object_filepath, content, etag)
            .await
    }

    async fn create_unbuffered_stream_writer(
        &self,
        object_filepath: &str,
    ) -> Result<Box<dyn BaseUnbufferedStreamWriter>> {
        self.perform_wrapper_function().await?;
        self.inner
            .create_unbuffered_stream_writer(object_filepath)
            .await
    }

    async fn delete_object(&self, object_filepath: &str) -> Result<()> {
        self.perform_wrapper_function().await?;
        self.inner.delete_object(object_filepath).await
    }

    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.perform_wrapper_function().await?;
        self.inner.copy_from_local_to_remote(src, dst).await
    }

    async fn copy_from_remote_to_local(&self, src: &str, dst: &str) -> Result<ObjectMetadata> {
        self.perform_wrapper_function().await?;
        self.inner.copy_from_remote_to_local(src, dst).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::Error as IcebergError;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read_write_with_latency() {
        let temp_dir = tempdir().unwrap();
        let config = FileSystemConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
        };
        let wrapper = FileSystemChaosWrapper::new(
            config,
            FileSystemChaosOption {
                min_latency: Duration::from_millis(10),
                max_latency: Duration::from_millis(100),
                injected_error: None,
                prob: 0,
            },
        );

        // Write object.
        let filename = "test_object.txt".to_string();
        let content = b"helloworld".to_vec();
        wrapper
            .write_object(&filename, content.clone())
            .await
            .unwrap();

        // Read object.
        let read_content = wrapper.read_object(&filename).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_injected_error() {
        let temp_dir = tempdir().unwrap();
        let config = FileSystemConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
        };

        let injected_error = IcebergError::new(
            iceberg::ErrorKind::CatalogCommitConflicts,
            "commit confliction",
        );
        let wrapper = FileSystemChaosWrapper::new(
            config,
            FileSystemChaosOption {
                min_latency: Duration::from_millis(0),
                max_latency: Duration::from_millis(0),
                injected_error: Some(Error::from(injected_error)),
                prob: 100,
            },
        );

        // Write object.
        let filename = "test_object.txt".to_string();
        let content = b"helloworld".to_vec();
        let res = wrapper.write_object(&filename, content.clone()).await;
        assert!(res.is_err());
    }
}

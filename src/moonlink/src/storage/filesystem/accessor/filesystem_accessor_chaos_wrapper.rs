/// A wrapper around filesystem accessor, which provides additional features like injected delay, intended errors, etc.
use more_asserts as ma;
use opendal::raw::{
    Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FileSystemChaosOption {
    /// Min and max latency introduced to all operation access, both inclusive.
    pub min_latency: std::time::Duration,
    pub max_latency: std::time::Duration,

    /// Probability ranges from [0, err_prob]; if not 0, will return retriable opendal error randomly.
    pub err_prob: usize,
}

impl FileSystemChaosOption {
    /// Validate whether the given option is valid.
    #[allow(dead_code)]
    fn validate(&self) {
        ma::assert_le!(self.min_latency, self.max_latency);
        ma::assert_le!(self.err_prob, 100);
    }
}

/// A wrapper that delegates all operations to an inner [`FileSystemAccessor`].
#[derive(Debug)]
pub struct ChaosLayer {
    /// Filesystem wrapper option.
    option: FileSystemChaosOption,
}

impl ChaosLayer {
    #[allow(dead_code)]
    pub fn new(option: FileSystemChaosOption) -> Self {
        option.validate();
        Self { option }
    }
}

impl<A: Access> Layer<A> for ChaosLayer {
    type LayeredAccess = ChaosAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let rng = Mutex::new(StdRng::seed_from_u64(nanos as u64));
        ChaosAccessor {
            rng,
            inner,
            option: self.option.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ChaosAccessor<A> {
    /// Randomness.
    rng: Mutex<StdRng>,
    /// Inner accessor.
    inner: A,
    /// Chao layer option.
    option: FileSystemChaosOption,
}

impl<A: Access> ChaosAccessor<A> {
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
        if self.option.err_prob == 0 {
            return Ok(());
        }

        let mut rng = self.rng.lock().await;
        let rand_val: usize = rng.random_range(0..=100);
        if rand_val <= self.option.err_prob {
            let err = opendal::Error::new(opendal::ErrorKind::Unexpected, "Injected error")
                .set_temporary();
            return Err(err);
        }

        Ok(())
    }

    /// Attempt injected delay and error.
    async fn perform_wrapper_function(&self) -> Result<()> {
        // Introduce latency for IO operations.
        let latency = self.get_random_duration().await;
        tokio::time::sleep(latency).await;

        // Get injected error status.
        self.get_random_error().await?;

        Ok(())
    }
}

// TODO(hjiang): Provide own reader and writer, so we could inject chaos ourselves.
impl<A: Access> LayeredAccess for ChaosAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.perform_wrapper_function().await?;
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.perform_wrapper_function().await?;
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.perform_wrapper_function().await?;
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.perform_wrapper_function().await?;
        self.inner.delete().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
    use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
    use crate::storage::filesystem::filesystem_config::FileSystemConfig;
    use tempfile::{tempdir, TempDir};

    /// Test util function to create a filesystem accessor, based on the given chaos option.
    fn create_filesystem_accessor(
        temp_dir: &TempDir,
        chaos_option: FileSystemChaosOption,
    ) -> FileSystemAccessor {
        let inner_config = FileSystemConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
        };
        let chaos_config = FileSystemConfig::ChaosWrapper {
            chaos_option,
            inner_config: Box::new(inner_config),
        };
        FileSystemAccessor::new(chaos_config)
    }

    /// Test util function to write and read an object, which should succeed whether delay injected.
    async fn perform_read_write_op(filesystem_accessor: &FileSystemAccessor) {
        // Write object.
        let filename = "test_object.txt".to_string();
        let content = b"helloworld".to_vec();
        filesystem_accessor
            .write_object(&filename, content.clone())
            .await
            .unwrap();

        // Read object.
        let read_content = filesystem_accessor.read_object(&filename).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_no_delay_no_error() {
        let temp_dir = tempdir().unwrap();
        let chaos_option = FileSystemChaosOption {
            min_latency: std::time::Duration::ZERO,
            max_latency: std::time::Duration::ZERO,
            err_prob: 0,
        };
        let filesystem_accessor = create_filesystem_accessor(&temp_dir, chaos_option);
        perform_read_write_op(&filesystem_accessor).await;
    }

    #[tokio::test]
    async fn test_delay_injected() {
        let temp_dir = tempdir().unwrap();
        let chaos_option = FileSystemChaosOption {
            min_latency: std::time::Duration::from_millis(500),
            max_latency: std::time::Duration::from_millis(1000),
            err_prob: 0,
        };
        let filesystem_accessor = create_filesystem_accessor(&temp_dir, chaos_option);
        perform_read_write_op(&filesystem_accessor).await;
    }
}

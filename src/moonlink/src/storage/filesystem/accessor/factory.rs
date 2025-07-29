use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::accessor::filesystem_accessor_retry_wrapper::FileSystemRetryWrapper;
#[cfg(feature = "chaos-test")]
use crate::storage::filesystem::accessor::filesystem_accessor_wrapper::FileSystemWrapper;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;

use std::sync::Arc;

/// A factory function to create a filesystem accessor based on the given [`config`].
pub(crate) fn create_filesystem_accessor(
    config: &FileSystemConfig,
) -> Arc<dyn BaseFileSystemAccess> {
    let inner: Arc<dyn BaseFileSystemAccess> = match config {
        #[cfg(feature = "chaos-test")]
        FileSystemConfig::Wrapper {
            wrapper_option,
            inner_config,
        } => Arc::new(FileSystemWrapper::new(
            inner_config.as_ref().clone(),
            wrapper_option.clone(),
        )),
        _ => Arc::new(FileSystemAccessor::new(config.clone())),
    };
    Arc::new(FileSystemRetryWrapper::new(inner))
}

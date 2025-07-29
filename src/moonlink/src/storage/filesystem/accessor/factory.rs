use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
#[cfg(feature = "chaos-test")]
use crate::storage::filesystem::accessor::filesystem_accessor_chaos_wrapper::FileSystemChaosWrapper;
use crate::storage::filesystem::accessor::filesystem_accessor_retry_wrapper::FileSystemRetryWrapper;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;

use std::sync::Arc;

/// A factory function to create a filesystem accessor based on the given [`config`].
pub(crate) fn create_filesystem_accessor(
    config: FileSystemConfig,
) -> Arc<dyn BaseFileSystemAccess> {
    let inner: Arc<dyn BaseFileSystemAccess> = match config {
        #[cfg(feature = "chaos-test")]
        FileSystemConfig::ChaosWrapper {
            chaos_option,
            inner_config,
        } => Arc::new(FileSystemChaosWrapper::new(
            inner_config.as_ref().clone(),
            chaos_option.clone(),
        )),
        _ => Arc::new(FileSystemAccessor::new(config)),
    };
    Arc::new(FileSystemRetryWrapper::new(inner))
}

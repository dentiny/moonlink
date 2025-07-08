use async_trait::async_trait;
#[cfg(feature = "storage-gcs")]
use futures::TryStreamExt;
use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Operator;
/// FileSystemAccessor built upon opendal.
use tokio::sync::OnceCell;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::configs::*;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::Result;

#[derive(Debug)]
pub struct FileSystemAccessor {
    /// Operator to manager all IO operations.
    operator: OnceCell<Operator>,
    /// Filesystem configuration.
    config: FileSystemConfig,
}

impl FileSystemAccessor {
    pub fn new(config: FileSystemConfig) -> Self {
        Self {
            operator: OnceCell::new(),
            config,
        }
    }

    /// Get IO operator from the catalog.
    async fn get_operator(&self) -> Result<&Operator> {
        let retry_layer = RetryLayer::new()
            .with_max_times(MAX_RETRY_COUNT)
            .with_jitter()
            .with_factor(RETRY_DELAY_FACTOR)
            .with_min_delay(MIN_RETRY_DELAY)
            .with_max_delay(MAX_RETRY_DELAY);

        self.operator
            .get_or_try_init(|| async {
                match &self.config {
                    #[cfg(feature = "storage-fs")]
                    FileSystemConfig::FileSystem { root_directory } => {
                        let builder = services::Fs::default().root(root_directory);
                        let op = Operator::new(builder)?.layer(retry_layer).finish();
                        Ok(op)
                    }
                    #[cfg(feature = "storage-gcs")]
                    FileSystemConfig::Gcs {
                        bucket,
                        endpoint,
                        disable_auth,
                        ..
                    } => {
                        let mut builder = services::Gcs::default()
                            .root("/")
                            .bucket(bucket)
                            .endpoint(endpoint);
                        if *disable_auth {
                            builder = builder
                                .disable_config_load()
                                .disable_vm_metadata()
                                .allow_anonymous();
                        }
                        let op = Operator::new(builder)?.layer(retry_layer).finish();
                        Ok(op)
                    }
                    #[cfg(feature = "storage-s3")]
                    FileSystemConfig::S3 {
                        access_key_id,
                        secret_access_key,
                        region,
                        bucket,
                        endpoint,
                        ..
                    } => {
                        let builder = services::S3::default()
                            .bucket(bucket)
                            .region(region)
                            .endpoint(endpoint)
                            .access_key_id(access_key_id)
                            .secret_access_key(secret_access_key);
                        let op = Operator::new(builder)?.layer(retry_layer).finish();
                        Ok(op)
                    }
                }
            })
            .await
    }
}

#[async_trait]
impl BaseFileSystemAccess for FileSystemAccessor {
    /// ===============================
    /// Directory operations
    /// ===============================
    ///
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>> {
        let prefix = format!("{}/", folder);
        let mut dirs = Vec::new();
        let lister = self.get_operator().await?.list(&prefix).await?;

        let entries = lister;
        for cur_entry in entries.iter() {
            // Both directories and objects will be returned, here we only care about sub-directories.
            if !cur_entry.path().ends_with('/') {
                continue;
            }
            let dir_name = cur_entry
                .path()
                .trim_start_matches(&prefix)
                .trim_end_matches('/')
                .to_string();
            if !dir_name.is_empty() {
                dirs.push(dir_name);
            }
        }

        Ok(dirs)
    }

    /// TODO(hjiang): Check whether we could unify the implementation with [`remove_directory`].
    #[cfg(feature = "storage-gcs")]
    async fn remove_directory(&self, directory: &str) -> Result<()> {
        let path = if directory.ends_with('/') {
            directory.to_string()
        } else {
            format!("{}/", directory)
        };

        let operator = self.get_operator().await?;
        let mut lister = operator.lister(&path).await?;
        let mut entries = Vec::new();

        while let Some(entry) = lister.try_next().await? {
            // List operation returns target path.
            if entry.path() != path {
                entries.push(entry.path().to_string());
            }
        }
        for entry_path in entries {
            if entry_path == path {
                continue;
            }
            if entry_path.ends_with('/') {
                Box::pin(self.remove_directory(&entry_path)).await?;
            } else {
                operator.delete(&entry_path).await?;
            }
        }

        if !path.is_empty() && path != "/" {
            operator.remove_all(&path).await?;
        }

        Ok(())
    }

    #[cfg(not(feature = "storage-gcs"))]
    async fn remove_directory(&self, directory: &str) -> Result<()> {
        let op = self.get_operator().await?.clone();
        op.remove_all(directory).await?;
        Ok(())
    }

    /// ===============================
    /// Object operations
    /// ===============================
    ///
    async fn object_exists(&self, object: &str) -> Result<bool> {
        match self.get_operator().await?.stat(object).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_object(&self, object: &str) -> Result<String> {
        let content = self.get_operator().await?.read(object).await?;
        Ok(String::from_utf8(content.to_vec())?)
    }

    async fn write_object(&self, object_filepath: &str, content: Vec<u8>) -> Result<()> {
        let operator = self.get_operator().await?;
        operator.write(object_filepath, content).await?;
        Ok(())
    }

    async fn delete_object(&self, object_filepath: &str) -> Result<()> {
        let operator = self.get_operator().await?;
        operator.delete(object_filepath).await?;
        Ok(())
    }

    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<()> {
        let content = tokio::fs::read(src).await?;
        self.write_object(dst, content).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::test_utils::*;

    #[tokio::test]
    async fn test_copy_from_local_to_remote() {
        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();
        let filesystem_accessor = FileSystemAccessor::new(FileSystemConfig::FileSystem {
            root_directory: root_directory.clone(),
        });
        const CONTENT: &str = "helloworld";

        // Prepare src file.
        let src_filepath = format!("{}/src", &root_directory);
        create_local_file(&src_filepath).await;

        // Copy from src to dst.
        let dst_filepath = format!("{}/dst", &root_directory);
        filesystem_accessor
            .copy_from_local_to_remote(&src_filepath, &dst_filepath)
            .await
            .unwrap();

        // Validate destination file content.
        let actual_content = filesystem_accessor
            .read_object(&dst_filepath)
            .await
            .unwrap();
        assert_eq!(actual_content, CONTENT);
    }
}

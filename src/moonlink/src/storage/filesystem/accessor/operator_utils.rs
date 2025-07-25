use crate::storage::filesystem::accessor::configs::*;
use crate::FileSystemConfig;
use crate::Result;

use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Operator;

/// Util function to create opendal operator from filesystem config.
pub(crate) fn create_opendal_operator(filesystem_config: &FileSystemConfig) -> Result<Operator> {
    let retry_layer = RetryLayer::new()
        .with_max_times(MAX_RETRY_COUNT)
        .with_jitter()
        .with_factor(RETRY_DELAY_FACTOR)
        .with_min_delay(MIN_RETRY_DELAY)
        .with_max_delay(MAX_RETRY_DELAY);

    match filesystem_config {
        #[cfg(feature = "storage-fs")]
        FileSystemConfig::FileSystem { root_directory } => {
            let builder = services::Fs::default().root(root_directory);
            let op = Operator::new(builder)?.layer(retry_layer).finish();
            Ok(op)
        }
        #[cfg(feature = "storage-gcs")]
        FileSystemConfig::Gcs {
            region,
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            disable_auth,
            ..
        } => {
            // Test environment.
            if *disable_auth {
                let builder = services::Gcs::default()
                    .root("/")
                    .bucket(bucket)
                    .endpoint(endpoint.as_ref().unwrap())
                    .disable_config_load()
                    .disable_vm_metadata()
                    .allow_anonymous();
                let op = Operator::new(builder)?.layer(retry_layer).finish();
                return Ok(op);
            }

            let builder = services::S3::default()
                .root("/")
                .region(region)
                .bucket(bucket)
                .endpoint("https://storage.googleapis.com")
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);
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
        } => {
            let mut builder = services::S3::default()
                .bucket(bucket)
                .region(region)
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);
            if let Some(endpoint) = endpoint {
                builder = builder.endpoint(endpoint);
            }
            let op = Operator::new(builder)?.layer(retry_layer).finish();
            Ok(op)
        }
    }
}

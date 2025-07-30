use crate::storage::filesystem::accessor::configs::*;
#[cfg(feature = "chaos-test")]
use crate::storage::filesystem::accessor::filesystem_accessor_chaos_wrapper::ChaosLayer;
use crate::FileSystemConfig;
use crate::Result;

use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Operator;

fn create_opendal_operator_impl(filesystem_config: &FileSystemConfig) -> Result<Operator> {
    match filesystem_config {
        #[cfg(feature = "storage-fs")]
        FileSystemConfig::FileSystem { root_directory } => {
            let builder = services::Fs::default().root(root_directory);
            Ok(Operator::new(builder)?.finish())
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
                return Ok(Operator::new(builder)?.finish());
            }

            let builder = services::S3::default()
                .root("/")
                .region(region)
                .bucket(bucket)
                .endpoint("https://storage.googleapis.com")
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);
            Ok(Operator::new(builder)?.finish())
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
            Ok(Operator::new(builder)?.finish())
        }
        #[cfg(feature = "chaos-test")]
        _ => panic!("Unknown filesystem config {filesystem_config:?}"),
    }
}

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
        FileSystemConfig::FileSystem { .. } => {
            let op = create_opendal_operator_impl(filesystem_config)?;
            Ok(op.layer(retry_layer))
        }
        #[cfg(feature = "storage-gcs")]
        FileSystemConfig::Gcs { .. } => {
            let op = create_opendal_operator_impl(filesystem_config)?;
            Ok(op.layer(retry_layer))
        }
        #[cfg(feature = "storage-s3")]
        FileSystemConfig::S3 { .. } => {
            let op = create_opendal_operator_impl(filesystem_config)?;
            Ok(op.layer(retry_layer))
        }
        #[cfg(feature = "chaos-test")]
        FileSystemConfig::ChaosWrapper {
            inner_config,
            chaos_option,
        } => {
            let op = create_opendal_operator_impl(&inner_config.as_ref().clone())?;
            let chaos_layer = ChaosLayer::new(chaos_option.clone());
            Ok(op.layer(chaos_layer).layer(retry_layer))
        }
    }
}

/// This module provides a few test util functions.
use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};
use crate::storage::iceberg::object_storage_test_utils::*;

use rand::Rng;

/// Minio related constants.
///
/// Local minio warehouse needs special handling, so we simply prefix with special token.
#[allow(dead_code)]
pub(crate) static MINIO_TEST_BUCKET_PREFIX: &str = "test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_TEST_WAREHOUSE_URI_PREFIX: &str = "s3://test-minio-warehouse-";
#[allow(dead_code)]
pub(crate) static MINIO_ACCESS_KEY_ID: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_SECRET_ACCESS_KEY: &str = "minioadmin";
#[allow(dead_code)]
pub(crate) static MINIO_ENDPOINT: &str = "http://minio:9000";

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_minio_s3_catalog(bucket: &str, warehouse_uri: &str) -> FileCatalog {
    let catalog_config = CatalogConfig::S3 {
        access_key_id: MINIO_ACCESS_KEY_ID.to_string(),
        secret_access_key: MINIO_SECRET_ACCESS_KEY.to_string(),
        region: "auto".to_string(), // minio doesn't care about region.
        bucket: bucket.to_string(),
        endpoint: MINIO_ENDPOINT.to_string(),
    };
    FileCatalog::new(warehouse_uri.to_string(), catalog_config).unwrap()
}

#[allow(dead_code)]
pub(crate) fn get_test_s3_bucket_and_warehouse(
) -> (String /*bucket_name*/, String /*warehouse_url*/) {
    // minio bucket name only allows lowercase case letters, digits and hyphen.
    const TEST_BUCKET_NAME_LEN: usize = 12;
    const ALLOWED_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let mut rng = rand::rng();
    let random_string: String = (0..TEST_BUCKET_NAME_LEN)
        .map(|_| {
            let idx = rng.random_range(0..ALLOWED_CHARS.len());
            ALLOWED_CHARS[idx] as char
        })
        .collect();
    (
        format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string),
        format!("{}{}", MINIO_TEST_WAREHOUSE_URI_PREFIX, random_string),
    )
}

#[allow(dead_code)]
pub(crate) fn get_test_minio_bucket(warehouse_uri: &str) -> String {
    let random_string = warehouse_uri
        .strip_prefix(MINIO_TEST_WAREHOUSE_URI_PREFIX)
        .unwrap()
        .to_string();
    format!("{}{}", MINIO_TEST_BUCKET_PREFIX, random_string)
}

#[cfg(test)]
pub(crate) mod object_store_test_utils {
    use super::*;

    use crate::storage::iceberg::tokio_retry_utils;

    use std::sync::Arc;

    use base64::engine::general_purpose::STANDARD as base64;
    use base64::Engine;
    use chrono::Utc;
    use hmac::{Hmac, Mac};
    use iceberg::Error as IcebergError;
    use iceberg::Result as IcebergResult;
    use sha1::Sha1;
    use tokio_retry2::strategy::{jitter, ExponentialBackoff};
    use tokio_retry2::Retry;

    type HmacSha1 = Hmac<Sha1>;

    /// Create test bucket in minio server.
    #[allow(dead_code)]
    async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let date = Utc::now().format("%a, %d %b %Y %T GMT").to_string();
        let string_to_sign = format!("PUT\n\n\n{}\n/{}", date, bucket);

        let mut mac = HmacSha1::new_from_slice(MINIO_SECRET_ACCESS_KEY.as_bytes()).unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = base64.encode(mac.finalize().into_bytes());

        let auth_header = format!("AWS {}:{}", MINIO_ACCESS_KEY_ID, signature);
        let url = format!("{}/{}", MINIO_ENDPOINT, bucket);
        let client = reqwest::Client::new();
        client
            .put(&url)
            .header("Authorization", auth_header)
            .header("Date", date)
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to create bucket {} in minio {}", bucket, e),
                )
            })?;
        Ok(())
    }

    /// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
    #[allow(dead_code)]
    pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    create_test_s3_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }

    /// Delete test bucket in minio server.
    pub async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let date = Utc::now().format("%a, %d %b %Y %T GMT").to_string();
        let string_to_sign = format!("DELETE\n\n\n{}\n/{}", date, bucket);

        let mut mac = HmacSha1::new_from_slice(MINIO_SECRET_ACCESS_KEY.as_bytes()).unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = base64.encode(mac.finalize().into_bytes());
        let auth_header = format!("AWS {}:{}", MINIO_ACCESS_KEY_ID, signature);
        let url = format!("{}/{}", MINIO_ENDPOINT, bucket);

        let client = reqwest::Client::new();
        client
            .delete(&url)
            .header("Authorization", auth_header)
            .header("Date", date)
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to delete bucket {} in minio {}", bucket, e),
                )
            })?;

        Ok(())
    }

    /// Delete the provided bucket with exponential backoff retry; this function assume bucket already exists and return error if not.
    #[allow(dead_code)]
    pub(crate) async fn delete_test_s3_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    delete_test_s3_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }
}

use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::object_storage_test_utils::*;

/// Fake GCS related constants.
///
#[allow(dead_code)]
pub(crate) static GCS_TEST_BUCKET_PREFIX: &str = "test-gcs-warehouse-";
#[allow(dead_code)]
pub(crate) static GCS_TEST_WAREHOUSE_URI_PREFIX: &str = "gs://test-gcs-warehouse-";
#[allow(dead_code)]
pub(crate) static GCS_TEST_ENDPOINT: &str = "http://gcs.local:4443";
#[allow(dead_code)]
pub(crate) static GCS_TEST_PROJECT: &str = "fake-project";

#[allow(dead_code)]
pub(crate) fn create_gcs_catalog_config(warehouse_uri: &str) -> FileSystemConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    FileSystemConfig::Gcs {
        bucket: bucket.to_string(),
        endpoint: GCS_TEST_ENDPOINT.to_string(),
        disable_auth: true,
        project: GCS_TEST_PROJECT.to_string(),
    }
}

#[allow(dead_code)]
pub(crate) fn create_gcs_catalog(warehouse_uri: &str) -> FileCatalog {
    let catalog_config = create_gcs_catalog_config(warehouse_uri);
    FileCatalog::new(warehouse_uri.to_string(), catalog_config).unwrap()
}

/// Get GCS bucket name from the warehouse uri.
#[allow(dead_code)]
pub(crate) fn get_test_gcs_bucket(warehouse_uri: &str) -> String {
    let random_string = warehouse_uri
        .strip_prefix(GCS_TEST_WAREHOUSE_URI_PREFIX)
        .unwrap()
        .to_string();
    format!("{}{}", GCS_TEST_BUCKET_PREFIX, random_string)
}

#[allow(dead_code)]
pub(crate) fn get_test_gcs_bucket_and_warehouse() -> (String /*bucket*/, String /*warehouse_uri*/) {
    get_bucket_and_warehouse(GCS_TEST_BUCKET_PREFIX, GCS_TEST_WAREHOUSE_URI_PREFIX)
}

#[cfg(test)]
pub(crate) mod object_store_test_utils {
    use super::*;
    use crate::storage::iceberg::tokio_retry_utils;

    use std::sync::Arc;

    use iceberg::{Error as IcebergError, Result as IcebergResult};
    use reqwest::StatusCode;
    use tokio_retry2::strategy::{jitter, ExponentialBackoff};
    use tokio_retry2::Retry;

    async fn create_gcs_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/storage/v1/b?project={}",
            GCS_TEST_ENDPOINT, GCS_TEST_PROJECT
        );
        let res = client
            .post(&url)
            .json(&serde_json::json!({ "name": *bucket }))
            .send()
            .await?;
        if res.status() != StatusCode::OK {
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to create bucket {} in fake-gcs-server: HTTP {}",
                    bucket,
                    res.status()
                ),
            ));
        }
        Ok(())
    }

    async fn delete_gcs_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/storage/v1/b/{}", GCS_TEST_ENDPOINT, bucket);
        let res = client.delete(&url).send().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to delete bucket {} in fake-gcs-server: {}",
                    bucket, e
                ),
            )
        })?;

        if res.status() != StatusCode::OK {
            return Err(IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to delete bucket {} in fake-gcs-server: HTTP {}",
                    bucket,
                    res.status()
                ),
            ));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn create_test_gcs_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    create_gcs_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn delete_test_gcs_bucket(bucket: String) -> IcebergResult<()> {
        let retry_strategy = ExponentialBackoff::from_millis(TEST_RETRY_INIT_MILLISEC)
            .map(jitter)
            .take(TEST_RETRY_COUNT);

        Retry::spawn(retry_strategy, {
            let bucket_name = Arc::new(bucket);
            move || {
                let bucket_name = Arc::clone(&bucket_name);
                async move {
                    delete_gcs_bucket_impl(bucket_name)
                        .await
                        .map_err(tokio_retry_utils::iceberg_to_tokio_retry_error)
                }
            }
        })
        .await?;
        Ok(())
    }
}

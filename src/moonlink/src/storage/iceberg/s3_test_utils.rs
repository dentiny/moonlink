use crate::storage::filesystem::s3::s3_test_utils::*;
/// This module provides a few test util functions.
use crate::storage::iceberg::file_catalog::FileCatalog;

/// Create a S3 catalog, which communicates with local minio server.
#[allow(dead_code)]
pub(crate) fn create_test_s3_catalog(warehouse_uri: &str) -> FileCatalog {
    let catalog_config = create_s3_filesystem_config(warehouse_uri);
    FileCatalog::new(warehouse_uri.to_string(), catalog_config).unwrap()
}

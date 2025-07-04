use crate::storage::filesystem::gcs::gcs_test_utils::*;
use crate::storage::iceberg::file_catalog::FileCatalog;

#[allow(dead_code)]
pub(crate) fn create_gcs_catalog(warehouse_uri: &str) -> FileCatalog {
    let catalog_config = create_gcs_filesystem_config(warehouse_uri);
    FileCatalog::new(warehouse_uri.to_string(), catalog_config).unwrap()
}

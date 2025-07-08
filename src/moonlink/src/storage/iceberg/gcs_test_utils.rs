use crate::storage::filesystem::gcs::gcs_test_utils::*;
use crate::storage::iceberg::file_catalog::FileCatalog;

#[allow(dead_code)]
pub(crate) fn create_gcs_catalog(warehouse_uri: &str) -> FileCatalog {
    let filesystem_config = create_gcs_filesystem_config(warehouse_uri);
    FileCatalog::new(filesystem_config).unwrap()
}

use iceberg::spec::{NestedField, Schema};
use tempfile::TempDir;

use crate::storage::iceberg::file_catalog::{CatalogConfig, FileCatalog};

/// Test util to create file catalog.
pub(crate) fn create_test_file_catalog(tmp_dir: &TempDir) -> FileCatalog {
    let warehouse_path = tmp_dir.path().to_str().unwrap();
    let catalog =
        FileCatalog::new(warehouse_path.to_string(), CatalogConfig::FileSystem {}).unwrap();
    catalog
}

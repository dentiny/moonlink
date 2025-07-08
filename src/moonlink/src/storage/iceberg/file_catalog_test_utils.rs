use iceberg::spec::PrimitiveType;
use iceberg::spec::Type as IcebergType;
use iceberg::Result as IcebergResult;
use iceberg::{
    spec::{NestedField, Schema},
    TableIdent, TableUpdate,
};
use std::sync::Arc;
use tempfile::TempDir;

use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::iceberg::file_catalog::FileCatalog;

/// Test struct which mimics [`TableCommit`], to workaround the limitation that [`TableCommit`] is not exposed to public.
#[repr(C)]
pub(crate) struct TableCommitProxy {
    pub(crate) ident: TableIdent,
    pub(crate) requirements: Vec<iceberg::TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

/// Test util to create file catalog.
pub(crate) fn create_test_file_catalog(tmp_dir: &TempDir) -> FileCatalog {
    let warehouse_path = tmp_dir.path().to_str().unwrap();
    FileCatalog::new(FileSystemConfig::FileSystem {
        root_directory: warehouse_path.to_string(),
    })
    .unwrap()
}

// Test util function to get iceberg schema,
pub(crate) async fn get_test_schema() -> IcebergResult<Schema> {
    let field = NestedField::required(
        /*id=*/ 1,
        "field_name".to_string(),
        IcebergType::Primitive(PrimitiveType::Int),
    );
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(field)])
        .build()?;

    Ok(schema)
}

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::cred_utils as gcs_cred_utils;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::parquet_utils;
use crate::storage::iceberg::table_property;

use std::collections::HashMap;
use std::path::Path;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::DataFile;
use iceberg::spec::TableMetadata as IcebergTableMetadata;
use iceberg::spec::{DataContentType, DataFileFormat, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use iceberg::{
    Error as IcebergError, NamespaceIdent, Result as IcebergResult, TableCreation, TableIdent,
};

/// Return whether the given manifest entry represents data files.
pub fn is_data_file_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_data_file =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Parquet;
    if !is_data_file {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Return whether the given manifest entry represents deletion vector.
pub fn is_deletion_vector_entry(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_deletion_vector = f.content_type() == DataContentType::PositionDeletes;
    if !is_deletion_vector {
        return false;
    }
    assert_eq!(f.file_format(), DataFileFormat::Puffin);
    assert!(f.referenced_data_file().is_some());
    assert!(f.content_offset().is_some());
    assert!(f.content_size_in_bytes().is_some());
    true
}

/// Return whether the given manifest entry represents file index.
pub fn is_file_index(entry: &ManifestEntry) -> bool {
    let f = entry.data_file();
    let is_file_index =
        f.content_type() == DataContentType::Data && f.file_format() == DataFileFormat::Puffin;
    if !is_file_index {
        return false;
    }
    assert!(f.referenced_data_file().is_none());
    assert!(f.content_offset().is_none());
    assert!(f.content_size_in_bytes().is_none());
    true
}

/// Create an iceberg table in the given catalog from the given namespace and table name.
/// Precondition: table doesn't exist in the given catalog.
async fn create_iceberg_table<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    table_name: &str,
    namespace_ident: NamespaceIdent,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_already_exists = catalog.namespace_exists(&namespace_ident).await?;
    if !namespace_already_exists {
        catalog
            .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
            .await?;
    }

    let iceberg_schema = IcebergArrow::arrow_schema_to_schema(arrow_schema)?;
    let tbl_creation = TableCreation::builder()
        .name(table_name.to_string())
        .location(format!(
            "{}/{}/{}",
            warehouse_uri,
            namespace_ident.to_url_string(),
            table_name
        ))
        .schema(iceberg_schema)
        .properties(table_property::create_iceberg_table_properties())
        .build();
    let table = catalog.create_table(&namespace_ident, tbl_creation).await?;
    Ok(table)
}

/// Get or create an iceberg table in the given catalog from the given namespace and table name.
///
/// There're several options:
/// - If the table doesn't exist, create a new one
/// - If the table already exists, and [drop_if_exists] true (overwrite use case), delete the table and re-create
/// - If already exists and not requested to drop (recovery use case), do nothing and return the table directly
pub(crate) async fn get_or_create_iceberg_table<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    namespace: &Vec<String>,
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    let should_create = match catalog.load_table(&table_ident).await {
        Ok(existing_table) => {
            return Ok(existing_table);
        }
        Err(_) => true,
    };

    if should_create {
        create_iceberg_table(
            catalog,
            warehouse_uri,
            table_name,
            namespace_ident,
            arrow_schema,
        )
        .await
    } else {
        unreachable!()
    }
}

/// Get iceberg table if exists.
pub(crate) async fn get_table_if_exists<C: MoonlinkCatalog + ?Sized>(
    catalog: &C,
    namespace: &Vec<String>,
    table_name: &str,
) -> IcebergResult<Option<IcebergTable>> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());

    let table_exists = catalog.table_exists(&table_ident).await?;
    if !table_exists {
        return Ok(None);
    }

    let table = catalog.load_table(&table_ident).await?;
    Ok(Some(table))
}

/// Write the given record batch in the given local file to the iceberg table (parquet file keeps unchanged).
pub(crate) async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    local_filepath: &String,
    table_metadata: &IcebergTableMetadata,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<DataFile> {
    let filename = Path::new(local_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let remote_filepath = location_generator.generate_location(&filename);

    // Import local parquet file to remote.
    filesystem_accessor
        .copy_from_local_to_remote(local_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to copy from {} to {}: {:?}",
                    local_filepath, remote_filepath, e
                ),
            )
        })?;

    // Get data file from local parquet file.
    let data_file = parquet_utils::get_data_file_from_local_parquet_file(
        local_filepath,
        remote_filepath,
        table_metadata,
    )
    .await?;
    Ok(data_file)
}

/// Copy the given local index file to iceberg table, and return filepath within iceberg table.
pub(crate) async fn upload_index_file(
    table: &IcebergTable,
    local_index_filepath: &str,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<String> {
    let filename = Path::new(local_index_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let remote_filepath = location_generator.generate_location(&filename);
    filesystem_accessor
        .copy_from_local_to_remote(local_index_filepath, &remote_filepath)
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to copy from {} to {}: {:?}",
                    local_index_filepath, remote_filepath, e
                ),
            )
        })?;
    Ok(remote_filepath)
}

/// Create iceberg [`FileIO`].
pub(crate) fn create_file_io(config: &FileSystemConfig) -> IcebergResult<FileIO> {
    match config {
        #[cfg(feature = "storage-fs")]
        FileSystemConfig::FileSystem { .. } => FileIOBuilder::new_fs_io().build(),
        #[cfg(feature = "storage-gcs")]
        FileSystemConfig::Gcs {
            endpoint,
            disable_auth,
            cred_path,
            ..
        } => {
            if *disable_auth {
                assert!(cred_path.is_none());
            }

            let mut file_io_builder = FileIOBuilder::new("GCS")
                .with_prop(iceberg::io::GCS_PROJECT_ID, "coral-ring-465417-r0");
            if let Some(endpoint) = endpoint {
                file_io_builder =
                    file_io_builder.with_prop(iceberg::io::GCS_SERVICE_PATH, endpoint);
            }
            if *disable_auth {
                file_io_builder = file_io_builder
                    .with_prop(iceberg::io::GCS_NO_AUTH, "true")
                    .with_prop(iceberg::io::GCS_ALLOW_ANONYMOUS, "true")
                    .with_prop(iceberg::io::GCS_DISABLE_CONFIG_LOAD, "true");
            } else {
                let cred_json = gcs_cred_utils::load_gcs_credentials(cred_path).map_err(|e| {
                    IcebergError::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("Failed to load GCS credential {:?}: {:?}", cred_path, e),
                    )
                })?;
                file_io_builder =
                    file_io_builder.with_prop(iceberg::io::GCS_CREDENTIALS_JSON, cred_json);
            }
            file_io_builder.build()
        }
        #[cfg(feature = "storage-s3")]
        FileSystemConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            ..
        } => {
            let mut file_io_builder = FileIOBuilder::new("s3")
                .with_prop(iceberg::io::S3_REGION, "us-west-1")
                .with_prop(iceberg::io::S3_ACCESS_KEY_ID, "aaa")
                .with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, "aaa/aaa");
            if let Some(endpoint) = endpoint {
                file_io_builder = file_io_builder.with_prop(iceberg::io::S3_ENDPOINT, endpoint);
            }
            file_io_builder.build()
        }
    }
}

/// Util function to convert the given error to iceberg "unexpected" error.
pub(crate) fn to_iceberg_error<E: std::fmt::Debug>(err: E) -> IcebergError {
    IcebergError::new(iceberg::ErrorKind::Unexpected, format!("Error: {:?}", err))
}

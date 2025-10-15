use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::logstore::StorageConfig as DeltaStorageConfig;
use deltalake::open_table_with_storage_options;
use deltalake::{open_table, operations::create::CreateBuilder, DeltaTable};
use deltalake::arrow::datatypes::{DataType, Field, Schema as DeltaSchema};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::storage::StorageConfig as MoonlinkStorgaeConfig;
use crate::CacheTrait;
use crate::Result;

/// Get storage option to access deltalake table.
fn get_storage_option(storage_config: &MoonlinkStorgaeConfig) -> HashMap<String, String> {
    let mut storage_options = HashMap::new();

    match storage_config {
        #[cfg(feature = "storage-s3")]
        MoonlinkStorgaeConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            bucket: _,
            endpoint,
        } => {
            storage_options.insert("AWS_ACCESS_KEY_ID".into(), access_key_id.clone());
            storage_options.insert("AWS_SECRET_ACCESS_KEY".into(), secret_access_key.clone());
            storage_options.insert("AWS_REGION".into(), region.clone());

            if let Some(endpoint) = endpoint {
                storage_options.insert("AWS_ENDPOINT_URL".into(), endpoint.clone());
            }
        }
        #[cfg(feature = "storage-gcs")]
        MoonlinkStorgaeConfig::Gcs {
            project,
            region,
            bucket: _,
            access_key_id,
            secret_access_key,
            endpoint,
            disable_auth,
            write_option: _,
        } => {
            storage_options.insert("GOOGLE_SERVICE_ACCOUNT".into(), project.clone());
            storage_options.insert("GOOGLE_REGION".into(), region.clone());
            storage_options.insert("GOOGLE_ACCESS_KEY_ID".into(), access_key_id.clone());
            storage_options.insert("GOOGLE_SECRET_ACCESS_KEY".into(), secret_access_key.clone());

            if let Some(endpoint) = endpoint {
                storage_options.insert("GOOGLE_ENDPOINT_URL".into(), endpoint.clone());
            }
            if *disable_auth {
                storage_options.insert("GOOGLE_DISABLE_AUTH".into(), "true".into());
            }
        }
        _ => {}
    }

    storage_options
}

/// Util function to convert arrow schema to delta schema.
fn to_deltalake_arrow_schema(schema: &arrow_schema::Schema) -> DeltaSchema {
    DeltaSchema::new(
        schema
            .fields()
            .iter()
            .map(|f| {
                // Clone the name, datatype, and nullability.
                // You can extend this later for metadata or dict ids if needed.
                Field::new(f.name(), f.data_type().clone(), f.is_nullable())
            })
            .collect::<Vec<_>>(),
    )
}

/// Get or create a Delta table at the given location.
///
/// - If the table doesn't exist → create a new one using the Arrow schema.
/// - If it already exists → load and return.
/// - This mirrors the Iceberg `get_or_create_iceberg_table` pattern.
#[allow(unused)]
pub(crate) async fn get_or_create_deltalake_table(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    _object_storage_cache: Arc<dyn CacheTrait>,
    _filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    config: DeltalakeTableConfig,
) -> Result<DeltaTable> {
    let storage_options = get_storage_option(&config.data_accessor_config.storage_config);

    println!("storage option = {:?}", storage_options);
    println!("location = {:?}", config.location);

    let table_uri = Url::parse(&config.location).unwrap();
    match open_table_with_storage_options(table_uri.clone(), storage_options).await {
        Ok(existing_table) => Ok(existing_table),
        Err(_) => {
            let arrow_schema = mooncake_table_metadata.schema.as_ref();
            let delta_schema_struct = deltalake::kernel::Schema::try_from_arrow(arrow_schema).unwrap();
            let delta_schema_fields = delta_schema_struct
                .fields
                .iter()
                .map(|(_, cur_field)| cur_field.clone())
                .collect::<Vec<_>>();

            let table = CreateBuilder::new()
                .with_location(table_uri)
                .with_columns(delta_schema_fields)
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
                .await?;
            Ok(table)
        }
    }
}

#[allow(unused)]
pub(crate) async fn get_deltalake_table_if_exists(
    config: &DeltalakeTableConfig,
) -> Result<Option<DeltaTable>> {
    match open_table(config.location.clone()).await {
        Ok(table) => Ok(Some(table)),
        Err(_) => Ok(None),
    }
}

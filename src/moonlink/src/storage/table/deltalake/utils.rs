use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::{open_table, operations::create::CreateBuilder, DeltaTable};
use std::sync::Arc;
use url::Url;

use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::table::deltalake::deltalake_table_config::DeltalakeTableConfig;
use crate::CacheTrait;
use crate::Result;

/// Known schema prefix for deltalake location.
const KNOWN_SCHEME_PREFIXS: &[&str] = &["file://", "http://", "https://", "s3://", "gs://"];

/// Sanitize deltalake table location, to ensure it conforms URL style.
#[allow(unused)]
fn sanitize_deltalake_table_location(location: &str) -> String {
    if KNOWN_SCHEME_PREFIXS
        .iter()
        .any(|prefix| location.starts_with(prefix))
    {
        location.to_string()
    } else {
        // By default assumes local table.
        format!("file://{}", location)
    }
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
    let table_location = sanitize_deltalake_table_location(&config.location);
    match open_table(Url::parse(&table_location)?).await {
        Ok(existing_table) => Ok(existing_table),
        Err(_) => {
            let arrow_schema = mooncake_table_metadata.schema.as_ref();
            let delta_schema_struct = deltalake::kernel::Schema::try_from_arrow(arrow_schema)?;
            let delta_schema_fields: Vec<deltalake::kernel::StructField> =
                delta_schema_struct.fields().cloned().collect();

            let table = CreateBuilder::new()
                .with_location(config.location.clone())
                .with_columns(delta_schema_fields)
                .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
                .await?;
            Ok(table)
        }
    }
}

#[allow(unused)]
fn get_deltalake_table_url(location: &str) -> Result<Url> {
    if KNOWN_SCHEME_PREFIXS
        .iter()
        .any(|prefix| location.starts_with(prefix))
    {
        let url = Url::parse(location)?;
        return Ok(url);
    }
    let url = Url::from_file_path(location).map_err(|_| url::ParseError::RelativeUrlWithoutBase)?;
    Ok(url)
}

#[allow(unused)]
pub(crate) async fn get_deltalake_table_if_exists(
    config: &DeltalakeTableConfig,
) -> Result<Option<DeltaTable>> {
    let table_url = get_deltalake_table_url(&config.location)?;
    match open_table(table_url).await {
        Ok(table) => Ok(Some(table)),
        Err(_) => Ok(None),
    }
}

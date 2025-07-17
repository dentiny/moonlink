use crate::base_metadata_store::MetadataStoreTrait;
use crate::error::Result;
use crate::sqlite::sqlite_metadata_store::SqliteMetadataStore;

/// A factory function to create metadata storage.
/// Return [`None`] if current database is not managed by moonlink.
pub async fn create_metadata_store_accessor(
    base_directory: &str,
) -> Result<Box<dyn MetadataStoreTrait>> {
    let sqlite_metadata_storage = SqliteMetadataStore::new_with_directory(base_directory).await?;
    Ok(Box::new(sqlite_metadata_storage))
}

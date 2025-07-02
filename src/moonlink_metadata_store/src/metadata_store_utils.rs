use crate::base_metadata_store::MetadataStoreTrait;
use crate::error::Result;
use crate::postgres::pg_metadata_store::PgMetadataStore;

/// A factory function to create metadata storage.
/// Return [`None`] if current database is not managed by moonlink.
pub async fn create_metadata_storage(uri: &str) -> Result<Option<Box<dyn MetadataStoreTrait>>> {
    let pg_metadata_storage = PgMetadataStore::new(uri).await?;
    if let Some(pg_metadata_storage) = pg_metadata_storage {
        return Ok(Some(Box::new(pg_metadata_storage)));
    }
    Ok(None)
}

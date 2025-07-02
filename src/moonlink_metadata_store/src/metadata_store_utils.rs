use crate::base_metadata_store::MetadataStoreTrait;
use crate::error::Result;
use crate::postgres::pg_metadata_store::PgMetadataStore;

/// A factory function to create metadata storage.
/// Return [`None`] if current database is not managed by moonlink.
pub fn create_metadata_storage(uri: String) -> Result<Box<dyn MetadataStoreTrait>> {
    let pg_metadata_storage = PgMetadataStore::new(uri)?;
    Ok(Box::new(pg_metadata_storage))
}

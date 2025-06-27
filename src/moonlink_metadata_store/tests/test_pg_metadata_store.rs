mod common;

use moonlink_metadata_store::base_metadata_store::MetadataStoreTrait;
use moonlink_metadata_store::PgMetadataStore;

use common::test_environment::*;
use common::test_utils::*;

/// Test connection string.
const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
/// Test table id.
const TABLE_ID: u32 = 0;

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_table_metadata_store_and_load() {
        let _test_environment = TestEnvironment::new(URI).await;
        let metadata_store = PgMetadataStore::new(URI).await.unwrap();
        let moonlink_table_config = get_moonlink_table_config();

        // Store moonlink table config to metadata storage.
        metadata_store
            .store_table_config(
                TABLE_ID,
                /*table_name=*/ "table",
                moonlink_table_config.clone(),
            )
            .await
            .unwrap();

        // Load moonlink table config from metadata config.
        let actual_config = metadata_store.load_table_config(TABLE_ID).await.unwrap();
        assert_eq!(moonlink_table_config, actual_config);
    }

    /// Test scenario: load from non-existent row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_table_metadata_load_from_non_existent_table() {
        let _test_environment = TestEnvironment::new(URI).await;
        let metadata_store = PgMetadataStore::new(URI).await.unwrap();

        // Load moonlink table config from metadata config.
        let res = metadata_store.load_table_config(TABLE_ID).await;
        assert!(res.is_err());
    }

    /// Test scenario: store for duplicate table ids.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_table_metadata_store_for_duplicate_tables() {
        let _test_environment = TestEnvironment::new(URI).await;
        let metadata_store = PgMetadataStore::new(URI).await.unwrap();
        let moonlink_table_config = get_moonlink_table_config();

        // Store moonlink table config to metadata storage.
        metadata_store
            .store_table_config(
                TABLE_ID,
                /*table_name=*/ "table",
                moonlink_table_config.clone(),
            )
            .await
            .unwrap();

        // Store moonlink table config to metadata storage.
        let res = metadata_store
            .store_table_config(
                TABLE_ID,
                /*table_name=*/ "table",
                moonlink_table_config.clone(),
            )
            .await;
        assert!(res.is_err());
    }
}

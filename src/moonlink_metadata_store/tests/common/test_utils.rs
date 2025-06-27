use moonlink::{IcebergTableConfig, MoonlinkTableConfig};

/// Test utils for postgres metadata storage tests.
///
/// Create a moonlink table config for test.
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            warehouse_uri: "/tmp/test_warehouse_uri".to_string(),
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
        },
        ..Default::default()
    }
}

use moonlink::{FileSystemConfig, IcebergTableConfig, MoonlinkTableConfig};

/// Test utils for postgres metadata storage tests.
///
/// Create a moonlink table config for test.
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            filesystem_config: FileSystemConfig::FileSystem {
                root_directory: "/tmp/iceberg_warehouse".to_string(),
            },
        },
        ..Default::default()
    }
}

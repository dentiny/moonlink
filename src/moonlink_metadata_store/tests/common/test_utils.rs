use moonlink::{FileSystemConfig, IcebergTableConfig, MoonlinkTableConfig, MoonlinkTableSecret};

/// Test utils for postgres metadata storage tests.
///
/// Create a moonlink table config for test.
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            filesystem_config: FileSystemConfig::FileSystem {
                root_directory: "/tmp/test_warehouse_uri".to_string(),
            },
        },
        ..Default::default()
    }
}

/// Create a moonlink secret entry for test.
pub(crate) fn get_moonlink_secret_entry() -> MoonlinkTableSecret {
    MoonlinkTableSecret {
        secret_type: moonlink::MoonlinkSecretType::Unknown,
        key_id: "key-id".to_string(),
        secret: "secret".to_string(),
        endpoint: None,
        region: None,
    }
}

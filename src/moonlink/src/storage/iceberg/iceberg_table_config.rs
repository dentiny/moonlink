use crate::FileSystemConfig;

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTableConfig {
    /// Table warehouse location.
    pub warehouse_uri: String,
    /// Namespace for the iceberg table.
    pub namespace: Vec<String>,
    /// Iceberg table name.
    pub table_name: String,
    // Filesystem config.
    pub filesystem_config: FileSystemConfig,
}

impl IcebergTableConfig {
    const DEFAULT_WAREHOUSE_URI: &str = "/tmp/moonlink_iceberg";
    const DEFAULT_NAMESPACE: &str = "namespace";
    const DEFAULT_TABLE: &str = "table";
}

impl Default for IcebergTableConfig {
    fn default() -> Self {
        Self {
            warehouse_uri: Self::DEFAULT_WAREHOUSE_URI.to_string(),
            namespace: vec![Self::DEFAULT_NAMESPACE.to_string()],
            table_name: Self::DEFAULT_TABLE.to_string(),
            filesystem_config: FileSystemConfig::FileSystem {
                root_directory: Self::DEFAULT_WAREHOUSE_URI.to_string(),
            },
        }
    }
}

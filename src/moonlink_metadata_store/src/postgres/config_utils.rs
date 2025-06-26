use crate::error::{Error, Result};
use moonlink::{IcebergTableConfig, MoonlinkTableConfig};
/// This module contains util functions related to moonlink config.
use serde::{Deserialize, Serialize};

/// Struct for iceberg table config.
/// Notice it's a subset of [`IcebergTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct IcebergTableConfigForPersistence {
    /// Table warehouse location.
    warehouse_uri: String,
    /// Namespace for the iceberg table.
    namespace: String,
    /// Iceberg table name.
    table_name: String,
}

/// Struct for moonlink table config.
/// Notice it's a subset of [`MoonlinkTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoonlinkTableConfigForPersistence {
    /// Iceberg table configuration.
    iceberg_table_config: IcebergTableConfigForPersistence,
}

/// Deserialize json value to moonlink table config.
pub(crate) fn deserialze_moonlink_table_config(
    config: serde_json::Value,
) -> Result<MoonlinkTableConfig> {
    let parsed: MoonlinkTableConfigForPersistence = serde_json::from_value(config)?;

    let moonlink_table_config = MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            warehouse_uri: parsed.iceberg_table_config.warehouse_uri,
            namespace: vec![parsed.iceberg_table_config.namespace],
            table_name: parsed.iceberg_table_config.table_name,
            ..Default::default()
        },
        ..Default::default()
    };

    Ok(moonlink_table_config)
}

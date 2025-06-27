use crate::error::Result;
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

/// Serialize moonlink table config into string.
/// TODO(hjiang): Handle namespace better.
pub(crate) fn serialize_moonlink_table_string(
    moonlink_table_config: MoonlinkTableConfig,
) -> Result<serde_json::Value> {
    let iceberg_config = moonlink_table_config.iceberg_table_config;
    let persisted = MoonlinkTableConfigForPersistence {
        iceberg_table_config: IcebergTableConfigForPersistence {
            warehouse_uri: iceberg_config.warehouse_uri,
            namespace: iceberg_config.namespace[0].to_string(),
            table_name: iceberg_config.table_name,
        },
    };

    let config_json = serde_json::to_value(&persisted)?;
    Ok(config_json)
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
        },
        ..Default::default()
    };

    Ok(moonlink_table_config)
}

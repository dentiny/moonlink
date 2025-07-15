use crate::error::Result;
use moonlink::{
    FileSystemConfig, IcebergTableConfig, MooncakeTableConfig, MoonlinkSecretType,
    MoonlinkTableConfig, MoonlinkTableSecret,
};
/// This module contains util functions related to moonlink config.
use serde::{Deserialize, Serialize};
use url::Url;

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

impl IcebergTableConfigForPersistence {
    /// Get bucket for iceberg table config, only applies to object storage backend.
    #[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
    fn get_bucket_name(&self) -> Option<String> {
        if let Ok(url) = Url::parse(&self.warehouse_uri) {
            return Some(url.host_str()?.to_string());
        }
        None
    }
}

/// Struct for moonlink table config.
/// Notice it's a subset of [`MoonlinkTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoonlinkTableConfigForPersistence {
    /// Iceberg table configuration.
    iceberg_table_config: IcebergTableConfigForPersistence,
}

/// Parse moonlink table config into json value to persist into postgres, and return the secret entry.
/// TODO(hjiang): Handle namespace better.
pub(crate) fn parse_moonlink_table_config(
    moonlink_table_config: MoonlinkTableConfig,
) -> Result<(serde_json::Value, Option<MoonlinkTableSecret>)> {
    // Serialize mooncake table config.
    let iceberg_config = moonlink_table_config.iceberg_table_config;
    let persisted = MoonlinkTableConfigForPersistence {
        iceberg_table_config: IcebergTableConfigForPersistence {
            warehouse_uri: iceberg_config.filesystem_config.get_root_path(),
            namespace: iceberg_config.namespace[0].to_string(),
            table_name: iceberg_config.table_name,
        },
    };
    let config_json = serde_json::to_value(&persisted)?;

    // Extract table secret entry.
    let security_metadata_entry = iceberg_config
        .filesystem_config
        .extract_security_metadata_entry();

    Ok((config_json, security_metadata_entry))
}

/// Recover filesystem config from persisted config and secret.
fn recover_filesystem_config(
    persisted_config: &MoonlinkTableConfigForPersistence,
    secret_entry: Option<MoonlinkTableSecret>,
) -> FileSystemConfig {
    if let Some(secret_entry) = secret_entry {
        match &secret_entry.secret_type {
            #[cfg(feature = "storage-gcs")]
            MoonlinkSecretType::Gcs => {
                return FileSystemConfig::Gcs {
                    project: secret_entry.project.unwrap(),
                    region: secret_entry.region.unwrap(),
                    bucket: persisted_config
                        .iceberg_table_config
                        .get_bucket_name()
                        .unwrap(),
                    access_key_id: secret_entry.key_id,
                    secret_access_key: secret_entry.secret,
                    endpoint: secret_entry.endpoint,
                    disable_auth: false,
                };
            }
            #[cfg(feature = "storage-s3")]
            MoonlinkSecretType::S3 => {
                return FileSystemConfig::S3 {
                    access_key_id: secret_entry.key_id,
                    secret_access_key: secret_entry.secret,
                    region: secret_entry.region.unwrap(),
                    bucket: persisted_config
                        .iceberg_table_config
                        .get_bucket_name()
                        .unwrap(),
                    endpoint: secret_entry.endpoint,
                };
            }
            #[cfg(feature = "storage-fs")]
            MoonlinkSecretType::FileSystem => {
                return FileSystemConfig::FileSystem {
                    root_directory: persisted_config.iceberg_table_config.warehouse_uri.clone(),
                };
            }
        }
    }
    FileSystemConfig::FileSystem {
        root_directory: persisted_config.iceberg_table_config.warehouse_uri.clone(),
    }
}

/// Deserialize json value to moonlink table config.
pub(crate) fn deserialze_moonlink_table_config(
    serialized_config: serde_json::Value,
    secret_entry: Option<MoonlinkTableSecret>,
) -> Result<MoonlinkTableConfig> {
    let parsed: MoonlinkTableConfigForPersistence = serde_json::from_value(serialized_config)?;
    let filesystem_config = recover_filesystem_config(&parsed, secret_entry);

    // TODO(hjiang): Need to recover iceberg table config from metadata.
    let moonlink_table_config = MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec![parsed.iceberg_table_config.namespace],
            table_name: parsed.iceberg_table_config.table_name,
            filesystem_config,
        },
        mooncake_table_config: MooncakeTableConfig::default(),
    };

    Ok(moonlink_table_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    use moonlink::{MooncakeTableConfig, MoonlinkTableConfig};

    #[test]
    fn test_moonlink_table_config_serde() {
        let old_moonlink_table_config = MoonlinkTableConfig {
            iceberg_table_config: IcebergTableConfig::default(),
            mooncake_table_config: MooncakeTableConfig::default(),
        };
        let (serialized_persisted_config, secret_entry) =
            parse_moonlink_table_config(old_moonlink_table_config.clone()).unwrap();
        let new_moonlink_table_config =
            deserialze_moonlink_table_config(serialized_persisted_config, secret_entry).unwrap();
        assert_eq!(old_moonlink_table_config, new_moonlink_table_config);
    }

    #[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
    #[test]
    fn test_get_bucket_name() {
        // Test on S3 bucket.
        let config = IcebergTableConfigForPersistence {
            warehouse_uri: "s3://my-bucket-name/path/to/table".to_string(),
            namespace: "test_ns".to_string(),
            table_name: "test_table".to_string(),
        };
        assert_eq!(config.get_bucket_name(), Some("my-bucket-name".to_string()));

        // Test on GCS bucket.
        let config = IcebergTableConfigForPersistence {
            warehouse_uri: "gs://my-bucket-name/path/to/table".to_string(),
            namespace: "test_ns".to_string(),
            table_name: "test_table".to_string(),
        };
        assert_eq!(config.get_bucket_name(), Some("my-bucket-name".to_string()));
    }
}

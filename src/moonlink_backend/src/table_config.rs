use moonlink::{
    FileIndexMergeConfig, FileSystemConfig, IcebergTableConfig, MooncakeTableConfig,
    MoonlinkTableConfig,
};
/// Configuration on table creation.
use serde::{Deserialize, Serialize};

/// Default namespace for all iceberg tables.
const DEFAULT_ICEBERG_NAMESPACE: &str = "default";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectStorageConfig {
    /// Either "s3" or "gcs".
    pub backend_type: String,
    /// Secret key id.
    pub key_id: String,
    /// Secret.
    pub secret: String,
    /// Bucket name.
    pub bucket: String,
    /// Project where the bucket belongs to.
    pub project: Option<String>,
    /// Endpoint for the object storage.
    pub endpoint: Option<String>,
    /// Region for the object storage.
    pub region: Option<String>,
}

impl ObjectStorageConfig {
    /// Get filesystem config.
    pub(crate) fn take_as_filesystem_config(&self) -> FileSystemConfig {
        let backend = self.backend_type.to_lowercase();
        match backend.as_str() {
            #[cfg(feature = "storage-s3")]
            "s3" => FileSystemConfig::S3 {
                access_key_id: self.key_id.clone(),
                secret_access_key: self.secret.clone(),
                region: self.region.as_ref().unwrap().to_string(),
                bucket: self.bucket.clone(),
                endpoint: self.endpoint.clone(),
            },
            #[cfg(feature = "storage-gcs")]
            "gcs" => FileSystemConfig::Gcs {
                project: self.project.as_ref().unwrap().to_string(),
                region: self.region.as_ref().unwrap().to_string(),
                bucket: self.bucket.clone(),
                access_key_id: self.key_id.clone(),
                secret_access_key: self.secret.clone(),
                endpoint: self.endpoint.clone(),
                disable_auth: false,
            },
            _ => panic!("Unrecognizable object storage config {:?}", &self),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FsConfig {
    /// Local filesystem directory.
    pub directory: String,
}

/// Storage backend configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum IcebergStorageConfig {
    ObjectStorageConfig(ObjectStorageConfig),
    FsStorageConfig(String),
}

impl IcebergStorageConfig {
    /// Get filesystem config.
    pub(crate) fn take_as_filesystem_config(self) -> FileSystemConfig {
        match self {
            IcebergStorageConfig::FsStorageConfig(root_directory) => {
                FileSystemConfig::FileSystem { root_directory }
            }
            IcebergStorageConfig::ObjectStorageConfig(obj_store_config) => {
                obj_store_config.take_as_filesystem_config()
            }
        }
    }
}

/// Mooncake table config.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableConfig {
    /// Whether background regular index merge is enabled.
    pub enable_index_merge: bool,
}

impl TableConfig {
    /// Convert to mooncake table config.
    pub(crate) fn take_as_mooncake_table_config(
        self,
        temp_files_dir: String,
    ) -> MooncakeTableConfig {
        let index_merge_config = if self.enable_index_merge {
            FileIndexMergeConfig::enabled()
        } else {
            FileIndexMergeConfig::disabled()
        };

        let mut mooncake_table_config = MooncakeTableConfig::new(temp_files_dir);
        mooncake_table_config.file_index_config = index_merge_config;
        mooncake_table_config
    }
}

/// Mooncake table configuration specified at creation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableCreationConfig {
    /// Mooncake table configuration.
    pub mooncake_creation_config: TableConfig,
    /// Object storage configuration, use local filesystem if unassigned.
    pub storage_creation_config: IcebergStorageConfig,
}

impl TableCreationConfig {
    /// Convert to moonlink config.
    pub(crate) fn take_as_moonlink_config(
        self,
        temp_files_dir: String,
        mooncake_table_id: String,
    ) -> MoonlinkTableConfig {
        MoonlinkTableConfig {
            mooncake_table_config: self
                .mooncake_creation_config
                .take_as_mooncake_table_config(temp_files_dir),
            iceberg_table_config: IcebergTableConfig {
                namespace: vec![DEFAULT_ICEBERG_NAMESPACE.to_string()],
                table_name: mooncake_table_id,
                filesystem_config: self.storage_creation_config.take_as_filesystem_config(),
            },
        }
    }
}

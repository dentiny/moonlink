use moonlink::{
    AccessorConfig as IcebergConfig, DataCompactionConfig, FileIndexMergeConfig,
    IcebergTableConfig, MooncakeTableConfig, MoonlinkTableConfig,
};
/// Configuration on table creation.
use serde::{Deserialize, Serialize};

/// Default namespace for all iceberg tables.
const DEFAULT_ICEBERG_NAMESPACE: &str = "default";

/// Mooncake table config.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MooncakeConfig {
    /// Whether background regular index merge is enabled.
    pub enable_index_merge: bool,
    /// Whether background regular data compaction is enabled.
    pub enable_data_compaction: bool,
}

impl MooncakeConfig {
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
        let data_compaction_config = if self.enable_data_compaction {
            DataCompactionConfig::enabled()
        } else {
            DataCompactionConfig::disabled()
        };

        let mut mooncake_table_config = MooncakeTableConfig::new(temp_files_dir);
        mooncake_table_config.file_index_config = index_merge_config;
        mooncake_table_config.data_compaction_config = data_compaction_config;
        mooncake_table_config
    }
}

/// Mooncake table configuration specified at creation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableConfig {
    /// Mooncake table configuration.
    pub mooncake_config: MooncakeConfig,
    /// Iceberg storage config.
    pub iceberg_config: IcebergConfig,
}

impl TableConfig {
    /// Convert to moonlink config.
    pub(crate) fn take_as_moonlink_config(
        self,
        temp_files_dir: String,
        mooncake_table_id: String,
    ) -> MoonlinkTableConfig {
        MoonlinkTableConfig {
            mooncake_table_config: self
                .mooncake_config
                .take_as_mooncake_table_config(temp_files_dir),
            iceberg_table_config: IcebergTableConfig {
                namespace: vec![DEFAULT_ICEBERG_NAMESPACE.to_string()],
                table_name: mooncake_table_id,
                accessor_config: self.iceberg_config,
            },
        }
    }
}

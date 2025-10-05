use crate::storage::filesystem::accessor_config::AccessorConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeltalakeTableConfig {
    /// Deltalake table name.
    pub table_name: String,
    /// Deltalake location.
    pub location: String,
    /// Accessor config for accessing data files.
    pub data_accessor_config: AccessorConfig,
}

/// Metadata on WAL persistence information.
use serde::{Deserialize, Serialize};

/// WAL persistence metadata, which will be persisted into iceberg snapshot property.
/// WARNING: it's supposed to be small.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WalPersistenceMetadata {
    /// Persisted WAL file number.
    pub(crate) persisted_file_num: u32,
}

use iceberg::spec::TableMetadata;

use crate::storage::iceberg::iceberg_table_manager::{
    MOONCAKE_TABLE_FLUSH_LSN, MOONCAKE_WAL_METADATA,
};
use crate::storage::wal::wal_persistence_metadata::WalPersistenceMetadata;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;

/// This file contains util functions on iceberg snapshot.
///
/// Moonlink snapshot properties.
pub(super) struct SnapshotProperty {
    /// Iceberg flush LSN.
    pub(super) flush_lsn: Option<u64>,
    /// WAL persisted metadata.
    pub(super) wal_persisted_metadata: Option<WalPersistenceMetadata>,
}

/// Get moonlink customized snapshot
pub(super) fn get_snapshot_properties(
    table_metadata: &TableMetadata,
) -> IcebergResult<SnapshotProperty> {
    let current_snapshot = table_metadata.current_snapshot().unwrap();
    let snapshot_summary = current_snapshot.summary();

    // Extract flush LSN.
    let mut flush_lsn: Option<u64> = None;
    if let Some(lsn) = snapshot_summary
        .additional_properties
        .get(MOONCAKE_TABLE_FLUSH_LSN)
    {
        flush_lsn = Some(lsn.parse().unwrap());
    }

    // Extract WAL persisted metadata.
    let mut wal_persisted_metadata: Option<WalPersistenceMetadata> = None;
    if let Some(wal) = snapshot_summary
        .additional_properties
        .get(MOONCAKE_WAL_METADATA)
    {
        let parsed_wal = serde_json::from_str(wal).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("failed to parse WAL metadata {wal}: {e:?}"),
            )
        })?;
        wal_persisted_metadata = Some(parsed_wal);
    }

    Ok(SnapshotProperty {
        flush_lsn,
        wal_persisted_metadata,
    })
}

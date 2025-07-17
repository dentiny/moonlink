/// This module interacts with iceberg snapshot status, which corresponds to one mooncake table.
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::event_sync::EventSyncReceiver;
use crate::Result;
use crate::TableEvent;

/// At most one outstanding snapshot request is allowed.
pub struct TableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of a drop table operation.
    drop_table_completion_rx: Option<oneshot::Receiver<Result<()>>>,
    /// Channel to observe latest flush LSN reported by iceberg.
    flush_lsn_rx: watch::Receiver<u64>,
    /// Sender which is used to create notification at latest force snapshot completion.
    force_snapshot_completion_tx: broadcast::Sender<Result<u64>>,
    /// Sender which is used to create notification at latest data compaction completion.
    table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
}

impl TableEventManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        table_event_sync_rx: EventSyncReceiver,
    ) -> Self {
        Self {
            table_event_tx,
            drop_table_completion_rx: Some(table_event_sync_rx.drop_table_completion_rx),
            flush_lsn_rx: table_event_sync_rx.flush_lsn_rx,
            force_snapshot_completion_tx: table_event_sync_rx.force_snapshot_completion_tx,
            table_maintenance_completion_tx: table_event_sync_rx.table_maintenance_completion_tx,
        }
    }

    /// Subscribe to flush LSN updates.
    pub fn subscribe_flush_lsn(&self) -> watch::Receiver<u64> {
        self.flush_lsn_rx.clone()
    }

    /// Initiate an iceberg snapshot event, return the channel for synchronization.
    pub async fn initiate_snapshot(&mut self, lsn: u64) -> broadcast::Receiver<Result<u64>> {
        let subscriber = self.force_snapshot_completion_tx.subscribe();
        self.table_event_tx
            .send(TableEvent::ForceSnapshot { lsn: Some(lsn) })
            .await
            .unwrap();
        subscriber
    }

    /// Initiate an index merge event, return the channel for synchronization.
    /// TODO(hjiang): Error status propagation.
    pub async fn initiate_index_merge(&mut self) -> broadcast::Receiver<Result<()>> {
        let subscriber = self.table_maintenance_completion_tx.subscribe();
        self.table_event_tx
            .send(TableEvent::ForceRegularIndexMerge)
            .await
            .unwrap();
        subscriber
    }

    /// Initialte a data compaction event, return the channel for synchronization.
    pub async fn initiate_data_compaction(&mut self) -> broadcast::Receiver<Result<()>> {
        let subscriber = self.table_maintenance_completion_tx.subscribe();
        self.table_event_tx
            .send(TableEvent::ForceRegularDataCompaction)
            .await
            .unwrap();
        subscriber
    }

    /// Initialte full table maintenance event, return the channel for synchronization.
    pub async fn initiate_full_compaction(&mut self) -> broadcast::Receiver<Result<()>> {
        let subscriber = self.table_maintenance_completion_tx.subscribe();
        self.table_event_tx
            .send(TableEvent::ForceFullMaintenance)
            .await
            .unwrap();
        subscriber
    }

    /// Drop a mooncake table.
    /// Each table event manager correspond to one mooncake table, so this function should be called at most once.
    pub async fn drop_table(&mut self) -> Result<()> {
        self.table_event_tx
            .send(TableEvent::DropTable)
            .await
            .unwrap();
        self.drop_table_completion_rx.take().unwrap().await.unwrap()
    }
}

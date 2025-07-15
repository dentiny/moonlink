/// This module interacts with iceberg snapshot status, which corresponds to one mooncake table.
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::Result;
use crate::TableEvent;

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct EventSyncReceiver {
    /// Get notified when drop table completes.
    pub drop_table_completion_rx: oneshot::Receiver<Result<()>>,
    /// Get notified when iceberg flush lsn advances.
    pub flush_lsn_rx: watch::Receiver<u64>,
    /// Used to create notifier when index merge completes.
    /// TODO(hjiang): Error status propagation.
    pub index_merge_completion_tx: broadcast::Sender<()>,
    /// Used to create notifier when data compaction completes.
    pub data_compaction_completion_tx: broadcast::Sender<Result<()>>,
}

/// At most one outstanding snapshot request is allowed.
pub struct TableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of a drop table operation.
    drop_table_completion_rx: Option<oneshot::Receiver<Result<()>>>,
    /// Channel to observe latest flush LSN reported by iceberg.
    flush_lsn_rx: watch::Receiver<u64>,
    /// Sender which is used to create notification at latest index merge completion.
    /// TODO(hjiang): Error status propagation.
    index_merge_completion_tx: broadcast::Sender<()>,
    /// Sender which is used to create notification at latest data compaction completion.
    data_compaction_completion_tx: broadcast::Sender<Result<()>>,
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
            index_merge_completion_tx: table_event_sync_rx.index_merge_completion_tx,
            data_compaction_completion_tx: table_event_sync_rx.data_compaction_completion_tx,
        }
    }

    /// Subscribe to flush LSN updates.
    pub fn subscribe_flush_lsn(&self) -> watch::Receiver<u64> {
        self.flush_lsn_rx.clone()
    }

    /// Initiate an iceberg snapshot event, return the channel for synchronization.
    pub async fn initiate_snapshot(&mut self, lsn: u64) -> mpsc::Receiver<Result<()>> {
        let (tx, rx) = mpsc::channel(1);
        self.table_event_tx
            .send(TableEvent::ForceSnapshot {
                lsn: Some(lsn),
                tx: Some(tx),
            })
            .await
            .unwrap();
        rx
    }

    /// Initiate an index merge event, return the channel for synchronization.
    /// TODO(hjiang): Error status propagation.
    pub async fn initiate_index_merge(&mut self) -> broadcast::Receiver<()> {
        self.table_event_tx
            .send(TableEvent::ForceIndexMerge)
            .await
            .unwrap();
        self.index_merge_completion_tx.subscribe()
    }

    /// Initialte a data compaction event, return the channel for synchronization/
    pub async fn initiate_data_compaction(&mut self) -> broadcast::Receiver<Result<()>> {
        self.table_event_tx
            .send(TableEvent::ForceDataCompaction)
            .await
            .unwrap();
        self.data_compaction_completion_tx.subscribe()
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

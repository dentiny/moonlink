/// This module interacts with iceberg snapshot status, which corresponds to one mooncake table.
use tokio::sync::{mpsc, oneshot, watch};

use crate::Result;
use crate::TableEvent;

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct EventSyncReceiver {
    /// Get notified when drop table completes.
    pub drop_table_completion_rx: oneshot::Receiver<Result<()>>,
    /// Get notified when iceberg flush lsn advances.
    pub flush_lsn_rx: watch::Receiver<u64>,
}

/// At most one outstanding snapshot request is allowed.
pub struct TableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of a drop table operation.
    drop_table_completion_rx: Option<oneshot::Receiver<Result<()>>>,
    /// Channel to observe latest flush LSN reported by iceberg.
    flush_lsn_rx: watch::Receiver<u64>,
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

/// This module contains sender and receiver for table events synchronization.
use tokio::sync::{broadcast, oneshot, watch};

use crate::Result;

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

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct EventSyncSender {
    /// Notifies when drop table completes.
    pub drop_table_completion_tx: oneshot::Sender<Result<()>>,
    /// Notifies when iceberg flush LSN advances.
    pub flush_lsn_tx: watch::Sender<u64>,
    /// Notifies when index merge finishes.
    /// TODO(hjiang): Error status propagation.
    pub index_merge_completion_tx: broadcast::Sender<()>,
    /// Notifies when data compaction finishes.
    pub data_compaction_completion_tx: broadcast::Sender<Result<()>>,
}

/// Create table event manager sender and receiver.
pub fn create_table_event_syncer() -> (EventSyncSender, EventSyncReceiver) {
    let (drop_table_completion_tx, drop_table_completion_rx) = oneshot::channel();
    let (flush_lsn_tx, flush_lsn_rx) = watch::channel(0u64);
    let (index_merge_completion_tx, _) = broadcast::channel(64usize);
    let (data_compaction_completion_tx, _) = broadcast::channel(64usize);
    let event_sync_sender = EventSyncSender {
        drop_table_completion_tx,
        flush_lsn_tx,
        index_merge_completion_tx: index_merge_completion_tx.clone(),
        data_compaction_completion_tx: data_compaction_completion_tx.clone(),
    };
    let event_sync_receiver = EventSyncReceiver {
        drop_table_completion_rx,
        flush_lsn_rx,
        index_merge_completion_tx,
        data_compaction_completion_tx,
    };
    (event_sync_sender, event_sync_receiver)
}

use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};
use std::sync::Arc;

pub(crate) struct SnapshotCreationStats {
    latency_hist: Histogram<u64>,
}

impl SnapshotCreationStats {
    pub(crate) fn new() -> Arc<Self> {
        let meter = global::meter("snapshot_creation");
        Arc::new(SnapshotCreationStats {
            latency_hist: meter
                .u64_histogram("snapshot_creation_latency")
                .with_description("snapshot create latency histogram (milliseconds)")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        })
    }

    pub(crate) fn update(&self, t: u64, mooncake_table_id: String) {
        self.latency_hist.record(
            t,
            &[KeyValue::new(
                "moonlink.mooncake_table_id",
                mooncake_table_id,
            )],
        );
    }
}

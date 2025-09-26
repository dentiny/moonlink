use crate::observability::latency_exporter::BaseLatencyExporter;
use crate::observability::latency_guard::LatencyGuard;
use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};

pub(crate) struct SnapshotCreationStats {
    /// Otel latency histogram exporter.
    latency_hist: Histogram<u64>,
    /// Mooncake table id.
    mooncake_table_id: String,
}

impl SnapshotCreationStats {
    pub(crate) fn new(mooncake_table_id: String) -> Self {
        let meter = global::meter("snapshot_creation");
        SnapshotCreationStats {
            mooncake_table_id,
            latency_hist: meter
                .u64_histogram("snapshot_creation_latency")
                .with_description("snapshot create latency histogram (milliseconds)")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        }
    }
}

impl BaseLatencyExporter for SnapshotCreationStats {
    fn start<'a>(&'a self) -> LatencyGuard<'a> {
        LatencyGuard::new(self.mooncake_table_id.clone(), self)
    }

    fn record(&self, latency: std::time::Duration, mooncake_table_id: String) {
        self.latency_hist.record(
            latency.as_millis() as u64,
            &[KeyValue::new(
                "moonlink.mooncake_table_id",
                mooncake_table_id,
            )],
        );
    }
}

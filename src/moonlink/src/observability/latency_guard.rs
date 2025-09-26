/// A RAII wrapper to record latency metrics.
use crate::observability::latency_exporter::BaseLatencyExporter;

pub(crate) struct LatencyGuard<'a> {
    /// Start timestamp.
    start_timestamp: std::time::Instant,
    /// Mooncake table id.
    mooncake_table_id: String,
    /// Latency exporter.
    latency_exporter: &'a dyn BaseLatencyExporter,
}

impl<'a> LatencyGuard<'a> {
    pub(crate) fn new(
        mooncake_table_id: String,
        latency_exporter: &'a dyn BaseLatencyExporter,
    ) -> Self {
        Self {
            start_timestamp: std::time::Instant::now(),
            mooncake_table_id,
            latency_exporter,
        }
    }
}

impl<'a> Drop for LatencyGuard<'a> {
    fn drop(&mut self) {
        let latency = self.start_timestamp.elapsed();
        let mooncake_table_id = std::mem::take(&mut self.mooncake_table_id);
        self.latency_exporter.record(latency, mooncake_table_id);
    }
}

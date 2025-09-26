use crate::observability::latency_guard::LatencyGuard;

/// An interface to export latency.
pub(crate) trait BaseLatencyExporter: Send + Sync {
    /// Start recording latency.
    /// Returned latency guard will automatically records latency stats.
    fn start<'a>(&'a self) -> LatencyGuard<'a>;
    /// Export latency stats.
    fn record(&self, latency: std::time::Duration, mooncake_table_id: String);
}

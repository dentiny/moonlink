#[derive(Clone, PartialEq, Eq)]
pub(crate) enum MetricsType {
    Histogram,
    Gauge,
    Sum,
}
impl std::fmt::Display for MetricsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetricsType::Histogram => "histogram",
            MetricsType::Gauge => "gauge",
            MetricsType::Sum => "sum",
        };
        write!(f, "{}", s)
    }
}

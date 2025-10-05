use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Delta Lake per-file statistics.
///
/// Reference: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stats {
    /// Total number of physical records in the file.
    #[serde(rename = "numRecords")]
    pub num_records: i64,

    /// Whether per-column min/max bounds are tight.
    #[serde(rename = "tightBounds")]
    pub tight_bounds: bool,

    /// Minimum values per column (may be nested).
    #[serde(rename = "minValues", skip_serializing_if = "Option::is_none")]
    pub min_values: Option<HashMap<String, serde_json::Value>>,

    /// Maximum values per column (may be nested).
    #[serde(rename = "maxValues", skip_serializing_if = "Option::is_none")]
    pub max_values: Option<HashMap<String, serde_json::Value>>,

    /// Null counts per column.
    #[serde(rename = "nullCount", skip_serializing_if = "Option::is_none")]
    pub null_count: Option<HashMap<String, i64>>,
}

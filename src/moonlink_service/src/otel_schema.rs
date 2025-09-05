use std::{collections::HashMap, sync::Arc};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

// ============================
// Parquet field-id aware Arrow schema builders
// ============================

fn next_md(ids: &mut i32) -> HashMap<String, String> {
    let mut md = HashMap::new();
    md.insert("PARQUET:field_id".to_string(), ids.to_string());
    *ids += 1;
    md
}

fn field_with_id(name: &str, dt: DataType, nullable: bool, ids: &mut i32) -> Field {
    Field::new(name, dt, nullable).with_metadata(next_md(ids))
}

// List<Item> where the *item* field is also tagged with an id.
fn list_of_with_id(name: &str, dt: DataType, nullable: bool, ids: &mut i32) -> DataType {
    let item = field_with_id(name, dt, nullable, ids);
    DataType::List(Arc::new(item))
}

// ---- AnyValue Struct: {string_value?, int_value?, double_value?, bool_value?, bytes_value?}
fn any_value_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id("string_value", DataType::Utf8, true, ids),
        field_with_id("int_value", DataType::Int64, true, ids),
        field_with_id("double_value", DataType::Float64, true, ids),
        field_with_id("bool_value", DataType::Boolean, true, ids),
        field_with_id("bytes_value", DataType::Binary, true, ids),
    ]))
}

/// attributes: List<Struct{ key: Utf8, value: AnyValueStruct }>
fn attributes_field(name: &str, ids: &mut i32) -> Field {
    let kv_struct = DataType::Struct(Fields::from(vec![
        field_with_id("key", DataType::Utf8, false, ids),
        field_with_id("value", any_value_struct(ids), true, ids),
    ]));
    field_with_id(
        name,
        list_of_with_id("item", kv_struct, true, ids),
        true,
        ids,
    )
}

/// kv_pairs: List<Struct{ key: Utf8, value: AnyValueStruct }>
fn kv_pairs_field(name: &str, ids: &mut i32) -> Field {
    let kv_struct = DataType::Struct(Fields::from(vec![
        field_with_id("key", DataType::Utf8, false, ids),
        field_with_id("value", any_value_struct(ids), true, ids),
    ]));
    field_with_id(
        name,
        list_of_with_id("item", kv_struct, true, ids),
        true,
        ids,
    )
}

/// One EntityRef: { type: Utf8, id_pairs: List<KV>, description_pairs: List<KV>, schema_url: Utf8 }
fn entity_ref_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id("type", DataType::Utf8, true, ids),
        // Build fresh kv_pairs (assign unique parquet field ids)
        kv_pairs_field("id_pairs", ids).clone(),
        kv_pairs_field("description_pairs", ids).clone(),
        field_with_id("schema_url", DataType::Utf8, true, ids),
    ]))
}

/// resource_entity_refs: List<entity_ref_struct>
fn entity_refs_field(ids: &mut i32) -> Field {
    field_with_id(
        "resource_entity_refs",
        list_of_with_id("item", entity_ref_struct(ids), true, ids),
        true,
        ids,
    )
}

/// Exemplar struct (int or double value) + filtered_attributes (as attributes-field)
fn exemplar_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id("time_unix_nano", DataType::Int64, false, ids),
        field_with_id("as_int", DataType::Int64, true, ids),
        field_with_id("as_double", DataType::Float64, true, ids),
        field_with_id("trace_id", DataType::FixedSizeBinary(16), true, ids),
        field_with_id("span_id", DataType::FixedSizeBinary(8), true, ids),
        attributes_field("filtered_attributes", ids),
    ]))
}

fn common_metric_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        // 0  kind
        field_with_id("kind", DataType::Utf8, false, ids),

        // 1  resource_attributes
        // TODO: resource_attributes
        attributes_field("resource_attributes", ids),

        // 2  resource_entity_refs
        // TODO: entity_refs_field
        entity_refs_field(ids), // <--- NEW at column index 2

        // 3  resource_dropped_attributes_count
        field_with_id(
            "resource_dropped_attributes_count",
            DataType::Int64,
            true,
            ids,
        ),
        // 4  resource_schema_url
        field_with_id("resource_schema_url", DataType::Utf8, true, ids),

        // 5  scope_name
        field_with_id("scope_name", DataType::Utf8, true, ids),

        // 6  scope_version
        field_with_id("scope_version", DataType::Utf8, true, ids),

        // 7  scope_attributes
        attributes_field("scope_attributes", ids),

        // 8  scope_dropped_attributes_count
        field_with_id("scope_dropped_attributes_count", DataType::Int64, true, ids),

        // 9  scope_schema_url
        field_with_id("scope_schema_url", DataType::Utf8, true, ids),

        // 10 metric_name
        field_with_id("metric_name", DataType::Utf8, false, ids),

        // 11 metric_description
        field_with_id("metric_description", DataType::Utf8, true, ids),

        // 12 metric_unit
        field_with_id("metric_unit", DataType::Utf8, true, ids),

        // 13 start_time_unix_nano
        field_with_id("start_time_unix_nano", DataType::Int64, true, ids),

        // 14 time_unix_nano
        field_with_id("time_unix_nano", DataType::Int64, false, ids),

        // 15 point_attributes
        attributes_field("point_attributes", ids),

        // 16 point_dropped_attributes_count
        field_with_id("point_dropped_attributes_count", DataType::Int64, true, ids),
    ]
}

fn number_point_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        field_with_id("number_int", DataType::Int64, true, ids),
        field_with_id("number_double", DataType::Float64, true, ids),

        field_with_id("temporality", DataType::Int32, true, ids),
        field_with_id("is_monotonic", DataType::Boolean, true, ids),

        field_with_id(
            "exemplars",
            list_of_with_id("item", exemplar_struct(ids), true, ids),
            true,
            ids,
        ),
    ]
}

fn histogram_point_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        field_with_id("hist_count", DataType::Int64, true, ids),
        field_with_id("hist_sum", DataType::Float64, true, ids),
        field_with_id("hist_min", DataType::Float64, true, ids),
        field_with_id("hist_max", DataType::Float64, true, ids),

        field_with_id(
            "explicit_bounds",
            list_of_with_id("item", DataType::Float64, true, ids),
            true,
            ids,
        ),
        field_with_id(
            "bucket_counts",
            list_of_with_id("item", DataType::Int64, true, ids),
            true,
            ids,
        ),

        field_with_id("hist_temporality", DataType::Int32, true, ids),

        field_with_id(
            "hist_exemplars",
            list_of_with_id("item", exemplar_struct(ids), true, ids),
            true,
            ids,
        ),
    ]
}

/// Unified Arrow schema for Gauge / Sum / Histogram rows (one row per datapoint).
pub fn otlp_metrics_gsh_schema() -> SchemaRef {
    let mut ids = 1; // start counter
    let mut fields = Vec::new();
    fields.extend(common_metric_fields(&mut ids));
    fields.extend(number_point_fields(&mut ids));
    fields.extend(histogram_point_fields(&mut ids));
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use crate::otel_to_moonlink_pb::export_metrics_to_moonlink_rows;

    use super::*;
    use moonlink::row::proto_to_moonlink_row;
    use tempfile::tempdir;
    use moonlink::{AccessorConfig, FileSystemAccessor, FsRetryConfig, FsTimeoutConfig, IcebergTableConfig, MooncakeTable, MooncakeTableConfig, MooncakeTableMetadata, ObjectStorageCache, ObjectStorageCacheConfig, StorageConfig, WalConfig, WalManager};
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, EntityRef, InstrumentationScope, KeyValue,
    };
    use moonlink_proto::moonlink::{row_value, Array, RowValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn make_req_with_metrics(
        metrics: Vec<Metric>,
        resource_attrs: Vec<KeyValue>,
        resource_entity_refs: Vec<EntityRef>,
        scope_name: &str,
        scope_attrs: Vec<KeyValue>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    dropped_attributes_count: 0,
                    entity_refs: resource_entity_refs,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: scope_name.to_string(),
                        version: "".into(),
                        attributes: scope_attrs,
                        dropped_attributes_count: 0,
                    }),
                    metrics,
                    schema_url: "".into(),
                }],
                schema_url: "".into(),
            }],
        }
    }

    fn kv_str(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.to_string())),
            }),
        }
    }
    fn kv_bool(key: &str, val: bool) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::BoolValue(val)),
            }),
        }
    }
    fn kv_i64(key: &str, val: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(val)),
            }),
        }
    }
    fn any_array(vals: Vec<AnyValue>) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values: vals },
            )),
        }
    }
    fn any_kvlist(kvs: Vec<KeyValue>) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvs },
            )),
        }
    }
    fn as_bytes(rv: &RowValue) -> Option<Vec<u8>> {
        match rv.kind.as_ref()? {
            row_value::Kind::Bytes(b) => Some(b.clone().to_vec()),
            _ => None,
        }
    }
    fn as_i32(rv: &RowValue) -> Option<i32> {
        match rv.kind.as_ref()? {
            row_value::Kind::Int32(v) => Some(*v),
            _ => None,
        }
    }
    fn as_i64(rv: &RowValue) -> Option<i64> {
        match rv.kind.as_ref()? {
            row_value::Kind::Int64(v) => Some(*v),
            _ => None,
        }
    }
    fn as_f64(rv: &RowValue) -> Option<f64> {
        match rv.kind.as_ref()? {
            row_value::Kind::Float64(v) => Some(*v),
            _ => None,
        }
    }
    fn as_bool(rv: &RowValue) -> Option<bool> {
        match rv.kind.as_ref()? {
            row_value::Kind::Bool(v) => Some(*v),
            _ => None,
        }
    }
    fn as_array(rv: &RowValue) -> Option<&Array> {
        match rv.kind.as_ref()? {
            row_value::Kind::Array(a) => Some(a),
            _ => None,
        }
    }
    fn is_null(rv: &RowValue) -> bool {
        matches!(rv.kind, Some(row_value::Kind::Null(_)))
    }

    #[tokio::test]
    async fn test_table_creation_and_ingestion() {
        let table_temp_dir = tempdir().unwrap();
        let table_path = table_temp_dir.path().to_str().unwrap().to_string();
        let iceberg_table_config = IcebergTableConfig::default();
        let table_config = MooncakeTableConfig::new(table_path.clone());
        let wal_config = WalConfig::default();
        let wal_manager = WalManager::new(&wal_config);
        let object_storage_cache_config = ObjectStorageCacheConfig::new(
            /*max_bytes=*/u64::MAX, 
            /*cache_directory=*/table_path.clone(), 
            /*optimize_local_filesystem=*/true);
        let object_storage_cache = Arc::new(ObjectStorageCache::new(object_storage_cache_config));
        let storage_config = StorageConfig::FileSystem { root_directory: table_path.clone(), atomic_write_dir: None };
        let accessor_config = AccessorConfig {
            storage_config,
            timeout_config: FsTimeoutConfig::default(),
            retry_config: FsRetryConfig::default(),
            chaos_config: None,
        };
        let table_filesystem_accessor = Arc::new(FileSystemAccessor::new(accessor_config));

        println!("schema = {:?}", otlp_metrics_gsh_schema());

        let mut table = MooncakeTable::new(
            otlp_metrics_gsh_schema().as_ref().clone(),
            /*name=*/"table".to_string(), 
            /*table_id=*/0, 
            /*base_path=*/std::path::PathBuf::from(table_path.clone()),
            iceberg_table_config, 
            table_config, 
            wal_manager, 
            object_storage_cache, 
            table_filesystem_accessor,
        ).await.unwrap();

        let dp = NumberDataPoint {
            attributes: vec![kv_str("dp_k", "dp_v")],
            start_time_unix_nano: 11,
            time_unix_nano: 22,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                    std::f64::consts::PI,
                ),
            ),
            exemplars: vec![],
            flags: 0,
        };

        let metric = Metric {
            name: "latency".into(),
            description: "".into(),
            unit: "ms".into(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
        };
        let req = make_req_with_metrics(
            vec![metric],
            vec![kv_str("res_k", "res_v")],
            vec![EntityRef {
                r#type: "service".into(),
                id_keys: vec!["res_k".into()],
                description_keys: vec![],
                schema_url: "".into(),
            }],
            "myscope",
            vec![kv_bool("scope_ok", true)],
        );
        let row_pbs = export_metrics_to_moonlink_rows(&req);
        for cur_row_pb in row_pbs.into_iter() {
            let cur_row = proto_to_moonlink_row(cur_row_pb).unwrap();
            table.append(cur_row).unwrap();
        }
    }
}

/*
[
  "gauge",
  [["res_k", "res_v"]],
  ["service", [["res_k", "res_v"]], [], ""],
  "myscope",
  [["scope_ok", true]],
  "latency",
  "ms",
  [["dp_k", "dp_v"]],
  11,
  22,
  3.141592653589793,
  -1,
  false
]
*/

/*

Schema
├─ kind: Utf8 (required) [fid=1]

├─ resource_attributes: List<item: Struct> (nullable) [fid=10]
│  └─ item: Struct (nullable) [fid=10]
│     ├─ key: Utf8 (required) [fid=2]
│     └─ value: Struct (nullable) [fid=8]
│        ├─ string_value: Utf8 (nullable) [fid=3]
│        ├─ int_value: Int64 (nullable) [fid=4]
│        ├─ double_value: Float64 (nullable) [fid=5]
│        ├─ bool_value: Boolean (nullable) [fid=6]
│        └─ bytes_value: Binary (nullable) [fid=7]

├─ resource_dropped_attributes_count: Int64 (nullable) [fid=11]
├─ resource_schema_url: Utf8 (nullable) [fid=12]

├─ scope_name: Utf8 (nullable) [fid=13]
├─ scope_version: Utf8 (nullable) [fid=14]

├─ scope_attributes: List<item: Struct> (nullable) [fid=23]
│  └─ item: Struct (nullable) [fid=22]
│     ├─ key: Utf8 (required) [fid=15]
│     └─ value: Struct (nullable) [fid=21]
│        ├─ string_value: Utf8 (nullable) [fid=16]
│        ├─ int_value: Int64 (nullable) [fid=17]
│        ├─ double_value: Float64 (nullable) [fid=18]
│        ├─ bool_value: Boolean (nullable) [fid=19]
│        └─ bytes_value: Binary (nullable) [fid=20]

├─ scope_dropped_attributes_count: Int64 (nullable) [fid=24]
├─ scope_schema_url: Utf8 (nullable) [fid=25]

├─ metric_name: Utf8 (required) [fid=26]
├─ metric_description: Utf8 (nullable) [fid=27]
├─ metric_unit: Utf8 (nullable) [fid=28]

├─ start_time_unix_nano: Int64 (nullable) [fid=29]
├─ time_unix_nano: Int64 (required) [fid=30]

├─ point_attributes: List<item: Struct> (nullable) [fid=39]
│  └─ item: Struct (nullable) [fid=38]
│     ├─ key: Utf8 (required) [fid=31]
│     └─ value: Struct (nullable) [fid=37]
│        ├─ string_value: Utf8 (nullable) [fid=32]
│        ├─ int_value: Int64 (nullable) [fid=33]
│        ├─ double_value: Float64 (nullable) [fid=34]
│        ├─ bool_value: Boolean (nullable) [fid=35]
│        └─ bytes_value: Binary (nullable) [fid=36]

├─ point_dropped_attributes_count: Int64 (nullable) [fid=40]

├─ number_int: Int64 (nullable) [fid=41]
├─ number_double: Float64 (nullable) [fid=42]
├─ temporality: Int32 (nullable) [fid=43]
├─ is_monotonic: Boolean (nullable) [fid=44]

├─ exemplars: List<item: Struct> (nullable) [fid=60]
│  └─ item: Struct (nullable) [fid=59]
│     ├─ time_unix_nano: Int64 (required) [fid=45]
│     ├─ as_int: Int64 (nullable) [fid=46]
│     ├─ as_double: Float64 (nullable) [fid=47]
│     ├─ trace_id: FixedSizeBinary(16) (nullable) [fid=48]
│     ├─ span_id: FixedSizeBinary(8) (nullable) [fid=49]
│     └─ filtered_attributes: List<item: Struct> (nullable) [fid=58]
│        └─ item: Struct (nullable) [fid=57]
│           ├─ key: Utf8 (required) [fid=50]
│           └─ value: Struct (nullable) [fid=56]
│              ├─ string_value: Utf8 (nullable) [fid=51]
│              ├─ int_value: Int64 (nullable) [fid=52]
│              ├─ double_value: Float64 (nullable) [fid=53]
│              ├─ bool_value: Boolean (nullable) [fid=54]
│              └─ bytes_value: Binary (nullable) [fid=55]

├─ hist_count: Int64 (nullable) [fid=61]
├─ hist_sum: Float64 (nullable) [fid=62]
├─ hist_min: Float64 (nullable) [fid=63]
├─ hist_max: Float64 (nullable) [fid=64]

├─ explicit_bounds: List<Float64> (nullable) [fid=66]
│  └─ item: Float64 (nullable) [fid=65]

├─ bucket_counts: List<Int64> (nullable) [fid=68]
│  └─ item: Int64 (nullable) [fid=67]

├─ hist_temporality: Int32 (nullable) [fid=69]

├─ hist_exemplars: List<item: Struct> (nullable) [fid=85]
│  └─ item: Struct (nullable) [fid=84]
│     ├─ time_unix_nano: Int64 (required) [fid=70]
│     ├─ as_int: Int64 (nullable) [fid=71]
│     ├─ as_double: Float64 (nullable) [fid=72]
│     ├─ trace_id: FixedSizeBinary(16) (nullable) [fid=73]
│     ├─ span_id: FixedSizeBinary(8) (nullable) [fid=74]
│     └─ filtered_attributes: List<item: Struct> (nullable) [fid=83]
│        └─ item: Struct (nullable) [fid=82]
│           ├─ key: Utf8 (required) [fid=75]
│           └─ value: Struct (nullable) [fid=81]
│              ├─ string_value: Utf8 (nullable) [fid=76]
│              ├─ int_value: Int64 (nullable) [fid=77]
│              ├─ double_value: Float64 (nullable) [fid=78]
│              ├─ bool_value: Boolean (nullable) [fid=79]
│              └─ bytes_value: Binary (nullable) [fid=80]

*/
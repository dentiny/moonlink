use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    metrics::v1::{
        number_data_point, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, Sum,
    },
};

use moonlink_proto::moonlink as moonlink_pb;

/// Top-level entry: flatten all metrics datapoints in the request into records.
pub fn export_metrics_to_moonlink_rows(
    req: &ExportMetricsServiceRequest,
) -> Vec<moonlink_pb::MoonlinkRow> {
    let mut rows = Vec::new();

    for rm in &req.resource_metrics {
        let resource_attrs = &rm
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        for sm in &rm.scope_metrics {
            let scope_name = sm
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_attrs = &sm
                .scope
                .as_ref()
                .map(|s| s.attributes.as_slice())
                .unwrap_or(&[]);

            for metric in &sm.metrics {
                match metric.data.as_ref() {
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                        data_points,
                    })) => {
                        for dp in data_points {
                            rows.push(number_point_row(
                                b"gauge",
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                /*temporality*/ -1,
                                /*is_monotonic*/ false,
                            ));
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(Sum {
                        data_points,
                        aggregation_temporality,
                        is_monotonic,
                    })) => {
                        let temp = *aggregation_temporality as i32;
                        for dp in data_points {
                            rows.push(number_point_row(
                                b"sum",
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                temp,
                                *is_monotonic,
                            ));
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                        Histogram {
                            data_points,
                            aggregation_temporality,
                        },
                    )) => {
                        let temp = *aggregation_temporality as i32;
                        for dp in data_points {
                            rows.push(hist_point_row(
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                temp,
                            ));
                        }
                    }
                    // TODO(hjiang)
                    _ => {
                        panic!("Unsupported metrics type: {:?}", metric);
                    }
                }
            }
        }
    }

    rows
}

/* ------------------------------ Row builders ------------------------------ */

fn number_point_row(
    kind: &[u8], // "gauge" | "sum"
    metric: &Metric,
    resource_attrs: &[KeyValue],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &NumberDataPoint,
    temporality: i32, // -1 for gauge
    is_monotonic: bool,
) -> moonlink_pb::MoonlinkRow {
    let mut values = Vec::with_capacity(12);

    // 0: kind
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(kind.to_vec().into())),
    });
    // 1: resource_attrs
    values.push(kvs_to_rowvalue_array(resource_attrs));
    // 2: scope_name
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            scope_name.to_string().into(),
        )),
    });
    // 3: scope_attrs
    values.push(kvs_to_rowvalue_array(scope_attrs));
    // 4: metric_name
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            metric.name.clone().into(),
        )),
    });
    // 5: metric_unit
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            metric.unit.clone().into(),
        )),
    });
    // 6: point_attrs
    values.push(kvs_to_rowvalue_array(&dp.attributes));
    // 7: start_time_unix_nano
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int64(
            dp.start_time_unix_nano as i64,
        )),
    });
    // 8: time_unix_nano
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int64(
            dp.time_unix_nano as i64,
        )),
    });
    // 9: value (nullable)
    values.push(moonlink_pb::RowValue {
        kind: Some(number_value_to_rowvalue(dp)),
    });
    // 10: temporality (int32; -1 for gauge)
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int32(temporality)),
    });
    // 11: is_monotonic
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bool(is_monotonic)),
    });

    moonlink_pb::MoonlinkRow { values }
}

fn hist_point_row(
    metric: &Metric,
    resource_attrs: &[KeyValue],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &HistogramDataPoint,
    temporality: i32,
) -> moonlink_pb::MoonlinkRow {
    let mut values = Vec::with_capacity(14);

    // 0: kind
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            b"histogram".to_vec().into(),
        )),
    });
    // 1: resource_attrs
    values.push(kvs_to_rowvalue_array(resource_attrs));
    // 2: scope_name
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            scope_name.to_string().into(),
        )),
    });
    // 3: scope_attrs
    values.push(kvs_to_rowvalue_array(scope_attrs));
    // 4: metric_name
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            metric.name.clone().into(),
        )),
    });
    // 5: metric_unit
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Bytes(
            metric.unit.clone().into(),
        )),
    });
    // 6: point_attrs
    values.push(kvs_to_rowvalue_array(&dp.attributes));
    // 7: start_time_unix_nano
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int64(
            dp.start_time_unix_nano as i64,
        )),
    });
    // 8: time_unix_nano
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int64(
            dp.time_unix_nano as i64,
        )),
    });
    // 9: count
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int64(dp.count as i64)),
    });
    // 10: sum (nullable)
    values.push(match dp.sum {
        Some(v) => moonlink_pb::RowValue {
            kind: Some(moonlink_pb::row_value::Kind::Float64(v)),
        },
        None => moonlink_pb::RowValue {
            kind: Some(moonlink_pb::row_value::Kind::Null(moonlink_pb::Null {})),
        },
    });
    // 11: explicit_bounds (array of float64)
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Array(moonlink_pb::Array {
            values: dp
                .explicit_bounds
                .iter()
                .map(|v| moonlink_pb::RowValue {
                    kind: Some(moonlink_pb::row_value::Kind::Float64(*v)),
                })
                .collect(),
        })),
    });
    // 12: bucket_counts (array of int64)
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Array(moonlink_pb::Array {
            values: dp
                .bucket_counts
                .iter()
                .map(|v| moonlink_pb::RowValue {
                    kind: Some(moonlink_pb::row_value::Kind::Int64(*v as i64)),
                })
                .collect(),
        })),
    });
    // 13: temporality
    values.push(moonlink_pb::RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Int32(temporality)),
    });

    moonlink_pb::MoonlinkRow { values }
}

/* ------------------------ Attribute & value helpers ----------------------- */

fn kvs_to_rowvalue_array(kvs: &[KeyValue]) -> moonlink_pb::RowValue {
    // Encoded as: array of [ key(bytes), value(moonlink_pb::RowValue) ]
    let mut out = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let key = moonlink_pb::RowValue {
            kind: Some(moonlink_pb::row_value::Kind::Bytes(kv.key.into())),
        };
        let val = moonlink_pb::RowValue {
            kind: Some(any_to_rowvalue(kv.value.as_ref())),
        };
        out.push(moonlink_pb::RowValue {
            kind: Some(moonlink_pb::row_value::Kind::Array(moonlink_pb::Array {
                values: vec![key, val],
            })),
        });
    }
    moonlink_pb::row_value::Kind::Array(moonlink_pb::Array { values: out })
}

fn any_to_rowvalue(v: Option<&AnyValue>) -> moonlink_pb::row_value::Kind {
    match v.and_then(|a| a.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => {
            moonlink_pb::row_value::Kind::Bytes(s.to_string().into())
        }
        Some(any_value::Value::BoolValue(b)) => moonlink_pb::row_value::Kind::Bool(*b),
        Some(any_value::Value::IntValue(i)) => moonlink_pb::row_value::Kind::Int64(*i),
        Some(any_value::Value::DoubleValue(f)) => moonlink_pb::row_value::Kind::Float64(*f),
        Some(any_value::Value::BytesValue(b)) => {
            moonlink_pb::row_value::Kind::Bytes(b.clone().into())
        }
        Some(any_value::Value::ArrayValue(arr)) => {
            moonlink_pb::row_value::Kind::Array(moonlink_pb::Array {
                values: arr
                    .values
                    .iter()
                    .map(|x| moonlink_pb::RowValue {
                        kind: Some(any_to_rowvalue(Some(x))),
                    })
                    .collect(),
            })
        }
        Some(any_value::Value::KvlistValue(kvl)) => {
            // Turn nested map into array of [key, value] entries
            let entries = kvl
                .values
                .iter()
                .map(|kv| {
                    moonlink_pb::row_value::Kind::Array(moonlink_pb::Array {
                        values: vec![
                            moonlink_pb::RowValue {
                                kind: Some(moonlink_pb::row_value::Kind::Bytes(kv.key.into())),
                            },
                            moonlink_pb::RowValue {
                                kind: Some(any_to_rowvalue(kv.value.as_ref())),
                            },
                        ],
                    })
                })
                .collect();
            moonlink_pb::row_value::Kind::Array(moonlink_pb::Array { values: entries })
        }
        None => moonlink_pb::row_value::Kind::Null(moonlink_pb::Null {}),
    }
}

fn number_value_to_rowvalue(dp: &NumberDataPoint) -> moonlink_pb::row_value::Kind {
    match dp.value.as_ref() {
        Some(number_data_point::Value::AsDouble(v)) => moonlink_pb::row_value::Kind::Float64(*v),
        Some(number_data_point::Value::AsInt(v)) => {
            moonlink_pb::row_value::Kind::Float64(*v as f64)
        }
        None => moonlink_pb::row_value::Kind::Null(moonlink_pb::Null {}),
    }
}

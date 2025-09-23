use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Handler to process metrics ingestion.
use crate::error::{Error, Result};
use crate::otel::otel_schema::otlp_metrics_gsh_schema;
use crate::otel::otel_to_moonlink_pb;
use crate::rest_api::ListTablesResponse;
use moonlink_backend::REST_API_URI;
use moonlink_proto::moonlink as moonlink_pb;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, HistogramDataPoint, NumberDataPoint};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{error, warn};

/// Database which manages all moonlink internal metrics.
const DATABASE: &str = "__reserved_moonlink_internal_metrics__";
/// Metrics attributes key for mooncake table id.
const MOONCAKE_TABLE_ID_KEY: &str = "moonlink.mooncake_table_id";

#[derive(Clone)]
pub(crate) struct MetricsHandler {
    /// IP/port for REST API.
    rest_addr: String,
    /// HTTP request client, used to access REST API.
    rest_client: reqwest::Client,
    /// All table names.
    tables: Arc<Mutex<HashSet<String>>>,
    /// Moonlink backend.
    moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
}

/// Get string value from otel anyvalue.
fn anyvalue_as_str(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> Option<&str> {
    match &v.value {
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

// A helper trait so we can unify NumberDataPoint and HistogramDataPoint.
trait HasAttributes {
    fn get_attributes(&self) -> &Vec<opentelemetry_proto::tonic::common::v1::KeyValue>;
}
impl HasAttributes for NumberDataPoint {
    fn get_attributes(&self) -> &Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
        &self.attributes
    }
}
impl HasAttributes for HistogramDataPoint {
    fn get_attributes(&self) -> &Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
        &self.attributes
    }
}

// Push data points into [`result`] map.
fn handle_data_points<T>(
    service_name: &str,
    metric_name: &str,
    kind: &str,
    dps: &[T],
    req: &ExportMetricsServiceRequest,
    result: &mut HashMap<String, Vec<ExportMetricsServiceRequest>>,
) where
    T: HasAttributes,
{
    for dp in dps {
        for attr in dp.get_attributes() {
            if attr.key == MOONCAKE_TABLE_ID_KEY {
                if let Some(value) = &attr.value {
                    if let Some(mooncake_table_id) = anyvalue_as_str(value) {
                        let key =
                            format!("{service_name}.{mooncake_table_id}.{kind}.{metric_name}");
                        result.entry(key).or_default().push(req.clone());
                    }
                }
            }
        }
    }
}

/// Get metrics data points map, which maps from mooncake table id to otel export request.
fn get_metrics_map_from_request(
    req: &ExportMetricsServiceRequest,
) -> HashMap<String, Vec<ExportMetricsServiceRequest>> {
    let mut result: HashMap<String, Vec<ExportMetricsServiceRequest>> = HashMap::new();

    let mut service_name = "unknown_service";
    for rm in &req.resource_metrics {
        if let Some(resource) = &rm.resource {
            for attr in &resource.attributes {
                if attr.key == "service.name" {
                    if let Some(value) = &attr.value {
                        if let Some(s) = anyvalue_as_str(value) {
                            service_name = s;
                        }
                    }
                }
            }
        }

        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let metric_name = &metric.name;

                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        handle_data_points(
                            service_name,
                            metric_name,
                            "gauge",
                            &g.data_points,
                            req,
                            &mut result,
                        );
                    }
                    Some(Data::Sum(s)) => {
                        handle_data_points(
                            service_name,
                            metric_name,
                            "sum",
                            &s.data_points,
                            req,
                            &mut result,
                        );
                    }
                    Some(Data::Histogram(h)) => {
                        handle_data_points(
                            service_name,
                            metric_name,
                            "histogram",
                            &h.data_points,
                            req,
                            &mut result,
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    if result.is_empty() {
        warn!("Cannot find mooncake table id from the data points");
    }

    result
}

impl MetricsHandler {
    pub(crate) async fn new(
        rest_port: u16,
        moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
    ) -> Result<Self> {
        let rest_addr = format!("http://0.0.0.0:{rest_port}");
        let rest_client = reqwest::Client::new();
        let response = rest_client
            .get(format!("{rest_addr}/tables"))
            .header("content-type", "application/json")
            .send()
            .await?;
        // TODO(hjiang): Error propagation.
        if !response.status().is_success() {
            return Err(Error::http_error(response.status()));
        }

        // List all internal metrics tables.
        let response: ListTablesResponse = response.json().await?;
        let tables = response
            .tables
            .into_iter()
            .filter(|cur_table_status| cur_table_status.database == DATABASE)
            .map(|cur_table_status| cur_table_status.table)
            .collect::<HashSet<_>>();
        let tables = Arc::new(Mutex::new(tables));
        Ok(Self {
            rest_addr,
            rest_client,
            tables,
            moonlink_backend,
        })
    }

    /// Create a mooncake table for once, if it hasn't been created.
    async fn create_table_for_once(&self, mooncake_table_id: &str) -> Result<()> {
        let crafted_src_table_name = format!("{DATABASE}.{mooncake_table_id}");
        // Fake REST ingestion.
        let serialized_table_config = json!({
            "mooncake": {
                "append_only": true,
                "row_identity": "None"
            }
        })
        .to_string();

        // Table creation for duplicate table name leads to error, so intentionally place table creation under critical section.
        // Performance is not a big concern here, since it only happens when new table metrics are received.
        {
            let mut guard = self.tables.lock().await;
            if guard.contains(mooncake_table_id) {
                return Ok(());
            }

            self.moonlink_backend
                .create_table(
                    DATABASE.to_string(),
                    mooncake_table_id.to_string(),
                    crafted_src_table_name,
                    REST_API_URI.to_string(),
                    serialized_table_config,
                    Some(otlp_metrics_gsh_schema()),
                )
                .await?;
            assert!(guard.insert(mooncake_table_id.to_string()));
        }

        Ok(())
    }

    /// Insert one single row via REST API, which handles LSN internally.
    /// Here we use asynchronous ingestion as best-effort attempt without flush or snapshot semantics.
    ///
    /// For any errors encountered during ingestion, simply log and proceed.
    async fn insert_row(&self, mooncake_table_id: &str, row_pb: moonlink_pb::MoonlinkRow) {
        let mut buf = Vec::new();
        // Serialization doesn't expect failure.
        prost::Message::encode(&row_pb, &mut buf).unwrap();
        let insert_payload = json!({
            "operation": "insert",
            "request_mode": "async",
            "data": buf
        });
        let crafted_src_table_name = format!("{DATABASE}.{mooncake_table_id}");
        let response = self
            .rest_client
            .post(format!(
                "{}/ingestpb/{}",
                self.rest_addr, crafted_src_table_name
            ))
            .header("content-type", "application/json")
            .json(&insert_payload)
            .send()
            .await;
        if response.is_err() {
            error!("Failed to ingest otel data: {:?}", response.unwrap_err());
        }
    }

    /// Handle request for the incoming metrics request.
    pub(crate) async fn handle_request(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<ExportMetricsServiceResponse> {
        let table_to_map = get_metrics_map_from_request(&request);
        for (mooncake_table_id, export_requests) in table_to_map.into_iter() {
            self.create_table_for_once(&mooncake_table_id).await?;
            for cur_request in export_requests.into_iter() {
                let moonlink_row_pbs =
                    otel_to_moonlink_pb::export_metrics_to_moonlink_rows(&cur_request);
                for cur_row_pb in moonlink_row_pbs.into_iter() {
                    self.insert_row(&mooncake_table_id, cur_row_pb).await;
                }
            }
        }
        Ok(ExportMetricsServiceResponse::default())
    }
}

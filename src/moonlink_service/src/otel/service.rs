use crate::otel::otel_state::OtelState;
use crate::{Error, Result};
use axum::error_handling::HandleErrorLayer;
use axum::http::Method;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use moonlink_error::{ErrorStatus, ErrorStruct};
use opentelemetry::global;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use prost::Message;
use std::sync::Arc;
use tokio::sync::oneshot;
use tower::timeout::TimeoutLayer;
use tower::{BoxError, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

/// Default timeout for otel API calls.
const DEFAULT_REST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Default otel endpoint.
const DEFAULT_HTTP_OTEL_ENDPOINT: &str = "http://127.0.0.1:3435/v1/metrics";
/// Default flush interval (seconds) for otel exporter.
const DEFAULT_EXPORTER_FLUSH_INTERVAL_IN_SECONDS: std::time::Duration =
    std::time::Duration::from_secs(2);

pub fn create_otel_router(state: OtelState) -> Router {
    let timeout_layer = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(|err: BoxError| async move {
            if err.is::<tower::timeout::error::Elapsed>() {
                return Response::builder()
                    .status(StatusCode::REQUEST_TIMEOUT)
                    .body::<String>("request timed out".into())
                    .unwrap();
            }
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("internal middleware error".into())
                .unwrap()
        }))
        .layer(TimeoutLayer::new(DEFAULT_REST_TIMEOUT));

    Router::new()
        .route("/v1/metrics", post(handle_metrics))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::POST])
                .allow_headers(Any),
        )
        .layer(timeout_layer)
}

pub async fn start_otel_service(
    otel_port: u16,
    rest_port: u16,
    moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<()> {
    let otel_state = OtelState::new(rest_port, moonlink_backend).await?;
    let app = create_otel_router(otel_state);
    let otel_addr = format!("0.0.0.0:{otel_port}");
    info!("Starting otel API server on {}", otel_addr);

    let listener = tokio::net::TcpListener::bind(&otel_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_signal.await.ok();
        })
        .await?;

    Ok(())
}

/// Initialize exporter and reader for global meter provider, if called repeated, old provider will be overwritten.
pub(crate) fn initialize_opentelemetry_meter_provider(target: String) -> Result<()> {
    let meter_provider = match target.as_str() {
        "otel" => {
            let otel_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(DEFAULT_HTTP_OTEL_ENDPOINT)
                .with_protocol(Protocol::HttpBinary) // send protobuf message
                .build()?;

            let reader = PeriodicReader::builder(otel_exporter)
                .with_interval(DEFAULT_EXPORTER_FLUSH_INTERVAL_IN_SECONDS)
                .build();

            Ok(SdkMeterProvider::builder().with_reader(reader).build())
        }
        "stdout" => {
            let stdout_exporter = opentelemetry_stdout::MetricExporter::builder().build();
            let reader = PeriodicReader::builder(stdout_exporter)
                .with_interval(DEFAULT_EXPORTER_FLUSH_INTERVAL_IN_SECONDS)
                .build();

            Ok(SdkMeterProvider::builder().with_reader(reader).build())
        }
        bad_option => Err(Error::OtelInvalidOption(ErrorStruct::new(
            format!(
                "Invalid otel target option {:?}, should be one of 'stdout' or 'otel'",
                bad_option
            ),
            ErrorStatus::Permanent,
        ))),
    }?;

    global::set_meter_provider(meter_provider);
    Ok(())
}

async fn handle_metrics(
    State(state): State<OtelState>,
    _headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], Vec<u8>) {
    match ExportMetricsServiceRequest::decode(body) {
        Ok(req) => {
            match state.metrics_handler.handle_request(req).await {
                Ok(resp) => {
                    let bytes = resp.encode_to_vec();
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "application/x-protobuf")],
                        bytes,
                    )
                }
                // TODO(hjiang): Better error propagation.
                Err(err) => {
                    // Different from general user-facing requests, failed otel request won't be processed usually, so to detect errors we log on server side.
                    error!("Failed to process otel ingestion request: {:?}", err);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CONTENT_TYPE, "text/plain")],
                        format!("protobuf decode failed: {err}").into_bytes(),
                    )
                }
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("protobuf decode failed: {e}").into_bytes(),
        ),
    }
}

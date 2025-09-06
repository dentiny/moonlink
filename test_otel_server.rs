use axum::{
    body::Bytes,
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use tower::{BoxError, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tower::timeout::TimeoutLayer;
use axum::error_handling::HandleErrorLayer;
use axum::http::Method;
use tracing::{error, info};

const DEFAULT_REST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub struct OtelState;

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

async fn handle_metrics(
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], Vec<u8>) {
    info!("Received metrics request with {} bytes", body.len());
    info!("Headers: {:?}", headers);
    
    match ExportMetricsServiceRequest::decode(body) {
        Ok(req) => {
            info!("Successfully decoded metrics request");
            info!("Resource metrics count: {}", req.resource_metrics.len());
            
            for (i, resource_metric) in req.resource_metrics.iter().enumerate() {
                info!("Resource metric {}: {} scope metrics", i, resource_metric.scope_metrics.len());
                for (j, scope_metric) in resource_metric.scope_metrics.iter().enumerate() {
                    info!("  Scope metric {}: {} metrics", j, scope_metric.metrics.len());
                    for (k, metric) in scope_metric.metrics.iter().enumerate() {
                        info!("    Metric {}: name='{}', data points={}", 
                              k, metric.name, 
                              match &metric.data {
                                Some(opentelemetry_proto::tonic::collector::metrics::v1::metric::Data::Gauge(g)) => g.data_points.len(),
                                Some(opentelemetry_proto::tonic::collector::metrics::v1::metric::Data::Sum(s)) => s.data_points.len(),
                                Some(opentelemetry_proto::tonic::collector::metrics::v1::metric::Data::Histogram(h)) => h.data_points.len(),
                                Some(opentelemetry_proto::tonic::collector::metrics::v1::metric::Data::ExponentialHistogram(e)) => e.data_points.len(),
                                Some(opentelemetry_proto::tonic::collector::metrics::v1::metric::Data::Summary(s)) => s.data_points.len(),
                                None => 0,
                            });
                    }
                }
            }

            let resp = ExportMetricsServiceResponse::default();
            let bytes = resp.encode_to_vec();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/x-protobuf")],
                bytes,
            )
        }
        Err(e) => {
            error!("decode metrics failed: {e}");
            error!("Body content (first 100 bytes): {:?}", &body[..body.len().min(100)]);
            (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("protobuf decode failed: {e}").into_bytes(),
            )
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let state = OtelState;
    let app = create_otel_router(state);
    let addr = "0.0.0.0:3435";

    info!("Starting standalone OTLP/HTTP metrics service on http://{}/v1/metrics", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

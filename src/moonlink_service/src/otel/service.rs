use axum::error_handling::HandleErrorLayer;
use axum::http::Method;
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
use tokio::sync::oneshot;
use tower::timeout::TimeoutLayer;
use tower::{BoxError, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::error;

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

pub async fn start_otel_service(
    state: OtelState,
    port: u16,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_otel_router(state);
    let addr = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_signal.await.ok();
        })
        .await?;

    Ok(())
}

async fn handle_metrics(
    _headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], Vec<u8>) {
    match ExportMetricsServiceRequest::decode(body) {
        Ok(req) => {
            // TODO(hjiang): Need to integrate with mooncake table.
            println!("request = {req:?}");

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
            (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("protobuf decode failed: {e}").into_bytes(),
            )
        }
    }
}

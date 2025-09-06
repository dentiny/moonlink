use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn test_opentelemetry_export() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()) // respects RUST_LOG
        .try_init();

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint("http://127.0.0.1:3435/v1/metrics")
        .with_protocol(Protocol::HttpBinary)
        .build()
        .unwrap();

    // Periodic reader to push metrics every 2s
    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(2))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let meter = global::meter("basic");
    let counter = meter
        .u64_counter("test_counter")
        .with_description("a simple counter for demo purposes.")
        .with_unit("my_unit")
        .build();

    for _ in 0..10 {
        counter.add(1, &[KeyValue::new("test_key", "test_value")]);
    }

    println!("Starting to record metrics…");

    // Give the reader a chance to export once
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Flush and shutdown cleanly
    meter_provider.shutdown().unwrap();
}

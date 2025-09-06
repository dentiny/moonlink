use opentelemetry_sdk::{
    metrics::{PeriodicReader},
    runtime,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry::metrics::MeterProvider;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    println!("Starting OTEL debug test...");

    // Start the OTEL service first
    let otel_state = moonlink_service::otel::service::OtelState{};
    let otel_handle = tokio::spawn(async move {
        if let Err(e) = moonlink_service::otel::service::start_otel_service(otel_state, 3435).await {
            eprintln!("OTEL service failed: {}", e);
        }
    });

    // Give the service time to start
    println!("Waiting for OTEL service to start...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // OTLP/HTTP (protobuf) exporter pointing to your server
    println!("Creating OTLP exporter...");
    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint("http://127.0.0.1:3435")
        .build_metrics_exporter(
            Box::new(opentelemetry_sdk::metrics::reader::DefaultAggregationSelector::new()),
            Box::new(opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector::new()),
        )?;

    // Periodic reader to push metrics every 2s
    println!("Creating periodic reader...");
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(std::time::Duration::from_secs(2))
        .with_timeout(std::time::Duration::from_secs(5))
        .build();

    // Create a new meter provider with the reader
    println!("Creating meter provider...");
    let provider = opentelemetry_sdk::metrics::MeterProvider::builder()
        .with_reader(reader)
        .build();
    
    // Set it as the global provider
    opentelemetry::global::set_meter_provider(provider.clone());

    let meter = provider.meter("debug-meter");
    let hist = meter.f64_histogram("debug_latency_ms").with_unit("ms").init();
    let counter = meter.u64_counter("debug_requests_total").init();

    println!("Starting to record metrics...");

    // record some data for ~10 seconds
    for i in 0..10 {
        counter.add(1, &[]);
        hist.record(10.0 + (i as f64), &[]);
        println!("Recorded metric batch {}", i + 1);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    println!("Finished recording metrics, waiting for export...");

    // Wait for metrics to be exported
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Flush and shutdown cleanly
    println!("Shutting down meter provider...");
    provider.shutdown().unwrap();
    
    // Cancel the OTEL service
    println!("Stopping OTEL service...");
    otel_handle.abort();

    println!("Debug test completed!");
    Ok(())
}

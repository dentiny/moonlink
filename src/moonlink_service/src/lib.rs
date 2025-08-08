mod error;
pub mod rest_api;
pub mod rpc_server;

pub use error::Result;
use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};

pub struct ServiceConfig {
    /// Base location for moonlink storage (including cache files, iceberg tables, etc).
    pub base_path: String,
    /// Used for REST API as ingestion source.
    pub rest_api_port: Option<u16>,
    /// Used for moonlink standalone deployment.
    pub tcp_port: Option<u16>,
}

pub async fn start_with_config(config: ServiceConfig) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let sqlite_metadata_accessor = SqliteMetadataStore::new_with_directory(&config.base_path)
        .await
        .unwrap();
    let mut backend =
        MoonlinkBackend::new(config.base_path.clone(), Box::new(sqlite_metadata_accessor)).await?;

    if config.rest_api_port.is_some() {
        backend.initialize_event_api().await?;
    }

    let backend = Arc::new(backend);

    // Start RPC server on Unix socket
    let socket_path = std::path::PathBuf::from(&config.base_path).join("moonlink.sock");
    let rpc_backend = backend.clone();
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server::start_unix_server(rpc_backend, socket_path).await {
            error!("RPC server failed: {}", e);
        }
    });

    // Optionally start REST API
    let (rest_api_handle, rest_api_shutdown_signal) = if let Some(port) = config.rest_api_port {
        let api_state = rest_api::ApiState::new(backend.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if let Err(e) = rest_api::start_server(api_state, port, shutdown_rx).await {
                error!("REST API server failed: {}", e);
            }
        });
        (Some(handle), Some(shutdown_tx))
    } else {
        (None, None)
    };

    // Optionally start TCP server.
    let tcp_api_handle = if let Some(port) = config.tcp_port {
        let backend_clone = backend.clone();
        let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        // TODO(hjiang): Implement graceful shutdown for TCP server.
        let handle = tokio::spawn(async move {
            if let Err(e) = rpc_server::start_tcp_server(backend_clone, addr).await {
                error!("TCP rpc server failed: {}", e);
            }
            println!("TCP rpc server starts at port {port}");
        });
        Some(handle)
    } else {
        None
    };

    info!("Moonlink service started successfully");

    // Wait for termination signal
    let _ = sigterm.recv().await;
    info!("Received SIGTERM, shutting down...");

    // Clean shutdown: abort background servers
    if let Some(handle) = rest_api_handle {
        rest_api_shutdown_signal
            .expect("REST API shutdown sender supposed to be valid")
            .send(())
            .unwrap();
        handle.await?;
    }

    if let Some(handle) = tcp_api_handle {
        handle.abort();
    }

    rpc_handle.abort();

    info!("Moonlink service shut down complete");
    Ok(())
}

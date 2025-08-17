use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use moonlink_backend::MoonlinkBackend;
use moonlink_rpc::{read, write, Request, Table};
use std::collections::HashMap;
use std::error;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tracing::info;

/// Start the Unix socket RPC server and serve requests until the task is aborted.
pub async fn start_unix_server(
    backend: Arc<MoonlinkBackend>,
    socket_path: std::path::PathBuf,
) -> Result<()> {
    if fs::metadata(&socket_path).await.is_ok() {
        fs::remove_file(&socket_path).await?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "Moonlink RPC server listening on Unix socket: {:?}",
        socket_path
    );

    loop {
        let (stream, _addr) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(Error::Rpc(moonlink_rpc::Error::Io(e)))
                    if error::Error::source(&e)
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("{e}"),
                Ok(()) => {}
            }
        });
    }
}

/// Start the TCP socket RPC server and serve requests until the task is aborted.
pub async fn start_tcp_server(backend: Arc<MoonlinkBackend>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Moonlink RPC server listening on TCP: {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(Error::Rpc(moonlink_rpc::Error::Io(e)))
                    if error::Error::source(&e)
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("{e}"),
                Ok(()) => {}
            }
        });
    }
}

async fn handle_stream<S>(backend: Arc<MoonlinkBackend>, mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut map = HashMap::new();
    loop {
        let request = read(&mut stream).await?;
        match request {
            Request::CreateSnapshot {
                mooncake_database,
                mooncake_table,
                lsn,
            } => {
                backend
                    .create_snapshot(mooncake_database, mooncake_table, lsn)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::CreateTable {
                mooncake_database,
                mooncake_table,
                src,
                src_uri,
                table_config,
            } => {
                // Use default mooncake config, and local filesystem for storage layer.
                backend
                    .create_table(
                        mooncake_database,
                        mooncake_table,
                        src,
                        src_uri,
                        table_config,
                        None, /* input_mooncake_database */
                    )
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::DropTable {
                mooncake_database,
                mooncake_table,
            } => {
                backend.drop_table(mooncake_database, mooncake_table).await;
                write(&mut stream, &()).await?;
            }
            Request::GetTableSchema {
                mooncake_database,
                mooncake_table,
            } => {
                let mooncake_database = backend
                    .get_table_schema(mooncake_database, mooncake_table)
                    .await?;
                let writer = StreamWriter::try_new(vec![], &mooncake_database)?;
                let data = writer.into_inner()?;
                write(&mut stream, &data).await?;
            }
            Request::ListTables {} => {
                let tables = backend.list_tables().await?;
                let tables: Vec<Table> = tables
                    .into_iter()
                    .map(|table| Table {
                        mooncake_database: table.mooncake_database,
                        mooncake_table: table.mooncake_table,
                        commit_lsn: table.commit_lsn,
                        flush_lsn: table.flush_lsn,
                        iceberg_warehouse_location: table.iceberg_warehouse_location,
                    })
                    .collect();
                write(&mut stream, &tables).await?;
            }
            Request::OptimizeTable {
                mooncake_database,
                mooncake_table,
                mode,
            } => {
                backend
                    .optimize_table(mooncake_database, mooncake_table, &mode)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::ScanTableBegin {
                mooncake_database,
                mooncake_table,
                lsn,
            } => {
                let state = backend
                    .scan_table(
                        mooncake_database.to_string(),
                        mooncake_table.to_string(),
                        Some(lsn),
                    )
                    .await
                    .unwrap();
                write(&mut stream, &state.data).await?;
                assert!(map
                    .insert((mooncake_database, mooncake_table), state)
                    .is_none());
            }
            Request::ScanTableEnd {
                mooncake_database,
                mooncake_table,
            } => {
                assert!(map.remove(&(mooncake_database, mooncake_table)).is_some());
                write(&mut stream, &()).await?;
            }
        }
    }
}

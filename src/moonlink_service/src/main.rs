use clap::Parser;
use moonlink_service::{start_with_config, Result, ServiceConfig};

#[derive(Parser)]
#[command(name = "moonlink-service")]
#[command(about = "Moonlink data ingestion service")]
struct Cli {
    /// Base path for Moonlink data storage
    base_path: String,

    /// Port for REST API server (optional, defaults to 3030)
    #[arg(long, short = 'p')]
    rest_api_port: Option<u16>,
    /// Disable REST API server
    #[arg(long)]
    no_rest_api: bool,

    /// Port for moonlink standalone server (optional, defaults to 3031).
    #[arg(long)]
    tcp_port: Option<u16>,
    /// Disable standalone deployment.
    #[arg(long)]
    no_tcp_api: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // By default enables backtrace for better troubleshooting capability, no performance overhead, only takes effect at panic.
    std::env::set_var("RUST_BACKTRACE", "1");

    let cli = Cli::parse();
    let config = ServiceConfig {
        base_path: cli.base_path,
        rest_api_port: if cli.no_rest_api {
            None
        } else {
            Some(cli.rest_api_port.unwrap_or(3030))
        },
        tcp_port: if cli.no_tcp_api {
            None
        } else {
            Some(cli.tcp_port.unwrap_or(3031))
        },
    };

    start_with_config(config).await
}

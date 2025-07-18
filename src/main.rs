use anyhow::Result;
use clap::Parser;
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{error, info};

mod server {
    pub mod connection;
    pub mod rtmp;
    pub mod stream;
}

mod utils {
    pub mod config;
    pub mod error;
    pub mod types;
}

mod helpers {
    pub mod session;
}

use crate::utils::types::{Args, Config, RtmpServer, StreamManager};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG) // always show debug and higher
        .with_target(true) // shows module path
        .with_thread_ids(true)
        .init();

    let args = Args::parse();

    // Load configuration
    let config = Config::load(&args.config).unwrap_or_default();

    // Create shared state
    let stream_manager = Arc::new(RwLock::new(StreamManager {
        streams: HashMap::new(),
    }));

    // Create server
    let server = Arc::new(RtmpServer::new(config));

    // Bind to address
    let listener = TcpListener::bind(&args.bind).await?;
    info!("✅ RTMP Server listening on {}", args.bind);

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("🎥 New connection from {}", addr);

                let server_clone = Arc::clone(&server);
                let manager_clone = Arc::clone(&stream_manager);

                tokio::spawn(async move {
                    if let Err(e) = server_clone
                        .handle_connection(stream, addr, manager_clone)
                        .await
                    {
                        error!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("❌ Failed to accept connection: {}", e);
            }
        }
    }
}

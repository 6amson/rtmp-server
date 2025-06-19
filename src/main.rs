use anyhow::Result;
use std::sync::Arc;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{info, error};

mod server {
    pub mod rtmp;
    pub mod connection;
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


use crate::utils::{
    types::{
        Config,
        Args,
        RtmpServer,
    },
};



#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    // Load configuration
    let config = Config::load(&args.config).unwrap_or_default();
    
    // Create server
    let server = Arc::new(RtmpServer::new(config));
    
    // Bind to address
    let listener = TcpListener::bind(&args.bind).await?;
    info!("RTMP Server listening on {}", args.bind);
    
    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("New connection from {}", addr);
                let server_clone = Arc::clone(&server);
                
                tokio::spawn(async move {
                    if let Err(e) = server_clone.handle_connection(stream, addr).await {
                        error!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
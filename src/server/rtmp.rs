use crate::utils::types::{Config, Connection, Result, RtmpServer,};
use crate::utils::error::RtmpError;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{RwLock};

impl RtmpServer {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) -> Result<()> {
        let connection = Arc::new(Connection::new(stream, addr, self.config.clone()));

        // Add to connections
        {
            let mut connections = self.connections.write().await;
            connections.insert(addr, Arc::clone(&connection));
        }

        // Remove from tracking and take ownership
        let connection = {
            let mut connections = self.connections.write().await;
            connections.remove(&addr).unwrap()
        };

        // Handle the connection
        let connection = Arc::try_unwrap(connection)
            .map_err(|_| RtmpError::Protocol("Failed to take connection ownership".to_string()))?;

        connection.handle().await
    }
}

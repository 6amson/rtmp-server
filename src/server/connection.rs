use crate::utils::types::*;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};



impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr, config: Config) -> Self {
        Self {
            stream,
            addr,
            config,
            state: ConnectionState::Handshake,
        }
    }

    
    pub async fn handle(mut self) -> Result<()> {
        info!("Handling connection from {}", self.addr);
        
        // Perform handshake
        self.handshake().await?;
        
        // Main message loop
        loop {
            let mut buf = vec![0u8; self.config.chunk_size + 1];
            match self.stream.read_exact(&mut buf).await {
                Ok(_) => {
                    // Process chunk
                    self.process_chunk(&buf).await?;
                }
                Err(e) => {
                    debug!("Connection {} ended: {}", self.addr, e);
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    async fn handshake(&mut self) -> Result<()> {
        // RTMP handshake implementation
        // C0, S0, C1, S1, C2, S2
        debug!("Starting handshake with {}", self.addr);
        
        // This is where your handshake logic goes
        // For now, placeholder
        
        Ok(())
    }
    
    async fn process_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        // Parse chunk header and payload
        // Route to appropriate handler
        debug!("Processing chunk of {} bytes", chunk.len());
        
        Ok(())
    }
}
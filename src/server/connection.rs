use crate::utils::types::*;
use crate::utils::error::RtmpError;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};
use std::time;



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
        debug!("Starting RTMP handshake with {}", self.addr);
        
        // === C0 + C1 ===
        // Read C0 (1 byte version) + C1 (1536 bytes timestamp + random)
        let mut c0_c1 = vec![0u8; 1 + RTMP_HANDSHAKE_SIZE];
        self.stream.read_exact(&mut c0_c1).await?;
        
        let version = c0_c1[0];
        if version != RTMP_VERSION {
            return Err(RtmpError::HandshakeFailed(format!("Unsupported version: {}", version)));
        }
        
        let c1 = &c0_c1[1..];
        debug!("Received C0 (version: {}) and C1", version);
        
        // === S0 + S1 ===
        // Send S0 (version) + S1 (echo timestamp + our random)
        let mut s0_s1 = vec![0u8; 1 + RTMP_HANDSHAKE_SIZE];
        s0_s1[0] = RTMP_VERSION;
        
        // S1 format: [timestamp: 4 bytes][zeros: 4 bytes][random: 1528 bytes]
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        
        s0_s1[1..5].copy_from_slice(&timestamp.to_be_bytes());
        s0_s1[5..9].fill(0); // Zero padding
        
        // Fill with random data
        for i in 9..s0_s1.len() {
            s0_s1[i] = (i % 256) as u8; // Simple pattern for now
        }
        
        self.stream.write_all(&s0_s1).await?;
        debug!("Sent S0 + S1");
        
        // === C2 ===
        // Read C2 (1536 bytes - should echo our S1)
        let mut c2 = vec![0u8; RTMP_HANDSHAKE_SIZE];
        self.stream.read_exact(&mut c2).await?;
        debug!("Received C2");
        
        // === S2 ===
        // Send S2 (echo C1)
        self.stream.write_all(c1).await?;
        debug!("Sent S2 - handshake complete!");
        
        // Update connection state
        self.state = ConnectionState::Connected;
        
        Ok(())
    }
    
    async fn process_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        // Parse chunk header and payload
        // Route to appropriate handler
        debug!("Processing chunk of {} bytes", chunk.len());
        
        Ok(())
    }
}
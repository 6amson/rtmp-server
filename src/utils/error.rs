use thiserror::Error;

#[derive(Error, Debug)]
pub enum RtmpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
    
    #[error("Invalid chunk format")]
    InvalidChunk,
    
    #[error("Connection timeout")]
    Timeout,
    
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("AMF0 error: {0}")]
    Amf0(String),
}

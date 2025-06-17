use crate::utils::error::RtmpError;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::sync::broadcast;


#[derive(Parser)]
#[command(name = "rtmp-server")]
#[command(about = "High-performance RTMP server")]
pub struct Args {
    #[arg(short, long, default_value = "127.0.0.1:1935")]
    pub bind: String,

    #[arg(short, long, default_value = "config.yaml")]
    pub config: String,
}

pub type Result<T> = std::result::Result<T, RtmpError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub chunk_size: usize,
    pub max_connections: usize,
    pub handshake_timeout: u64,
    pub stream_timeout: u64,
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub stream_key: String,
    pub publisher: Option<SocketAddr>,
    pub viewers: Vec<SocketAddr>,
}
pub struct RtmpServer {
    pub config: Config,
    pub connections: Arc<RwLock<HashMap<SocketAddr, Arc<Connection>>>>,
    pub streams: Arc<RwLock<HashMap<String, StreamInfo>>>,
}

pub const RTMP_VERSION: u8 = 3;
pub const RTMP_HANDSHAKE_SIZE: usize = 1536;

impl TryFrom<u8> for MessageType {
    type Error = RtmpError;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(MessageType::SetChunkSize),
            2 => Ok(MessageType::Abort),
            3 => Ok(MessageType::Acknowledgement),
            4 => Ok(MessageType::UserControl),
            5 => Ok(MessageType::WindowAckSize),
            6 => Ok(MessageType::SetPeerBandwidth),
            8 => Ok(MessageType::Audio),
            9 => Ok(MessageType::Video),
            15 => Ok(MessageType::DataAmf3),
            16 => Ok(MessageType::SharedObjectAmf3),
            17 => Ok(MessageType::CommandAmf3),
            18 => Ok(MessageType::DataAmf0),
            19 => Ok(MessageType::SharedObjectAmf0),
            20 => Ok(MessageType::CommandAmf0),
            22 => Ok(MessageType::Aggregate),
            _ => Err(RtmpError::Protocol(format!("Unknown message type: {}", value))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageType {
    SetChunkSize = 1,
    Abort = 2,
    Acknowledgement = 3,
    UserControl = 4,
    WindowAckSize = 5,
    SetPeerBandwidth = 6,
    Audio = 8,
    Video = 9,
    DataAmf3 = 15,
    SharedObjectAmf3 = 16,
    CommandAmf3 = 17,
    DataAmf0 = 18,
    SharedObjectAmf0 = 19,
    CommandAmf0 = 20,
    Aggregate = 22,
}

#[derive(Debug, Clone)]
pub struct ChunkHeader {
    pub format: u8,
    pub chunk_stream_id: u32,
    pub timestamp: u32,
    pub message_length: u32,
    pub message_type: u8,
    pub message_stream_id: u32,
}

pub struct Connection {
    pub stream: TcpStream,
    pub addr: SocketAddr,
    pub config: Config,
    pub state: ConnectionState,
}

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Handshake,
    Connected,
    Publishing,
    Playing,
}

pub struct StreamManager {
   pub streams: HashMap<String, Stream>,
}

pub struct Stream {
    pub key: String,
    pub sender: broadcast::Sender<Vec<u8>>,
    pub receiver_count: usize,
}

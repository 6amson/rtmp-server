use crate::utils::error::RtmpError;
use crate::utils::types::*;
use bytes::BytesMut;
use rand::{thread_rng, RngCore};
use rml_amf0::{serialize, Amf0Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};
use tokio::sync::RwLock;
use tracing::{debug, info};

impl Connection {
    pub fn new(
        stream: TcpStream,
        addr: SocketAddr,
        config: Config,
        stream_manager: Arc<RwLock<StreamManager>>,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
            addr,
            config,
            state: ConnectionState::Handshake,
            session: None,
            stream_manager,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        info!("Handling connection from {}", self.addr);

        // Perform handshake
        self.handshake().await?;

        // Main message loop
        loop {
            {
                let mut buf = vec![0u8; self.config.chunk_size + 1];
                let read_result = {
                    let mut stream = self.stream.lock().await;
                    stream.read_exact(&mut buf).await
                };
                match read_result {
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
        }

        Ok(())
    }

    async fn handshake(&mut self) -> Result<()> {
        debug!("Starting RTMP handshake with {}", self.addr);

        // === C0 + C1 ===
        // Read C0 (1 byte version) + C1 (1536 bytes timestamp + random)
        let mut c0_c1 = vec![0u8; 1 + RTMP_HANDSHAKE_SIZE];
        let read_result = {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut c0_c1).await
        };
        match read_result {
            Ok(_) => {}
            Err(e) => {
                debug!("Connection {} ended: {}", self.addr, e);
                return Err(e.into());
            }
        }

        let version = c0_c1[0];
        if version != RTMP_VERSION {
            return Err(RtmpError::HandshakeFailed(format!(
                "Unsupported version: {}",
                version
            )));
        }

        let c1 = &mut c0_c1[1..];
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
        rand::thread_rng().fill_bytes(&mut s0_s1[9..]);

        {
            let mut stream = self.stream.lock().await;
            stream.write_all(&mut &s0_s1).await;
        debug!("Sent S0 + S1");
        };

        // === C2 ===
        // Read C2 (1536 bytes - should echo our S1)
        let _read_result = {
        let mut c2 = vec![0u8; RTMP_HANDSHAKE_SIZE];
        let mut stream = self.stream.lock().await;
        stream.read_exact(&mut c2).await?;
        debug!("Received C2");
        };


        // === S2 ===
        // Send S2 (echo C1)
        let _write_all_result = {
            let mut stream = self.stream.lock().await;
            stream.write_all(&c1).await?;

        debug!("Sent S2 - handshake complete!");
        };


        // Update connection state
        self.state = ConnectionState::Connected;

        Ok(())
    }

    async fn process_chunk(&mut self, chunk_data: &[u8]) -> Result<()> {
        if chunk_data.is_empty() {
            return Ok(());
        }

        debug!(
            "Processing chunk of {} bytes from {}",
            chunk_data.len(),
            self.addr
        );

        // Parse chunk header
        let header = self.parse_chunk_header(chunk_data)?;
        let header_size = self.get_header_size(header.format);

        if chunk_data.len() < header_size {
            return Err(RtmpError::InvalidChunk);
        }

        let payload = &chunk_data[header_size..];
        debug!(
            "Chunk: stream_id={}, type={}, length={}, payload={}bytes",
            header.chunk_stream_id,
            header.message_type,
            header.message_length,
            payload.len()
        );

        // Route message based on type
        match MessageType::try_from(header.message_type)? {
            MessageType::SetChunkSize => self.handle_set_chunk_size(payload).await?,
            MessageType::Acknowledgement => self.handle_acknowledgement(payload).await?,
            MessageType::UserControl => self.handle_user_control(payload).await?,
            MessageType::WindowAckSize => self.handle_window_ack_size(payload).await?,
            MessageType::SetPeerBandwidth => self.handle_set_peer_bandwidth(payload).await?,
            MessageType::CommandAmf0 => self.handle_command_amf0(payload).await?,
            MessageType::Audio => self.handle_audio(payload).await?,
            MessageType::Video => self.handle_video(payload).await?,
            _ => {
                debug!("Unhandled message type: {}", header.message_type);
            }
        }

        Ok(())
    }

    fn get_header_size(&self, format: u8) -> usize {
        match format {
            0 => 11,
            1 => 7,
            2 => 3,
            3 => 0,
            _ => {
                eprintln!("Warning: Unknown RTMP format {}; defaulting to 0", format);
                0
            }
        }
    }

    fn parse_chunk_header(&self, data: &[u8]) -> Result<ChunkHeader> {
        if data.is_empty() {
            return Err(RtmpError::InvalidChunk);
        }

        let first_byte = data[0];
        let format = (first_byte >> 6) & 0x03;
        let chunk_stream_id = (first_byte & 0x3F) as u32;

        // Handle extended chunk stream IDs
        let (chunk_stream_id, offset) = match chunk_stream_id {
            0 => {
                if data.len() < 2 {
                    return Err(RtmpError::InvalidChunk);
                }
                ((data[1] as u32) + 64, 2)
            }
            1 => {
                if data.len() < 3 {
                    return Err(RtmpError::InvalidChunk);
                }
                ((data[1] as u32) + ((data[2] as u32) << 8) + 64, 3)
            }
            _ => (chunk_stream_id, 1),
        };

        let mut header = ChunkHeader {
            format,
            chunk_stream_id,
            timestamp: 0,
            message_length: 0,
            message_type: 0,
            message_stream_id: 0,
        };

        // Parse based on format type
        match format {
            0 => {
                // Type 0: Full header (11 or 15 bytes)
                if data.len() < offset + 11 {
                    return Err(RtmpError::InvalidChunk);
                }
                header.timestamp =
                    u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]);
                header.message_length =
                    u32::from_be_bytes([0, data[offset + 3], data[offset + 4], data[offset + 5]]);
                header.message_type = data[offset + 6];
                header.message_stream_id = u32::from_le_bytes([
                    data[offset + 7],
                    data[offset + 8],
                    data[offset + 9],
                    data[offset + 10],
                ]);
            }
            1 => {
                // Type 1: No message stream ID (7 or 11 bytes)
                if data.len() < offset + 7 {
                    return Err(RtmpError::InvalidChunk);
                }
                header.timestamp =
                    u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]);
                header.message_length =
                    u32::from_be_bytes([0, data[offset + 3], data[offset + 4], data[offset + 5]]);
                header.message_type = data[offset + 6];

                // message_stream_id inherited from previous chunk
            }
            2 => {
                // Type 2: Only timestamp delta (3 or 7 bytes)
                if data.len() < offset + 3 {
                    return Err(RtmpError::InvalidChunk);
                }
                header.timestamp =
                    u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]);
                // Other fields inherited
            }
            3 => {
                // Type 3: No header (just basic header)
                // All fields inherited from previous chunk
            }
            _ => return Err(RtmpError::InvalidChunk),
        }

        Ok(header)
    }

    // === Message Handlers ===

    async fn handle_set_chunk_size(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol(
                "Invalid chunk size message".to_string(),
            ));
        }
        let chunk_size = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        debug!("Client set chunk size to: {}", chunk_size);
        // Update our chunk size for reading
        Ok(())
    }

    async fn handle_acknowledgement(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol("Invalid acknowledgement".to_string()));
        }
        let sequence_number = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        debug!("Received acknowledgement: {}", sequence_number);
        Ok(())
    }

    async fn handle_user_control(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 {
            return Err(RtmpError::Protocol(
                "Invalid user control message".to_string(),
            ));
        }
        let event_type = u16::from_be_bytes([payload[0], payload[1]]);
        debug!("User control event: {}", event_type);
        Ok(())
    }

    async fn handle_window_ack_size(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol("Invalid window ack size".to_string()));
        }
        let window_size = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        debug!("Window acknowledgement size: {}", window_size);
        Ok(())
    }

    async fn handle_set_peer_bandwidth(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 5 {
            return Err(RtmpError::Protocol(
                "Invalid peer bandwidth message".to_string(),
            ));
        }
        let bandwidth = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let limit_type = payload[4];
        debug!("Set peer bandwidth: {} (type: {})", bandwidth, limit_type);
        Ok(())
    }

    async fn handle_command_amf0(&mut self, payload: &[u8]) -> Result<()> {
        debug!("Parsing AMF0 command");

        let values = rml_amf0::deserialize(&mut &payload[..])
            .map_err(|e| RtmpError::Amf0(format!("AMF0 deserialization error: {:?}", e)))?;

        if values.is_empty() {
            return Err(RtmpError::Protocol("Empty AMF0 command".into()));
        }

        if let Amf0Value::Utf8String(command) = &values[0] {
            match command.as_str() {
                "connect" => {
                    info!("Received connect command");
                    self.send_connect_response().await?;
                }
                "createStream" => {
                    info!("Received createStream command");
                    self.send_create_stream_response().await?;
                }
                "publish" => {
                    if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
                        info!("Client wants to publish to stream: {}", stream_key);
                        self.register_publisher(stream_key.clone()).await?;
                    }
                }
                "play" => {
                    if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
                        info!("Client wants to play stream: {}", stream_key);
                        self.register_viewer(stream_key.clone()).await?;
                    }
                }
                _ => {
                    debug!("Unhandled AMF0 command: {}", command);
                }
            }
        }

        Ok(())
    }

    async fn send_create_stream_response(&mut self) -> Result<()> {
    let response = vec![
        Amf0Value::Utf8String("_result".into()),
        Amf0Value::Number(2.0), // Transaction ID
        Amf0Value::Null,        // No properties
        Amf0Value::Number(1.0), // Stream ID (1)
    ];

    let data = serialize(&response)
        .map_err(|e| RtmpError::Amf0(format!("serialize error: {:?}", e)))?;

    let mut stream = self.stream.lock().await;
    stream.write_all(&data).await?;

    Ok(())
    }


    async fn send_connect_response(&mut self) -> Result<()> {

    let props = Amf0Value::Object(vec![
        ("fmsVer".to_string(), Amf0Value::Utf8String("FMS/3,5,7,7009".into())),
        ("capabilities".to_string(), Amf0Value::Number(31.0)),
    ].iter()
    .map(|(k, v)| (k.to_string(), match v {
        Amf0Value::Utf8String(s) => Amf0Value::Utf8String(s.into()),
        Amf0Value::Number(n) => Amf0Value::Number(*n),
        _ => Amf0Value::Null,
    }))
    .collect::<HashMap<_, _>>( )
    );

    let info = Amf0Value::Object(
        vec![
            ("level".to_string(), Amf0Value::Utf8String("status".into())),
            ("code".to_string(), Amf0Value::Utf8String("NetConnection.Connect.Success".into())),
            ("description".to_string(), Amf0Value::Utf8String("Connection succeeded.".into()))
        ].iter()
        .map(|(k, v)| (k.to_string(), match v {
            Amf0Value::Utf8String(s) => Amf0Value::Utf8String(s.into()),
            Amf0Value::Number(n) => Amf0Value::Number(*n),
            _ => Amf0Value::Null,
        }))
        .collect::<HashMap<_, _>>( )
    );

    let response = vec![
        Amf0Value::Utf8String("_result".into()),
        Amf0Value::Number(1.0),
        props,
        info,
    ];

    let data = serialize(&response)
        .map_err(|e| RtmpError::Amf0(format!("serialize error: {:?}", e)))?;

    let mut stream = self.stream.lock().await;
    stream.write_all(&data).await?;

    Ok(())
    }




    async fn handle_audio(&mut self, payload: &[u8]) -> Result<()> {
        if let Some(Session {
            stream_key: Some(key),
            role: Some(Role::Publisher),
            ..
        }) = &self.session
        {
            let manager = self.stream_manager.read().await;
            if let Some(stream) = manager.streams.get(key) {
                let _ = stream.sender.send(payload.to_vec());
            }
        }

        Ok(())
    }

    async fn handle_video(&mut self, payload: &[u8]) -> Result<()> {
        if let Some(Session {
            stream_key: Some(key),
            role: Some(Role::Publisher),
            ..
        }) = &self.session
        {
            let manager = self.stream_manager.read().await;
            if let Some(stream) = manager.streams.get(key) {
                let _ = stream.sender.send(payload.to_vec());
            }
        }

        Ok(())
    }

    async fn register_publisher(&mut self, key: String) -> Result<()> {
        let mut manager = self.stream_manager.write().await;

        // Insert or get stream
        let entry = manager.streams.entry(key.clone()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(100);
            Stream {
                key: key.clone(),
                sender,
                receiver_count: 0,
            }
        });

        self.session = Some(Session {
            addr: self.addr,
            stream_key: Some(key.clone()),
            role: Some(Role::Publisher),
        });

        self.state = ConnectionState::Publishing;
        info!("Registered {} as publisher for stream {}", self.addr, key);

        Ok(())
    }

    async fn register_viewer(&mut self, key: String) -> Result<()> {
        let mut manager = self.stream_manager.write().await;

        let entry = manager.streams.entry(key.clone()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(100);
            Stream {
                key: key.clone(),
                sender,
                receiver_count: 0,
            }
        });

        entry.receiver_count += 1;
        let mut receiver = entry.sender.subscribe();

        let writer = Arc::clone(&self.stream);

        tokio::spawn(async move {
            while let Ok(chunk) = receiver.recv().await {
                let mut stream = writer.lock().await;
                if let Err(e) = stream.write_all(&chunk).await {
                    tracing::error!("Failed to write to viewer: {}", e);
                    break;
                }
            }
        });

        self.session = Some(Session {
            addr: self.addr,
            stream_key: Some(key.clone()),
            role: Some(Role::Viewer),
        });

        self.state = ConnectionState::Playing;
        info!("Viewer {} subscribed to {}", self.addr, key);

        Ok(())
    }
}

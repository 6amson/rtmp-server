use crate::utils::error::*;
use crate::utils::types::*;
// use bytes::BytesMut;
use rand::{thread_rng, RngCore};
use rml_amf0::{serialize, Amf0Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

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
            state: Arc::new(Mutex::new(ConnectionState::Handshake)),
            session: Arc::new(Mutex::new(None)),
            stream_manager,
        }
    }

    pub async fn handle(self: Arc<Self>) -> Result<()> {
        info!("Handling connection from {}", self.addr);

        let conn = Arc::clone(&self); // master clone

        // Separate clone just for handshake
        let handshake_conn = Arc::clone(&conn);
        if let Err(e) = handshake_conn.perform_handshake().await {
            error!("Handshake failed for {}: {}", conn.addr, e); // safe: `conn` not moved
            return Err(e);
        }

        let mut chunk_buffers: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut chunk_headers: HashMap<u32, ChunkHeader> = HashMap::new();

        loop {
            let chunk_conn = Arc::clone(&conn); // only for processing
            let log_conn = Arc::clone(&conn); // preserved for logging after .await

            match chunk_conn
                .read_and_process_chunk(&mut chunk_buffers, &mut chunk_headers)
                .await
            {
                Ok(()) => continue,
                Err(e) => {
                    debug!("Connection {} ended: {}", log_conn.addr, e); // safe
                    break;
                }
            }
        }

        Ok(())
    }

    async fn read_and_process_chunk(
        self: Arc<Self>,
        chunk_buffers: &mut HashMap<u32, Vec<u8>>,
        chunk_headers: &mut HashMap<u32, ChunkHeader>,
    ) -> Result<()> {
        // Read basic header (1-3 bytes)
        let mut basic_header = vec![0u8; 1];
        {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut basic_header).await?;
        }

        let first_byte = basic_header[0];
        let format = (first_byte >> 6) & 0x03;
        let mut chunk_stream_id = (first_byte & 0x3F) as u32;

        // Handle extended chunk stream ID
        let mut header_data = basic_header;
        match chunk_stream_id {
            0 => {
                // 1 byte extended CSID
                let mut ext_csid = vec![0u8; 1];
                {
                    let mut stream = self.stream.lock().await;
                    stream.read_exact(&mut ext_csid).await?;
                }
                chunk_stream_id = (ext_csid[0] as u32) + 64;
                header_data.extend_from_slice(&ext_csid);
            }
            1 => {
                // 2 byte extended CSID
                let mut ext_csid = vec![0u8; 2];
                {
                    let mut stream = self.stream.lock().await;
                    stream.read_exact(&mut ext_csid).await?;
                }
                chunk_stream_id = (ext_csid[0] as u32) + ((ext_csid[1] as u32) << 8) + 64;
                header_data.extend_from_slice(&ext_csid);
            }
            _ => {} // Use chunk_stream_id as-is
        }

        // Read message header based on format
        let message_header_size = match format {
            0 => 11, // timestamp(3) + length(3) + type(1) + stream_id(4)
            1 => 7,  // timestamp_delta(3) + length(3) + type(1)
            2 => 3,  // timestamp_delta(3)
            3 => 0,  // no additional header
            _ => return Err(RtmpError::InvalidChunk),
        };

        if message_header_size > 0 {
            let mut message_header = vec![0u8; message_header_size];
            {
                let mut stream = self.stream.lock().await;
                stream.read_exact(&mut message_header).await?;
            }
            header_data.extend_from_slice(&message_header);
        }

        // Parse the complete header
        let self_clone = Arc::clone(&self);
        // self_clone.process_message(&header, &complete_message).await?;

        let header = self_clone.parse_chunk_header(&header_data, chunk_headers)?;

        // Check for extended timestamp
        let mut extended_timestamp_bytes = 0;
        if (format == 0 || format == 1 || format == 2)
            && (header.timestamp == 0xFFFFFF || header.timestamp >= 0xFFFFFF)
        {
            let mut ext_timestamp = vec![0u8; 4];
            {
                let mut stream = self.stream.lock().await;
                stream.read_exact(&mut ext_timestamp).await?;
            }
            header_data.extend_from_slice(&ext_timestamp);
            extended_timestamp_bytes = 4;
        }

        // let self_clone = Arc::clone(self);

        // if (format == 0 || format == 1 || format == 2)
        //     && (header.timestamp == 0xFFFFFF || header.timestamp >= 0xFFFFFF)
        // {
        //     let mut ext_timestamp = vec![0u8; 4];
        //     {
        //         let mut stream = self_clone.stream.lock().await;
        //         stream.read_exact(&mut ext_timestamp).await?;
        //     }
        //     header_data.extend_from_slice(&ext_timestamp);
        //     extended_timestamp_bytes = 4;
        // }

        // Store header for future type 1,2,3 chunks
        chunk_headers.insert(chunk_stream_id, header.clone());

        // Calculate payload size (respecting chunk size limit)
        let remaining_in_message = {
            let empty = Vec::new();
            let buffer = chunk_buffers.get(&chunk_stream_id).unwrap_or(&empty);
            let remaining_in_message =
                (header.message_length as usize).saturating_sub(buffer.len());
            remaining_in_message
        };

        let chunk_size = *self.config.chunk_size.lock().await;
        let payload_size = std::cmp::min(remaining_in_message, chunk_size);

        // Read payload
        let mut payload = vec![0u8; payload_size];
        if payload_size > 0 {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut payload).await?;
        }

        // Accumulate payload in buffer
        let buffer = chunk_buffers.entry(chunk_stream_id).or_default();
        buffer.extend_from_slice(&payload);

        // Check if we have a complete message
        if buffer.len() >= header.message_length as usize {
            let complete_message = buffer.split_off(0); // Take complete message, reset buffer

            debug!(
                "Complete message: stream_id={}, type={}, length={}, payload={}bytes",
                chunk_stream_id,
                header.message_type,
                header.message_length,
                complete_message.len()
            );

            // Process the complete message
            let self_clone = Arc::clone(&self);
            self_clone
                .process_message(&header, &complete_message)
                .await?;
        }

        Ok(())
    }

    pub async fn perform_handshake(self: Arc<Self>) -> Result<()> {
        self.handshake().await
    }

    async fn handshake(self: Arc<Self>) -> Result<()> {
        debug!("Starting RTMP handshake with {}", self.addr);

        // === C0 + C1 ===
        let mut c0_c1 = vec![0u8; 1 + RTMP_HANDSHAKE_SIZE];
        {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut c0_c1).await.map_err(|e| {
                error!("Failed to read C0+C1 from {}: {}", self.addr, e);
                e
            })?;
        }

        let version = c0_c1[0];
        if version != RTMP_VERSION {
            error!(
                "Unsupported RTMP version: {} (expected {})",
                version, RTMP_VERSION
            );
            return Err(RtmpError::HandshakeFailed(format!(
                "Unsupported version: {}",
                version
            )));
        }

        let c1 = &c0_c1[1..];
        debug!(
            "Received C0 (version: {}) and C1 ({} bytes)",
            version,
            c1.len()
        );

        // === S0 + S1 + S2 ===
        // Create S0+S1+S2 response - EXACTLY 1537 + 1536 = 3073 bytes
        let mut s0_s1_s2 = vec![0u8; 1 + RTMP_HANDSHAKE_SIZE + RTMP_HANDSHAKE_SIZE];

        // S0 = version
        s0_s1_s2[0] = RTMP_VERSION;

        // Current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;

        s0_s1_s2[1..5].copy_from_slice(&timestamp.to_be_bytes());
        s0_s1_s2[5..9].fill(0); // Zero field

        // Fill remaining 1528 bytes with random data
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut s0_s1_s2[9..1537]);

        // S2 should be exactly the same as C1
        s0_s1_s2[1537..].copy_from_slice(c1);

        debug!("S0+S1+S2 size: {}", s0_s1_s2.len());
        debug!("S1 timestamp: {:02x?}", &s0_s1_s2[1..5]);

        // Send S0+S1+S2 all together
        {
            let mut stream = self.stream.lock().await;
            stream.write_all(&s0_s1_s2).await.map_err(|e| {
                error!("Failed to write S0+S1+S2 to {}: {}", self.addr, e);
                e
            })?;
            stream.flush().await.map_err(|e| {
                error!("Failed to flush S0+S1+S2 to {}: {}", self.addr, e);
                e
            })?;
            debug!("Sent S0+S1+S2 ({} bytes)", s0_s1_s2.len());
        }

        // === C2 ===
        let mut c2 = vec![0u8; RTMP_HANDSHAKE_SIZE];
        {
            let mut stream = self.stream.lock().await;
            debug!("Waiting for C2 from {}", self.addr);

            let c2_future = stream.read_exact(&mut c2);
            match tokio::time::timeout(std::time::Duration::from_secs(5), c2_future).await {
                Ok(Ok(_)) => {
                    debug!("Received C2 ({} bytes)", c2.len());
                }
                Ok(Err(e)) => {
                    error!("Failed to read C2 from {}: {}", self.addr, e);
                    return Err(e.into());
                }
                Err(_) => {
                    error!("Timeout waiting for C2 from {}", self.addr);
                    return Err(RtmpError::HandshakeFailed("C2 timeout".to_string()));
                }
            }
        }

        // Update connection state
        *self.state.lock().await = ConnectionState::Connected;
        info!("RTMP handshake completed successfully with {}", self.addr);

        Ok(())
    }

    fn parse_chunk_header(
        self: Arc<Self>,
        data: &[u8],
        prev_headers: &HashMap<u32, ChunkHeader>,
    ) -> Result<ChunkHeader> {
        if data.is_empty() {
            return Err(RtmpError::InvalidChunk);
        }

        let first_byte = data[0];
        let format = (first_byte >> 6) & 0x03;
        let mut chunk_stream_id = (first_byte & 0x3F) as u32;

        // Handle extended CSID
        let mut offset = 1;
        match chunk_stream_id {
            0 => {
                if data.len() < 2 {
                    return Err(RtmpError::InvalidChunk);
                }
                chunk_stream_id = (data[1] as u32) + 64;
                offset = 2;
            }
            1 => {
                if data.len() < 3 {
                    return Err(RtmpError::InvalidChunk);
                }
                chunk_stream_id = (data[1] as u32) + ((data[2] as u32) << 8) + 64;
                offset = 3;
            }
            _ => {} // Use chunk_stream_id as-is
        }

        let mut header = ChunkHeader {
            format,
            chunk_stream_id,
            timestamp: 0,
            message_length: 0,
            message_type: 0,
            message_stream_id: 0,
        };

        match format {
            0 => {
                // Type 0: Full header
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
                // Type 1: No message stream ID
                if data.len() < offset + 7 {
                    return Err(RtmpError::InvalidChunk);
                }

                let prev = prev_headers
                    .get(&chunk_stream_id)
                    .ok_or(RtmpError::InvalidChunk)?;
                let timestamp_delta =
                    u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]);

                header.timestamp = prev.timestamp.wrapping_add(timestamp_delta);
                header.message_length =
                    u32::from_be_bytes([0, data[offset + 3], data[offset + 4], data[offset + 5]]);
                header.message_type = data[offset + 6];
                header.message_stream_id = prev.message_stream_id;
            }
            2 => {
                // Type 2: Only timestamp delta
                if data.len() < offset + 3 {
                    return Err(RtmpError::InvalidChunk);
                }

                let prev = prev_headers
                    .get(&chunk_stream_id)
                    .ok_or(RtmpError::InvalidChunk)?;
                let timestamp_delta =
                    u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]);

                header.timestamp = prev.timestamp.wrapping_add(timestamp_delta);
                header.message_length = prev.message_length;
                header.message_type = prev.message_type;
                header.message_stream_id = prev.message_stream_id;
            }
            3 => {
                // Type 3: No header
                let prev = prev_headers
                    .get(&chunk_stream_id)
                    .ok_or(RtmpError::InvalidChunk)?;
                header.timestamp = prev.timestamp;
                header.message_length = prev.message_length;
                header.message_type = prev.message_type;
                header.message_stream_id = prev.message_stream_id;
            }
            _ => return Err(RtmpError::InvalidChunk),
        }

        Ok(header)
    }

    async fn process_message(self: Arc<Self>, header: &ChunkHeader, payload: &[u8]) -> Result<()> {
        debug!(
            "Processing message: stream_id={}, type={}, length={}, payload={}bytes",
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

    // === Message Handlers ===

    async fn handle_set_chunk_size(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol(
                "Invalid chunk size message".to_string(),
            ));
        }
        let chunk_size = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        info!("Client set chunk size to: {}", chunk_size);
        // Update our chunk size for reading
        *self.config.chunk_size.lock().await = chunk_size as usize;
        Ok(())
    }

    async fn handle_acknowledgement(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol("Invalid acknowledgement".to_string()));
        }
        let sequence_number = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        debug!("Received acknowledgement: {}", sequence_number);
        Ok(())
    }

    async fn handle_user_control(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 {
            return Err(RtmpError::Protocol(
                "Invalid user control message".to_string(),
            ));
        }
        let event_type = u16::from_be_bytes([payload[0], payload[1]]);
        debug!("User control event: {}", event_type);
        Ok(())
    }

    async fn handle_window_ack_size(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        if payload.len() < 4 {
            return Err(RtmpError::Protocol("Invalid window ack size".to_string()));
        }
        let window_size = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        debug!("Window acknowledgement size: {}", window_size);
        Ok(())
    }

    async fn handle_set_peer_bandwidth(self: Arc<Self>, payload: &[u8]) -> Result<()> {
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

    // async fn handle_command_amf0(self: Arc<Self>, payload: &[u8]) -> Result<()> {
    //     debug!("Parsing AMF0 command from {} bytes", payload.len());

    //     let values = rml_amf0::deserialize(&mut &payload[..])
    //         .map_err(|e| RtmpError::Amf0(format!("AMF0 deserialization error: {:?}", e)))?;

    //     if values.is_empty() {
    //         return Err(RtmpError::Protocol("Empty AMF0 command".into()));
    //     }

    //     if let Amf0Value::Utf8String(command) = &values[0] {
    //         info!("Received AMF0 command: {}", command);
    //         match command.as_str() {
    //             "connect" => {
    //                 info!("Processing connect command");
    //                 self.send_connect_response().await?;
    //             }
    //             "createStream" => {
    //                 info!("Processing createStream command");
    //                 self.send_create_stream_response().await?;
    //             }
    //             "publish" => {
    //                 if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
    //                     info!("Client wants to publish to stream: {}", stream_key);
    //                     self.register_publisher(stream_key.clone()).await?;
    //                 } else {
    //                     warn!("Publish command missing stream key");
    //                 }
    //             }
    //             "play" => {
    //                 if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
    //                     info!("Client wants to play stream: {}", stream_key);
    //                     self.register_viewer(stream_key.clone()).await?;
    //                 } else {
    //                     warn!("Play command missing stream key");
    //                 }
    //             }
    //             _ => {
    //                 debug!("Unhandled AMF0 command: {}", command);
    //             }
    //         }
    //     } else {
    //         warn!("First AMF0 value is not a string: {:?}", values[0]);
    //     }

    //     Ok(())
    // }

    async fn handle_command_amf0(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        debug!("Parsing AMF0 command from {} bytes", payload.len());

        // Add hex dump for debugging
        debug!("AMF0 payload hex dump:");
        for (i, chunk) in payload.chunks(16).enumerate() {
            let hex: String = chunk
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            let ascii: String = chunk
                .iter()
                .map(|&b| if b >= 32 && b <= 126 { b as char } else { '.' })
                .collect();
            debug!("{:04x}: {:<47} {}", i * 16, hex, ascii);
        }

        // Try to parse AMF0 values one by one
        let mut cursor = &payload[..];
        let mut values = Vec::new();
        let mut value_index = 0;

        while !cursor.is_empty() {
            match rml_amf0::deserialize(&mut cursor) {
                Ok(mut parsed_values) => {
                    debug!(
                        "Successfully parsed {} AMF0 values starting at index {}",
                        parsed_values.len(),
                        value_index
                    );
                    for (i, value) in parsed_values.iter().enumerate() {
                        debug!("  Value {}: {:?}", value_index + i, value);
                    }
                    values.append(&mut parsed_values);
                    value_index += parsed_values.len();
                }
                Err(e) => {
                    error!(
                        "AMF0 parsing failed at byte offset {}: {:?}",
                        payload.len() - cursor.len(),
                        e
                    );
                    debug!("Remaining bytes: {:?}", cursor);
                    return Err(RtmpError::Amf0(format!(
                        "AMF0 deserialization error: {:?}",
                        e
                    )));
                }
            }
        }

        if values.is_empty() {
            return Err(RtmpError::Protocol("Empty AMF0 command".into()));
        }

        debug!("Total AMF0 values parsed: {}", values.len());
        for (i, value) in values.iter().enumerate() {
            debug!("Final value {}: {:?}", i, value);
        }

        if let Amf0Value::Utf8String(command) = &values[0] {
            info!("Received AMF0 command: {}", command);
            match command.as_str() {
                "connect" => {
                    info!("Processing connect command");
                    // Debug the connect command parameters
                    if values.len() > 1 {
                        debug!("Connect command parameters: {:?}", &values[1..]);
                    }
                    self.send_connect_response().await?;
                }
                "createStream" => {
                    info!("Processing createStream command");
                    self.send_create_stream_response().await?;
                }
                "publish" => {
                    if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
                        info!("Client wants to publish to stream: {}", stream_key);
                        self.register_publisher(stream_key.clone()).await?;
                    } else {
                        warn!("Publish command missing stream key");
                        debug!("Publish command values: {:?}", values);
                    }
                }
                "play" => {
                    if let Some(Amf0Value::Utf8String(stream_key)) = values.get(3) {
                        info!("Client wants to play stream: {}", stream_key);
                        self.register_viewer(stream_key.clone()).await?;
                    } else {
                        warn!("Play command missing stream key");
                        debug!("Play command values: {:?}", values);
                    }
                }
                _ => {
                    debug!("Unhandled AMF0 command: {}", command);
                }
            }
        } else {
            warn!("First AMF0 value is not a string: {:?}", values[0]);
        }

        Ok(())
    }

    async fn send_create_stream_response(self: Arc<Self>) -> Result<()> {
        let response = vec![
            Amf0Value::Utf8String("_result".into()),
            Amf0Value::Number(2.0), // Transaction ID
            Amf0Value::Null,        // No properties
            Amf0Value::Number(1.0), // Stream ID (1)
        ];

        let data = serialize(&response)
            .map_err(|e| RtmpError::Amf0(format!("serialize error: {:?}", e)))?;

        // Create proper RTMP message
        let message = self.create_rtmp_message(MessageType::CommandAmf0 as u8, 0, &data)?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        info!("Sent createStream response");
        Ok(())
    }

    async fn send_connect_response(self: Arc<Self>) -> Result<()> {
        let props = Amf0Value::Object(
            vec![
                (
                    "fmsVer".to_string(),
                    Amf0Value::Utf8String("FMS/3,5,7,7009".into()),
                ),
                ("capabilities".to_string(), Amf0Value::Number(31.0)),
                ("mode".to_string(), Amf0Value::Number(1.0)),
            ]
            .into_iter()
            .collect(),
        );

        let info = Amf0Value::Object(
            vec![
                ("level".to_string(), Amf0Value::Utf8String("status".into())),
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetConnection.Connect.Success".into()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String("Connection succeeded.".into()),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let response = vec![
            Amf0Value::Utf8String("_result".into()),
            Amf0Value::Number(1.0), // Transaction ID
            props,
            info,
        ];

        let data = serialize(&response)
            .map_err(|e| RtmpError::Amf0(format!("serialize error: {:?}", e)))?;

        // Create proper RTMP message
        let message = self.create_rtmp_message(MessageType::CommandAmf0 as u8, 0, &data)?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        info!("Sent connect response");
        Ok(())
    }

    fn create_rtmp_message(
        &self,
        message_type: u8,
        stream_id: u32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Basic header (format 0, chunk stream ID 3)
        message.push(0x03); // Format 0, CSID 3

        // Message header (11 bytes for format 0)
        // Timestamp (3 bytes) - using 0 for now
        message.extend_from_slice(&[0, 0, 0]);

        // Message length (3 bytes)
        let length = payload.len() as u32;
        message.extend_from_slice(&[(length >> 16) as u8, (length >> 8) as u8, length as u8]);

        // Message type (1 byte)
        message.push(message_type);

        // Stream ID (4 bytes, little endian)
        message.extend_from_slice(&stream_id.to_le_bytes());

        // Payload
        message.extend_from_slice(payload);

        Ok(message)
    }

    async fn handle_audio(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        debug!("Received audio data: {} bytes", payload.len());

        let session_key = {
            let session_lock = self.session.lock().await;
            if let Some(Session {
                stream_key: Some(key),
                role: Some(Role::Publisher),
                ..
            }) = &*session_lock
            {
                debug!("Found stream key: {}", key);
                key.clone()
            } else {
                return Ok(()); // Not publishing
            }
        };

        let manager = self.stream_manager.read().await;
        if let Some(stream) = manager.streams.get(&session_key) {
            let _ = stream.sender.send(payload.to_vec());
        }

        Ok(())
    }

    async fn handle_video(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        debug!("Received video data: {} bytes", payload.len());

        let session_key = {
            let session_lock = self.session.lock().await;
            if let Some(Session {
                stream_key: Some(key),
                role: Some(Role::Publisher),
                ..
            }) = &*session_lock
            {
                debug!("Found stream key: {}", key);
                key.clone()
            } else {
                return Ok(()); // Not publishing
            }
        };

        let manager = self.stream_manager.read().await;
        if let Some(stream) = manager.streams.get(&session_key) {
            let _ = stream.sender.send(payload.to_vec());
        }

        Ok(())
    }

    async fn register_publisher(self: Arc<Self>, key: String) -> Result<()> {
        {
            let mut manager = self.stream_manager.write().await;
            let _stream = manager.get_or_create(&key);
        }

        {
            let mut session_lock = self.session.lock().await;
            *session_lock = Some(Session {
                addr: self.addr,
                stream_key: Some(key.clone()),
                role: Some(Role::Publisher),
            });
        }

        {
            let mut state_lock = self.state.lock().await;
            *state_lock = ConnectionState::Publishing;
        }

        info!("Registered {} as publisher for stream {}", self.addr, key);

        self.send_publish_response(&key).await?;

        Ok(())
    }

    async fn send_publish_response(self: Arc<Self>, stream_key: &str) -> Result<()> {
        let response = vec![
            Amf0Value::Utf8String("onStatus".into()),
            Amf0Value::Number(0.0), // Transaction ID
            Amf0Value::Null,        // No properties
            Amf0Value::Object(
                vec![
                    ("level".to_string(), Amf0Value::Utf8String("status".into())),
                    (
                        "code".to_string(),
                        Amf0Value::Utf8String("NetStream.Publish.Start".into()),
                    ),
                    (
                        "description".to_string(),
                        Amf0Value::Utf8String(format!("Started publishing stream {}", stream_key)),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        ];

        let data = serialize(&response)
            .map_err(|e| RtmpError::Amf0(format!("serialize error: {:?}", e)))?;

        let message = self.create_rtmp_message(MessageType::CommandAmf0 as u8, 1, &data)?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        info!("Sent publish response for stream {}", stream_key);
        Ok(())
    }

    async fn register_viewer(self: Arc<Self>, key: String) -> Result<()> {
        let mut manager = self.stream_manager.write().await;

        let stream = manager.get_or_create(&key);
        stream.receiver_count += 1;
        let mut receiver = stream.sender.subscribe();

        let writer = Arc::clone(&self.stream);

        tokio::spawn(async move {
            while let Ok(chunk) = receiver.recv().await {
                let mut stream = writer.lock().await;
                if let Err(e) = stream.write_all(&chunk).await {
                    error!("Failed to write to viewer: {}", e);
                    break;
                }
                if let Err(e) = stream.flush().await {
                    error!("Failed to flush to viewer: {}", e);
                    break;
                }
            }
        });

        {
            let mut session_lock = self.session.lock().await;
            *session_lock = Some(Session {
                addr: self.addr,
                stream_key: Some(key.clone()),
                role: Some(Role::Viewer),
            });
        }

        {
            let mut state_lock = self.state.lock().await;
            *state_lock = ConnectionState::Playing;
        }

        info!("Viewer {} subscribed to {}", self.addr, key);

        Ok(())
    }
}

use crate::utils::error::*;
use crate::utils::types::*;
// use bytes::BytesMut;
// use rand::{thread_rng, RngCore};
use rml_amf0::{serialize, Amf0Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
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
            next_stream_id: AtomicU32::new(1),
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
            let log_conn = Arc::clone(&conn); // preserved for logging

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
        let header = self_clone.parse_chunk_header(&header_data, chunk_headers)?;

        // Check for extended timestamp
        if (format == 0 || format == 1 || format == 2)
            && (header.timestamp == 0xFFFFFF || header.timestamp >= 0xFFFFFF)
        {
            let mut ext_timestamp = vec![0u8; 4];
            {
                let mut stream = self.stream.lock().await;
                stream.read_exact(&mut ext_timestamp).await?;
            }
            header_data.extend_from_slice(&ext_timestamp);
        }

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

        debug!(
            "Chunk: stream_id={}, format={}, remaining={}, chunk_size={}, payload_size={}",
            chunk_stream_id, format, remaining_in_message, chunk_size, payload_size
        );

        // Read payload
        let mut payload = vec![0u8; payload_size];
        if payload_size > 0 {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut payload).await?;
        }

        // Accumulate payload in buffer
        let buffer = chunk_buffers.entry(chunk_stream_id).or_default();
        buffer.extend_from_slice(&payload);

        debug!(
            "Buffer state: stream_id={}, buffer_len={}, expected_len={}",
            chunk_stream_id,
            buffer.len(),
            header.message_length
        );

        // Check if we have a complete message
        if buffer.len() >= header.message_length as usize {
            // CRITICAL FIX: Take only the exact message length, not the entire buffer
            debug!("Pre-reassembly state:");
            debug!("  Buffer length: {}", buffer.len());
            debug!("  Expected message length: {}", header.message_length);
            debug!(
                "  Buffer content (first 50 bytes): {:?}",
                &buffer[..std::cmp::min(50, buffer.len())]
            );
            let message_len = header.message_length as usize;
            let complete_message = buffer.drain(..message_len).collect::<Vec<u8>>();

            // Keep any remaining bytes for the next message
            debug!(
            "Complete message: stream_id={}, type={}, length={}, payload={}bytes, remaining_in_buffer={}",
            chunk_stream_id,
            header.message_type,
            header.message_length,
            complete_message.len(),
            buffer.len()
        );

            // Validate message length
            if complete_message.len() != header.message_length as usize {
                error!(
                    "Message length mismatch: expected={}, actual={}",
                    header.message_length,
                    complete_message.len()
                );
                return Err(RtmpError::Protocol("Message length mismatch".into()));
            }

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
            MessageType::CommandAmf3 => self.handle_command_amf3(payload).await?, // Type 17
            MessageType::DataAmf0 => self.handle_data_amf0(payload).await?,       // Type 18
            MessageType::DataAmf3 => self.handle_data_amf3(payload).await?,       // Type 15
            MessageType::SharedObjectAmf0 => self.handle_shared_object_amf0(payload).await?, // Type 19
            MessageType::SharedObjectAmf3 => self.handle_shared_object_amf3(payload).await?, // Type 16
            MessageType::Aggregate => self.handle_aggregate(payload).await?,
            _ => {
                debug!("Unhandled message type: {}", header.message_type);
            }
        }

        Ok(())
    }

    // === Message Handlers ===

    // Handle AMF3 commands (Type 17)
    async fn handle_command_amf3(&self, payload: &[u8]) -> Result<()> {
        debug!("Received AMF3 command message ({} bytes)", payload.len());
        // AMF3 is more complex than AMF0 - for now just log and ignore
        // You'd need an AMF3 parser to handle this properly
        debug!(
            "AMF3 command payload: {:02x?}",
            &payload[..payload.len().min(50)]
        );
        Ok(())
    }

    // Handle AMF0 data messages (Type 18)
    async fn handle_data_amf0(&self, payload: &[u8]) -> Result<()> {
        debug!("Received AMF0 data message ({} bytes)", payload.len());

        // Add hex dump for debugging (same as your command handler)
        debug!("AMF0 data payload hex dump:");
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

        // Try to parse AMF0 values (same pattern as your command handler)
        let mut cursor = &payload[..];
        let mut values = Vec::new();
        let mut value_index = 0;

        while !cursor.is_empty() {
            match rml_amf0::deserialize(&mut cursor) {
                Ok(mut parsed_values) => {
                    debug!(
                        "Successfully parsed {} AMF0 data values starting at index {}",
                        parsed_values.len(),
                        value_index
                    );
                    for (i, value) in parsed_values.iter().enumerate() {
                        debug!("  Data value {}: {:?}", value_index + i, value);
                    }
                    values.append(&mut parsed_values);
                    value_index += parsed_values.len();
                }
                Err(e) => {
                    debug!(
                        "AMF0 data parsing failed at byte offset {}: {:?}",
                        payload.len() - cursor.len(),
                        e
                    );
                    debug!("Remaining bytes: {:?}", cursor);
                    // For data messages, we can be more lenient and just log the error
                    break;
                }
            }
        }

        if values.is_empty() {
            debug!("No AMF0 data values parsed");
            return Ok(());
        }

        debug!("Total AMF0 data values parsed: {}", values.len());
        for (i, value) in values.iter().enumerate() {
            debug!("Final data value {}: {:?}", i, value);
        }

        // Handle common data message types
        if let Some(Amf0Value::Utf8String(command)) = values.get(0) {
            match command.as_str() {
                "@setDataFrame" => {
                    info!("Received metadata frame");
                    if let Some(Amf0Value::Utf8String(data_type)) = values.get(1) {
                        debug!("Metadata type: {}", data_type);
                        if data_type == "onMetaData" {
                            info!("Received onMetaData - stream metadata");
                            // This contains video/audio codec info, dimensions, etc.
                        }
                    }
                }
                "onMetaData" => {
                    info!("Received onMetaData directly");
                    // Sometimes sent directly without @setDataFrame wrapper
                }
                _ => {
                    debug!("Unknown AMF0 data command: {}", command);
                }
            }
        } else {
            debug!("First AMF0 data value is not a string: {:?}", values.get(0));
        }

        Ok(())
    }

    // Handle AMF3 data messages (Type 15)
    async fn handle_data_amf3(&self, payload: &[u8]) -> Result<()> {
        debug!("Received AMF3 data message ({} bytes)", payload.len());
        debug!(
            "AMF3 data payload: {:02x?}",
            &payload[..payload.len().min(50)]
        );
        // AMF3 parsing would be needed here
        Ok(())
    }

    // Handle AMF0 shared objects (Type 19)
    async fn handle_shared_object_amf0(&self, payload: &[u8]) -> Result<()> {
        debug!(
            "Received AMF0 shared object message ({} bytes)",
            payload.len()
        );
        debug!(
            "Shared object payload: {:02x?}",
            &payload[..payload.len().min(50)]
        );
        // Shared objects are used for synchronized data between client/server
        // For basic streaming, these can usually be ignored
        Ok(())
    }

    // Handle AMF3 shared objects (Type 16)
    async fn handle_shared_object_amf3(&self, payload: &[u8]) -> Result<()> {
        debug!(
            "Received AMF3 shared object message ({} bytes)",
            payload.len()
        );
        debug!(
            "Shared object payload: {:02x?}",
            &payload[..payload.len().min(50)]
        );
        Ok(())
    }

    // Handle aggregate messages (Type 22)
    async fn handle_aggregate(self: Arc<Self>, payload: &[u8]) -> Result<()> {
        debug!("Received aggregate message ({} bytes)", payload.len());

        // Aggregate messages contain multiple sub-messages
        // Format: [previous_tag_size][tag_header][tag_data][previous_tag_size][tag_header][tag_data]...
        let mut offset = 0;
        let mut tag_count = 0;

        while offset + 4 <= payload.len() {
            // Skip previous tag size (4 bytes)
            offset += 4;

            if offset + 11 > payload.len() {
                break;
            }

            // Read tag header (11 bytes)
            let tag_type = payload[offset];
            let data_size = u32::from_be_bytes([
                0,
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]) as usize;
            let timestamp = u32::from_be_bytes([
                0,
                payload[offset + 4],
                payload[offset + 5],
                payload[offset + 6],
            ]);
            let timestamp_extended = payload[offset + 7];
            let stream_id = u32::from_be_bytes([
                0,
                payload[offset + 8],
                payload[offset + 9],
                payload[offset + 10],
            ]);

            offset += 11;

            if offset + data_size > payload.len() {
                break;
            }

            debug!(
                "Aggregate tag {}: type={}, size={}, timestamp={}",
                tag_count, tag_type, data_size, timestamp
            );

            let data = &payload[offset..offset + data_size];

            // Process the tag data based on type
            match tag_type {
                8 => {
                    debug!("Processing audio tag from aggregate");
                    Arc::clone(&self).handle_audio(data).await?;
                }
                9 => {
                    debug!("Processing video tag from aggregate");
                    Arc::clone(&self).handle_video(data).await?;
                }
                18 => {
                    debug!("Processing script data from aggregate");
                    Arc::clone(&self).handle_data_amf0(data).await?;
                }
                _ => {
                    debug!("Unknown tag type in aggregate: {}", tag_type);
                }
            }

            offset += data_size;
            tag_count += 1;
        }

        debug!("Processed {} tags from aggregate message", tag_count);
        Ok(())
    }

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

    async fn send_window_ack_size(&self, size: u32) -> Result<()> {
        let data = size.to_be_bytes();
        // Use chunk stream ID 2 for protocol control messages
        let message = self.create_rtmp_message(
            2, // chunk_stream_id for protocol control
            MessageType::WindowAckSize as u8,
            0, // message_stream_id
            &data,
        )?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        debug!("Sent window ack size: {}", size);
        Ok(())
    }

    async fn send_set_peer_bandwidth(&self, size: u32, limit_type: u8) -> Result<()> {
        let mut data = Vec::new();
        data.extend_from_slice(&size.to_be_bytes());
        data.push(limit_type);

        // Use chunk stream ID 2 for protocol control messages
        let message = self.create_rtmp_message(
            2, // chunk_stream_id for protocol control
            MessageType::SetPeerBandwidth as u8,
            0, // message_stream_id
            &data,
        )?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        debug!("Sent set peer bandwidth: {} (type: {})", size, limit_type);
        Ok(())
    }

    async fn send_set_chunk_size(&self, size: u32) -> Result<()> {
        let data = size.to_be_bytes();
        // Use chunk stream ID 2 for protocol control messages
        let message = self.create_rtmp_message(
            2, // chunk_stream_id for protocol control
            MessageType::SetChunkSize as u8,
            0, // message_stream_id
            &data,
        )?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&message).await?;
        stream.flush().await?;

        debug!("Sent set chunk size: {}", size);
        Ok(())
    }

    
    // Replace your handle_command_amf0 method with this more robust version
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

        // Robust AMF0 parsing - handle partial failures gracefully
        let values = match self.parse_amf0_with_fallback(payload) {
            Ok(vals) => vals,
            Err(e) => {
                error!("Critical AMF0 parsing failure: {:?}", e);
                return Err(e);
            }
        };

        if values.is_empty() {
            debug!("No AMF0 values parsed");
            return Ok(());
        }

        debug!("Total AMF0 values parsed: {}", values.len());
        for (i, value) in values.iter().enumerate() {
            debug!("Final value {}: {:?}", i, value);
        }

        // Extract command name
        let command = match values.get(0) {
            Some(Amf0Value::Utf8String(cmd)) => cmd.clone(),
            _ => {
                debug!("First AMF0 value is not a string command");
                return Ok(());
            }
        };

        info!("Received AMF0 command: {}", command);

        // Handle the command
        match command.as_str() {
            "connect" => {
                info!("Processing connect command");
                let params = &values[1..];
                debug!("Connect command parameters: {:?}", params);
                self.send_connect_response(params).await?;
                // send_connect_response
            }
            "releaseStream" => {
                let params = &values[1..];
                self.handle_release_stream_command(params).await?;
            }
            "FCPublish" => {
                let params = &values[1..];
                self.handle_fc_publish_command(params).await?;
            }
            "FCUnpublish" => {
                let params = &values[1..];
                self.handle_fc_unpublish_command(params).await?;
            }
            "createStream" => {
                info!("Processing createStream command");
                let params = &values[1..];
                self.handle_create_stream_command(params).await?;
            }
            "publish" => {
                info!("Processing publish command");
                let params = &values[1..];
                self.handle_publish_command(params).await?;
            }
            "deleteStream" => {
                let params = &values[1..];
                self.handle_delete_stream_command(params).await?;
            }
            "closeStream" => {
                info!("Processing closeStream command");
                // Similar to deleteStream, cleanup resources
                debug!("Stream closed by client");
            }
            "play" => {
                info!("Processing play command");
                let params = &values[1..];
                // Handle play command for viewers
                self.handle_play_command(params).await?;
            }
            _ => {
                debug!("Unknown AMF0 command: {}", command);
            }
        }

        Ok(())
    }

    // New robust parsing function that handles UTF-8 errors gracefully
    fn parse_amf0_with_fallback(&self, payload: &[u8]) -> Result<Vec<Amf0Value>> {
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
                    warn!(
                        "AMF0 parsing failed at byte offset {}: {:?}",
                        payload.len() - cursor.len(),
                        e
                    );
                    debug!("Remaining bytes: {:?}", cursor);

                    // Try to recover by skipping problematic data
                    if let Some(recovery_point) = self.find_next_amf0_value(&cursor) {
                        warn!("Attempting recovery at offset {}", recovery_point);
                        cursor = &cursor[recovery_point..];
                        continue;
                    } else {
                        // If we can't recover and we have some values, return what we have
                        if !values.is_empty() {
                            warn!("Partial AMF0 parse - returning {} values", values.len());
                            break;
                        }
                        // If no values were parsed, this is a critical error
                        return Err(RtmpError::Amf0(format!(
                            "AMF0 deserialization error: {:?}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(values)
    }

    // Helper function to find the next valid AMF0 type marker
    fn find_next_amf0_value(&self, data: &[u8]) -> Option<usize> {
        // AMF0 type markers
        const AMF0_NUMBER: u8 = 0x00;
        const AMF0_BOOLEAN: u8 = 0x01;
        const AMF0_STRING: u8 = 0x02;
        const AMF0_OBJECT: u8 = 0x03;
        const AMF0_NULL: u8 = 0x05;
        const AMF0_UNDEFINED: u8 = 0x06;
        const AMF0_ARRAY: u8 = 0x08;
        const AMF0_OBJECT_END: u8 = 0x09;

        for (i, &byte) in data.iter().enumerate() {
            match byte {
                AMF0_NUMBER | AMF0_BOOLEAN | AMF0_STRING | AMF0_OBJECT | AMF0_NULL
                | AMF0_UNDEFINED | AMF0_ARRAY | AMF0_OBJECT_END => {
                    if i > 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        None
    }

    async fn send_connect_response(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        // Extract transaction ID from params (usually at index 0)
        let transaction_id = match params.get(0) {
            Some(Amf0Value::Number(id)) => *id,
            _ => 1.0, // Default transaction ID
        };

        // Send _result response
        let response_values = vec![
            Amf0Value::Utf8String("_result".to_string()),
            Amf0Value::Number(transaction_id),
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "fmsVer".to_string(),
                    Amf0Value::Utf8String("FMS/3,0,1,123".to_string()),
                ),
                ("capabilities".to_string(), Amf0Value::Number(31.0)),
            ])),
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "level".to_string(),
                    Amf0Value::Utf8String("status".to_string()),
                ),
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetConnection.Connect.Success".to_string()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String("Connection succeeded.".to_string()),
                ),
                ("objectEncoding".to_string(), Amf0Value::Number(0.0)),
            ])),
        ];

        self.send_amf0_command(3, &response_values).await?;
        info!("Sent connect response");
        Ok(())
    }

    async fn handle_create_stream_command(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        // Extract transaction ID from params
        let transaction_id = match params.get(0) {
            Some(Amf0Value::Number(id)) => *id,
            _ => 2.0, // Default transaction ID
        };

        // Generate a new stream ID using atomic counter
        let stream_id = self
            .next_stream_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as f64;

        let response_values = vec![
            Amf0Value::Utf8String("_result".to_string()),
            Amf0Value::Number(transaction_id),
            Amf0Value::Null,
            Amf0Value::Number(stream_id),
        ];

        self.send_amf0_command(3, &response_values).await?;
        info!(
            "Sent createStream response with stream ID: {} (transaction: {})",
            stream_id, transaction_id
        );
        Ok(())
    }

    async fn handle_publish_command(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        // Extract stream name from params
        let stream_name = match params.get(1) {
            Some(Amf0Value::Utf8String(name)) => name.clone(),
            _ => {
                return Err(RtmpError::Protocol(
                    "Missing stream name in publish command".to_string(),
                ))
            }
        };

        // Extract publish type (optional)
        let publish_type = match params.get(2) {
            Some(Amf0Value::Utf8String(ptype)) => ptype.clone(),
            _ => "live".to_string(), // Default to live
        };

        info!(
            "Publishing stream: {} (type: {})",
            stream_name, publish_type
        );

        // Send onStatus with NetStream.Publish.Start
        let response_values = vec![
            Amf0Value::Utf8String("onStatus".to_string()),
            Amf0Value::Number(0.0), // Transaction ID
            Amf0Value::Null,
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "level".to_string(),
                    Amf0Value::Utf8String("status".to_string()),
                ),
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetStream.Publish.Start".to_string()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String(format!("Started publishing stream '{}'.", stream_name)),
                ),
                (
                    "details".to_string(),
                    Amf0Value::Utf8String(stream_name.clone()),
                ),
            ])),
        ];

        Arc::clone(&self)
            .register_publisher(stream_name.clone())
            .await?;
        self.send_amf0_command(3, &response_values).await?;
        info!("Sent publish response for stream: {}", stream_name);
        Ok(())
    }

    async fn handle_play_command(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        info!("Processing play command");

        let stream_name = if params.len() >= 3 {
            match &params[2] {
                Amf0Value::Utf8String(name) => Some(name.clone()),
                _ => None,
            }
        } else {
            None
        };

        if let Some(name) = stream_name {
            info!("Client wants to play stream: {}", name);

            // Send play response
            Arc::clone(&self).register_viewer(name.clone()).await?;
            // Arc::clone(&self).register_viewer(&name).await?;
            self.send_play_response(&name).await?;
        } else {
            warn!("Play command missing stream name");
        }

        Ok(())
    }

    // Send play response
    async fn send_play_response(self: Arc<Self>, stream_name: &str) -> Result<()> {
        // Send onStatus with NetStream.Play.Start
        let response_values = vec![
            Amf0Value::Utf8String("onStatus".to_string()),
            Amf0Value::Number(0.0),
            Amf0Value::Null,
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "level".to_string(),
                    Amf0Value::Utf8String("status".to_string()),
                ),
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetStream.Play.Start".to_string()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String(format!("Started playing stream '{}'.", stream_name)),
                ),
                (
                    "details".to_string(),
                    Amf0Value::Utf8String(stream_name.to_string()),
                ),
            ])),
        ];

        self.send_amf0_command(3, &response_values).await?;
        info!("Sent play response for stream: {}", stream_name);
        Ok(())
    }

    async fn handle_fc_publish_command(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        info!("Processing FCPublish command");

        // Extract stream name from parameters
        let stream_name = if params.len() >= 3 {
            match &params[2] {
                Amf0Value::Utf8String(name) => Some(name.clone()),
                _ => None,
            }
        } else {
            None
        };

        if let Some(name) = stream_name {
            info!("FCPublish for stream: {}", name);

            // Send FCPublish response (onFCPublish)
            self.send_fc_publish_response(&name).await?;
        } else {
            warn!("FCPublish command missing stream name");
        }

        Ok(())
    }

    // Handle releaseStream command - releases a stream for publishing
    async fn handle_release_stream_command(&self, params: &[Amf0Value]) -> Result<()> {
        info!("Processing releaseStream command");

        // Extract stream name from parameters
        let stream_name = if params.len() >= 3 {
            match &params[2] {
                Amf0Value::Utf8String(name) => Some(name.clone()),
                _ => None,
            }
        } else {
            None
        };

        if let Some(name) = stream_name {
            info!("Releasing stream: {}", name);

            // In a full implementation, you'd remove the stream from active streams
            // For now, just acknowledge the command
            debug!("Stream '{}' released (no-op in this implementation)", name);
        } else {
            warn!("releaseStream command missing stream name");
        }

        // releaseStream typically doesn't send a response
        Ok(())
    }

    // Handle FCUnpublish command - counterpart to FCPublish
    async fn handle_fc_unpublish_command(self: Arc<Self>, params: &[Amf0Value]) -> Result<()> {
        info!("Processing FCUnpublish command");

        let stream_name = if params.len() >= 3 {
            match &params[2] {
                Amf0Value::Utf8String(name) => Some(name.clone()),
                _ => None,
            }
        } else {
            None
        };

        if let Some(name) = stream_name {
            info!("FCUnpublish for stream: {}", name);

            // Send FCUnpublish response
            self.send_fc_unpublish_response(&name).await?;
        } else {
            warn!("FCUnpublish command missing stream name");
        }

        Ok(())
    }

    // Handle deleteStream command
    async fn handle_delete_stream_command(&self, params: &[Amf0Value]) -> Result<()> {
        info!("Processing deleteStream command");

        let stream_id = if params.len() >= 3 {
            match &params[2] {
                Amf0Value::Number(id) => Some(*id as u32),
                _ => None,
            }
        } else {
            None
        };

        if let Some(id) = stream_id {
            info!("Deleting stream ID: {}", id);
            // In a full implementation, clean up the stream resources
        } else {
            warn!("deleteStream command missing stream ID");
        }

        // deleteStream typically doesn't send a response
        Ok(())
    }

    // Send FCPublish response
    async fn send_fc_publish_response(self: Arc<Self>, stream_name: &str) -> Result<()> {
        let response_values = vec![
            Amf0Value::Utf8String("onFCPublish".to_string()),
            Amf0Value::Number(0.0), // Transaction ID
            Amf0Value::Null,
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetStream.Publish.Start".to_string()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String(format!("Started publishing stream '{}'.", stream_name)),
                ),
            ])),
        ];

        self.send_amf0_command(3, &response_values).await?;
        debug!("Sent FCPublish response for stream: {}", stream_name);
        Ok(())
    }

    // Send FCUnpublish response
    async fn send_fc_unpublish_response(self: Arc<Self>, stream_name: &str) -> Result<()> {
        let response_values = vec![
            Amf0Value::Utf8String("onFCUnpublish".to_string()),
            Amf0Value::Number(0.0), // Transaction ID
            Amf0Value::Null,
            Amf0Value::Object(std::collections::HashMap::from([
                (
                    "code".to_string(),
                    Amf0Value::Utf8String("NetStream.Unpublish.Success".to_string()),
                ),
                (
                    "description".to_string(),
                    Amf0Value::Utf8String(format!("Stopped publishing stream '{}'.", stream_name)),
                ),
            ])),
        ];

        self.send_amf0_command(3, &response_values).await?;
        debug!("Sent FCUnpublish response for stream: {}", stream_name);
        Ok(())
    }

    fn create_rtmp_message(
        &self,
        chunk_stream_id: u32,
        message_type: u8,
        message_stream_id: u32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut message = Vec::new();

        // Basic header (format 0, dynamic chunk stream ID)
        if chunk_stream_id <= 63 {
            // Single byte basic header
            message.push(chunk_stream_id as u8);
        } else if chunk_stream_id <= 319 {
            // Two byte basic header
            message.push(0); // Format 0, CSID = 0 (indicates 2-byte form)
            message.push((chunk_stream_id - 64) as u8);
        } else {
            // Three byte basic header
            message.push(1); // Format 0, CSID = 1 (indicates 3-byte form)
            let csid_minus_64 = chunk_stream_id - 64;
            message.push((csid_minus_64 & 0xFF) as u8);
            message.push(((csid_minus_64 >> 8) & 0xFF) as u8);
        }

        // Message header (11 bytes for format 0)
        // Timestamp (3 bytes) - using 0 for now
        message.extend_from_slice(&[0, 0, 0]);

        // Message length (3 bytes)
        let length = payload.len() as u32;
        message.extend_from_slice(&[(length >> 16) as u8, (length >> 8) as u8, length as u8]);

        // Message type (1 byte)
        message.push(message_type);

        // Message Stream ID (4 bytes, little endian)
        message.extend_from_slice(&message_stream_id.to_le_bytes());

        // Payload
        message.extend_from_slice(payload);

        Ok(message)
    }

    async fn send_amf0_command(
        self: Arc<Self>,
        chunk_stream_id: u32,
        values: &[Amf0Value],
    ) -> Result<()> {
        // Serialize AMF0 values to payload
        let payload = serialize(&values.to_vec()).unwrap();

        // Use the dynamic create_rtmp_message function
        let message_type = 20; // AMF0 Command Message
        let message_stream_id = 0; // Message stream ID (usually 0 for control messages)

        let rtmp_message =
            self.create_rtmp_message(chunk_stream_id, message_type, message_stream_id, &payload)?;

        // Send the message using Arc<Mutex<>>
        let mut stream = self.stream.lock().await;
        stream.write_all(&rtmp_message).await?;
        stream.flush().await?;

        Ok(())
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

        let message = self.create_rtmp_message(3, MessageType::CommandAmf0 as u8, 1, &data)?;

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

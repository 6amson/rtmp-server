use std::collections::HashMap;
use tokio::sync::broadcast;
use crate::utils::types::{StreamManager, Stream};


impl StreamManager {
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }
    
    pub fn create_stream(&mut self, key: String) -> broadcast::Receiver<Vec<u8>> {
        let (tx, rx) = broadcast::channel(1024);
        let stream = Stream {
            key: key.clone(),
            sender: tx,
            receiver_count: 0,
        };
        self.streams.insert(key, stream);
        rx
    }
}
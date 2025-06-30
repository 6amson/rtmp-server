use std::collections::HashMap;
use tokio::sync::broadcast;
use crate::utils::types::{StreamManager, Stream};


impl StreamManager {
    pub fn new() -> Self {
        Self { streams: HashMap::new() }
    }

    pub fn get_or_create(&mut self, key: &str) -> &mut Stream {
        self.streams
            .entry(key.to_string())
            .or_insert_with(|| Stream {
                key: key.to_string(),
                sender: broadcast::channel(100).0,
                receiver_count: 0,
            })
    }

    pub fn get(&self, key: &str) -> Option<&Stream> {
        self.streams.get(key)
    }

}
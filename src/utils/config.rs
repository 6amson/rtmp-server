use serde::{Deserialize, Serialize};
use std::fs;

use crate::utils::types::Config;

impl Default for Config {
    fn default() -> Self {
        Self {
            chunk_size: 1536,
            max_connections: 1000,
            handshake_timeout: 5000, // ms
            stream_timeout: 30000,   // ms
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct GeneralConfig {
    pub device_type: String,
    pub log_directory: String,
    pub log_rotation_hours: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum BaudRateSetting {
    Numeric(u32),
    StringVal(String),
}

#[derive(Debug, Deserialize, Clone)]
pub struct SerialConfig {
    pub port: String,
    pub baud_rate: BaudRateSetting,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NtripConfig {
    pub enabled: bool,
    pub caster_host: String,
    pub caster_port: u16,
    pub username: String,
    pub password: String,
    pub mountpoint: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MqttConfig {
    pub enabled: bool,
    pub broker_host: String,
    pub broker_port: u16,
    pub client_id: String,
    pub topic: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WatchdogConfig {
    pub check_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub serial: SerialConfig,
    pub ntrip: NtripConfig,
    pub mqtt: MqttConfig,
    pub watchdog: WatchdogConfig,
}

impl AppConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)
            .context("Failed to read configuration file")?;
        let config: AppConfig = serde_json::from_str(&content)
            .context("Failed to parse config.json")?;
        Ok(config)
    }
}

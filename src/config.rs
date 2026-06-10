use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct GeneralConfig {
    pub device_id: String,
    pub device_type: String,
    pub log_directory: String,
    pub log_rotation_hours: u64,
    #[serde(default)]
    pub utc_offset_hours: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum BaudRateSetting {
    Numeric(u32),
    StringVal(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SerialConfig {
    pub port: String,
    pub baud_rate: BaudRateSetting,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct NtripConfig {
    pub enabled: bool,
    pub caster_host: String,
    pub caster_port: u16,
    pub username: String,
    pub password: String,
    pub mountpoint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MqttConfig {
    pub enabled: bool,
    pub broker_host: String,
    pub broker_port: u16,
    pub client_id: String,
    pub topic: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WatchdogConfig {
    #[allow(dead_code)]
    pub check_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WebConfig {
    pub enabled: bool,
    pub port: u16,
    /// Random secret used as URL path prefix to gate access. When set, all
    /// HTTP / WS routes are nested under `/<access_token>/`. Auto-generated
    /// (64 hex chars) on first run if missing or empty, and persisted back
    /// to config.json. Set to `""` to disable (NOT recommended when the
    /// web server is exposed via a public tunnel).
    #[serde(default)]
    pub access_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub serial: SerialConfig,
    pub ntrip: NtripConfig,
    pub mqtt: MqttConfig,
    pub watchdog: WatchdogConfig,
    pub web: WebConfig,
}

impl AppConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)
            .context("Failed to read configuration file")?;
        let config: AppConfig = serde_json::from_str(&content)
            .context("Failed to parse config.json")?;
        Ok(config)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize configuration to JSON")?;
        fs::write(path, content)
            .context("Failed to write configuration file")?;
        Ok(())
    }

    /// Ensure `web.access_token` is set on first run. Behaviour:
    ///   * missing field (None)   → generate a fresh 64-hex-char token and
    ///                              persist it (first-time setup).
    ///   * explicitly `""`        → user disabled auth on purpose — leave
    ///                              alone; the web server runs without a
    ///                              URL-prefix gate.
    ///   * non-empty string       → keep as-is.
    /// Returns true if the config was modified.
    pub fn ensure_web_access_token<P: AsRef<Path>>(&mut self, path: P) -> Result<bool> {
        if self.web.access_token.is_some() {
            return Ok(false);
        }
        let token = generate_secret_hex(32)?;
        log::info!("Generated new web access_token and saving to config.");
        self.web.access_token = Some(token);
        self.save_to_file(path)?;
        Ok(true)
    }
}

/// Read `n_bytes` from /dev/urandom and return as lowercase hex.
fn generate_secret_hex(n_bytes: usize) -> Result<String> {
    use std::io::Read;
    let mut buf = vec![0u8; n_bytes];
    let mut f = fs::File::open("/dev/urandom")
        .context("Failed to open /dev/urandom for token generation")?;
    f.read_exact(&mut buf)
        .context("Failed to read entropy from /dev/urandom")?;
    let mut s = String::with_capacity(n_bytes * 2);
    for b in &buf {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_serialize_deserialize() {
        let json_str = r#"
        {
          "general": {
            "device_id": "test-device",
            "device_type": "ublox",
            "log_directory": "./logs",
            "log_rotation_hours": 24,
            "utc_offset_hours": 7
          },
          "serial": {
            "port": "/dev/ttyTest",
            "baud_rate": 115200
          },
          "ntrip": {
            "enabled": true,
            "caster_host": "127.0.0.1",
            "caster_port": 2101,
            "username": "user",
            "password": "pwd",
            "mountpoint": "TEST"
          },
          "mqtt": {
            "enabled": false,
            "broker_host": "localhost",
            "broker_port": 1883,
            "client_id": "test_client",
            "topic": "test/topic",
            "username": "",
            "password": ""
          },
          "watchdog": {
            "check_interval_secs": 1,
            "heartbeat_timeout_secs": 5
          },
          "web": {
            "enabled": true,
            "port": 8081
          }
        }
        "#;

        let config: AppConfig = serde_json::from_str(json_str).unwrap();
        assert_eq!(config.general.device_id, "test-device");
        assert_eq!(config.serial.port, "/dev/ttyTest");
        
        let matches_baud = match config.serial.baud_rate {
            BaudRateSetting::Numeric(n) => n == 115200,
            _ => false,
        };
        assert!(matches_baud);

        // Serialize back
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized_again: AppConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized_again);
    }
}

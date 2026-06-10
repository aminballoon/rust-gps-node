mod config;
mod logger;
mod mqtt_reporter;
mod ntrip;
mod parser;
mod serial_gps;
mod watchdog;
mod web_server;

use config::AppConfig;
use watchdog::Supervisor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set default log level to 'info' if not already specified by the environment
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }

    // Check if command line argument is a scan command
    let args: Vec<String> = std::env::args().collect();
    
    // Resolve config path:
    // 1. Check GPS_CONFIG_PATH environment variable
    // 2. Or check if a path is passed as a command line argument (excluding "scan" commands)
    // 3. Fall back to "config.json"
    let mut config_path = std::env::var("GPS_CONFIG_PATH").unwrap_or_else(|_| "config.json".to_string());
    if args.len() > 1 && !args[1].starts_with('-') && !args[1].contains("scan") {
        config_path = args[1].clone();
    }

    // Load config first to read UTC offset
    let config_res = AppConfig::load_from_file(&config_path);
    let utc_offset = match &config_res {
        Ok(cfg) => cfg.general.utc_offset_hours,
        Err(_) => 0,
    };

    let mut builder = env_logger::Builder::from_default_env();
    builder.format(move |buf, record| {
        use std::io::Write;
        use chrono::FixedOffset;
        let tz = FixedOffset::east_opt(utc_offset * 3600).unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
        let ts = chrono::Utc::now().with_timezone(&tz).format("%Y-%m-%dT%H:%M:%S%.3f%:z");
        writeln!(
            buf,
            "[{} {} {}] {}",
            ts,
            record.level(),
            record.target(),
            record.args()
        )
    });
    builder.init();

    if args.iter().any(|arg| {
        arg == "scan"
            || arg == "scan-mountpoints"
            || arg == "scan_mountpoints"
            || arg == "scan_mountpoint"
            || arg == "scan-mountpoint"
            || arg == "--scan"
    }) {
        log::info!("Loading configuration for scan from: {}", config_path);
        let config = config_res?;
        ntrip::scan_mountpoints(&config.ntrip).await?;
        return Ok(());
    }

    log::info!("=== Starting GPS RTK/PPK System Node ===");

    // Load configuration file
    log::info!("Loading configuration from: {}", config_path);
    let mut config = config_res?;

    // Generate web access_token on first run if missing — gates the web/ws
    // endpoints behind a long random URL prefix.
    if let Err(e) = config.ensure_web_access_token(&config_path) {
        log::warn!("Could not persist generated access_token: {:?}", e);
    }

    // Print parsed parameters to verify load
    log::info!("Device Type Configured: {}", config.general.device_type);
    let baud_str = match &config.serial.baud_rate {
        config::BaudRateSetting::Numeric(n) => n.to_string(),
        config::BaudRateSetting::StringVal(s) => s.clone(),
    };
    log::info!("Serial Port Configured: {} at {} baud", config.serial.port, baud_str);
    if config.ntrip.enabled {
        log::info!("NTRIP Client Configured to: {}@{}", config.ntrip.mountpoint, config.ntrip.caster_host);
    } else {
        log::info!("NTRIP Client is Disabled");
    }
    if config.mqtt.enabled {
        log::info!("MQTT Reporter Configured to: {}", config.mqtt.broker_host);
    } else {
        log::info!("MQTT Reporter is Disabled");
    }
    if config.web.enabled {
        log::info!("Web Server Configured on Port: {}", config.web.port);
    } else {
        log::info!("Web Server is Disabled");
    }

    // Start Supervisor / Watchdog Monitor
    let supervisor = Supervisor::new(config, config_path);
    supervisor.run().await;

    Ok(())
}

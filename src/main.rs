mod config;
mod logger;
mod mqtt_reporter;
mod ntrip;
mod parser;
mod serial_gps;
mod watchdog;

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
    env_logger::init();

    // Check if command line argument is a scan command
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| {
        arg == "scan"
            || arg == "scan-mountpoints"
            || arg == "scan_mountpoints"
            || arg == "scan_mountpoint"
            || arg == "scan-mountpoint"
            || arg == "--scan"
    }) {
        let config_path = "config.json";
        log::info!("Loading configuration for scan from: {}", config_path);
        let config = AppConfig::load_from_file(config_path)?;
        ntrip::scan_mountpoints(&config.ntrip).await?;
        return Ok(());
    }

    log::info!("=== Starting GPS RTK/PPK System Node ===");

    // Load configuration file
    let config_path = "config.json";
    log::info!("Loading configuration from: {}", config_path);
    let config = AppConfig::load_from_file(config_path)?;

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

    // Start Supervisor / Watchdog Monitor
    let supervisor = Supervisor::new(config);
    supervisor.run().await;

    Ok(())
}

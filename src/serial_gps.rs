use crate::config::SerialConfig;
use crate::logger::PpkLogger;
use crate::parser::Parser;
use crate::watchdog::WatchdogMsg;
use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver as TokioReceiver, Sender as TokioSender};
use tokio::time::sleep;

pub async fn run_serial(
    config: SerialConfig,
    device_type: String,
    log_dir: String,
    log_rotation_hours: u64,
    mut rtcm_rx: TokioReceiver<Vec<u8>>,
    watchdog_tx: TokioSender<WatchdogMsg>,
) -> Result<()> {
    let (tx_err, rx_err) = std::sync::mpsc::channel::<anyhow::Error>();
    let (sync_rtcm_tx, sync_rtcm_rx) = std::sync::mpsc::channel::<Vec<u8>>();

    // Bridge task to forward async RTCM messages to the sync thread channel
    let rtcm_bridge_handle = tokio::spawn(async move {
        while let Some(data) = rtcm_rx.recv().await {
            if sync_rtcm_tx.send(data).is_err() {
                break;
            }
        }
    });

    let config_clone = config.clone();
    let watchdog_tx_clone = watchdog_tx.clone();
    let device_type_clone = device_type.clone();
    
    let serial_thread_handle = thread::spawn(move || {
        let result = (move || -> Result<()> {
            let baud_rate = match &config_clone.baud_rate {
                crate::config::BaudRateSetting::Numeric(n) => *n,
                crate::config::BaudRateSetting::StringVal(s) => {
                    if s.to_lowercase() == "auto" {
                        match auto_detect_baud_rate(&config_clone.port) {
                            Some(detected) => detected,
                            None => {
                                log::warn!("Baud rate auto-detection failed. Falling back to 115200");
                                115200
                            }
                        }
                    } else {
                        s.parse::<u32>().unwrap_or(115200)
                    }
                }
            };

            log::info!("Opening serial port: {} at {} baud", config_clone.port, baud_rate);
            let mut port_reader = serialport::new(&config_clone.port, baud_rate)
                .timeout(Duration::from_millis(100))
                .open()
                .context(format!("Failed to open serial port {}", config_clone.port))?;

            let mut port_writer = port_reader
                .try_clone()
                .context("Failed to clone serial port for writing")?;

            // Configure receiver raw logs for PPK
            log::info!("Configuring GNSS receiver ({}) for PPK raw logging...", device_type_clone);
            if device_type_clone == "ublox" {
                let rawx_cmd = make_ubx_cfg_msg(0x02, 0x15);
                let sfrbx_cmd = make_ubx_cfg_msg(0x02, 0x13);
                
                if let Err(e) = port_writer.write_all(&rawx_cmd) {
                    log::error!("Failed to write UBX-RXM-RAWX command: {:?}", e);
                }
                let _ = port_writer.flush();
                thread::sleep(Duration::from_millis(100));
                
                if let Err(e) = port_writer.write_all(&sfrbx_cmd) {
                    log::error!("Failed to write UBX-RXM-SFRBX command: {:?}", e);
                }
                let _ = port_writer.flush();
                thread::sleep(Duration::from_millis(100));
                log::info!("Sent U-blox raw logging configuration (RXM-RAWX, RXM-SFRBX).");
            } else if device_type_clone == "unicore" {
                let commands = [
                    "unlogall\r\n",
                    "gngga 1\r\n",
                    "gnrmc 1\r\n",
                    "gnhdt 1\r\n",
                    "obsvmb 1\r\n",
                    "obsvhb 1\r\n",
                    "gpsephb onchanged\r\n",
                    "bdsephb onchanged\r\n",
                    "gloephb onchanged\r\n",
                    "galephb onchanged\r\n",
                    "saveconfig\r\n",
                ];
                for cmd in &commands {
                    if let Err(e) = port_writer.write_all(cmd.as_bytes()) {
                        log::error!("Failed to write Unicore command '{}': {:?}", cmd.trim(), e);
                    }
                    let _ = port_writer.flush();
                    thread::sleep(Duration::from_millis(100));
                }
                log::info!("Sent Unicore raw logging configuration (obsv, constellation ephemerides, NMEA).");
            }

            // Spawn the serial writer thread
            thread::spawn(move || {
                while let Ok(rtcm_data) = sync_rtcm_rx.recv() {
                    if let Err(e) = port_writer.write_all(&rtcm_data) {
                        log::error!("Failed to write RTCM to serial port: {:?}", e);
                        break;
                    }
                    if let Err(e) = port_writer.flush() {
                        log::error!("Failed to flush serial port writer: {:?}", e);
                        break;
                    }
                    log::debug!("Sent {} bytes of RTCM to GPS", rtcm_data.len());
                }
                log::warn!("Serial writer thread exited");
            });

            // Reader loop
            let mut ppk_logger = PpkLogger::new(log_dir, log_rotation_hours);
            let mut parser = Parser::new();
            let mut read_buf = vec![0u8; 1024];
            let mut last_heartbeat = Instant::now();
            let mut last_telemetry_send = Instant::now();

            loop {
                // Send heartbeat to watchdog
                if last_heartbeat.elapsed() >= Duration::from_secs(1) {
                    if watchdog_tx_clone.blocking_send(WatchdogMsg::Heartbeat("serial".to_string())).is_err() {
                        log::error!("Watchdog channel closed");
                        break;
                    }
                    last_heartbeat = Instant::now();
                }

                // Blocking read from serial port
                match port_reader.read(&mut read_buf) {
                    Ok(n) if n > 0 => {
                        let data = &read_buf[..n];
                        // 1. Log to PPK raw file
                        if let Err(e) = ppk_logger.write(data) {
                            log::error!("PPK logger error: {:?}", e);
                        }
                        
                        // 2. Feed to stream parser & extract GGA NMEA sentences for NTRIP
                        let ggas = parser.consume(data);
                        for gga in ggas {
                            let _ = watchdog_tx_clone.blocking_send(WatchdogMsg::Gga(gga));
                        }

                        // 3. Send telemetry at 1Hz
                        if last_telemetry_send.elapsed() >= Duration::from_secs(1) {
                            if watchdog_tx_clone.blocking_send(WatchdogMsg::Telemetry(parser.telemetry.clone())).is_err() {
                                log::error!("Watchdog channel closed when sending telemetry");
                                break;
                            }
                            last_telemetry_send = Instant::now();
                        }
                    }
                    Ok(_) => {} // Timeout (0 bytes)
                    Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {} // Expected
                    Err(e) => {
                        return Err(anyhow::anyhow!("Serial read error: {:?}", e));
                    }
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            let _ = tx_err.send(e);
        }
    });

    // Monitor the native threads from this async function
    loop {
        if let Ok(err) = rx_err.try_recv() {
            rtcm_bridge_handle.abort();
            return Err(err);
        }
        if serial_thread_handle.is_finished() {
            rtcm_bridge_handle.abort();
            return Err(anyhow::anyhow!("Serial reader thread exited unexpectedly"));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn auto_detect_baud_rate(port_path: &str) -> Option<u32> {
    let candidate_rates = [115200, 9600, 230400, 38400, 57600, 921600, 460800, 19200];
    
    for &baud in &candidate_rates {
        log::info!("Scanning baud rate {} on {}...", baud, port_path);
        
        let mut port = match serialport::new(port_path, baud)
            .timeout(Duration::from_millis(100))
            .open() 
        {
            Ok(p) => p,
            Err(_) => continue,
        };
        
        let mut parser = Parser::new();
        let mut read_buf = vec![0u8; 1024];
        let start = Instant::now();
        let mut detected = false;
        
        // Read for up to 800ms
        while start.elapsed() < Duration::from_millis(800) {
            match port.read(&mut read_buf) {
                Ok(n) if n > 0 => {
                    let data = &read_buf[..n];
                    parser.consume(data);
                    // Check if we parsed anything valid
                    if parser.telemetry.satellites > 0 
                        || !parser.telemetry.fix_type.is_empty() 
                        || parser.telemetry.latitude.is_some()
                        || parser.telemetry.heading.is_some()
                    {
                        detected = true;
                        break;
                    }
                }
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => break,
            }
            thread::sleep(Duration::from_millis(10));
        }
        
        if detected {
            log::info!("Auto-detected correct baud rate: {}!", baud);
            return Some(baud);
        }
    }
    None
}

fn make_ubx_cfg_msg(target_class: u8, target_id: u8) -> Vec<u8> {
    let mut packet = vec![
        0xB5, 0x62, // Sync
        0x06, 0x01, // Class, ID of CFG-MSG
        0x03, 0x00, // Length (3 bytes)
        target_class,
        target_id,
        0x01, // Rate on current port (1 means output every epoch)
    ];
    let (ck_a, ck_b) = crate::parser::ublox::calc_fletcher(&packet[2..]);
    packet.push(ck_a);
    packet.push(ck_b);
    packet
}

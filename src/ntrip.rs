use crate::config::NtripConfig;
use crate::watchdog::WatchdogMsg;
use anyhow::{anyhow, Context, Result};
use base64::prelude::*;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver as TokioReceiver, Sender as TokioSender};
use tokio::time::sleep;

async fn read_headers(stream: &mut TcpStream) -> Result<String> {
    let mut header_bytes = Vec::new();
    let mut buffer = [0u8; 1];
    
    // Read headers until double CRLF
    while header_bytes.len() < 8192 {
        stream.read_exact(&mut buffer).await
            .context("Failed reading header bytes from caster")?;
        header_bytes.push(buffer[0]);
        
        if header_bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    
    let header_str = String::from_utf8(header_bytes)
        .context("NTRIP Caster headers were not valid UTF-8")?;
    Ok(header_str)
}

pub async fn run_ntrip(
    config: NtripConfig,
    mut gga_rx: TokioReceiver<String>,
    watchdog_tx: TokioSender<WatchdogMsg>,
) -> Result<()> {
    if !config.enabled {
        log::info!("NTRIP client is disabled in configuration.");
        loop {
            // Keep task alive so watchdog doesn't think it failed
            let _ = watchdog_tx.send(WatchdogMsg::Heartbeat("ntrip".to_string())).await;
            sleep(Duration::from_secs(5)).await;
        }
    }

    let caster_addr = format!("{}:{}", config.caster_host, config.caster_port);
    log::info!("Connecting to NTRIP Caster at {}", caster_addr);
    
    let mut stream = TcpStream::connect(&caster_addr).await
        .context(format!("Failed to connect to NTRIP caster {}", caster_addr))?;

    // Build HTTP GET request
    let request = if config.mountpoint.is_empty() {
        // Query sourcetable (Mountpoint list)
        format!(
            "GET / HTTP/1.1\r\n\
             Host: {}\r\n\
             User-Agent: NTRIP RustClient/0.1\r\n\
             Accept: */*\r\n\
             Connection: close\r\n\r\n",
            config.caster_host
        )
    } else {
        // Stream RTCM corrections
        let auth_raw = format!("{}:{}", config.username, config.password);
        let auth_b64 = BASE64_STANDARD.encode(auth_raw.as_bytes());
        format!(
            "GET /{} HTTP/1.1\r\n\
             Host: {}\r\n\
             User-Agent: NTRIP RustClient/0.1\r\n\
             Authorization: Basic {}\r\n\
             Ntrip-Version: Ntrip/2.0\r\n\
             Accept: */*\r\n\
             Connection: close\r\n\r\n",
            config.mountpoint, config.caster_host, auth_b64
        )
    };

    stream.write_all(request.as_bytes()).await
        .context("Failed sending NTRIP HTTP request")?;
    stream.flush().await?;

    // Read Response HTTP Headers
    let headers = read_headers(&mut stream).await?;
    log::info!("NTRIP Caster response headers:\n{}", headers.trim());

    // Validate connection status
    let status_line = headers.lines().next().unwrap_or("");
    if !status_line.contains("200") && !status_line.contains("ICY") {
        return Err(anyhow!("NTRIP Caster returned non-success status: {}", status_line));
    }

    if config.mountpoint.is_empty() {
        // Sourcetable mode - Read and parse all sourcetable entries
        log::info!("Fetching NTRIP Caster sourcetable (Mountpoints)...");
        let mut body = String::new();
        stream.read_to_string(&mut body).await
            .context("Failed to read sourcetable body")?;
        
        println!("\n========= NTRIP MOUNTPOINTS (SOURCETABLE) =========");
        for line in body.lines() {
            if line.starts_with("STR;") {
                let parts: Vec<&str> = line.split(';').collect();
                if parts.len() > 3 {
                    let mp_name = parts[1];
                    let city = parts[2];
                    let format = parts[3];
                    println!("Mountpoint: {:<15} | Location: {:<20} | Format: {}", mp_name, city, format);
                }
            }
        }
        println!("===================================================\n");
        log::info!("Sourcetable successfully fetched. NTRIP task exiting.");
        
        // Loop forever sending heartbeats so watchdog doesn't restart us
        loop {
            let _ = watchdog_tx.send(WatchdogMsg::Heartbeat("ntrip".to_string())).await;
            sleep(Duration::from_secs(5)).await;
        }
    }

    log::info!("Successfully connected to mountpoint: {}. Starting stream...", config.mountpoint);

    let mut last_gga_sent = Instant::now() - Duration::from_secs(10); // send immediately if available
    let mut last_heartbeat = Instant::now();
    let mut buffer = [0u8; 4096];
    let mut current_gga: Option<String> = None;

    loop {
        // Watchdog Heartbeat
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            if watchdog_tx.send(WatchdogMsg::Heartbeat("ntrip".to_string())).await.is_err() {
                log::error!("Watchdog channel closed");
                break;
            }
            last_heartbeat = Instant::now();
        }

        tokio::select! {
            // Listen for dynamic GGA position sentences from serial port
            Some(gga) = gga_rx.recv() => {
                current_gga = Some(gga);
            }

            // Stream RTCM packets from the caster socket
            read_res = stream.read(&mut buffer) => {
                match read_res {
                    Ok(n) if n > 0 => {
                        let rtcm_chunk = buffer[..n].to_vec();
                        if let Err(e) = watchdog_tx.send(WatchdogMsg::Rtcm(rtcm_chunk)).await {
                            log::error!("Failed to send RTCM to watchdog: {:?}", e);
                            break;
                        }
                    }
                    Ok(_) => {
                        return Err(anyhow!("NTRIP caster closed connection"));
                    }
                    Err(e) => {
                        return Err(anyhow!("NTRIP caster read error: {:?}", e));
                    }
                }
            }

            // Periodic VRS GGA transmission back to NTRIP caster
            _ = sleep(Duration::from_millis(50)) => {
                if let Some(ref gga) = current_gga {
                    if last_gga_sent.elapsed() >= Duration::from_secs(10) {
                        log::info!("Uploading GGA to caster (VRS): {}", gga.trim());
                        if let Err(e) = stream.write_all(gga.as_bytes()).await {
                            return Err(anyhow!("Failed uploading GGA to caster: {:?}", e));
                        }
                        let _ = stream.flush().await;
                        last_gga_sent = Instant::now();
                    }
                }
            }
        }
    }

    Err(anyhow!("NTRIP client task terminated unexpectedly"))
}

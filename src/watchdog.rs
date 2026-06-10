use crate::config::AppConfig;
use crate::mqtt_reporter::run_mqtt;
use crate::ntrip::run_ntrip;
use crate::parser::GpsTelemetry;
use crate::serial_gps::run_serial;
use crate::web_server::{run_web, WebMsg};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub enum WatchdogMsg {
    Heartbeat(String),       // task name
    Rtcm(Vec<u8>),           // RTCM corrections from NTRIP -> Serial
    Gga(String),             // GGA sentence from Serial -> NTRIP
    Telemetry(GpsTelemetry), // Telemetry from Serial -> MQTT
    SetConfig(AppConfig),
    ActiveBinFile(String),
    /// Latest publicly reachable URL of the WS/Web server (e.g. cloudflared
    /// tunnel). `None` = tunnel down. Sourced by a small background poller
    /// that watches /run/gps-project/public_url.
    PublicUrl(Option<String>),
    /// Latest publicly reachable SSH command string (e.g. produced by a
    /// `bore local 22 --to bore.pub` wrapper). `None` = tunnel down.
    /// Sourced from /run/gps-project/ssh_url.
    SshEndpoint(Option<String>),
}

/// Path written by an external tunnel agent (e.g. cloudflared wrapper) to
/// publish the current public URL of the web server. Empty/missing file =
/// no tunnel up.
const PUBLIC_URL_FILE: &str = "/run/gps-project/public_url";
/// Path written by the bore SSH tunnel wrapper.
const SSH_URL_FILE: &str = "/run/gps-project/ssh_url";

struct ComponentState {
    name: String,
    last_heartbeat: Instant,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    restart_at: Option<Instant>,
}

pub struct Supervisor {
    config: AppConfig,
    config_path: String,
}

impl Supervisor {
    pub fn new(config: AppConfig, config_path: String) -> Self {
        Self { config, config_path }
    }

    pub async fn run(&self) {
        log::info!("Starting Supervisor watchdog loop...");

        // Create the main watchdog channel. 
        // Tasks send heartbeats and data to this central channel.
        let (watchdog_tx, mut watchdog_rx) = mpsc::channel::<WatchdogMsg>(1000);

        // Routing senders to forward messages to currently active component tasks
        let mut serial_route_tx: Option<mpsc::Sender<Vec<u8>>> = None;
        let mut ntrip_route_tx: Option<mpsc::Sender<String>> = None;
        let mut mqtt_route_tx: Option<mpsc::Sender<crate::mqtt_reporter::MqttTaskInput>> = None;
        let mut web_route_tx: Option<mpsc::Sender<WebMsg>> = None;

        let mut rtcm_bytes_received: u64 = 0;
        let mut last_rtcm_timestamp: String = "Never".to_string();

        let mut gps_error: Option<String> = None;
        let mut ntrip_error: Option<String> = None;
        let mut mqtt_error: Option<String> = None;

        let mut active_config = self.config.clone();
        let mut timeout_dur = Duration::from_secs(active_config.watchdog.heartbeat_timeout_secs);
        let mut active_bin_file: Option<String> = None;
        let mut public_url: Option<String> = None;
        let mut ssh_endpoint: Option<String> = None;

        // Background poller: watch a file for changes and dispatch via the
        // watchdog channel. Decoupled from the tunnel agent so any source
        // (cloudflared, bore, ngrok, custom) can populate the file.
        fn spawn_file_watcher<F>(
            tx: mpsc::Sender<WatchdogMsg>,
            path: &'static str,
            label: &'static str,
            wrap: F,
        ) where
            F: Fn(Option<String>) -> WatchdogMsg + Send + 'static,
        {
            tokio::spawn(async move {
                let mut last: Option<String> = None;
                loop {
                    let cur = tokio::fs::read_to_string(path)
                        .await
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty());
                    if cur != last {
                        log::info!("[WATCHDOG] {} changed: {:?} -> {:?}", label, last, cur);
                        if tx.send(wrap(cur.clone())).await.is_err() {
                            break;
                        }
                        last = cur;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        spawn_file_watcher(watchdog_tx.clone(), PUBLIC_URL_FILE, "public_url", WatchdogMsg::PublicUrl);
        spawn_file_watcher(watchdog_tx.clone(), SSH_URL_FILE,    "ssh_endpoint", WatchdogMsg::SshEndpoint);

        // Initialize component states
        let now = Instant::now();
        let mut serial_state = ComponentState {
            name: "serial".to_string(),
            last_heartbeat: now,
            handle: None,
            restart_at: None,
        };
        let mut ntrip_state = ComponentState {
            name: "ntrip".to_string(),
            last_heartbeat: now,
            handle: None,
            restart_at: None,
        };
        let mut mqtt_state = ComponentState {
            name: "mqtt".to_string(),
            last_heartbeat: now,
            handle: None,
            restart_at: None,
        };
        let mut web_state = ComponentState {
            name: "web".to_string(),
            last_heartbeat: now,
            handle: None,
            restart_at: None,
        };

        let mut latest_telemetry = GpsTelemetry::default();
        let mut last_telemetry_received: Option<Instant> = None;
        let mut publish_timer = tokio::time::interval(Duration::from_secs(1));
        let mut last_mqtt_publish = Instant::now() - Duration::from_secs(1);

        loop {
            let now_inst = Instant::now();

            // 1. Monitor and spawn/restart Serial
            if serial_state.handle.is_none() {
                let should_spawn = match serial_state.restart_at {
                    None => true,
                    Some(t) => now_inst >= t,
                };
                if should_spawn {
                    log::info!("[WATCHDOG] Spawning serial GPS task...");
                    serial_state.last_heartbeat = Instant::now();
                    let (tx, rx) = mpsc::channel::<Vec<u8>>(1000);
                    serial_route_tx = Some(tx);
                    
                    let serial_cfg = active_config.serial.clone();
                    let device_type = active_config.general.device_type.clone();
                    let log_dir = active_config.general.log_directory.clone();
                    let log_rotation = active_config.general.log_rotation_hours;
                    let utc_offset = active_config.general.utc_offset_hours;
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_serial(serial_cfg, device_type, log_dir, log_rotation, utc_offset, rx, watchdog_tx_clone).await
                    });
                    
                    serial_state.handle = Some(handle);
                    serial_state.restart_at = None;
                }
            }

            // 2. Monitor and spawn/restart NTRIP
            if ntrip_state.handle.is_none() {
                let should_spawn = match ntrip_state.restart_at {
                    None => true,
                    Some(t) => now_inst >= t,
                };
                if should_spawn {
                    log::info!("[WATCHDOG] Spawning NTRIP client task...");
                    ntrip_state.last_heartbeat = Instant::now();
                    let (tx, rx) = mpsc::channel::<String>(100);
                    ntrip_route_tx = Some(tx);
                    
                    let ntrip_cfg = active_config.ntrip.clone();
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_ntrip(ntrip_cfg, rx, watchdog_tx_clone).await
                    });
                    
                    ntrip_state.handle = Some(handle);
                    ntrip_state.restart_at = None;
                }
            }

            // 3. Monitor and spawn/restart MQTT
            if mqtt_state.handle.is_none() {
                let should_spawn = match mqtt_state.restart_at {
                    None => true,
                    Some(t) => now_inst >= t,
                };
                if should_spawn {
                    log::info!("[WATCHDOG] Spawning MQTT reporter task...");
                    mqtt_state.last_heartbeat = Instant::now();
                    let (tx, rx) = mpsc::channel::<crate::mqtt_reporter::MqttTaskInput>(100);
                    mqtt_route_tx = Some(tx);
                    
                    let app_cfg = active_config.clone();
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_mqtt(app_cfg, rx, watchdog_tx_clone).await
                    });
                    
                    mqtt_state.handle = Some(handle);
                    mqtt_state.restart_at = None;
                }
            }

            // 4. Monitor and spawn/restart Web Server
            if web_state.handle.is_none() {
                let should_spawn = match web_state.restart_at {
                    None => true,
                    Some(t) => now_inst >= t,
                };
                if should_spawn {
                    log::info!("[WATCHDOG] Spawning Web Server task...");
                    web_state.last_heartbeat = Instant::now();
                    let (tx, rx) = mpsc::channel::<WebMsg>(1000);
                    web_route_tx = Some(tx);
                    
                    let app_cfg = active_config.clone();
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_web(app_cfg, rx, watchdog_tx_clone).await
                    });
                    
                    web_state.handle = Some(handle);
                    web_state.restart_at = None;
                }
            }

            // Active event monitoring
            tokio::select! {
                // Handle centrally routed communications
                Some(msg) = watchdog_rx.recv() => {
                    match msg {
                        WatchdogMsg::Heartbeat(name) => {
                            if name == "serial" {
                                serial_state.last_heartbeat = Instant::now();
                                gps_error = None;
                            } else if name == "ntrip" {
                                ntrip_state.last_heartbeat = Instant::now();
                                ntrip_error = None;
                            } else if name == "mqtt" {
                                mqtt_state.last_heartbeat = Instant::now();
                                mqtt_error = None;
                            } else if name == "web" {
                                web_state.last_heartbeat = Instant::now();
                            }
                        }
                        WatchdogMsg::Rtcm(data) => {
                            rtcm_bytes_received += data.len() as u64;
                            last_rtcm_timestamp = chrono::Local::now().format("%H:%M:%S").to_string();

                            if let Some(ref tx) = serial_route_tx {
                                let _ = tx.try_send(data);
                            }
                        }
                        WatchdogMsg::Gga(data) => {
                            if let Some(ref tx) = ntrip_route_tx {
                                let _ = tx.try_send(data.clone());
                            }
                            if let Some(ref tx) = web_route_tx {
                                let _ = tx.try_send(WebMsg::Gga(data));
                            }
                        }
                        WatchdogMsg::Telemetry(data) => {
                            latest_telemetry = data;
                            last_telemetry_received = Some(Instant::now());
                        }
                        WatchdogMsg::SetConfig(new_config) => {
                            if new_config != active_config {
                                log::info!("[WATCHDOG] Configuration changed! Saving and applying new config...");
                                if let Err(e) = new_config.save_to_file(&self.config_path) {
                                    log::error!("[WATCHDOG] Failed to save config to file: {:?}", e);
                                }
                                
                                let serial_changed = new_config.serial != active_config.serial 
                                    || new_config.general.device_type != active_config.general.device_type
                                    || new_config.general.log_directory != active_config.general.log_directory
                                    || new_config.general.log_rotation_hours != active_config.general.log_rotation_hours
                                    || new_config.general.utc_offset_hours != active_config.general.utc_offset_hours;
                                let general_changed = new_config.general != active_config.general;
                                let ntrip_changed = new_config.ntrip != active_config.ntrip;
                                let mqtt_changed = new_config.mqtt != active_config.mqtt;
                                let web_changed = new_config.web != active_config.web;
                                
                                active_config = new_config;
                                timeout_dur = Duration::from_secs(active_config.watchdog.heartbeat_timeout_secs);
                                
                                if serial_changed {
                                    log::info!("[WATCHDOG] Restarting Serial component due to config change...");
                                    if let Some(h) = serial_state.handle.take() {
                                        h.abort();
                                    }
                                    serial_route_tx = None;
                                    serial_state.restart_at = Some(Instant::now());
                                }
                                if ntrip_changed {
                                    log::info!("[WATCHDOG] Restarting NTRIP component due to config change...");
                                    if let Some(h) = ntrip_state.handle.take() {
                                        h.abort();
                                    }
                                    ntrip_route_tx = None;
                                    ntrip_state.restart_at = Some(Instant::now());
                                }
                                if mqtt_changed || serial_changed || general_changed || ntrip_changed || web_changed {
                                    log::info!("[WATCHDOG] Restarting MQTT component due to config change...");
                                    if let Some(h) = mqtt_state.handle.take() {
                                        h.abort();
                                    }
                                    mqtt_route_tx = None;
                                    mqtt_state.restart_at = Some(Instant::now());
                                }
                                if web_changed {
                                    log::info!("[WATCHDOG] Restarting Web Server component due to config change...");
                                    if let Some(h) = web_state.handle.take() {
                                        h.abort();
                                    }
                                    web_route_tx = None;
                                    web_state.restart_at = Some(Instant::now());
                                } else {
                                    // Broadcast new config to WebSocket clients if web server did not restart
                                    if let Some(ref tx) = web_route_tx {
                                        let _ = tx.try_send(WebMsg::Config(active_config.clone()));
                                    }
                                }
                            }
                        }
                        WatchdogMsg::ActiveBinFile(name) => {
                            active_bin_file = Some(name);
                        }
                        WatchdogMsg::PublicUrl(url) => {
                            public_url = url;
                        }
                        WatchdogMsg::SshEndpoint(ep) => {
                            ssh_endpoint = ep;
                        }
                    }
                }
                
                // Publish metrics and status at 1Hz
                _ = publish_timer.tick() => {
                    let now_check = Instant::now();

                    // 1. Verify Serial Status
                    if let Some(ref h) = serial_state.handle {
                        if h.is_finished() {
                            let handle = serial_state.handle.take().unwrap();
                            let res = match handle.await {
                                Ok(Ok(_)) => "Serial task exited normally".to_string(),
                                Ok(Err(e)) => format!("Serial task failed: {:?}", e),
                                Err(e) => format!("Serial task panicked or was aborted: {:?}", e),
                            };
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' failed! Reason: {}", serial_state.name, res);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", serial_state.name);
                            gps_error = Some(res);
                            serial_route_tx = None;
                            serial_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(serial_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", serial_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", serial_state.name);
                            gps_error = Some("Heartbeat timed out".to_string());
                            h.abort();
                            serial_route_tx = None;
                            serial_state.handle = None;
                            serial_state.restart_at = Some(now_check + Duration::from_secs(5));
                        }
                    }

                    // 2. Verify NTRIP Status
                    if let Some(ref h) = ntrip_state.handle {
                        if h.is_finished() {
                            let handle = ntrip_state.handle.take().unwrap();
                            let res = match handle.await {
                                Ok(Ok(_)) => "NTRIP task exited normally".to_string(),
                                Ok(Err(e)) => format!("NTRIP task failed: {:?}", e),
                                Err(e) => format!("NTRIP task panicked or was aborted: {:?}", e),
                            };
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' failed! Reason: {}", ntrip_state.name, res);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", ntrip_state.name);
                            ntrip_error = Some(res);
                            ntrip_route_tx = None;
                            ntrip_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(ntrip_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", ntrip_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", ntrip_state.name);
                            ntrip_error = Some("Heartbeat timed out".to_string());
                            h.abort();
                            ntrip_route_tx = None;
                            ntrip_state.handle = None;
                            ntrip_state.restart_at = Some(now_check + Duration::from_secs(5));
                        }
                    }

                    // 3. Verify MQTT Status
                    if let Some(ref h) = mqtt_state.handle {
                        if h.is_finished() {
                            let handle = mqtt_state.handle.take().unwrap();
                            let res = match handle.await {
                                Ok(Ok(_)) => "MQTT task exited normally".to_string(),
                                Ok(Err(e)) => format!("MQTT task failed: {:?}", e),
                                Err(e) => format!("MQTT task panicked or was aborted: {:?}", e),
                            };
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' failed! Reason: {}", mqtt_state.name, res);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", mqtt_state.name);
                            mqtt_error = Some(res);
                            mqtt_route_tx = None;
                            mqtt_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(mqtt_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", mqtt_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", mqtt_state.name);
                            mqtt_error = Some("Heartbeat timed out".to_string());
                            h.abort();
                            mqtt_route_tx = None;
                            mqtt_state.handle = None;
                            mqtt_state.restart_at = Some(now_check + Duration::from_secs(5));
                        }
                    }

                    // 4. Verify Web Status
                    if let Some(ref h) = web_state.handle {
                        if h.is_finished() {
                            let handle = web_state.handle.take().unwrap();
                            let res = match handle.await {
                                Ok(Ok(_)) => "Web task exited normally".to_string(),
                                Ok(Err(e)) => format!("Web task failed: {:?}", e),
                                Err(e) => format!("Web task panicked or was aborted: {:?}", e),
                            };
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' failed! Reason: {}", web_state.name, res);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", web_state.name);
                            web_route_tx = None;
                            web_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(web_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", web_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", web_state.name);
                            h.abort();
                            web_route_tx = None;
                            web_state.handle = None;
                            web_state.restart_at = Some(now_check + Duration::from_secs(5));
                        }
                    }

                    let now_publish = Instant::now();
                    let gps_connected = serial_state.handle.is_some()
                        && last_telemetry_received.map_or(false, |t| t.elapsed() < Duration::from_secs(5));
                    let ntrip_connected = ntrip_state.handle.is_some() && now_publish.duration_since(ntrip_state.last_heartbeat) < timeout_dur;
                    let mqtt_connected = mqtt_state.handle.is_some() && now_publish.duration_since(mqtt_state.last_heartbeat) < timeout_dur;

                    let mut current_telemetry = latest_telemetry.clone();
                    if !gps_connected {
                        current_telemetry = GpsTelemetry::default();
                    }
                    current_telemetry.device_id = active_config.general.device_id.clone();

                     // Build the tokenised public URL (base + /<access_token>/)
                     // so subscribers receive a directly-usable link.
                     let token = active_config.web.access_token.as_deref().unwrap_or("");
                     let tokenised_url = public_url.as_ref().map(|base| {
                         let trimmed = base.trim_end_matches('/');
                         if token.is_empty() {
                             trimmed.to_string()
                         } else {
                             format!("{}/{}/", trimmed, token)
                         }
                     });

                     // Send telemetry and status to Web UI
                     if let Some(ref tx) = web_route_tx {
                         let _ = tx.try_send(WebMsg::Telemetry(current_telemetry.clone()));
                         let status = crate::web_server::SystemStatus {
                             ntrip_enabled: active_config.ntrip.enabled,
                             ntrip_connected,
                             ntrip_error: ntrip_error.clone(),
                             mqtt_enabled: active_config.mqtt.enabled,
                             mqtt_connected,
                             mqtt_error: mqtt_error.clone(),
                             rtcm_bytes_received,
                             last_rtcm_timestamp: last_rtcm_timestamp.clone(),
                             gps_connected,
                             gps_error: gps_error.clone(),
                             active_bin_file: active_bin_file.clone(),
                             public_url: tokenised_url.clone(),
                             ssh_endpoint: ssh_endpoint.clone(),
                         };
                         let _ = tx.try_send(WebMsg::Status(status));
                     }

                     // Send telemetry and status to MQTT Reporter (every 1 second)
                     if last_mqtt_publish.elapsed() >= Duration::from_secs(1) {
                         if let Some(ref tx) = mqtt_route_tx {
                             let input = crate::mqtt_reporter::MqttTaskInput {
                                 telemetry: current_telemetry,
                                 gps_connected,
                                 gps_error: gps_error.clone(),
                                 ntrip_connected,
                                 ntrip_error: ntrip_error.clone(),
                                 mqtt_error: mqtt_error.clone(),
                                 active_bin_file: active_bin_file.clone(),
                                 public_url: tokenised_url,
                                 ssh_endpoint: ssh_endpoint.clone(),
                             };
                             let _ = tx.try_send(input);
                         }
                         last_mqtt_publish = Instant::now();
                     }
                }
            }
        }
    }
}

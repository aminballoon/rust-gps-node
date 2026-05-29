use crate::config::AppConfig;
use crate::mqtt_reporter::run_mqtt;
use crate::ntrip::run_ntrip;
use crate::parser::GpsTelemetry;
use crate::serial_gps::run_serial;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub enum WatchdogMsg {
    Heartbeat(String),       // task name
    Rtcm(Vec<u8>),           // RTCM corrections from NTRIP -> Serial
    Gga(String),             // GGA sentence from Serial -> NTRIP
    Telemetry(GpsTelemetry), // Telemetry from Serial -> MQTT
}

struct ComponentState {
    name: String,
    last_heartbeat: Instant,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    restart_at: Option<Instant>,
}

pub struct Supervisor {
    config: AppConfig,
}

impl Supervisor {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self) {
        log::info!("Starting Supervisor watchdog loop...");

        // Create the main watchdog channel. 
        // Tasks send heartbeats and data to this central channel.
        let (watchdog_tx, mut watchdog_rx) = mpsc::channel::<WatchdogMsg>(1000);

        // Routing senders to forward messages to currently active component tasks
        let mut serial_route_tx: Option<mpsc::Sender<Vec<u8>>> = None;
        let mut ntrip_route_tx: Option<mpsc::Sender<String>> = None;
        let mut mqtt_route_tx: Option<mpsc::Sender<GpsTelemetry>> = None;

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

        let check_interval = Duration::from_secs(self.config.watchdog.check_interval_secs);
        let timeout_dur = Duration::from_secs(self.config.watchdog.heartbeat_timeout_secs);

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
                    let (tx, rx) = mpsc::channel::<Vec<u8>>(1000);
                    serial_route_tx = Some(tx);
                    
                    let serial_cfg = self.config.serial.clone();
                    let device_type = self.config.general.device_type.clone();
                    let log_dir = self.config.general.log_directory.clone();
                    let log_rotation = self.config.general.log_rotation_hours;
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_serial(serial_cfg, device_type, log_dir, log_rotation, rx, watchdog_tx_clone).await
                    });
                    
                    serial_state.handle = Some(handle);
                    serial_state.last_heartbeat = Instant::now();
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
                    let (tx, rx) = mpsc::channel::<String>(100);
                    ntrip_route_tx = Some(tx);
                    
                    let ntrip_cfg = self.config.ntrip.clone();
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_ntrip(ntrip_cfg, rx, watchdog_tx_clone).await
                    });
                    
                    ntrip_state.handle = Some(handle);
                    ntrip_state.last_heartbeat = Instant::now();
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
                    let (tx, rx) = mpsc::channel::<GpsTelemetry>(100);
                    mqtt_route_tx = Some(tx);
                    
                    let mqtt_cfg = self.config.mqtt.clone();
                    let watchdog_tx_clone = watchdog_tx.clone();
                    
                    let handle = tokio::spawn(async move {
                        run_mqtt(mqtt_cfg, rx, watchdog_tx_clone).await
                    });
                    
                    mqtt_state.handle = Some(handle);
                    mqtt_state.last_heartbeat = Instant::now();
                    mqtt_state.restart_at = None;
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
                            } else if name == "ntrip" {
                                ntrip_state.last_heartbeat = Instant::now();
                            } else if name == "mqtt" {
                                mqtt_state.last_heartbeat = Instant::now();
                            }
                        }
                        WatchdogMsg::Rtcm(data) => {
                            if let Some(ref tx) = serial_route_tx {
                                let _ = tx.try_send(data);
                            }
                        }
                        WatchdogMsg::Gga(data) => {
                            if let Some(ref tx) = ntrip_route_tx {
                                let _ = tx.try_send(data);
                            }
                        }
                        WatchdogMsg::Telemetry(data) => {
                            if let Some(ref tx) = mqtt_route_tx {
                                let _ = tx.try_send(data);
                            }
                        }
                    }
                }
                
                // Heartbeat timeout and exited thread checks
                _ = sleep(check_interval) => {
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
                            serial_route_tx = None;
                            serial_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(serial_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", serial_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", serial_state.name);
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
                            ntrip_route_tx = None;
                            ntrip_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(ntrip_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", ntrip_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", ntrip_state.name);
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
                            mqtt_route_tx = None;
                            mqtt_state.restart_at = Some(now_check + Duration::from_secs(5));
                        } else if now_check.duration_since(mqtt_state.last_heartbeat) > timeout_dur {
                            log::error!("[WATCHDOG / NODE MONITOR] Component '{}' heartbeat timed out!", mqtt_state.name);
                            log::error!("[WATCHDOG / NODE MONITOR] Restarting component '{}' in 5 seconds...", mqtt_state.name);
                            h.abort();
                            mqtt_route_tx = None;
                            mqtt_state.handle = None;
                            mqtt_state.restart_at = Some(now_check + Duration::from_secs(5));
                        }
                    }
                }
            }
        }
    }
}

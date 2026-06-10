use crate::config::AppConfig;
use crate::parser::GpsTelemetry;
use crate::watchdog::WatchdogMsg;
use anyhow::{anyhow, Result};
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver as TokioReceiver;
use tokio::sync::mpsc::Sender as TokioSender;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct MqttTaskInput {
    pub telemetry: GpsTelemetry,
    pub gps_connected: bool,
    pub gps_error: Option<String>,
    pub ntrip_connected: bool,
    pub ntrip_error: Option<String>,
    pub mqtt_error: Option<String>,
    pub active_bin_file: Option<String>,
    pub public_url: Option<String>,
    pub ssh_endpoint: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MqttReport {
    pub telemetry: GpsTelemetry,
    pub status: MqttStatusFields,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MqttStatusFields {
    pub mqtt_connected: bool,
    pub mqtt_error: Option<String>,
    pub gps_connected: bool,
    pub gps_error: Option<String>,
    pub ntrip_connected: bool,
    pub ntrip_error: Option<String>,
    pub active_bin_file: Option<String>,
    /// Publicly reachable URL of the on-board Web/WS server (e.g. cloudflared
    /// tunnel). `None` if no tunnel is up.
    pub public_url: Option<String>,
    /// Publicly reachable SSH command (e.g. `ssh -p 12345 pico@bore.pub`).
    /// `None` if no SSH TCP tunnel is up.
    pub ssh_endpoint: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum MqttConfigPayload {
    Command {
        action: String,
        config: Option<AppConfig>,
    },
    RawConfig(AppConfig),
    StringCommand(String),
}

pub async fn run_mqtt(
    app_config: AppConfig,
    mut telemetry_rx: TokioReceiver<MqttTaskInput>,
    watchdog_tx: TokioSender<WatchdogMsg>,
) -> Result<()> {
    let config = &app_config.mqtt;

    if !config.enabled {
        log::info!("MQTT client is disabled in configuration.");
        loop {
            let _ = watchdog_tx.send(WatchdogMsg::Heartbeat("mqtt".to_string())).await;
            sleep(Duration::from_secs(5)).await;
        }
    }

    log::info!("Connecting to MQTT Broker {}:{}", config.broker_host, config.broker_port);
    let mut mqttoptions = MqttOptions::new(&config.client_id, &config.broker_host, config.broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    
    if !config.username.is_empty() {
        mqttoptions.set_credentials(&config.username, &config.password);
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let mut last_heartbeat = Instant::now();
    let mut mqtt_connected = false;

    loop {
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            if watchdog_tx.send(WatchdogMsg::Heartbeat("mqtt".to_string())).await.is_err() {
                log::error!("Watchdog channel closed");
                break;
            }
            last_heartbeat = Instant::now();
        }

        tokio::select! {
            // Read from eventloop to drive MQTT connection
            event = eventloop.poll() => {
                match event {
                    Ok(notification) => {
                        log::trace!("MQTT Event: {:?}", notification);
                        match notification {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                log::info!("MQTT Connection established. Subscribing to /gps/node_config...");
                                mqtt_connected = true;
                                if let Err(e) = client.subscribe("/gps/node_config", QoS::AtLeastOnce).await {
                                    log::error!("Failed to subscribe to /gps/node_config: {:?}", e);
                                }
                            }
                            Event::Incoming(Incoming::Publish(publish)) => {
                                if publish.topic == "/gps/node_config" {
                                    let payload = String::from_utf8_lossy(&publish.payload);
                                    log::info!("Received MQTT message on /gps/node_config: {}", payload);
                                    
                                    match serde_json::from_str::<MqttConfigPayload>(&payload) {
                                        Ok(MqttConfigPayload::Command { action, config }) => {
                                            if action == "get" {
                                                log::info!("MQTT get request received. Publishing current config.");
                                                let resp = serde_json::json!({
                                                    "action": "config",
                                                    "config": app_config
                                                });
                                                if let Ok(resp_str) = serde_json::to_string(&resp) {
                                                    let _ = client.publish("/gps/node_config", QoS::AtMostOnce, false, resp_str.as_bytes().to_vec()).await;
                                                }
                                            } else if action == "set" || action == "write" || action == "change" {
                                                if let Some(new_cfg) = config {
                                                    log::info!("MQTT set request received. Updating config.");
                                                    let _ = watchdog_tx.send(WatchdogMsg::SetConfig(new_cfg)).await;
                                                }
                                            } else if action == "config" {
                                                log::trace!("Received config broadcast, ignoring loopback");
                                            }
                                        }
                                        Ok(MqttConfigPayload::RawConfig(new_cfg)) => {
                                            if new_cfg != app_config {
                                                log::info!("MQTT raw config update received. Updating config.");
                                                let _ = watchdog_tx.send(WatchdogMsg::SetConfig(new_cfg)).await;
                                            } else {
                                                log::trace!("MQTT raw config received matches current config. Ignoring.");
                                            }
                                        }
                                        Ok(MqttConfigPayload::StringCommand(cmd)) => {
                                            if cmd == "get" {
                                                log::info!("MQTT string command 'get' received. Publishing current config.");
                                                let resp = serde_json::json!({
                                                    "action": "config",
                                                    "config": app_config
                                                });
                                                if let Ok(resp_str) = serde_json::to_string(&resp) {
                                                    let _ = client.publish("/gps/node_config", QoS::AtMostOnce, false, resp_str.as_bytes().to_vec()).await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("Failed to parse MQTT config payload: {:?}", e);
                                        }
                                    }
                                }
                            }
                            Event::Outgoing(rumqttc::Outgoing::Disconnect) => {
                                mqtt_connected = false;
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        return Err(anyhow!("MQTT event loop error: {:?}", e));
                    }
                }
            }

            // Read telemetry updates and publish them
            Some(input) = telemetry_rx.recv() => {
                let report = MqttReport {
                    telemetry: input.telemetry,
                    status: MqttStatusFields {
                        mqtt_connected,
                        mqtt_error: if mqtt_connected { None } else { input.mqtt_error },
                        gps_connected: input.gps_connected,
                        gps_error: input.gps_error,
                        ntrip_connected: input.ntrip_connected,
                        ntrip_error: input.ntrip_error,
                        active_bin_file: input.active_bin_file,
                        public_url: input.public_url,
                        ssh_endpoint: input.ssh_endpoint,
                    },
                };

                let payload = match serde_json::to_string(&report) {
                    Ok(json) => json,
                    Err(e) => {
                        log::error!("Failed to serialize telemetry report to JSON: {:?}", e);
                        continue;
                    }
                };

                log::info!("Publishing telemetry via MQTT to {}: {}", config.topic, payload);
                if let Err(e) = client.publish(&config.topic, QoS::AtMostOnce, false, payload.as_bytes().to_vec()).await {
                    return Err(anyhow!("Failed to publish MQTT message: {:?}", e));
                }
            }
        }
    }

    Err(anyhow!("MQTT task terminated unexpectedly"))
}

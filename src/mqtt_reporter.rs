use crate::config::MqttConfig;
use crate::parser::GpsTelemetry;
use crate::watchdog::WatchdogMsg;
use anyhow::{anyhow, Result};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver as TokioReceiver;
use tokio::sync::mpsc::Sender as TokioSender;
use tokio::time::sleep;

pub async fn run_mqtt(
    config: MqttConfig,
    mut telemetry_rx: TokioReceiver<GpsTelemetry>,
    watchdog_tx: TokioSender<WatchdogMsg>,
) -> Result<()> {
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
                    }
                    Err(e) => {
                        return Err(anyhow!("MQTT event loop error: {:?}", e));
                    }
                }
            }

            // Read telemetry updates and publish them
            Some(telemetry) = telemetry_rx.recv() => {
                let payload = match serde_json::to_string(&telemetry) {
                    Ok(json) => json,
                    Err(e) => {
                        log::error!("Failed to serialize telemetry to JSON: {:?}", e);
                        continue;
                    }
                };

                log::info!("Publishing 1Hz telemetry via MQTT to {}: {}", config.topic, payload);
                if let Err(e) = client.publish(&config.topic, QoS::AtMostOnce, false, payload.as_bytes().to_vec()).await {
                    return Err(anyhow!("Failed to publish MQTT message: {:?}", e));
                }
            }
        }
    }

    Err(anyhow!("MQTT task terminated unexpectedly"))
}

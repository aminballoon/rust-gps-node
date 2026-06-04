use crate::config::WebConfig;
use crate::parser::GpsTelemetry;
use crate::watchdog::WatchdogMsg;
use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};

#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemStatus {
    pub ntrip_enabled: bool,
    pub ntrip_connected: bool,
    pub ntrip_error: Option<String>,
    pub mqtt_enabled: bool,
    pub mqtt_connected: bool,
    pub mqtt_error: Option<String>,
    pub rtcm_bytes_received: u64,
    pub last_rtcm_timestamp: String,
    pub gps_connected: bool,
    pub gps_error: Option<String>,
}

impl Default for SystemStatus {
    fn default() -> Self {
        Self {
            ntrip_enabled: false,
            ntrip_connected: false,
            ntrip_error: None,
            mqtt_enabled: false,
            mqtt_connected: false,
            mqtt_error: None,
            rtcm_bytes_received: 0,
            last_rtcm_timestamp: "Never".to_string(),
            gps_connected: false,
            gps_error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum WebMsg {
    Telemetry(GpsTelemetry),
    Gga(String),
    Status(SystemStatus),
}

#[derive(Clone)]
struct AppState {
    latest_telemetry: Arc<RwLock<GpsTelemetry>>,
    latest_status: Arc<RwLock<SystemStatus>>,
    tx_broadcast: broadcast::Sender<WebMsg>,
}

pub async fn run_web(
    config: WebConfig,
    mut rx_msg: mpsc::Receiver<WebMsg>,
    watchdog_tx: mpsc::Sender<WatchdogMsg>,
) -> Result<()> {
    if !config.enabled {
        log::info!("Web server is disabled in configuration.");
        // Keep task alive and report heartbeat so watchdog is happy
        loop {
            let _ = watchdog_tx.send(WatchdogMsg::Heartbeat("web".to_string())).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    log::info!("Initializing Web Server states...");
    let latest_telemetry = Arc::new(RwLock::new(GpsTelemetry::default()));
    let latest_status = Arc::new(RwLock::new(SystemStatus::default()));
    let (tx_broadcast, _) = broadcast::channel::<WebMsg>(500);

    let state = AppState {
        latest_telemetry: latest_telemetry.clone(),
        latest_status: latest_status.clone(),
        tx_broadcast: tx_broadcast.clone(),
    };

    // Task to process incoming WebMsgs and update status/broadcast to WS
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx_msg.recv().await {
            match msg {
                WebMsg::Telemetry(ref t) => {
                    let mut lock = state_clone.latest_telemetry.write().await;
                    *lock = t.clone();
                }
                WebMsg::Status(ref s) => {
                    let mut lock = state_clone.latest_status.write().await;
                    *lock = s.clone();
                }
                _ => {}
            }
            // Send to WebSocket broadcast channel
            let _ = state_clone.tx_broadcast.send(msg);
        }
    });

    // Task to report heartbeat to Supervisor watchdog
    let watchdog_tx_clone = watchdog_tx.clone();
    tokio::spawn(async move {
        loop {
            if watchdog_tx_clone
                .send(WatchdogMsg::Heartbeat("web".to_string()))
                .await
                .is_err()
            {
                log::error!("Watchdog channel closed. Web server monitor task shutting down.");
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // Configure Axum router
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/telemetry", get(api_telemetry_handler))
        .route("/api/status", get(api_status_handler))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.port);
    log::info!("Starting Web Server at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind web server to address {}", addr))?;

    axum::serve(listener, app)
        .await
        .context("Error running Axum server")?;

    Ok(())
}

// Router handler for root dashboard
async fn index_handler() -> impl IntoResponse {
    Html(include_str!("web/index.html"))
}

// Router handler for active JSON telemetry query
async fn api_telemetry_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let telemetry = state.latest_telemetry.read().await;
    axum::Json(telemetry.clone())
}

// Router handler for active JSON status query
async fn api_status_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let status = state.latest_status.read().await;
    axum::Json(status.clone())
}

// Upgrade HTTP to WS connection
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut rx = state.tx_broadcast.subscribe();
    log::debug!("New WebSocket connection established.");

    loop {
        tokio::select! {
            // Receive from broadcast channel
            res = rx.recv() => {
                match res {
                    Ok(msg) => {
                        let payload = match msg {
                            WebMsg::Telemetry(t) => {
                                match serde_json::to_string(&t) {
                                    Ok(json) => json,
                                    Err(e) => {
                                        log::error!("WS failed to serialize telemetry: {:?}", e);
                                        continue;
                                    }
                                }
                            }
                            WebMsg::Gga(sentence) => {
                                serde_json::json!({
                                    "type": "gga",
                                    "data": sentence.trim()
                                }).to_string()
                            }
                            WebMsg::Status(s) => {
                                serde_json::json!({
                                    "type": "status",
                                    "data": s
                                }).to_string()
                            }
                        };

                        if socket.send(Message::Text(payload)).await.is_err() {
                            break; // Connection closed
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("WebSocket receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Also check for messages/closes from client socket to avoid hanging connections
            client_msg = socket.recv() => {
                if client_msg.is_none() {
                    break; // Connection closed by client
                }
            }
        }
    }
    log::debug!("WebSocket connection closed.");
}

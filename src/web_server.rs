use crate::config::AppConfig;
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
    pub active_bin_file: Option<String>,
    /// Publicly reachable URL of this Web/WS server (e.g. cloudflared tunnel).
    /// `None` if no tunnel is configured / not yet up.
    pub public_url: Option<String>,
    /// Publicly reachable SSH command (e.g. `ssh -p 12345 pico@bore.pub`).
    /// `None` if no SSH tunnel is up.
    pub ssh_endpoint: Option<String>,
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
            active_bin_file: None,
            public_url: None,
            ssh_endpoint: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum WebMsg {
    Telemetry(GpsTelemetry),
    Gga(String),
    Status(SystemStatus),
    Config(AppConfig),
}

#[derive(Clone)]
struct AppState {
    latest_telemetry: Arc<RwLock<GpsTelemetry>>,
    latest_status: Arc<RwLock<SystemStatus>>,
    latest_config: Arc<RwLock<AppConfig>>,
    tx_broadcast: broadcast::Sender<WebMsg>,
    watchdog_tx: mpsc::Sender<WatchdogMsg>,
}

pub async fn run_web(
    config: AppConfig,
    mut rx_msg: mpsc::Receiver<WebMsg>,
    watchdog_tx: mpsc::Sender<WatchdogMsg>,
) -> Result<()> {
    if !config.web.enabled {
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
    let latest_config = Arc::new(RwLock::new(config.clone()));
    let (tx_broadcast, _) = broadcast::channel::<WebMsg>(500);

    let state = AppState {
        latest_telemetry: latest_telemetry.clone(),
        latest_status: latest_status.clone(),
        latest_config: latest_config.clone(),
        tx_broadcast: tx_broadcast.clone(),
        watchdog_tx: watchdog_tx.clone(),
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
                WebMsg::Config(ref c) => {
                    let mut lock = state_clone.latest_config.write().await;
                    *lock = c.clone();
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

    // Configure Axum router. All routes optionally live under
    // /<access_token>/ so the URL itself acts as a shared secret.
    //
    // We build routes with the token baked into the path explicitly instead
    // of using `nest`, because `Router::nest("/<token>", inner)` in axum 0.7
    // does not match the bare `/<token>/` URL for an inner `route("/")`
    // (only `/<token>` without trailing slash matches). Browsers append the
    // trailing slash when the user types just the host+path, so the
    // dashboard would 404 — we mount both forms here.
    let prefix = match config.web.access_token.as_deref() {
        Some(t) if !t.is_empty() => {
            log::info!(
                "Web routes mounted under /{}…/ (token length {} chars).",
                &t[..t.len().min(6)],
                t.len()
            );
            format!("/{}", t)
        }
        _ => {
            log::warn!("web.access_token is empty — routes are exposed without auth!");
            String::new()
        }
    };

    let index_path        = if prefix.is_empty() { "/".to_string() }              else { format!("{}/", prefix) };
    let index_alias       = if prefix.is_empty() { "/".to_string() }              else { prefix.clone() };
    let api_telemetry     = format!("{}/api/telemetry", prefix);
    let api_status        = format!("{}/api/status",    prefix);
    let api_config        = format!("{}/api/config",    prefix);
    let ws_path           = format!("{}/ws",            prefix);

    let mut router: Router<AppState> = Router::new()
        .route(&index_path,    get(index_handler))
        .route(&api_telemetry, get(api_telemetry_handler))
        .route(&api_status,    get(api_status_handler))
        .route(&api_config,    get(api_config_handler))
        .route(&ws_path,       get(ws_handler));
    if index_alias != index_path {
        router = router.route(&index_alias, get(index_handler));
    }
    let app = router.with_state(state);

    let addr = format!("0.0.0.0:{}", config.web.port);
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

// Router handler for active JSON config query
async fn api_config_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let config = state.latest_config.read().await;
    axum::Json(config.clone())
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

    // Send initial config to client immediately on connection
    {
        let c = state.latest_config.read().await;
        let payload = serde_json::json!({
            "type": "config",
            "data": *c
        }).to_string();
        if socket.send(Message::Text(payload)).await.is_err() {
            return;
        }
    }

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
                            WebMsg::Config(c) => {
                                serde_json::json!({
                                    "type": "config",
                                    "data": c
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
                match client_msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            if val["type"] == "get_config" {
                                let c = state.latest_config.read().await;
                                let payload = serde_json::json!({
                                    "type": "config",
                                    "data": *c
                                }).to_string();
                                let _ = socket.send(Message::Text(payload)).await;
                            } else if val["type"] == "set_config" {
                                if let Some(config_data) = val.get("data") {
                                    if let Ok(new_cfg) = serde_json::from_value::<AppConfig>(config_data.clone()) {
                                        log::info!("Received set_config request via WebSocket");
                                        let _ = state.watchdog_tx.send(WatchdogMsg::SetConfig(new_cfg)).await;
                                    } else {
                                        log::error!("Failed to parse AppConfig from WebSocket set_config data");
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break; // Connection closed
                    }
                    _ => {}
                }
            }
        }
    }
    log::debug!("WebSocket connection closed.");
}

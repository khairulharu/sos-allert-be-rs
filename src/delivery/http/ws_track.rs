//! `WS /ws/sos/{alertId}/track`.
//!
//! Auth is in-band (first frame `{"type":"auth","token":"..."}` then
//! `{"type":"auth_ok"}`) or a raw `Authorization` header for native clients;
//! `?token=` is ignored. The socket is mounted outside the HTTP auth
//! middleware, so it enforces its own budgets: per-IP before the upgrade and
//! per-user after the in-band auth (close code 1013).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio::time::{interval, timeout};

use crate::bootstrap::AppState;
use crate::model::sos::BreadcrumbRequest;
use crate::tracking::{ConnectionPermit, Hub};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(25);
const CLOSE_TRY_AGAIN_LATER: u16 = 1013;
const CLOSE_POLICY: u16 = 1008;

pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(alert_id): Path<String>,
    headers: HeaderMap,
    // The server is always served with connect info (see `main`), so the
    // per-IP budget has a real address.
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    let ip = addr.ip().to_string();

    let Some(permit) = state.hub().acquire(&ip) else {
        // 429 before the upgrade, like the Go per-IP limiter.
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many tracking connections",
        )
            .into_response();
    };

    let header_token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    ws.on_upgrade(move |socket| session(socket, state, alert_id, header_token, permit))
}

async fn session(
    mut socket: WebSocket,
    state: AppState,
    alert_id: String,
    header_token: Option<String>,
    mut permit: ConnectionPermit,
) {
    // 1. Resolve the token: header first, else the first in-band frame.
    let token = match header_token {
        Some(token) => token,
        None => match read_auth_frame(&mut socket).await {
            Some(token) => token,
            None => {
                reject(&mut socket, "unauthorized", CLOSE_POLICY).await;
                return;
            }
        },
    };

    // 2. Verify the session. An unknown token is 1008; a store error is 1013
    //    so the client retries instead of dropping its token.
    let auth = match state.auth().verify(&token).await {
        Ok(auth) => auth,
        Err(error) => {
            let close = if error.status() == 503 {
                CLOSE_TRY_AGAIN_LATER
            } else {
                CLOSE_POLICY
            };
            reject(&mut socket, "unauthorized", close).await;
            return;
        }
    };

    // 3. Per-user budget (identity is only known now).
    if !permit.claim_user(&auth.id) {
        reject(
            &mut socket,
            "too many tracking connections",
            CLOSE_TRY_AGAIN_LATER,
        )
        .await;
        return;
    }

    // 4. Only the sender, a responder or a notified recipient may track.
    if let Err(error) = state.sos().authorize_tracking(&auth.id, &alert_id).await {
        reject(&mut socket, &error.to_string(), CLOSE_POLICY).await;
        return;
    }

    // Subscribe before `auth_ok` so no broadcast is missed.
    let hub = state.hub().clone();
    let mut updates = hub.subscribe(&alert_id);

    if send_json(&mut socket, serde_json::json!({"type": "auth_ok"}))
        .await
        .is_err()
    {
        return;
    }

    let mut ping = interval(PING_INTERVAL);
    ping.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if socket.send(Message::Ping(Default::default())).await.is_err() {
                    return;
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(payload) => {
                        let text = String::from_utf8_lossy(&payload).into_owned();
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            frame = socket.next() => {
                let Some(Ok(frame)) = frame else { return };
                match frame {
                    Message::Text(text) => {
                        handle_client_frame(&mut socket, &state, &alert_id, &auth.id, &text).await;
                    }
                    Message::Close(_) => return,
                    Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => {}
                }
            }
        }
    }
}

/// Only the sender's breadcrumbs are accepted; the use case enforces
/// ownership and the active status.
async fn handle_client_frame(
    socket: &mut WebSocket,
    state: &AppState,
    alert_id: &str,
    user_id: &str,
    text: &str,
) {
    let request: BreadcrumbRequest = match serde_json::from_str(text) {
        Ok(request) => request,
        Err(_) => {
            let _ = send_json(socket, serde_json::json!({"error": "invalid breadcrumb"})).await;
            return;
        }
    };

    match timeout(
        Duration::from_secs(5),
        state.sos().add_breadcrumb(user_id, alert_id, request),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            let _ = send_json(socket, serde_json::json!({"error": error.to_string()})).await;
        }
        Err(_) => {
            let _ = send_json(socket, serde_json::json!({"error": "breadcrumb timed out"})).await;
        }
    }
}

async fn read_auth_frame(socket: &mut WebSocket) -> Option<String> {
    let frame = timeout(AUTH_TIMEOUT, socket.next()).await.ok()??.ok()?;
    let Message::Text(text) = frame else {
        return None;
    };
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if value.get("type").and_then(|v| v.as_str()) != Some("auth") {
        return None;
    }
    value
        .get("token")
        .and_then(|v| v.as_str())
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

async fn send_json(socket: &mut WebSocket, value: serde_json::Value) -> Result<(), ()> {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|_| ())
}

async fn reject(socket: &mut WebSocket, message: &str, code: u16) {
    let _ = send_json(socket, serde_json::json!({ "error": message })).await;
    let _ = socket
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code,
            reason: message.to_string().into(),
        })))
        .await;
    let _ = socket.close().await;
}

/// Keeps the `Hub` import used by the type annotation above; the handler takes
/// the hub from the state.
#[allow(dead_code)]
fn _hub_type(hub: Arc<Hub>) -> Arc<Hub> {
    hub
}

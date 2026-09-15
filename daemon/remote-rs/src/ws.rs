//! Cloud WebSocket transport: `/healthz`, `/terminal` (PTY over WebSocket),
//! `/rpc` (JSON-RPC over WebSocket) and `/admin/leases` (lease installation).
//! Mirrors the HTTP/WebSocket parts of `ws_pty.go`.

use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cloud_cli_bridge::CloudCliBridge;
use crate::pty_hub::{
    normalize_pty_size, truncate_websocket_close_reason, FrameKind, InputWriteStatus,
    OutgoingFrame, PtyAttachment, PtyHub, PtyHubConfig, WsPtyControlFrame, WsPtyEventFrame,
    DEFAULT_WEBSOCKET_WRITE_TIMEOUT,
};
use crate::rpc::{FrameWriter, RpcEvent, RpcRequest, RpcResponse, RpcServer, MAX_RPC_FRAME_BYTES};
use crate::util::{constant_time_eq, go_json, path_dir, DoneSignal, LogSink};

pub const WS_STATUS_NORMAL_CLOSURE: u16 = 1000;
pub const WS_STATUS_UNSUPPORTED_DATA: u16 = 1003;
pub const WS_STATUS_POLICY_VIOLATION: u16 = 1008;
pub const WS_STATUS_INTERNAL_ERROR: u16 = 1011;

const RPC_CLIENT_FILE: &str = "/tmp/cmux/attach-rpc-client.json";

#[derive(Clone, Default)]
pub struct WsServerConfig {
    pub listen_addr: String,
    pub pty_auth_lease_file: String,
    pub rpc_auth_lease_file: String,
    pub admin_token_sha256: String,
    pub admin_ed25519_pub_key: String,
    pub cli_bridge_socket_path: String,
    pub cli_bridge: Option<Arc<CloudCliBridge>>,
    pub shell: String,
    pub pty_hub: Option<Arc<PtyHub>>,
    pub scrollback_limit: usize,
    pub session_idle_ttl: Option<Duration>,
}

impl WsServerConfig {
    fn hub_config(&self) -> PtyHubConfig {
        PtyHubConfig {
            shell: self.shell.clone(),
            scrollback_limit: self.scrollback_limit,
            session_idle_ttl: self.session_idle_ttl,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WsLease {
    #[serde(default)]
    pub version: i64,
    #[serde(default)]
    pub token_sha256: String,
    #[serde(default)]
    pub expires_at_unix: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default)]
    pub single_use: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WsLeaseInstallRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pty_lease: Option<WsLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_lease: Option<WsLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_client: Option<WsRpcClientPayload>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WsRpcClientPayload {
    #[serde(default)]
    pub token: String,
    #[serde(default, rename = "sessionId")]
    pub session_id: String,
    #[serde(default, rename = "expiresAtUnix")]
    pub expires_at_unix: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WsAuthFrame {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cols: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub rows: i64,
    #[serde(skip)]
    pub session_id_explicit: bool,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Debug)]
pub enum LeaseError {
    Missing,
    Expired,
    Forbidden,
    Io(io::Error),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Missing => write!(f, "attach lease missing"),
            LeaseError::Expired => write!(f, "attach lease expired"),
            LeaseError::Forbidden => write!(f, "attach lease rejected"),
            LeaseError::Io(err) => write!(f, "{err}"),
        }
    }
}

fn ws_lease_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub fn consume_websocket_lease(path: &str, auth: &WsAuthFrame) -> Result<(), LeaseError> {
    let _guard = ws_lease_mutex().lock().unwrap();
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(LeaseError::Missing),
        Err(err) => return Err(LeaseError::Io(err)),
    };
    let lease: WsLease = serde_json::from_slice(&data).map_err(|_| LeaseError::Forbidden)?;
    if lease.version != 1 {
        return Err(LeaseError::Forbidden);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if lease.expires_at_unix <= now {
        return Err(LeaseError::Expired);
    }
    if !lease.session_id.is_empty() && lease.session_id != auth.session_id {
        return Err(LeaseError::Forbidden);
    }
    let expected = hex::decode(lease.token_sha256.trim()).map_err(|_| LeaseError::Forbidden)?;
    if expected.len() != 32 {
        return Err(LeaseError::Forbidden);
    }
    let actual = Sha256::digest(auth.token.as_bytes());
    if !constant_time_eq(&expected, &actual) {
        return Err(LeaseError::Forbidden);
    }
    if lease.single_use {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(LeaseError::Io(err)),
        }
    }
    Ok(())
}

pub fn decode_admin_ed25519_public_key(raw: &str) -> Result<VerifyingKey, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("missing ed25519 public key".to_string());
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed))
        .map_err(|_| "invalid ed25519 public key".to_string())?;
    let bytes: [u8; 32] = decoded
        .try_into()
        .map_err(|_| "invalid ed25519 public key".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| "invalid ed25519 public key".to_string())
}

pub fn verify_admin_lease_install_auth(
    headers: &HeaderMap,
    body: &[u8],
    expected_hash: Option<&[u8]>,
    public_key: Option<&VerifyingKey>,
) -> bool {
    const BEARER_PREFIX: &str = "Bearer ";
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if let Some(expected) = expected_hash {
        if expected.len() == 32 {
            if let Some(token) = auth.strip_prefix(BEARER_PREFIX) {
                let actual = Sha256::digest(token.as_bytes());
                if constant_time_eq(expected, &actual) {
                    return true;
                }
            }
        }
    }
    if let Some(key) = public_key {
        let raw = headers
            .get("x-cmux-admin-signature-ed25519")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .trim();
        let signature = base64::engine::general_purpose::STANDARD
            .decode(raw)
            .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(raw));
        if let Ok(signature) = signature {
            if signature.len() == 64 {
                if let Ok(signature) = Signature::from_slice(&signature) {
                    if key.verify(body, &signature).is_ok() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

pub fn write_lease_file(path: &str, lease: &WsLease) -> io::Result<()> {
    if path.trim().is_empty() {
        return Err(io::Error::other("lease path is empty"));
    }
    write_json_file(
        path,
        &serde_json::to_value(lease).map_err(io::Error::other)?,
    )
}

pub fn write_json_file(path: &str, value: &serde_json::Value) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path_dir(path))?;
    let data = format!("{}\n", go_json(value));
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
    let mut file = options.open(path)?;
    use std::io::Write;
    file.write_all(data.as_bytes())?;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    Ok(())
}

struct AppState {
    cfg: WsServerConfig,
    hub: Arc<PtyHub>,
    stderr: LogSink,
}

pub fn new_websocket_pty_handler(cfg: WsServerConfig, stderr: LogSink) -> Router {
    let hub = cfg
        .pty_hub
        .clone()
        .unwrap_or_else(|| PtyHub::new(cfg.hub_config(), Some(stderr.clone())));
    let state = Arc::new(AppState { cfg, hub, stderr });
    Router::new()
        .route("/healthz", any(handle_healthz))
        .route("/terminal", any(handle_terminal_upgrade))
        .route("/rpc", any(handle_rpc_upgrade))
        .route("/admin/leases", any(handle_lease_install))
        .with_state(state)
}

/// Hub backing a router built by [`new_websocket_pty_handler`] when the config
/// did not provide one.
pub fn hub_for_config(cfg: &WsServerConfig, stderr: &LogSink) -> Arc<PtyHub> {
    cfg.pty_hub
        .clone()
        .unwrap_or_else(|| PtyHub::new(cfg.hub_config(), Some(stderr.clone())))
}

fn text_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        format!("{message}\n"),
    )
        .into_response()
}

async fn handle_healthz(State(state): State<Arc<AppState>>) -> Response {
    let locked = fs::metadata(&state.cfg.pty_auth_lease_file).is_err();
    let body = format!(
        "{}\n",
        go_json(&serde_json::json!({"ok": true, "locked": locked}))
    );
    (StatusCode::OK, [("content-type", "application/json")], body).into_response()
}

async fn handle_lease_install(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let expected_hash = hex::decode(state.cfg.admin_token_sha256.trim())
        .ok()
        .filter(|hash| hash.len() == 32);
    let public_key = decode_admin_ed25519_public_key(&state.cfg.admin_ed25519_pub_key).ok();
    if expected_hash.is_none() && public_key.is_none() {
        return text_response(StatusCode::NOT_FOUND, "lease install disabled");
    }
    let body = if body.len() > 1 << 20 {
        &body[..1 << 20]
    } else {
        &body[..]
    };
    if !verify_admin_lease_install_auth(
        &headers,
        body,
        expected_hash.as_deref(),
        public_key.as_ref(),
    ) {
        return text_response(StatusCode::FORBIDDEN, "forbidden");
    }
    let request: WsLeaseInstallRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(_) => return text_response(StatusCode::BAD_REQUEST, "invalid JSON"),
    };
    if request.pty_lease.is_none() && request.rpc_lease.is_none() {
        return text_response(StatusCode::BAD_REQUEST, "missing lease");
    }
    if let Some(lease) = &request.pty_lease {
        if write_lease_file(&state.cfg.pty_auth_lease_file, lease).is_err() {
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, "write pty lease failed");
        }
    }
    if let Some(lease) = &request.rpc_lease {
        if state.cfg.rpc_auth_lease_file.trim().is_empty() {
            return text_response(StatusCode::BAD_REQUEST, "rpc lease disabled");
        }
        if write_lease_file(&state.cfg.rpc_auth_lease_file, lease).is_err() {
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, "write rpc lease failed");
        }
    }
    if let Some(client) = &request.rpc_client {
        let value = serde_json::to_value(client).unwrap_or(serde_json::Value::Null);
        if write_json_file(RPC_CLIENT_FILE, &value).is_err() {
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, "write rpc client failed");
        }
    }
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        "{\"ok\":true}",
    )
        .into_response()
}

async fn handle_terminal_upgrade(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.max_message_size(1 << 20)
        .on_upgrade(move |socket| handle_websocket_pty(socket, state))
}

async fn handle_rpc_upgrade(
    State(state): State<Arc<AppState>>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    // Go answers 404 for a disabled RPC endpoint before looking at the
    // upgrade headers, so check the config before honoring the extractor.
    if state.cfg.rpc_auth_lease_file.trim().is_empty() {
        return text_response(StatusCode::NOT_FOUND, "404 page not found");
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(rejection) => return rejection.into_response(),
    };
    ws.max_message_size(MAX_RPC_FRAME_BYTES)
        .on_upgrade(move |socket| handle_websocket_rpc(socket, state))
}

type WsSink = SplitSink<WebSocket, Message>;
type WsStream = SplitStream<WebSocket>;

fn close_frame(code: u16, reason: &str) -> CloseFrame {
    CloseFrame {
        code,
        reason: truncate_websocket_close_reason(reason).into(),
    }
}

async fn send_close(sink: &mut WsSink, code: u16, reason: &str) {
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        sink.send(Message::Close(Some(close_frame(code, reason)))),
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
}

/// Read the first data frame (skipping ping/pong control messages).
async fn read_data_message(stream: &mut WsStream) -> Option<Message> {
    loop {
        match stream.next().await? {
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(message) => return Some(message),
            Err(_) => return None,
        }
    }
}

enum AuthOutcome {
    Frame(WsAuthFrame),
    Closed,
}

async fn read_auth_frame(sink: &mut WsSink, stream: &mut WsStream) -> AuthOutcome {
    let message =
        match tokio::time::timeout(Duration::from_secs(5), read_data_message(stream)).await {
            Ok(Some(message)) => message,
            _ => {
                send_close(sink, WS_STATUS_POLICY_VIOLATION, "auth required").await;
                return AuthOutcome::Closed;
            }
        };
    let payload = match message {
        Message::Text(text) => text.to_string(),
        Message::Close(_) => {
            send_close(sink, WS_STATUS_POLICY_VIOLATION, "auth required").await;
            return AuthOutcome::Closed;
        }
        _ => {
            send_close(sink, WS_STATUS_UNSUPPORTED_DATA, "auth must be text JSON").await;
            return AuthOutcome::Closed;
        }
    };
    match serde_json::from_str::<WsAuthFrame>(&payload) {
        Ok(auth) if auth.kind == "auth" && !auth.token.is_empty() => AuthOutcome::Frame(auth),
        _ => {
            send_close(sink, WS_STATUS_POLICY_VIOLATION, "invalid auth").await;
            AuthOutcome::Closed
        }
    }
}

async fn reject_lease(sink: &mut WsSink, err: LeaseError) {
    match err {
        LeaseError::Missing => {
            send_close(sink, WS_STATUS_POLICY_VIOLATION, "no active lease").await
        }
        LeaseError::Expired => send_close(sink, WS_STATUS_POLICY_VIOLATION, "lease expired").await,
        _ => send_close(sink, WS_STATUS_POLICY_VIOLATION, "lease rejected").await,
    }
}

async fn handle_websocket_pty(socket: WebSocket, state: Arc<AppState>) {
    let (mut sink, mut stream) = socket.split();
    let mut auth = match read_auth_frame(&mut sink, &mut stream).await {
        AuthOutcome::Frame(auth) => auth,
        AuthOutcome::Closed => return,
    };
    auth.session_id = auth.session_id.trim().to_string();
    auth.session_id_explicit = !auth.session_id.is_empty();
    let (cols, rows) = normalize_pty_size(auth.cols, auth.rows);
    auth.cols = cols;
    auth.rows = rows;
    if auth.session_id.is_empty() {
        auth.session_id = "default".to_string();
    }
    if let Err(err) = consume_websocket_lease(&state.cfg.pty_auth_lease_file, &auth) {
        reject_lease(&mut sink, err).await;
        return;
    }

    let hub = Arc::clone(&state.hub);
    let (attachment, session_done) = match hub.attach_ws(
        &auth.session_id,
        auth.session_id_explicit,
        &auth.attachment_id,
        auth.cols,
        auth.rows,
    ) {
        Ok(result) => result,
        Err(err) => {
            state
                .stderr
                .write_str(&format!("ws pty attach failed: {err}\n"));
            send_close(&mut sink, WS_STATUS_INTERNAL_ERROR, &err).await;
            return;
        }
    };

    let final_close: Arc<Mutex<Option<CloseFrame>>> = Arc::new(Mutex::new(None));
    let writer = tokio::spawn(terminal_write_loop(
        sink,
        Arc::clone(&attachment),
        session_done.clone(),
        Arc::clone(&final_close),
    ));

    pump_websocket_to_pty(&hub, &attachment, &mut stream).await;

    *final_close.lock().unwrap() = Some(close_frame(WS_STATUS_NORMAL_CLOSURE, "closed"));
    attachment.cancel();
    if attachment.persistent {
        hub.detach(&attachment);
    } else {
        hub.close_session_for_attachment(&attachment);
    }
    let _ = writer.await;
}

async fn write_frame(sink: &mut WsSink, frame: OutgoingFrame) -> bool {
    let message = match frame.kind {
        FrameKind::Binary => Message::Binary(Bytes::from(frame.payload)),
        FrameKind::Text => match String::from_utf8(frame.payload) {
            Ok(text) => Message::Text(text.into()),
            Err(err) => Message::Binary(Bytes::from(err.into_bytes())),
        },
    };
    matches!(
        tokio::time::timeout(DEFAULT_WEBSOCKET_WRITE_TIMEOUT, sink.send(message)).await,
        Ok(Ok(()))
    )
}

async fn terminal_write_loop(
    mut sink: WsSink,
    attachment: Arc<PtyAttachment>,
    session_done: DoneSignal,
    final_close: Arc<Mutex<Option<CloseFrame>>>,
) {
    let frames = attachment.frames().clone();
    let cancel = attachment.cancel_token().clone();
    loop {
        if attachment.is_cancelled() {
            let frame = final_close.lock().unwrap().take();
            if let Some(frame) = frame {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    sink.send(Message::Close(Some(frame))),
                )
                .await;
                let _ = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
            }
            return;
        }
        if session_done.is_closed() {
            while let Ok(frame) = frames.try_recv() {
                if frame.input_ack {
                    continue;
                }
                if !write_frame(&mut sink, frame).await {
                    attachment.cancel();
                    return;
                }
            }
            send_close(&mut sink, WS_STATUS_NORMAL_CLOSURE, "pty closed").await;
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = session_done.wait_async() => {}
            frame = frames.recv_async() => {
                match frame {
                    Ok(frame) => {
                        if frame.input_ack {
                            continue;
                        }
                        if !write_frame(&mut sink, frame).await {
                            attachment.cancel();
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        }
    }
}

async fn pump_websocket_to_pty(
    hub: &Arc<PtyHub>,
    attachment: &Arc<PtyAttachment>,
    stream: &mut WsStream,
) {
    let cancel = attachment.cancel_token().clone();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            message = stream.next() => {
                match message {
                    None | Some(Err(_)) => return,
                    Some(Ok(Message::Binary(payload))) => {
                        if hub.write_input(attachment, &payload, 0, false).status != InputWriteStatus::Ok {
                            return;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        let control: WsPtyControlFrame = match serde_json::from_str(&text) {
                            Ok(control) => control,
                            Err(_) => continue,
                        };
                        match control.kind.as_str() {
                            "resize" => hub.resize(attachment, control.cols, control.rows),
                            "close" => {
                                hub.close_session_for_attachment(attachment);
                                return;
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) => return,
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

enum WsOut {
    Text(String),
    Close(u16, String),
}

/// Frame writer that funnels JSON frames through the WebSocket writer task.
pub struct WsRpcFrameWriter {
    tx: flume::Sender<WsOut>,
}

impl FrameWriter for WsRpcFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.send(resp.to_json())
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.send(event.to_json())
    }
}

impl WsRpcFrameWriter {
    fn send(&self, text: String) -> io::Result<()> {
        self.tx
            .send_timeout(WsOut::Text(text), DEFAULT_WEBSOCKET_WRITE_TIMEOUT)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "websocket closed"))
    }
}

async fn rpc_write_loop(mut sink: WsSink, rx: flume::Receiver<WsOut>) {
    loop {
        match rx.recv_async().await {
            Ok(WsOut::Text(text)) => {
                if tokio::time::timeout(
                    DEFAULT_WEBSOCKET_WRITE_TIMEOUT,
                    sink.send(Message::Text(text.into())),
                )
                .await
                .map(|r| r.is_err())
                .unwrap_or(true)
                {
                    return;
                }
            }
            Ok(WsOut::Close(code, reason)) => {
                send_close(&mut sink, code, &reason).await;
                return;
            }
            Err(_) => return,
        }
    }
}

async fn handle_websocket_rpc(socket: WebSocket, state: Arc<AppState>) {
    let (mut sink, mut stream) = socket.split();
    let mut auth = match read_auth_frame(&mut sink, &mut stream).await {
        AuthOutcome::Frame(auth) => auth,
        AuthOutcome::Closed => return,
    };
    if auth.session_id.is_empty() {
        auth.session_id = "default".to_string();
    }
    if let Err(err) = consume_websocket_lease(&state.cfg.rpc_auth_lease_file, &auth) {
        reject_lease(&mut sink, err).await;
        return;
    }

    let (out_tx, out_rx) = flume::bounded::<WsOut>(4096);
    let writer = tokio::spawn(rpc_write_loop(sink, out_rx));
    let ready = WsPtyEventFrame {
        kind: "ready".to_string(),
        session_id: auth.session_id.clone(),
        ..Default::default()
    };
    let ready_json =
        crate::util::go_json_escape(&serde_json::to_string(&ready).unwrap_or_default());
    if out_tx.send_async(WsOut::Text(ready_json)).await.is_err() {
        let _ = writer.await;
        return;
    }

    let frame_writer: Arc<dyn FrameWriter> = Arc::new(WsRpcFrameWriter { tx: out_tx.clone() });
    let mut builder = RpcServer::builder()
        .pty_hub(Arc::clone(&state.hub), false)
        .frame_writer(Arc::clone(&frame_writer))
        .stderr(state.stderr.clone());
    if let Some(bridge) = &state.cfg.cli_bridge {
        builder = builder.cli_bridge(Arc::clone(bridge));
    }
    let server = builder.build();
    let registration = state
        .cfg
        .cli_bridge
        .as_ref()
        .map(|bridge| bridge.register(&server));

    let close = |code: u16, reason: &str| {
        let _ = out_tx.try_send(WsOut::Close(code, reason.to_string()));
    };

    loop {
        let message = match stream.next().await {
            None | Some(Err(_)) => {
                close(WS_STATUS_NORMAL_CLOSURE, "closed");
                break;
            }
            Some(Ok(message)) => message,
        };
        let payload = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(_) => {
                close(WS_STATUS_UNSUPPORTED_DATA, "rpc frames must be text JSON");
                break;
            }
            Message::Close(_) => {
                close(WS_STATUS_NORMAL_CLOSURE, "closed");
                break;
            }
            _ => continue,
        };
        let payload = payload.trim();
        if payload.is_empty() {
            continue;
        }
        let req = match RpcRequest::parse(payload.as_bytes()) {
            Ok(req) => req,
            Err(_) => {
                if frame_writer
                    .write_response(&RpcResponse::err(
                        None,
                        "invalid_request",
                        "invalid JSON request",
                    ))
                    .is_err()
                {
                    close(WS_STATUS_INTERNAL_ERROR, "write failed");
                    break;
                }
                continue;
            }
        };
        let server_ref = Arc::clone(&server);
        let result =
            tokio::task::spawn_blocking(move || server_ref.handle_request_and_write_response(&req))
                .await;
        match result {
            Ok(Ok(())) => {}
            _ => {
                close(WS_STATUS_INTERNAL_ERROR, "write failed");
                break;
            }
        }
    }

    drop(registration);
    let server_ref = Arc::clone(&server);
    let _ = tokio::task::spawn_blocking(move || server_ref.close_all()).await;
    drop(out_tx);
    let _ = writer.await;
}

/// Bind and serve the WebSocket transport until `shutdown` resolves.
pub async fn run_websocket_pty_server<F>(
    mut cfg: WsServerConfig,
    stderr: LogSink,
    shutdown: F,
) -> io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let mut addr = cfg.listen_addr.trim().to_string();
    if addr.is_empty() {
        addr = "127.0.0.1:7777".to_string();
    }
    if cfg.pty_auth_lease_file.trim().is_empty() {
        return Err(io::Error::other("auth lease file is required"));
    }
    let hub = hub_for_config(&cfg, &stderr);
    cfg.pty_hub = Some(Arc::clone(&hub));
    let mut bridge_stop = None;
    if !cfg.rpc_auth_lease_file.trim().is_empty() {
        let bridge = cfg.cli_bridge.clone().unwrap_or_default();
        match bridge.start(&cfg.cli_bridge_socket_path, stderr.clone()) {
            Ok(stop) => bridge_stop = Some(stop),
            Err(err) => {
                hub.close_all();
                return Err(io::Error::other(format!("cloud CLI bridge: {err}")));
            }
        }
        cfg.cli_bridge = Some(bridge);
    }
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(err) => {
            hub.close_all();
            if let Some(stop) = &bridge_stop {
                stop.stop();
            }
            return Err(err);
        }
    };
    let local = listener.local_addr().map(|a| a.to_string()).unwrap_or(addr);
    stderr.write_str(&format!("cmuxd-remote ws listening on {local}\n"));
    let router = new_websocket_pty_handler(cfg, stderr);
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await;
    hub.close_all();
    if let Some(stop) = &bridge_stop {
        stop.stop();
    }
    result
}

/// Blocking entry point used by `serve --ws`.
pub fn run_websocket_pty_server_blocking(cfg: WsServerConfig, stderr: LogSink) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_websocket_pty_server(
        cfg,
        stderr,
        std::future::pending(),
    ))
}

/// Serve a router on an already-bound listener (tests).
pub async fn serve_router<F>(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: F,
) -> io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

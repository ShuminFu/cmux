//! Lease-gated WebSocket transport for cloud VMs: `/terminal` streams a PTY,
//! `/rpc` carries the same JSON-RPC as stdio, `/admin/leases` installs
//! leases, and `/healthz` reports whether a PTY lease is present.

pub mod frame;
pub mod http;
pub mod lease;

use std::io::{self, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use sha2::Digest as _;

use crate::cli_bridge::CloudCliBridge;
use crate::logger::Logger;
use crate::pty::{
    Attachment, InputWriteStatus, MAX_RPC_FRAME_BYTES_FOR_WS, PtyEventFrame, PtyHub, PtyHubConfig,
    normalize_pty_size_i64, truncate_websocket_close_reason,
};
use crate::rpc::server::CliBridge;
use crate::rpc::{FrameWriter, RpcEvent, RpcRequest, RpcResponse, RpcServer};
use crate::signal::Signal;
use frame::{
    STATUS_INTERNAL_ERROR, STATUS_NORMAL_CLOSURE, STATUS_POLICY_VIOLATION, STATUS_UNSUPPORTED_DATA,
    WsConn, WsMessage,
};
use http::{Request, accept_websocket, read_request, write_error, write_response};
use lease::{
    ConsumeError, LeaseError, WsAuthFrame, WsLeaseInstallRequest, consume_websocket_lease,
    decode_admin_ed25519_public_key, verify_admin_lease_install_auth, write_json_file,
    write_lease_file,
};

pub use lease::WsLease;

const TERMINAL_READ_LIMIT: usize = 1 << 20;
const ADMIN_BODY_LIMIT: usize = 1 << 20;
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
pub const RPC_CLIENT_FILE: &str = "/tmp/cmux/attach-rpc-client.json";

#[derive(Default)]
pub struct WsPtyServerConfig {
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
    pub session_idle_ttl: Duration,
}

impl WsPtyServerConfig {
    fn hub_config(&self) -> PtyHubConfig {
        PtyHubConfig {
            shell: self.shell.clone(),
            scrollback_limit: self.scrollback_limit,
            session_idle_ttl: self.session_idle_ttl,
        }
    }
}

/// Serve until the listener fails. The Go version stops on context
/// cancellation; the daemon runs until its process is terminated.
pub fn run_websocket_pty_server(
    mut cfg: WsPtyServerConfig,
    logger: Arc<dyn Logger>,
) -> io::Result<()> {
    let addr = if cfg.listen_addr.trim().is_empty() {
        "127.0.0.1:7777".to_string()
    } else {
        cfg.listen_addr.clone()
    };
    if cfg.pty_auth_lease_file.trim().is_empty() {
        return Err(io::Error::other("auth lease file is required"));
    }
    if cfg.pty_hub.is_none() {
        cfg.pty_hub = Some(PtyHub::new(cfg.hub_config(), Arc::clone(&logger)));
    }
    if !cfg.rpc_auth_lease_file.trim().is_empty() {
        let bridge = cfg.cli_bridge.get_or_insert_with(CloudCliBridge::new);
        bridge.start(&cfg.cli_bridge_socket_path, &logger).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("cloud CLI bridge: {}", crate::util::io_error_text(&e)),
            )
        })?;
    }
    let listener = TcpListener::bind(&addr).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("listen tcp {addr}: bind: {}", crate::util::io_error_text(&e)),
        )
    })?;
    let hub = Arc::clone(cfg.pty_hub.as_ref().expect("hub set above"));
    let result = serve_listener(listener, cfg, logger);
    hub.close_all();
    result
}

/// Serve an already-bound listener (tests pick an ephemeral port).
pub fn serve_listener(
    listener: TcpListener,
    cfg: WsPtyServerConfig,
    logger: Arc<dyn Logger>,
) -> io::Result<()> {
    let handler = Arc::new(WsHandler::new(cfg, Arc::clone(&logger)));
    let local = listener.local_addr().map(|a| a.to_string()).unwrap_or_default();
    logger.log(&format!("cmuxd-remote ws listening on {local}\n"));
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let handler = Arc::clone(&handler);
                std::thread::spawn(move || handler.serve_connection(stream));
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

pub struct WsHandler {
    cfg: WsPtyServerConfig,
    hub: Arc<PtyHub>,
    logger: Arc<dyn Logger>,
}

impl WsHandler {
    pub fn new(mut cfg: WsPtyServerConfig, logger: Arc<dyn Logger>) -> Self {
        let hub = match cfg.pty_hub.take() {
            Some(hub) => hub,
            None => PtyHub::new(cfg.hub_config(), Arc::clone(&logger)),
        };
        cfg.pty_hub = Some(Arc::clone(&hub));
        Self { cfg, hub, logger }
    }

    #[must_use]
    pub fn hub(&self) -> &Arc<PtyHub> {
        &self.hub
    }

    pub fn serve_connection(&self, stream: TcpStream) {
        let _ = stream.set_read_timeout(Some(READ_HEADER_TIMEOUT));
        let Ok(reader_stream) = stream.try_clone() else { return };
        let mut reader = BufReader::with_capacity(16 * 1024, reader_stream);
        let request = match read_request(&mut reader, ADMIN_BODY_LIMIT) {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(_) => {
                let mut stream = stream;
                let _ = write_error(&mut stream, 400, "bad request");
                return;
            }
        };
        let _ = stream.set_read_timeout(None);
        let path = request.path.split('?').next().unwrap_or("");
        match path {
            "/healthz" => {
                let locked = std::fs::metadata(&self.cfg.pty_auth_lease_file).is_err();
                let body = format!("{{\"locked\":{locked},\"ok\":true}}\n");
                let mut stream = stream;
                let _ = write_response(&mut stream, 200, "application/json", body.as_bytes());
            }
            "/terminal" => self.handle_terminal(stream, reader, &request),
            "/rpc" => self.handle_rpc(stream, reader, &request),
            "/admin/leases" => {
                let mut stream = stream;
                self.handle_lease_install(&mut stream, &request);
            }
            _ => {
                let mut stream = stream;
                let _ = write_error(&mut stream, 404, "404 page not found");
            }
        }
    }

    fn handle_lease_install(&self, stream: &mut TcpStream, request: &Request) {
        if request.method != "POST" {
            let _ = write_error(stream, 405, "method not allowed");
            return;
        }
        let expected_hash =
            hex::decode(self.cfg.admin_token_sha256.trim()).ok().filter(|h| h.len() == 32);
        let public_key = decode_admin_ed25519_public_key(&self.cfg.admin_ed25519_pub_key).ok();
        if expected_hash.is_none() && public_key.is_none() {
            let _ = write_error(stream, 404, "lease install disabled");
            return;
        }
        let body = &request.body;
        if !verify_admin_lease_install_auth(
            request,
            body,
            expected_hash.as_deref(),
            public_key.as_ref(),
        ) {
            let _ = write_error(stream, 403, "forbidden");
            return;
        }
        let Ok(install) = serde_json::from_slice::<WsLeaseInstallRequest>(body) else {
            let _ = write_error(stream, 400, "invalid JSON");
            return;
        };
        if install.pty_lease.is_none() && install.rpc_lease.is_none() {
            let _ = write_error(stream, 400, "missing lease");
            return;
        }
        if let Some(lease) = &install.pty_lease
            && write_lease_file(&self.cfg.pty_auth_lease_file, lease).is_err()
        {
            let _ = write_error(stream, 500, "write pty lease failed");
            return;
        }
        if let Some(lease) = &install.rpc_lease {
            if self.cfg.rpc_auth_lease_file.trim().is_empty() {
                let _ = write_error(stream, 400, "rpc lease disabled");
                return;
            }
            if write_lease_file(&self.cfg.rpc_auth_lease_file, lease).is_err() {
                let _ = write_error(stream, 500, "write rpc lease failed");
                return;
            }
        }
        if let Some(client) = &install.rpc_client
            && write_json_file(RPC_CLIENT_FILE, client).is_err()
        {
            let _ = write_error(stream, 500, "write rpc client failed");
            return;
        }
        let _ = write_response(stream, 200, "application/json", b"{\"ok\":true}");
    }

    /// Read and validate the auth frame shared by `/terminal` and `/rpc`.
    fn read_auth(conn: &WsConn) -> Option<WsAuthFrame> {
        let _ = conn.set_read_timeout(Some(AUTH_TIMEOUT));
        let message = conn.read();
        let _ = conn.set_read_timeout(None);
        let payload = match message {
            Ok(WsMessage::Text(payload)) => payload,
            Ok(WsMessage::Binary(_)) => {
                conn.close(STATUS_UNSUPPORTED_DATA, "auth must be text JSON");
                return None;
            }
            Ok(WsMessage::Close { .. }) => {
                conn.close(STATUS_POLICY_VIOLATION, "auth required");
                return None;
            }
            Err(_) => {
                // A failed read (timeout or transport error) has already
                // torn the connection down in the Go implementation.
                conn.close_now();
                return None;
            }
        };
        let auth = serde_json::from_slice::<WsAuthFrame>(&payload).ok();
        let Some(auth) = auth.filter(|a| a.kind == "auth" && !a.token.is_empty()) else {
            conn.close(STATUS_POLICY_VIOLATION, "invalid auth");
            return None;
        };
        Some(auth)
    }

    fn check_lease(conn: &WsConn, lease_file: &str, auth: &WsAuthFrame) -> bool {
        match consume_websocket_lease(lease_file, auth) {
            Ok(()) => true,
            Err(ConsumeError::Lease(LeaseError::Missing)) => {
                conn.close(STATUS_POLICY_VIOLATION, "no active lease");
                false
            }
            Err(ConsumeError::Lease(LeaseError::Expired)) => {
                conn.close(STATUS_POLICY_VIOLATION, "lease expired");
                false
            }
            Err(_) => {
                conn.close(STATUS_POLICY_VIOLATION, "lease rejected");
                false
            }
        }
    }

    fn handle_terminal(&self, stream: TcpStream, reader: BufReader<TcpStream>, request: &Request) {
        drop(reader);
        let Ok(conn) = accept_websocket(stream, request, TERMINAL_READ_LIMIT) else { return };
        let _ = conn.set_write_timeout(Some(crate::pty::DEFAULT_WRITE_TIMEOUT));
        let conn = Arc::new(conn);
        let Some(mut auth) = Self::read_auth(&conn) else { return };
        auth.session_id = auth.session_id.trim().to_string();
        auth.session_id_explicit = !auth.session_id.is_empty();
        let (cols, rows) = normalize_pty_size_i64(auth.cols, auth.rows);
        auth.cols = i64::try_from(cols).unwrap_or(i64::MAX);
        auth.rows = i64::try_from(rows).unwrap_or(i64::MAX);
        if auth.session_id.is_empty() {
            auth.session_id = "default".to_string();
        }
        if !Self::check_lease(&conn, &self.cfg.pty_auth_lease_file, &auth) {
            return;
        }
        let attachment = match attach_websocket(&self.hub, &conn, &auth) {
            Ok(attachment) => attachment,
            Err(err) => {
                self.logger.log(&format!("ws pty attach failed: {err}\n"));
                conn.close(STATUS_INTERNAL_ERROR, &truncate_websocket_close_reason(&err));
                return;
            }
        };
        pump_websocket_to_pty(&self.hub, &attachment, &conn);
        conn.close(STATUS_NORMAL_CLOSURE, "closed");
        if attachment.persistent {
            self.hub.detach(&attachment);
        } else {
            self.hub.close_session_for_attachment(&attachment);
        }
    }

    fn handle_rpc(&self, stream: TcpStream, reader: BufReader<TcpStream>, request: &Request) {
        drop(reader);
        if self.cfg.rpc_auth_lease_file.trim().is_empty() {
            let mut stream = stream;
            let _ = write_error(&mut stream, 404, "404 page not found");
            return;
        }
        let Ok(conn) = accept_websocket(stream, request, MAX_RPC_FRAME_BYTES_FOR_WS) else {
            return;
        };
        let conn = Arc::new(conn);
        let Some(mut auth) = Self::read_auth(&conn) else { return };
        if auth.session_id.is_empty() {
            auth.session_id = "default".to_string();
        }
        if !Self::check_lease(&conn, &self.cfg.rpc_auth_lease_file, &auth) {
            return;
        }
        let writer: Arc<WsRpcFrameWriter> = Arc::new(WsRpcFrameWriter { conn: Arc::clone(&conn) });
        let ready = PtyEventFrame {
            kind: "ready".to_string(),
            session_id: auth.session_id,
            ..PtyEventFrame::default()
        };
        if writer.write_json(&ready).is_err() {
            return;
        }
        let bridge: Option<Arc<dyn CliBridge>> =
            self.cfg.cli_bridge.clone().map(|b| b as Arc<dyn CliBridge>);
        let server = RpcServer::new(
            writer.clone() as Arc<dyn FrameWriter>,
            Some(Arc::clone(&self.hub)),
            false,
            bridge,
        );
        let _registration = self
            .cfg
            .cli_bridge
            .as_ref()
            .map(|b| b.register(writer.clone() as Arc<dyn FrameWriter>));
        loop {
            let payload = match conn.read() {
                Ok(WsMessage::Text(payload)) => payload,
                Ok(WsMessage::Binary(_)) => {
                    conn.close(STATUS_UNSUPPORTED_DATA, "rpc frames must be text JSON");
                    break;
                }
                Ok(WsMessage::Close { .. }) | Err(_) => {
                    conn.close(STATUS_NORMAL_CLOSURE, "closed");
                    break;
                }
            };
            let trimmed = trim_ascii(&payload);
            if trimmed.is_empty() {
                continue;
            }
            let Ok(req) = RpcRequest::parse(trimmed) else {
                if writer
                    .write_response(&RpcResponse::failure(
                        None,
                        "invalid_request",
                        "invalid JSON request",
                    ))
                    .is_err()
                {
                    conn.close(STATUS_INTERNAL_ERROR, "write failed");
                    break;
                }
                continue;
            };
            if server.handle_request_and_write_response(&req).is_err() {
                conn.close(STATUS_INTERNAL_ERROR, "write failed");
                break;
            }
        }
        writer.close();
        server.close_all();
    }
}

fn trim_ascii(payload: &[u8]) -> &[u8] {
    let start = payload.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(payload.len());
    let end = payload.iter().rposition(|b| !b.is_ascii_whitespace()).map_or(start, |i| i + 1);
    &payload[start..end]
}

/// JSON text frames over the `/rpc` WebSocket.
pub struct WsRpcFrameWriter {
    conn: Arc<WsConn>,
}

impl WsRpcFrameWriter {
    fn write_json(&self, payload: &impl serde::Serialize) -> io::Result<()> {
        if self.conn.is_closed() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "websocket closed"));
        }
        let data = serde_json::to_vec(payload).map_err(io::Error::other)?;
        self.conn.write_text(&data)
    }

    fn close(&self) {
        // Mark closed so pumps stop racing events onto a finished connection.
        self.conn.close(STATUS_NORMAL_CLOSURE, "closed");
    }
}

impl FrameWriter for WsRpcFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.write_json(resp)
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.write_json(event)
    }
}

/// `wsPTYHub.attach`: bind a WebSocket to a session and start its writer.
pub fn attach_websocket(
    hub: &Arc<PtyHub>,
    conn: &Arc<WsConn>,
    auth: &WsAuthFrame,
) -> Result<Arc<Attachment>, String> {
    let mut session_id = auth.session_id.trim().to_string();
    if session_id.is_empty() {
        session_id = "default".to_string();
    }
    let (cols, rows) = normalize_pty_size_i64(auth.cols, auth.rows);
    let attachment_id = auth.attachment_id.trim().to_string();
    let persistent = !attachment_id.is_empty() && auth.session_id_explicit;
    let (attachment, ctx, session_done) = hub.prepare_attachment(
        Some(Arc::clone(conn) as Arc<dyn crate::pty::AttachmentConn>),
        &session_id,
        &attachment_id,
        cols,
        rows,
        persistent,
        "",
        "",
        false,
        false,
    )?;
    let writer_attachment = Arc::clone(&attachment);
    let writer_conn = Arc::clone(conn);
    std::thread::spawn(move || write_loop(&writer_attachment, &writer_conn, &ctx, &session_done));
    Ok(attachment)
}

fn write_loop(
    attachment: &Arc<Attachment>,
    conn: &Arc<WsConn>,
    ctx: &Signal,
    session_done: &Signal,
) {
    use crossbeam_channel::select;
    loop {
        select! {
            recv(ctx.receiver()) -> _ => return,
            recv(session_done.receiver()) -> _ => {
                while let Ok(frame) = attachment.frames().try_recv() {
                    if !write_frame(attachment, conn, ctx, &frame) {
                        return;
                    }
                }
                conn.close(STATUS_NORMAL_CLOSURE, "pty closed");
                return;
            }
            recv(attachment.frames()) -> frame => {
                let Ok(frame) = frame else { return };
                if !write_frame(attachment, conn, ctx, &frame) {
                    return;
                }
            }
        }
    }
}

fn write_frame(
    attachment: &Attachment,
    conn: &WsConn,
    ctx: &Signal,
    frame: &crate::pty::OutgoingFrame,
) -> bool {
    if ctx.is_fired() {
        attachment.cancel();
        conn.close_now();
        return false;
    }
    let result = match frame.kind {
        crate::pty::FrameKind::Text => conn.write_text(&frame.payload),
        crate::pty::FrameKind::Binary => conn.write_binary(&frame.payload),
    };
    if result.is_err() {
        attachment.cancel();
        conn.close_now();
        return false;
    }
    true
}

/// Text control frame on `/terminal` after auth (`resize`, `close`).
#[derive(Debug, Default, Deserialize)]
pub struct PtyControlFrame {
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub cols: i64,
    #[serde(default)]
    pub rows: i64,
}

pub fn pump_websocket_to_pty(hub: &Arc<PtyHub>, attachment: &Arc<Attachment>, conn: &WsConn) {
    loop {
        match conn.read() {
            Ok(WsMessage::Binary(payload)) => {
                if hub.write_input(attachment, &payload, 0, false).status != InputWriteStatus::Ok {
                    return;
                }
            }
            Ok(WsMessage::Text(payload)) => {
                let Ok(control) = serde_json::from_slice::<PtyControlFrame>(&payload) else {
                    continue;
                };
                match control.kind.as_str() {
                    "resize" => {
                        let cols = usize::try_from(control.cols).unwrap_or(0);
                        let rows = usize::try_from(control.rows).unwrap_or(0);
                        hub.resize(attachment, cols, rows);
                    }
                    "close" => {
                        hub.close_session_for_attachment(attachment);
                        return;
                    }
                    _ => {}
                }
            }
            Ok(WsMessage::Close { .. }) | Err(_) => return,
        }
    }
}

/// SHA-256 hex of a token, as stored in lease files.
#[must_use]
pub fn token_sha256_hex(token: &str) -> String {
    hex::encode(sha2::Sha256::digest(token.as_bytes()))
}

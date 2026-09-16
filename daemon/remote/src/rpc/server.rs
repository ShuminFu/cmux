//! Request dispatch for the newline-delimited JSON-RPC protocol.

use std::collections::HashMap;
use std::io;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use crossbeam_channel::select;
use serde_json::{Value, json};

use super::frame::FrameWriter;
use super::types::{
    RpcEvent, RpcRequest, RpcResponse, get_bool_param, get_int_param, get_string_param,
};
use crate::VERSION;
use crate::pty::{
    Attachment, InputWriteResult, InputWriteStatus, PtyHub, rpc_pty_event_for_frame,
    rpc_pty_exit_event,
};
use crate::signal::Signal;
use crate::util::{io_error_text, quote, rfc3339_nano_utc};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A bidirectional byte stream backing a `proxy.*` stream. `TcpStream` in
/// production; tests substitute fakes.
pub trait StreamConn: Send + Sync {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&self, buf: &[u8]) -> io::Result<usize>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn close(&self);
}

impl StreamConn for TcpStream {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut &*self, buf)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        io::Write::write(&mut &*self, buf)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }

    fn close(&self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

pub struct StreamState {
    conn: Box<dyn StreamConn>,
    reader_started: AtomicBool,
    /// Set before the connection is closed locally so the pump can tell a
    /// deliberate close (no event, like Go's `net.ErrClosed`) from a peer EOF.
    closed: AtomicBool,
}

/// Response payload delivered to the cloud CLI bridge for `cli.response`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliResponse {
    pub data: Vec<u8>,
    pub err: String,
}

/// Bridge that forwards `cli.request` events to the app and receives the
/// matching `cli.response` frames.
pub trait CliBridge: Send + Sync {
    fn deliver_response(&self, request_id: &str, response: CliResponse) -> bool;
}

#[derive(Debug, Clone)]
struct SessionAttachment {
    cols: i64,
    rows: i64,
    updated_at: SystemTime,
}

#[derive(Debug, Default)]
struct SessionState {
    attachments: HashMap<String, SessionAttachment>,
    effective_cols: i64,
    effective_rows: i64,
    last_known_cols: i64,
    last_known_rows: i64,
}

struct ServerState {
    next_stream_id: u64,
    next_session_id: u64,
    streams: HashMap<String, Arc<StreamState>>,
    sessions: HashMap<String, SessionState>,
    pty_attachments: HashMap<String, Arc<Attachment>>,
}

pub struct RpcServer {
    state: Mutex<ServerState>,
    pty_hub: Option<Arc<PtyHub>>,
    owns_pty_hub: bool,
    frame_writer: Arc<dyn FrameWriter>,
    cli_bridge: Option<Arc<dyn CliBridge>>,
}

fn rpc_pty_attachment_key(attachment: &Attachment) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        attachment.session_key.kind as u8,
        attachment.session_key.session_id,
        attachment.session_key.anonymous_id,
        attachment.id,
        attachment.client_token
    )
}

fn invalid_params(req: &RpcRequest, message: impl Into<String>) -> RpcResponse {
    RpcResponse::failure(req.id.clone(), "invalid_params", message)
}

fn not_found(req: &RpcRequest, message: &str) -> RpcResponse {
    RpcResponse::failure(req.id.clone(), "not_found", message)
}

fn ok(req: &RpcRequest, result: Value) -> RpcResponse {
    RpcResponse::success(req.id.clone(), result)
}

fn string_param_nonempty(req: &RpcRequest, key: &str) -> Option<String> {
    get_string_param(&req.params, key).filter(|v| !v.is_empty())
}

/// `parsePTYAttachmentIdentity`: trimmed session/attachment ids plus the
/// (possibly empty) client token.
fn parse_pty_attachment_identity(
    req: &RpcRequest,
    method: &str,
) -> Result<(String, String, String), RpcResponse> {
    let session_id = get_string_param(&req.params, "session_id").unwrap_or_default();
    if session_id.trim().is_empty() {
        return Err(invalid_params(req, format!("{method} requires session_id")));
    }
    let attachment_id = get_string_param(&req.params, "attachment_id").unwrap_or_default();
    if attachment_id.trim().is_empty() {
        return Err(invalid_params(req, format!("{method} requires attachment_id")));
    }
    let token = get_string_param(&req.params, "client_attachment_token").unwrap_or_default();
    Ok((session_id.trim().to_string(), attachment_id.trim().to_string(), token.trim().to_string()))
}

fn parse_session_attachment_params(
    req: &RpcRequest,
    method: &str,
) -> Result<(String, String, String, i64, i64), RpcResponse> {
    let (session_id, attachment_id, token) = parse_pty_attachment_identity(req, method)?;
    let cols = get_int_param(&req.params, "cols").filter(|c| *c > 0);
    let Some(cols) = cols else {
        return Err(invalid_params(req, format!("{method} requires cols > 0")));
    };
    let rows = get_int_param(&req.params, "rows").filter(|r| *r > 0);
    let Some(rows) = rows else {
        return Err(invalid_params(req, format!("{method} requires rows > 0")));
    };
    Ok((session_id, attachment_id, token, cols, rows))
}

fn missing_pty_attachment_token_response(req: &RpcRequest, method: &str) -> RpcResponse {
    invalid_params(req, format!("{method} requires client_attachment_token"))
}

fn recompute_session_size(session: &mut SessionState) {
    if session.attachments.is_empty() {
        session.effective_cols = session.last_known_cols;
        session.effective_rows = session.last_known_rows;
        return;
    }
    let mut min_cols = 0;
    let mut min_rows = 0;
    for attachment in session.attachments.values() {
        if min_cols == 0 || attachment.cols < min_cols {
            min_cols = attachment.cols;
        }
        if min_rows == 0 || attachment.rows < min_rows {
            min_rows = attachment.rows;
        }
    }
    session.effective_cols = min_cols;
    session.effective_rows = min_rows;
    session.last_known_cols = min_cols;
    session.last_known_rows = min_rows;
}

fn session_snapshot(session_id: &str, session: &SessionState) -> Value {
    let mut ids: Vec<&String> = session.attachments.keys().collect();
    ids.sort();
    let attachments: Vec<Value> = ids
        .into_iter()
        .map(|id| {
            let attachment = &session.attachments[id];
            json!({
                "attachment_id": id,
                "cols": attachment.cols,
                "rows": attachment.rows,
                "updated_at": rfc3339_nano_utc(attachment.updated_at),
            })
        })
        .collect();
    json!({
        "session_id": session_id,
        "attachments": attachments,
        "effective_cols": session.effective_cols,
        "effective_rows": session.effective_rows,
        "last_known_cols": session.last_known_cols,
        "last_known_rows": session.last_known_rows,
    })
}

impl RpcServer {
    #[must_use]
    pub fn new(
        frame_writer: Arc<dyn FrameWriter>,
        pty_hub: Option<Arc<PtyHub>>,
        owns_pty_hub: bool,
        cli_bridge: Option<Arc<dyn CliBridge>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ServerState {
                next_stream_id: 1,
                next_session_id: 1,
                streams: HashMap::new(),
                sessions: HashMap::new(),
                pty_attachments: HashMap::new(),
            }),
            pty_hub,
            owns_pty_hub,
            frame_writer,
            cli_bridge,
        })
    }

    #[must_use]
    pub fn pty_hub(&self) -> Option<&Arc<PtyHub>> {
        self.pty_hub.as_ref()
    }

    /// Register a stream with an injected connection (tests).
    #[doc(hidden)]
    pub fn insert_stream_for_test(&self, stream_id: &str, conn: Box<dyn StreamConn>) {
        lock(&self.state).streams.insert(
            stream_id.to_string(),
            Arc::new(StreamState {
                conn,
                reader_started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
            }),
        );
    }

    #[doc(hidden)]
    pub fn track_pty_attachment_for_test(&self, attachment: &Arc<Attachment>) {
        self.track_pty_attachment(attachment);
    }

    #[must_use]
    pub fn stream_count(&self) -> usize {
        lock(&self.state).streams.len()
    }

    #[must_use]
    pub fn pty_attachment_count(&self) -> usize {
        lock(&self.state).pty_attachments.len()
    }

    pub fn handle_request_and_write_response(self: &Arc<Self>, req: &RpcRequest) -> io::Result<()> {
        let response = self.handle_request(req);
        if rpc_request_is_pty_attachment_notification(req) {
            return self.handle_notification_response(req, &response);
        }
        self.frame_writer.write_response(&response)
    }

    fn handle_notification_response(&self, req: &RpcRequest, resp: &RpcResponse) -> io::Result<()> {
        if resp.ok {
            return Ok(());
        }
        let Ok((session_id, attachment_id, token)) =
            parse_pty_attachment_identity(req, &req.method)
        else {
            return Ok(());
        };
        if token.is_empty() {
            return Ok(());
        }
        let mut detail = match req.method.as_str() {
            "pty.write" => "PTY write failed",
            "pty.resize" => "PTY resize failed",
            _ => "PTY operation failed",
        }
        .to_string();
        if !resp.error_message().trim().is_empty() {
            detail = resp.error_message().trim().to_string();
        }
        let result = self.frame_writer.write_event(&RpcEvent {
            event: "pty.error".to_string(),
            session_id: session_id.clone(),
            attachment_id: attachment_id.clone(),
            attachment_token: token.clone(),
            error: detail.clone(),
            message: detail,
            ..RpcEvent::default()
        });
        if req.method == "pty.write"
            && matches!(resp.error_code(), "pty_input_queue_full" | "pty_input_seq_gap")
            && let Some(hub) = &self.pty_hub
        {
            hub.detach_by_id(&session_id, &attachment_id, &token);
        }
        result
    }

    pub fn handle_request(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        if req.method.is_empty() {
            return RpcResponse::failure(req.id.clone(), "invalid_request", "method is required");
        }
        match req.method.as_str() {
            "hello" => ok(
                req,
                json!({
                    "name": "cmuxd-remote",
                    "version": VERSION,
                    "capabilities": [
                        "session.basic",
                        "session.resize.min",
                        "proxy.http_connect",
                        "proxy.socks5",
                        "proxy.stream",
                        "proxy.stream.push",
                        "pty.session",
                        "pty.session.token",
                        "pty.session.persistent_daemon",
                        "pty.write.notification",
                        "pty.resize.notification",
                        "pty.input.seq_ack",
                        "cli.bridge",
                    ],
                }),
            ),
            "ping" => ok(req, json!({"pong": true})),
            "proxy.open" => self.handle_proxy_open(req),
            "proxy.close" => self.handle_proxy_close(req),
            "proxy.write" => self.handle_proxy_write(req),
            "proxy.stream.subscribe" => self.handle_proxy_stream_subscribe(req),
            "session.open" => self.handle_session_open(req),
            "session.close" => self.handle_session_close(req),
            "session.attach" => self.handle_session_attach(req),
            "session.resize" => self.handle_session_resize(req),
            "session.detach" => self.handle_session_detach(req),
            "session.status" => self.handle_session_status(req),
            "pty.attach" => self.handle_pty_attach(req),
            "pty.write" => self.handle_pty_write(req),
            "pty.resize" => self.handle_pty_resize(req),
            "pty.detach" => self.handle_pty_detach(req),
            "pty.close" => self.handle_pty_close(req),
            "pty.list" => self.handle_pty_list(req),
            "cli.response" => self.handle_cli_response(req),
            _ => RpcResponse::failure(
                req.id.clone(),
                "method_not_found",
                format!("unknown method {}", quote(&req.method)),
            ),
        }
    }

    fn handle_proxy_open(&self, req: &RpcRequest) -> RpcResponse {
        let Some(host) = string_param_nonempty(req, "host") else {
            return invalid_params(req, "proxy.open requires host");
        };
        let port = get_int_param(&req.params, "port").filter(|p| *p > 0 && *p <= 65535);
        let Some(port) = port else {
            return invalid_params(req, "proxy.open requires port in range 1-65535");
        };
        let mut timeout_ms: i64 = 10000;
        if let Some(parsed) = get_int_param(&req.params, "timeout_ms")
            && parsed >= 0
        {
            timeout_ms = parsed;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let port_u16 = port as u16;
        let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
        let conn = match dial_timeout(&host, port_u16, timeout) {
            Ok(conn) => conn,
            Err(err) => return RpcResponse::failure(req.id.clone(), "open_failed", err),
        };
        let _ = conn.set_nodelay(true);
        let stream_id = {
            let mut state = lock(&self.state);
            let id = format!("s-{}", state.next_stream_id);
            state.next_stream_id += 1;
            state.streams.insert(
                id.clone(),
                Arc::new(StreamState {
                    conn: Box::new(conn),
                    reader_started: AtomicBool::new(false),
                    closed: AtomicBool::new(false),
                }),
            );
            id
        };
        ok(req, json!({"stream_id": stream_id}))
    }

    fn handle_proxy_close(&self, req: &RpcRequest) -> RpcResponse {
        let Some(stream_id) = string_param_nonempty(req, "stream_id") else {
            return invalid_params(req, "proxy.close requires stream_id");
        };
        let stream = lock(&self.state).streams.remove(&stream_id);
        if let Some(stream) = stream {
            stream.closed.store(true, Ordering::SeqCst);
            stream.conn.close();
        }
        ok(req, json!({"closed": true}))
    }

    fn handle_proxy_write(&self, req: &RpcRequest) -> RpcResponse {
        let Some(stream_id) = string_param_nonempty(req, "stream_id") else {
            return invalid_params(req, "proxy.write requires stream_id");
        };
        let Some(data_base64) = get_string_param(&req.params, "data_base64") else {
            return invalid_params(req, "proxy.write requires data_base64");
        };
        let Ok(payload) = BASE64.decode(data_base64.as_bytes()) else {
            return invalid_params(req, "data_base64 must be valid base64");
        };
        let Some(stream) = self.get_stream(&stream_id) else {
            return not_found(req, "stream not found");
        };
        let mut timeout_ms: i64 = 8000;
        if let Some(parsed) = get_int_param(&req.params, "timeout_ms") {
            timeout_ms = parsed;
        }
        let timed = timeout_ms > 0;
        if timed {
            let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
            if let Err(err) = stream.conn.set_write_timeout(Some(timeout)) {
                return RpcResponse::failure(req.id.clone(), "stream_error", io_error_text(&err));
            }
        }
        let result = write_all_progress(stream.conn.as_ref(), &payload);
        if timed {
            let _ = stream.conn.set_write_timeout(None);
        }
        match result {
            Ok(total) => ok(req, json!({"written": total})),
            Err(message) => RpcResponse::failure(req.id.clone(), "stream_error", message),
        }
    }

    fn handle_proxy_stream_subscribe(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        let Some(stream_id) = string_param_nonempty(req, "stream_id") else {
            return invalid_params(req, "proxy.stream.subscribe requires stream_id");
        };
        let Some(stream) = self.get_stream(&stream_id) else {
            return not_found(req, "stream not found");
        };
        let already_subscribed = stream.reader_started.swap(true, Ordering::SeqCst);
        if !already_subscribed {
            let server = Arc::clone(self);
            std::thread::spawn(move || server.stream_pump(&stream_id, &stream));
        }
        ok(req, json!({"subscribed": true, "already_subscribed": already_subscribed}))
    }

    fn handle_session_open(&self, req: &RpcRequest) -> RpcResponse {
        let mut state = lock(&self.state);
        let mut session_id = get_string_param(&req.params, "session_id").unwrap_or_default();
        if session_id.is_empty() {
            session_id = format!("sess-{}", state.next_session_id);
            state.next_session_id += 1;
        }
        let session = state.sessions.entry(session_id.clone()).or_default();
        ok(req, session_snapshot(&session_id, session))
    }

    fn handle_session_close(&self, req: &RpcRequest) -> RpcResponse {
        let Some(session_id) = string_param_nonempty(req, "session_id") else {
            return invalid_params(req, "session.close requires session_id");
        };
        if lock(&self.state).sessions.remove(&session_id).is_none() {
            return not_found(req, "session not found");
        }
        ok(req, json!({"session_id": session_id, "closed": true}))
    }

    fn handle_session_attach(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, _, cols, rows) =
            match parse_session_attachment_params(req, "session.attach") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
        let mut state = lock(&self.state);
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return not_found(req, "session not found");
        };
        session
            .attachments
            .insert(attachment_id, SessionAttachment { cols, rows, updated_at: SystemTime::now() });
        recompute_session_size(session);
        ok(req, session_snapshot(&session_id, session))
    }

    fn handle_session_resize(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, _, cols, rows) =
            match parse_session_attachment_params(req, "session.resize") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
        let mut state = lock(&self.state);
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return not_found(req, "session not found");
        };
        if !session.attachments.contains_key(&attachment_id) {
            return not_found(req, "attachment not found");
        }
        session
            .attachments
            .insert(attachment_id, SessionAttachment { cols, rows, updated_at: SystemTime::now() });
        recompute_session_size(session);
        ok(req, session_snapshot(&session_id, session))
    }

    fn handle_session_detach(&self, req: &RpcRequest) -> RpcResponse {
        let Some(session_id) = string_param_nonempty(req, "session_id") else {
            return invalid_params(req, "session.detach requires session_id");
        };
        let Some(attachment_id) = string_param_nonempty(req, "attachment_id") else {
            return invalid_params(req, "session.detach requires attachment_id");
        };
        let mut state = lock(&self.state);
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return not_found(req, "session not found");
        };
        if session.attachments.remove(&attachment_id).is_none() {
            return not_found(req, "attachment not found");
        }
        recompute_session_size(session);
        ok(req, session_snapshot(&session_id, session))
    }

    fn handle_session_status(&self, req: &RpcRequest) -> RpcResponse {
        let Some(session_id) = string_param_nonempty(req, "session_id") else {
            return invalid_params(req, "session.status requires session_id");
        };
        let state = lock(&self.state);
        let Some(session) = state.sessions.get(&session_id) else {
            return not_found(req, "session not found");
        };
        ok(req, session_snapshot(&session_id, session))
    }

    fn handle_pty_attach(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        let session_id = get_string_param(&req.params, "session_id").unwrap_or_default();
        if session_id.trim().is_empty() {
            return invalid_params(req, "pty.attach requires session_id");
        }
        let attachment_id = get_string_param(&req.params, "attachment_id").unwrap_or_default();
        let token = get_string_param(&req.params, "client_attachment_token").unwrap_or_default();
        let token = token.trim().to_string();
        if token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.attach");
        }
        let Some(cols) = get_int_param(&req.params, "cols").filter(|c| *c > 0) else {
            return invalid_params(req, "pty.attach requires cols > 0");
        };
        let Some(rows) = get_int_param(&req.params, "rows").filter(|r| *r > 0) else {
            return invalid_params(req, "pty.attach requires rows > 0");
        };
        let command = get_string_param(&req.params, "command").unwrap_or_default();
        let require_existing = get_bool_param(&req.params, "require_existing").unwrap_or(false);
        let input_seq_ack = get_bool_param(&req.params, "input_seq_ack").unwrap_or(false);
        let Some(hub) = &self.pty_hub else {
            return RpcResponse::failure(req.id.clone(), "unavailable", "PTY hub is not available");
        };
        let (attachment, ctx, session_done) = match hub.attach_rpc(
            &session_id,
            &attachment_id,
            usize::try_from(cols).unwrap_or(usize::MAX),
            usize::try_from(rows).unwrap_or(usize::MAX),
            &command,
            &token,
            require_existing,
            input_seq_ack,
        ) {
            Ok(v) => v,
            Err(err) => {
                let code =
                    if require_existing { "pty_session_not_found" } else { "pty_start_failed" };
                return RpcResponse::failure(req.id.clone(), code, err);
            }
        };
        self.track_pty_attachment(&attachment);
        let server = Arc::clone(self);
        let pump_attachment = Arc::clone(&attachment);
        std::thread::spawn(move || {
            server.pty_attachment_pump(&pump_attachment, &ctx, &session_done);
        });
        ok(
            req,
            json!({
                "session_id": session_id.trim(),
                "attachment_id": attachment.id,
                "attachment_token": attachment.client_token,
                "attached": true,
            }),
        )
    }

    fn handle_pty_write(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, token) =
            match parse_pty_attachment_identity(req, "pty.write") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
        if token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.write");
        }
        let Some(data_base64) = get_string_param(&req.params, "data_base64") else {
            return invalid_params(req, "pty.write requires data_base64");
        };
        let Ok(payload) = BASE64.decode(data_base64.as_bytes()) else {
            return invalid_params(req, "data_base64 must be valid base64");
        };
        let mut seq = 0u64;
        let mut has_seq = false;
        if req.params.contains_key("seq") {
            let parsed = get_int_param(&req.params, "seq").filter(|s| *s >= 0);
            let Some(parsed) = parsed else {
                return invalid_params(req, "seq must be a non-negative integer");
            };
            seq = u64::try_from(parsed).unwrap_or(0);
            has_seq = true;
        }
        let mut result = InputWriteResult { status: InputWriteStatus::NotFound, got: 0, want: 0 };
        if let Some(hub) = &self.pty_hub {
            result = hub.write_input_by_id_with_seq(
                &session_id,
                &attachment_id,
                &token,
                &payload,
                seq,
                has_seq,
            );
        }
        match result.status {
            InputWriteStatus::SeqGap => RpcResponse::failure(
                req.id.clone(),
                "pty_input_seq_gap",
                format!("PTY input sequence gap: got {}, want {}", result.got, result.want),
            ),
            InputWriteStatus::QueueFull => RpcResponse::failure(
                req.id.clone(),
                "pty_input_queue_full",
                "PTY input queue is full",
            ),
            InputWriteStatus::NotFound => not_found(req, "PTY attachment not found"),
            InputWriteStatus::Ok => ok(req, json!({"written": payload.len()})),
        }
    }

    fn handle_pty_resize(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, token, cols, rows) =
            match parse_session_attachment_params(req, "pty.resize") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
        if token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.resize");
        }
        let resized = self.pty_hub.as_ref().is_some_and(|hub| {
            hub.resize_by_id(
                &session_id,
                &attachment_id,
                &token,
                usize::try_from(cols).unwrap_or(usize::MAX),
                usize::try_from(rows).unwrap_or(usize::MAX),
            )
        });
        if !resized {
            return not_found(req, "PTY attachment not found");
        }
        ok(req, json!({"resized": true}))
    }

    fn handle_pty_detach(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, token) =
            match parse_pty_attachment_identity(req, "pty.detach") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
        if token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.detach");
        }
        let detached = self
            .pty_hub
            .as_ref()
            .is_some_and(|hub| hub.detach_by_id(&session_id, &attachment_id, &token));
        if !detached {
            return not_found(req, "PTY attachment not found");
        }
        ok(req, json!({"detached": true}))
    }

    fn handle_pty_close(&self, req: &RpcRequest) -> RpcResponse {
        let session_id = get_string_param(&req.params, "session_id").unwrap_or_default();
        if session_id.trim().is_empty() {
            return invalid_params(req, "pty.close requires session_id");
        }
        let closed = self.pty_hub.as_ref().is_some_and(|hub| hub.close_session_by_id(&session_id));
        if !closed {
            return not_found(req, "PTY session not found");
        }
        ok(req, json!({"session_id": session_id.trim(), "closed": true}))
    }

    fn handle_pty_list(&self, req: &RpcRequest) -> RpcResponse {
        let sessions = self.pty_hub.as_ref().map(|hub| hub.session_snapshots()).unwrap_or_default();
        ok(req, json!({"sessions": sessions}))
    }

    fn handle_cli_response(&self, req: &RpcRequest) -> RpcResponse {
        let Some(bridge) = &self.cli_bridge else {
            return RpcResponse::failure(
                req.id.clone(),
                "unavailable",
                "cloud CLI bridge is not enabled",
            );
        };
        let Some(request_id) = string_param_nonempty(req, "request_id") else {
            return invalid_params(req, "cli.response requires request_id");
        };
        let response_ok = get_bool_param(&req.params, "ok").unwrap_or(true);
        let mut response = CliResponse::default();
        if response_ok {
            let Some(data_base64) = get_string_param(&req.params, "data_base64") else {
                return invalid_params(req, "cli.response requires data_base64");
            };
            let Ok(data) = BASE64.decode(data_base64.as_bytes()) else {
                return invalid_params(req, "data_base64 must be valid base64");
            };
            response.data = data;
        } else {
            response.err = get_string_param(&req.params, "error").unwrap_or_default();
            if response.err.is_empty() {
                response.err = "cmux app rejected cloud CLI request".to_string();
            }
        }
        if !bridge.deliver_response(&request_id, response) {
            return not_found(req, "cloud CLI request not found");
        }
        ok(req, json!({"delivered": true}))
    }

    fn pty_attachment_pump(
        &self,
        attachment: &Arc<Attachment>,
        ctx: &Signal,
        session_done: &Signal,
    ) {
        self.pty_attachment_pump_inner(attachment, ctx, session_done);
        self.untrack_pty_attachment(attachment);
    }

    fn pty_attachment_pump_inner(
        &self,
        attachment: &Arc<Attachment>,
        ctx: &Signal,
        session_done: &Signal,
    ) {
        let write_frame = |frame| -> bool {
            if self.frame_writer.write_event(&rpc_pty_event_for_frame(attachment, &frame)).is_err()
            {
                if let Some(hub) = &self.pty_hub {
                    hub.drop_attachment(attachment);
                }
                return false;
            }
            true
        };
        loop {
            select! {
                recv(ctx.receiver()) -> _ => {
                    let _ = self.frame_writer.write_event(&rpc_pty_exit_event(attachment));
                    return;
                }
                recv(session_done.receiver()) -> _ => {
                    while let Ok(frame) = attachment.frames().try_recv() {
                        if !write_frame(frame) {
                            return;
                        }
                    }
                    let _ = self.frame_writer.write_event(&rpc_pty_exit_event(attachment));
                    return;
                }
                recv(attachment.frames()) -> frame => {
                    let Ok(frame) = frame else { return };
                    if !write_frame(frame) {
                        return;
                    }
                }
            }
        }
    }

    fn track_pty_attachment(&self, attachment: &Arc<Attachment>) {
        lock(&self.state)
            .pty_attachments
            .insert(rpc_pty_attachment_key(attachment), Arc::clone(attachment));
    }

    fn untrack_pty_attachment(&self, attachment: &Arc<Attachment>) {
        let mut state = lock(&self.state);
        let key = rpc_pty_attachment_key(attachment);
        if state.pty_attachments.get(&key).is_some_and(|current| Arc::ptr_eq(current, attachment)) {
            state.pty_attachments.remove(&key);
        }
    }

    /// Close every proxy stream, forget every legacy session, and drop every
    /// PTY attachment this server created. A server that owns its hub (stdio
    /// mode) also kills every PTY session; one that borrowed a shared hub (the
    /// persistent daemon) leaves sessions alive for the next client.
    pub fn close_all(&self) {
        let (streams, attachments): (Vec<Arc<StreamState>>, Vec<Arc<Attachment>>) = {
            let mut state = lock(&self.state);
            let streams = state.streams.drain().map(|(_, s)| s).collect();
            state.sessions.clear();
            let attachments = state.pty_attachments.drain().map(|(_, a)| a).collect();
            (streams, attachments)
        };
        for stream in streams {
            stream.closed.store(true, Ordering::SeqCst);
            stream.conn.close();
        }
        for attachment in attachments {
            match &self.pty_hub {
                Some(hub) => hub.drop_attachment(&attachment),
                None => attachment.close_now(),
            }
        }
        if self.owns_pty_hub
            && let Some(hub) = &self.pty_hub
        {
            hub.close_all();
        }
    }

    fn stream_pump(&self, stream_id: &str, stream: &Arc<StreamState>) {
        let mut buffer = vec![0u8; 32768];
        loop {
            let read = stream.conn.read(&mut buffer);
            match read {
                Ok(n) if n > 0 => {
                    let _ = self.frame_writer.write_event(&RpcEvent {
                        event: "proxy.stream.data".to_string(),
                        stream_id: stream_id.to_string(),
                        data_base64: BASE64.encode(&buffer[..n]),
                        ..RpcEvent::default()
                    });
                }
                Ok(_) => {
                    if !stream.closed.load(Ordering::SeqCst) {
                        let _ = self.frame_writer.write_event(&RpcEvent {
                            event: "proxy.stream.eof".to_string(),
                            stream_id: stream_id.to_string(),
                            ..RpcEvent::default()
                        });
                    }
                    self.drop_stream(stream_id);
                    return;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => {
                    if !stream.closed.load(Ordering::SeqCst) {
                        let _ = self.frame_writer.write_event(&RpcEvent {
                            event: "proxy.stream.error".to_string(),
                            stream_id: stream_id.to_string(),
                            error: io_error_text(&err),
                            ..RpcEvent::default()
                        });
                    }
                    self.drop_stream(stream_id);
                    return;
                }
            }
        }
    }

    fn get_stream(&self, stream_id: &str) -> Option<Arc<StreamState>> {
        lock(&self.state).streams.get(stream_id).cloned()
    }

    fn drop_stream(&self, stream_id: &str) {
        let removed = lock(&self.state).streams.remove(stream_id);
        if let Some(stream) = removed {
            stream.closed.store(true, Ordering::SeqCst);
            stream.conn.close();
        }
    }
}

fn rpc_request_is_pty_attachment_notification(req: &RpcRequest) -> bool {
    !req.has_id && (req.method == "pty.write" || req.method == "pty.resize")
}

fn write_all_progress(conn: &dyn StreamConn, payload: &[u8]) -> Result<usize, String> {
    let mut total = 0;
    while total < payload.len() {
        match conn.write(&payload[total..]) {
            Ok(0) => return Err("write made no progress".to_string()),
            Ok(n) => total += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err)
                if matches!(err.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
            {
                return Err("i/o timeout".to_string());
            }
            Err(err) => return Err(io_error_text(&err)),
        }
    }
    Ok(total)
}

/// Connect with a timeout, trying every resolved address like Go's dialer
/// and reporting failures in `dial tcp` form.
fn dial_timeout(host: &str, port: u16, timeout: Duration) -> Result<TcpStream, String> {
    let addrs: Vec<std::net::SocketAddr> = match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(err) => return Err(format!("dial tcp: lookup {host}: {}", io_error_text(&err))),
    };
    if addrs.is_empty() {
        return Err(format!("dial tcp: lookup {host}: no such host"));
    }
    let deadline = std::time::Instant::now() + timeout;
    let mut last_err = String::new();
    for addr in addrs {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            last_err = format!("dial tcp {addr}: i/o timeout");
            break;
        }
        match TcpStream::connect_timeout(&addr, remaining) {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last_err = if err.kind() == io::ErrorKind::TimedOut {
                    format!("dial tcp {addr}: i/o timeout")
                } else {
                    format!("dial tcp {addr}: connect: {}", io_error_text(&err))
                };
            }
        }
    }
    Err(last_err)
}

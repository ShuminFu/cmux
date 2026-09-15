//! Newline-delimited JSON-RPC framing, request routing, proxy stream RPC, the
//! session resize coordinator, and the PTY RPC surface. Mirrors `main.go`.

use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Map, Value};

use crate::cloud_cli_bridge::CloudCliBridge;
use crate::pty_hub::{
    FrameKind, InputWriteStatus, OutgoingFrame, PtyAttachment, PtyHub, SessionKind, WsPtyEventFrame,
};
use crate::util::{go_io_error, go_json, now_rfc3339_nano, version, DoneSignal, LogSink};

pub const MAX_RPC_FRAME_BYTES: usize = 4 * 1024 * 1024;
pub const PERSISTENT_DAEMON_SHUTDOWN_METHOD: &str = "daemon.shutdown";

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

pub fn base64_encode(data: &[u8]) -> String {
    BASE64.encode(data)
}

pub fn base64_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    BASE64.decode(data)
}

/// A JSON-RPC request. `has_id` distinguishes `"id": null` (a request that
/// still expects a response) from a missing id (a notification).
#[derive(Clone, Debug, Default)]
pub struct RpcRequest {
    pub id: Option<Value>,
    pub has_id: bool,
    pub method: String,
    pub params: Option<Map<String, Value>>,
}

fn deserialize_some<'de, D>(deserializer: D) -> Result<Option<Option<Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Value>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct RpcRequestWire {
    #[serde(default, deserialize_with = "deserialize_some")]
    id: Option<Option<Value>>,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Option<Map<String, Value>>,
}

impl RpcRequest {
    pub fn new(id: impl Into<Value>, method: &str, params: Option<Map<String, Value>>) -> Self {
        let id = id.into();
        Self {
            has_id: true,
            id: if id.is_null() { None } else { Some(id) },
            method: method.to_string(),
            params,
        }
    }

    pub fn notification(method: &str, params: Option<Map<String, Value>>) -> Self {
        Self {
            has_id: false,
            id: None,
            method: method.to_string(),
            params,
        }
    }

    pub fn parse(data: &[u8]) -> Result<Self, serde_json::Error> {
        // Go unmarshals into a struct, which rejects arrays and scalars;
        // serde would happily read a struct from a sequence, so gate on the
        // value shape first.
        let value: Value = serde_json::from_slice(data)?;
        if !value.is_object() {
            return Err(serde::de::Error::custom(
                "request frame must be a JSON object",
            ));
        }
        let wire: RpcRequestWire = serde_json::from_value(value)?;
        Ok(Self {
            has_id: wire.id.is_some(),
            id: wire.id.flatten(),
            method: wire.method,
            params: wire.params,
        })
    }

    pub fn params(&self) -> Option<&Map<String, Value>> {
        self.params.as_ref()
    }

    /// Serialize like Go's `json.Marshal(rpcRequest{...})`.
    pub fn to_json(&self) -> String {
        let mut map = Map::new();
        map.insert("id".to_string(), self.id.clone().unwrap_or(Value::Null));
        map.insert("method".to_string(), Value::String(self.method.clone()));
        map.insert(
            "params".to_string(),
            self.params
                .clone()
                .map(Value::Object)
                .unwrap_or(Value::Null),
        );
        go_json(&Value::Object(map))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RpcResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    pub fn ok(id: Option<Value>, result: Value) -> Self {
        Self {
            id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Option<Value>, code: &str, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(RpcError {
                code: code.to_string(),
                message: message.into(),
            }),
        }
    }

    pub fn error_code(&self) -> &str {
        self.error.as_ref().map(|e| e.code.as_str()).unwrap_or("")
    }

    pub fn error_message(&self) -> &str {
        self.error
            .as_ref()
            .map(|e| e.message.as_str())
            .unwrap_or("")
    }

    pub fn result_object(&self) -> Option<&Map<String, Value>> {
        self.result.as_ref().and_then(Value::as_object)
    }

    /// Serialize with Go's struct field order (`id, ok, result, error`) and
    /// HTML-safe escaping so frames are byte-identical to the Go daemon.
    pub fn to_json(&self) -> String {
        crate::util::go_json_escape(
            &serde_json::to_string(self).unwrap_or_else(|_| "null".to_string()),
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcEvent {
    pub event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data_base64: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub seq: u64,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

impl RpcEvent {
    pub fn named(event: &str) -> Self {
        Self {
            event: event.to_string(),
            ..Default::default()
        }
    }

    pub fn to_json(&self) -> String {
        crate::util::go_json_escape(
            &serde_json::to_string(self).unwrap_or_else(|_| "null".to_string()),
        )
    }
}

pub trait FrameWriter: Send + Sync {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()>;
    fn write_event(&self, event: &RpcEvent) -> io::Result<()>;
}

/// Line-oriented frame writer over any `Write` (stdout or a socket).
pub struct StdioFrameWriter {
    inner: Mutex<Box<dyn Write + Send>>,
}

impl StdioFrameWriter {
    pub fn new(writer: Box<dyn Write + Send>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(writer),
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        self.inner.lock().unwrap().flush()
    }

    fn write_json_frame(&self, payload: &str) -> io::Result<()> {
        let mut guard = self.inner.lock().unwrap();
        guard.write_all(payload.as_bytes())?;
        guard.write_all(b"\n")?;
        guard.flush()
    }
}

impl FrameWriter for StdioFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.write_json_frame(&resp.to_json())
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.write_json_frame(&event.to_json())
    }
}

/// Frame writer that records events (tests) or discards everything.
#[derive(Default)]
pub struct CaptureFrameWriter {
    pub responses: Mutex<Vec<RpcResponse>>,
    pub events: Mutex<Vec<RpcEvent>>,
}

impl CaptureFrameWriter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn events(&self) -> Vec<RpcEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl FrameWriter for CaptureFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.responses.lock().unwrap().push(resp.clone());
        Ok(())
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

/// Frame writer backed by a closure (used by the cloud CLI bridge tests and
/// the WebSocket RPC transport).
type ResponseHook = Box<dyn Fn(&RpcResponse) -> io::Result<()> + Send + Sync>;
type EventHook = Box<dyn Fn(&RpcEvent) -> io::Result<()> + Send + Sync>;

pub struct FnFrameWriter {
    on_response: ResponseHook,
    on_event: EventHook,
}

impl FnFrameWriter {
    pub fn new(
        on_response: impl Fn(&RpcResponse) -> io::Result<()> + Send + Sync + 'static,
        on_event: impl Fn(&RpcEvent) -> io::Result<()> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            on_response: Box::new(on_response),
            on_event: Box::new(on_event),
        })
    }
}

impl FrameWriter for FnFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        (self.on_response)(resp)
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        (self.on_event)(event)
    }
}

/// Read one newline-terminated frame. Returns `(frame, oversized)`; an
/// oversized frame is drained through its newline so the stream stays in
/// sync. EOF with no pending bytes surfaces as `UnexpectedEof`.
pub fn read_rpc_frame<R: BufRead>(reader: &mut R, max_bytes: usize) -> io::Result<(Vec<u8>, bool)> {
    enum Step {
        Complete,
        Partial,
        Oversized { needs_drain: bool },
    }
    let mut frame: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let (consumed, step) = {
            let buf = match reader.fill_buf() {
                Ok(buf) => buf,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            };
            if buf.is_empty() {
                if frame.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
                }
                return Ok((frame, false));
            }
            match buf.iter().position(|b| *b == b'\n') {
                Some(idx) => {
                    let chunk = &buf[..=idx];
                    if frame.len() + chunk.len() > max_bytes {
                        (idx + 1, Step::Oversized { needs_drain: false })
                    } else {
                        frame.extend_from_slice(chunk);
                        (idx + 1, Step::Complete)
                    }
                }
                None => {
                    if frame.len() + buf.len() > max_bytes {
                        (buf.len(), Step::Oversized { needs_drain: true })
                    } else {
                        frame.extend_from_slice(buf);
                        (buf.len(), Step::Partial)
                    }
                }
            }
        };
        reader.consume(consumed);
        match step {
            Step::Complete => return Ok((frame, false)),
            Step::Partial => {}
            Step::Oversized { needs_drain } => {
                if needs_drain {
                    discard_until_newline(reader)?;
                }
                return Ok((Vec::new(), true));
            }
        }
    }
}

fn discard_until_newline<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let (consumed, found) = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                return Ok(());
            }
            match buf.iter().position(|b| *b == b'\n') {
                Some(idx) => (idx + 1, true),
                None => (buf.len(), false),
            }
        };
        reader.consume(consumed);
        if found {
            return Ok(());
        }
    }
}

pub fn get_string_param(params: Option<&Map<String, Value>>, key: &str) -> Option<String> {
    match params?.get(key)? {
        Value::String(value) => Some(value.clone()),
        _ => None,
    }
}

pub fn get_int_param(params: Option<&Map<String, Value>>, key: &str) -> Option<i64> {
    match params?.get(key)? {
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Some(value);
            }
            if let Some(value) = number.as_u64() {
                return i64::try_from(value).ok();
            }
            let value = number.as_f64()?;
            if value.trunc() != value || !value.is_finite() {
                return None;
            }
            Some(value as i64)
        }
        _ => None,
    }
}

pub fn get_bool_param(params: Option<&Map<String, Value>>, key: &str) -> Option<bool> {
    match params?.get(key)? {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

/// Abstract byte stream for proxied TCP connections (tests inject fakes).
pub trait ProxyStream: Send + Sync {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn write(&self, buf: &[u8]) -> io::Result<usize>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn shutdown(&self);
}

impl ProxyStream for TcpStream {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut stream: &TcpStream = self;
        io::Read::read(&mut stream, buf)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let mut stream: &TcpStream = self;
        io::Write::write(&mut stream, buf)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }

    fn shutdown(&self) {
        let _ = TcpStream::shutdown(self, Shutdown::Both);
    }
}

pub struct StreamConn {
    io: Box<dyn ProxyStream>,
    closed: AtomicBool,
}

impl StreamConn {
    pub fn new(io: Box<dyn ProxyStream>) -> Arc<Self> {
        Arc::new(Self {
            io,
            closed: AtomicBool::new(false),
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.io.shutdown();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

struct StreamEntry {
    conn: Arc<StreamConn>,
    reader_started: bool,
}

#[derive(Clone, Debug)]
struct SessionAttachment {
    cols: i64,
    rows: i64,
    updated_at: String,
}

#[derive(Default, Debug)]
pub struct SessionState {
    attachments: HashMap<String, SessionAttachment>,
    effective_cols: i64,
    effective_rows: i64,
    last_known_cols: i64,
    last_known_rows: i64,
}

#[derive(Default)]
struct ServerState {
    next_stream_id: u64,
    next_session_id: u64,
    streams: HashMap<String, StreamEntry>,
    sessions: HashMap<String, SessionState>,
    pty_attachments: HashMap<String, Arc<PtyAttachment>>,
}

pub struct RpcServer {
    state: Mutex<ServerState>,
    pty_hub: Option<Arc<PtyHub>>,
    owns_pty_hub: bool,
    frame_writer: Arc<dyn FrameWriter>,
    cli_bridge: Option<Arc<CloudCliBridge>>,
    stderr: LogSink,
}

pub struct RpcServerBuilder {
    pty_hub: Option<Arc<PtyHub>>,
    owns_pty_hub: bool,
    frame_writer: Option<Arc<dyn FrameWriter>>,
    cli_bridge: Option<Arc<CloudCliBridge>>,
    stderr: Option<LogSink>,
}

impl Default for RpcServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServerBuilder {
    pub fn new() -> Self {
        Self {
            pty_hub: None,
            owns_pty_hub: false,
            frame_writer: None,
            cli_bridge: None,
            stderr: None,
        }
    }

    pub fn pty_hub(mut self, hub: Arc<PtyHub>, owns: bool) -> Self {
        self.pty_hub = Some(hub);
        self.owns_pty_hub = owns;
        self
    }

    pub fn frame_writer(mut self, writer: Arc<dyn FrameWriter>) -> Self {
        self.frame_writer = Some(writer);
        self
    }

    pub fn cli_bridge(mut self, bridge: Arc<CloudCliBridge>) -> Self {
        self.cli_bridge = Some(bridge);
        self
    }

    pub fn stderr(mut self, sink: LogSink) -> Self {
        self.stderr = Some(sink);
        self
    }

    pub fn build(self) -> Arc<RpcServer> {
        Arc::new(RpcServer {
            state: Mutex::new(ServerState {
                next_stream_id: 1,
                next_session_id: 1,
                ..Default::default()
            }),
            pty_hub: self.pty_hub,
            owns_pty_hub: self.owns_pty_hub,
            frame_writer: self
                .frame_writer
                .unwrap_or_else(|| CaptureFrameWriter::new()),
            cli_bridge: self.cli_bridge,
            stderr: self.stderr.unwrap_or_else(LogSink::stderr),
        })
    }
}

pub fn hello_capabilities() -> Vec<&'static str> {
    vec![
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
    ]
}

impl RpcServer {
    pub fn builder() -> RpcServerBuilder {
        RpcServerBuilder::new()
    }

    pub fn frame_writer(&self) -> &Arc<dyn FrameWriter> {
        &self.frame_writer
    }

    pub fn pty_hub(&self) -> Option<&Arc<PtyHub>> {
        self.pty_hub.as_ref()
    }

    pub fn cli_bridge(&self) -> Option<&Arc<CloudCliBridge>> {
        self.cli_bridge.as_ref()
    }

    pub fn handle_request(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        if req.method.is_empty() {
            return RpcResponse::err(req.id.clone(), "invalid_request", "method is required");
        }
        match req.method.as_str() {
            "hello" => RpcResponse::ok(
                req.id.clone(),
                json!({
                    "name": "cmuxd-remote",
                    "version": version(),
                    "capabilities": hello_capabilities(),
                }),
            ),
            "ping" => RpcResponse::ok(req.id.clone(), json!({"pong": true})),
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
            other => RpcResponse::err(
                req.id.clone(),
                "method_not_found",
                format!("unknown method {other:?}"),
            ),
        }
    }

    pub fn handle_request_and_write_response(self: &Arc<Self>, req: &RpcRequest) -> io::Result<()> {
        let resp = self.handle_request(req);
        if !rpc_request_expects_response(req) {
            return self.handle_notification_response(req, &resp);
        }
        self.frame_writer.write_response(&resp)
    }

    pub fn handle_notification_response(
        &self,
        req: &RpcRequest,
        resp: &RpcResponse,
    ) -> io::Result<()> {
        if !rpc_request_is_pty_attachment_notification(req) || resp.ok {
            return Ok(());
        }
        let (session_id, attachment_id, attachment_token) =
            match parse_pty_attachment_identity(req, &req.method) {
                Ok(identity) => identity,
                Err(_) => return Ok(()),
            };
        if attachment_token.trim().is_empty() {
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
            attachment_token: attachment_token.clone(),
            error: detail.clone(),
            message: detail,
            ..Default::default()
        });
        if req.method == "pty.write" {
            let code = resp.error_code();
            if let Some(hub) = &self.pty_hub {
                if code == "pty_input_queue_full" || code == "pty_input_seq_gap" {
                    hub.detach_by_id(&session_id, &attachment_id, &attachment_token);
                }
            }
        }
        result
    }

    fn handle_proxy_open(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        let host = match get_string_param(req.params(), "host") {
            Some(host) if !host.is_empty() => host,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.open requires host",
                )
            }
        };
        let port = match get_int_param(req.params(), "port") {
            Some(port) if port > 0 && port <= 65535 => port,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.open requires port in range 1-65535",
                )
            }
        };
        let mut timeout_ms: i64 = 10000;
        if let Some(parsed) = get_int_param(req.params(), "timeout_ms") {
            if parsed >= 0 {
                timeout_ms = parsed;
            }
        }
        let conn =
            match dial_tcp_timeout(&host, port as u16, Duration::from_millis(timeout_ms as u64)) {
                Ok(conn) => conn,
                Err(err) => return RpcResponse::err(req.id.clone(), "open_failed", err),
            };
        let _ = conn.set_nodelay(true);

        let mut state = self.state.lock().unwrap();
        let stream_id = format!("s-{}", state.next_stream_id);
        state.next_stream_id += 1;
        state.streams.insert(
            stream_id.clone(),
            StreamEntry {
                conn: StreamConn::new(Box::new(conn)),
                reader_started: false,
            },
        );
        drop(state);
        RpcResponse::ok(req.id.clone(), json!({"stream_id": stream_id}))
    }

    /// Register an already-open stream (tests inject fake connections).
    #[doc(hidden)]
    pub fn insert_stream_for_test(&self, stream_id: &str, io: Box<dyn ProxyStream>) {
        let mut state = self.state.lock().unwrap();
        state.streams.insert(
            stream_id.to_string(),
            StreamEntry {
                conn: StreamConn::new(io),
                reader_started: false,
            },
        );
    }

    fn handle_proxy_close(&self, req: &RpcRequest) -> RpcResponse {
        let stream_id = match get_string_param(req.params(), "stream_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.close requires stream_id",
                )
            }
        };
        let removed = self.state.lock().unwrap().streams.remove(&stream_id);
        if let Some(entry) = removed {
            entry.conn.close();
        }
        RpcResponse::ok(req.id.clone(), json!({"closed": true}))
    }

    fn handle_proxy_write(&self, req: &RpcRequest) -> RpcResponse {
        let stream_id = match get_string_param(req.params(), "stream_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.write requires stream_id",
                )
            }
        };
        let data_base64 = match get_string_param(req.params(), "data_base64") {
            Some(data) => data,
            None => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.write requires data_base64",
                )
            }
        };
        let payload = match base64_decode(&data_base64) {
            Ok(payload) => payload,
            Err(_) => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "data_base64 must be valid base64",
                )
            }
        };
        let conn = match self.get_stream(&stream_id) {
            Some(conn) => conn,
            None => return RpcResponse::err(req.id.clone(), "not_found", "stream not found"),
        };
        let mut timeout_ms: i64 = 8000;
        if let Some(parsed) = get_int_param(req.params(), "timeout_ms") {
            timeout_ms = parsed;
        }
        let mut deadline_set = false;
        if timeout_ms > 0 {
            if let Err(err) = conn
                .io
                .set_write_timeout(Some(Duration::from_millis(timeout_ms as u64)))
            {
                return RpcResponse::err(req.id.clone(), "stream_error", err.to_string());
            }
            deadline_set = true;
        }
        let response = write_all_progress(&*conn.io, &payload, req.id.clone());
        if deadline_set {
            let _ = conn.io.set_write_timeout(None);
        }
        response
    }

    fn handle_proxy_stream_subscribe(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        let stream_id = match get_string_param(req.params(), "stream_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "proxy.stream.subscribe requires stream_id",
                )
            }
        };
        let (already_subscribed, conn) = {
            let mut state = self.state.lock().unwrap();
            let entry = match state.streams.get_mut(&stream_id) {
                Some(entry) => entry,
                None => return RpcResponse::err(req.id.clone(), "not_found", "stream not found"),
            };
            let already = entry.reader_started;
            entry.reader_started = true;
            (already, Arc::clone(&entry.conn))
        };
        if !already_subscribed {
            let server = Arc::clone(self);
            let id = stream_id.clone();
            std::thread::Builder::new()
                .name("cmuxd-proxy-pump".to_string())
                .spawn(move || server.stream_pump(&id, &conn))
                .expect("spawn proxy pump thread");
        }
        RpcResponse::ok(
            req.id.clone(),
            json!({"subscribed": true, "already_subscribed": already_subscribed}),
        )
    }

    fn handle_session_open(&self, req: &RpcRequest) -> RpcResponse {
        let mut session_id = get_string_param(req.params(), "session_id").unwrap_or_default();
        let mut state = self.state.lock().unwrap();
        if session_id.is_empty() {
            session_id = format!("sess-{}", state.next_session_id);
            state.next_session_id += 1;
        }
        let session = state.sessions.entry(session_id.clone()).or_default();
        RpcResponse::ok(req.id.clone(), session_snapshot(&session_id, session))
    }

    fn handle_session_close(&self, req: &RpcRequest) -> RpcResponse {
        let session_id = match get_string_param(req.params(), "session_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "session.close requires session_id",
                )
            }
        };
        let existed = self
            .state
            .lock()
            .unwrap()
            .sessions
            .remove(&session_id)
            .is_some();
        if !existed {
            return RpcResponse::err(req.id.clone(), "not_found", "session not found");
        }
        RpcResponse::ok(
            req.id.clone(),
            json!({"session_id": session_id, "closed": true}),
        )
    }

    fn handle_session_attach(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, _, cols, rows) =
            match parse_session_attachment_params(req, "session.attach") {
                Ok(parsed) => parsed,
                Err(resp) => return resp,
            };
        let mut state = self.state.lock().unwrap();
        let session = match state.sessions.get_mut(&session_id) {
            Some(session) => session,
            None => return RpcResponse::err(req.id.clone(), "not_found", "session not found"),
        };
        session.attachments.insert(
            attachment_id,
            SessionAttachment {
                cols,
                rows,
                updated_at: now_rfc3339_nano(),
            },
        );
        recompute_session_size(session);
        RpcResponse::ok(req.id.clone(), session_snapshot(&session_id, session))
    }

    fn handle_session_resize(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, _, cols, rows) =
            match parse_session_attachment_params(req, "session.resize") {
                Ok(parsed) => parsed,
                Err(resp) => return resp,
            };
        let mut state = self.state.lock().unwrap();
        let session = match state.sessions.get_mut(&session_id) {
            Some(session) => session,
            None => return RpcResponse::err(req.id.clone(), "not_found", "session not found"),
        };
        if !session.attachments.contains_key(&attachment_id) {
            return RpcResponse::err(req.id.clone(), "not_found", "attachment not found");
        }
        session.attachments.insert(
            attachment_id,
            SessionAttachment {
                cols,
                rows,
                updated_at: now_rfc3339_nano(),
            },
        );
        recompute_session_size(session);
        RpcResponse::ok(req.id.clone(), session_snapshot(&session_id, session))
    }

    fn handle_session_detach(&self, req: &RpcRequest) -> RpcResponse {
        let session_id = match get_string_param(req.params(), "session_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "session.detach requires session_id",
                )
            }
        };
        let attachment_id = match get_string_param(req.params(), "attachment_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "session.detach requires attachment_id",
                )
            }
        };
        let mut state = self.state.lock().unwrap();
        let session = match state.sessions.get_mut(&session_id) {
            Some(session) => session,
            None => return RpcResponse::err(req.id.clone(), "not_found", "session not found"),
        };
        if session.attachments.remove(&attachment_id).is_none() {
            return RpcResponse::err(req.id.clone(), "not_found", "attachment not found");
        }
        recompute_session_size(session);
        RpcResponse::ok(req.id.clone(), session_snapshot(&session_id, session))
    }

    fn handle_session_status(&self, req: &RpcRequest) -> RpcResponse {
        let session_id = match get_string_param(req.params(), "session_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "session.status requires session_id",
                )
            }
        };
        let state = self.state.lock().unwrap();
        match state.sessions.get(&session_id) {
            Some(session) => {
                RpcResponse::ok(req.id.clone(), session_snapshot(&session_id, session))
            }
            None => RpcResponse::err(req.id.clone(), "not_found", "session not found"),
        }
    }

    fn handle_pty_attach(self: &Arc<Self>, req: &RpcRequest) -> RpcResponse {
        let session_id = match get_string_param(req.params(), "session_id") {
            Some(id) if !id.trim().is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "pty.attach requires session_id",
                )
            }
        };
        let attachment_id = get_string_param(req.params(), "attachment_id").unwrap_or_default();
        let attachment_token =
            get_string_param(req.params(), "client_attachment_token").unwrap_or_default();
        let attachment_token = attachment_token.trim().to_string();
        if attachment_token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.attach");
        }
        let cols = match get_int_param(req.params(), "cols") {
            Some(cols) if cols > 0 => cols,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "pty.attach requires cols > 0",
                )
            }
        };
        let rows = match get_int_param(req.params(), "rows") {
            Some(rows) if rows > 0 => rows,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "pty.attach requires rows > 0",
                )
            }
        };
        let command = get_string_param(req.params(), "command").unwrap_or_default();
        let require_existing = get_bool_param(req.params(), "require_existing").unwrap_or(false);
        let input_seq_ack = get_bool_param(req.params(), "input_seq_ack").unwrap_or(false);

        let hub = match &self.pty_hub {
            Some(hub) => hub,
            None => {
                return RpcResponse::err(req.id.clone(), "unavailable", "PTY hub is not available")
            }
        };
        let (attachment, session_done) = match hub.attach_rpc(
            &session_id,
            &attachment_id,
            cols,
            rows,
            &command,
            &attachment_token,
            require_existing,
            input_seq_ack,
        ) {
            Ok(result) => result,
            Err(err) => {
                return RpcResponse::err(
                    req.id.clone(),
                    pty_attach_error_code(require_existing),
                    err,
                )
            }
        };
        self.track_pty_attachment(&attachment);
        {
            let server = Arc::clone(self);
            let attachment = Arc::clone(&attachment);
            std::thread::Builder::new()
                .name("cmuxd-pty-attachment-pump".to_string())
                .spawn(move || server.pty_attachment_pump(&attachment, &session_done))
                .expect("spawn pty attachment pump");
        }
        RpcResponse::ok(
            req.id.clone(),
            json!({
                "session_id": session_id.trim(),
                "attachment_id": attachment.id,
                "attachment_token": attachment.client_token,
                "attached": true,
            }),
        )
    }

    pub fn handle_pty_write(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, attachment_token) =
            match parse_pty_attachment_identity(req, "pty.write") {
                Ok(identity) => identity,
                Err(resp) => return resp,
            };
        if attachment_token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.write");
        }
        let data_base64 = match get_string_param(req.params(), "data_base64") {
            Some(data) => data,
            None => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "pty.write requires data_base64",
                )
            }
        };
        let payload = match base64_decode(&data_base64) {
            Ok(payload) => payload,
            Err(_) => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "data_base64 must be valid base64",
                )
            }
        };
        let mut seq: u64 = 0;
        let mut has_seq = false;
        if req.params().map(|p| p.contains_key("seq")).unwrap_or(false) {
            match get_int_param(req.params(), "seq") {
                Some(parsed) if parsed >= 0 => {
                    seq = parsed as u64;
                    has_seq = true;
                }
                _ => {
                    return RpcResponse::err(
                        req.id.clone(),
                        "invalid_params",
                        "seq must be a non-negative integer",
                    )
                }
            }
        }
        let result = match &self.pty_hub {
            Some(hub) => hub.write_input_by_id_with_seq(
                &session_id,
                &attachment_id,
                &attachment_token,
                &payload,
                seq,
                has_seq,
            ),
            None => crate::pty_hub::InputWriteResult {
                status: InputWriteStatus::NotFound,
                got: 0,
                want: 0,
            },
        };
        match result.status {
            InputWriteStatus::SeqGap => RpcResponse::err(
                req.id.clone(),
                "pty_input_seq_gap",
                format!(
                    "PTY input sequence gap: got {}, want {}",
                    result.got, result.want
                ),
            ),
            InputWriteStatus::QueueFull => RpcResponse::err(
                req.id.clone(),
                "pty_input_queue_full",
                "PTY input queue is full",
            ),
            InputWriteStatus::NotFound => {
                RpcResponse::err(req.id.clone(), "not_found", "PTY attachment not found")
            }
            InputWriteStatus::Ok => {
                RpcResponse::ok(req.id.clone(), json!({"written": payload.len()}))
            }
        }
    }

    fn handle_pty_resize(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, attachment_token, cols, rows) =
            match parse_session_attachment_params(req, "pty.resize") {
                Ok(parsed) => parsed,
                Err(resp) => return resp,
            };
        if attachment_token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.resize");
        }
        let resized = self
            .pty_hub
            .as_ref()
            .map(|hub| hub.resize_by_id(&session_id, &attachment_id, &attachment_token, cols, rows))
            .unwrap_or(false);
        if !resized {
            return RpcResponse::err(req.id.clone(), "not_found", "PTY attachment not found");
        }
        RpcResponse::ok(req.id.clone(), json!({"resized": true}))
    }

    fn handle_pty_detach(&self, req: &RpcRequest) -> RpcResponse {
        let (session_id, attachment_id, attachment_token) =
            match parse_pty_attachment_identity(req, "pty.detach") {
                Ok(identity) => identity,
                Err(resp) => return resp,
            };
        if attachment_token.is_empty() {
            return missing_pty_attachment_token_response(req, "pty.detach");
        }
        let detached = self
            .pty_hub
            .as_ref()
            .map(|hub| hub.detach_by_id(&session_id, &attachment_id, &attachment_token))
            .unwrap_or(false);
        if !detached {
            return RpcResponse::err(req.id.clone(), "not_found", "PTY attachment not found");
        }
        RpcResponse::ok(req.id.clone(), json!({"detached": true}))
    }

    fn handle_pty_close(&self, req: &RpcRequest) -> RpcResponse {
        let session_id = match get_string_param(req.params(), "session_id") {
            Some(id) if !id.trim().is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "pty.close requires session_id",
                )
            }
        };
        let closed = self
            .pty_hub
            .as_ref()
            .map(|hub| hub.close_session_by_id(&session_id))
            .unwrap_or(false);
        if !closed {
            return RpcResponse::err(req.id.clone(), "not_found", "PTY session not found");
        }
        RpcResponse::ok(
            req.id.clone(),
            json!({"session_id": session_id.trim(), "closed": true}),
        )
    }

    fn handle_pty_list(&self, req: &RpcRequest) -> RpcResponse {
        let sessions = match &self.pty_hub {
            Some(hub) => hub.session_snapshots(),
            None => Vec::new(),
        };
        RpcResponse::ok(req.id.clone(), json!({"sessions": sessions}))
    }

    pub fn pty_attachment_pump(
        self: &Arc<Self>,
        attachment: &Arc<PtyAttachment>,
        session_done: &DoneSignal,
    ) {
        let result = self.pty_attachment_pump_inner(attachment, session_done);
        self.untrack_pty_attachment(attachment);
        if let Err(()) = result {
            match &self.pty_hub {
                Some(hub) => hub.drop_attachment(attachment),
                None => attachment.close_now(),
            }
        }
    }

    fn pty_attachment_pump_inner(
        &self,
        attachment: &Arc<PtyAttachment>,
        session_done: &DoneSignal,
    ) -> Result<(), ()> {
        let frames = attachment.frames();
        loop {
            if attachment.is_cancelled() {
                let _ = self
                    .frame_writer
                    .write_event(&rpc_pty_exit_event(attachment));
                return Ok(());
            }
            if session_done.is_closed() {
                while let Ok(frame) = frames.try_recv() {
                    if self
                        .frame_writer
                        .write_event(&rpc_pty_event_for_frame(attachment, &frame))
                        .is_err()
                    {
                        return Err(());
                    }
                }
                let _ = self
                    .frame_writer
                    .write_event(&rpc_pty_exit_event(attachment));
                return Ok(());
            }
            let frame = flume::Selector::new()
                .recv(attachment.cancel_token().receiver(), |_| None)
                .recv(session_done.receiver(), |_| None)
                .recv(frames, |frame| frame.ok())
                .wait();
            if let Some(frame) = frame {
                if self
                    .frame_writer
                    .write_event(&rpc_pty_event_for_frame(attachment, &frame))
                    .is_err()
                {
                    return Err(());
                }
            }
        }
    }

    pub fn track_pty_attachment(&self, attachment: &Arc<PtyAttachment>) {
        let mut state = self.state.lock().unwrap();
        state
            .pty_attachments
            .insert(rpc_pty_attachment_key(attachment), Arc::clone(attachment));
    }

    pub fn untrack_pty_attachment(&self, attachment: &Arc<PtyAttachment>) {
        let mut state = self.state.lock().unwrap();
        let key = rpc_pty_attachment_key(attachment);
        if let Some(current) = state.pty_attachments.get(&key) {
            if Arc::ptr_eq(current, attachment) {
                state.pty_attachments.remove(&key);
            }
        }
    }

    #[doc(hidden)]
    pub fn tracked_pty_attachment(&self, key: &str) -> Option<Arc<PtyAttachment>> {
        self.state.lock().unwrap().pty_attachments.get(key).cloned()
    }

    fn get_stream(&self, stream_id: &str) -> Option<Arc<StreamConn>> {
        self.state
            .lock()
            .unwrap()
            .streams
            .get(stream_id)
            .map(|entry| Arc::clone(&entry.conn))
    }

    fn drop_stream(&self, stream_id: &str) {
        let removed = self.state.lock().unwrap().streams.remove(stream_id);
        if let Some(entry) = removed {
            entry.conn.close();
        }
    }

    pub fn close_all(&self) {
        let (streams, attachments) = {
            let mut state = self.state.lock().unwrap();
            let streams: Vec<Arc<StreamConn>> =
                state.streams.drain().map(|(_, entry)| entry.conn).collect();
            state.sessions.clear();
            let attachments: Vec<Arc<PtyAttachment>> =
                state.pty_attachments.drain().map(|(_, a)| a).collect();
            (streams, attachments)
        };
        for conn in streams {
            conn.close();
        }
        for attachment in attachments {
            match &self.pty_hub {
                Some(hub) => hub.drop_attachment(&attachment),
                None => attachment.close_now(),
            }
        }
        if self.owns_pty_hub {
            if let Some(hub) = &self.pty_hub {
                hub.close_all();
            }
        }
    }

    fn stream_pump(&self, stream_id: &str, conn: &Arc<StreamConn>) {
        let mut buffer = vec![0u8; 32768];
        loop {
            let read = conn.io.read(&mut buffer);
            let n = match &read {
                Ok(n) => *n,
                Err(_) => 0,
            };
            if n > 0 {
                let _ = self.frame_writer.write_event(&RpcEvent {
                    event: "proxy.stream.data".to_string(),
                    stream_id: stream_id.to_string(),
                    data_base64: base64_encode(&buffer[..n]),
                    ..Default::default()
                });
            }
            match read {
                Ok(n) if n > 0 => continue,
                Ok(_) => {
                    if conn.is_closed() {
                        // Closed locally through proxy.close: no event, mirrors
                        // Go's net.ErrClosed suppression.
                    } else {
                        let _ = self.frame_writer.write_event(&RpcEvent {
                            event: "proxy.stream.eof".to_string(),
                            stream_id: stream_id.to_string(),
                            ..Default::default()
                        });
                    }
                }
                Err(err) => {
                    if !conn.is_closed() {
                        let _ = self.frame_writer.write_event(&RpcEvent {
                            event: "proxy.stream.error".to_string(),
                            stream_id: stream_id.to_string(),
                            error: err.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }
            self.drop_stream(stream_id);
            return;
        }
    }

    pub fn log_stderr(&self, text: &str) {
        self.stderr.write_str(text);
    }
}

fn write_all_progress(io: &dyn ProxyStream, payload: &[u8], id: Option<Value>) -> RpcResponse {
    let mut total = 0;
    while total < payload.len() {
        match io.write(&payload[total..]) {
            Ok(0) => return RpcResponse::err(id, "stream_error", "write made no progress"),
            Ok(n) => total += n,
            Err(err) => return RpcResponse::err(id, "stream_error", err.to_string()),
        }
    }
    RpcResponse::ok(id, json!({"written": total}))
}

pub fn dial_tcp_timeout(host: &str, port: u16, timeout: Duration) -> Result<TcpStream, String> {
    let target = format!("{host}:{port}");
    let addrs: Vec<std::net::SocketAddr> = match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(err) => return Err(format!("dial tcp {target}: {err}")),
    };
    if addrs.is_empty() {
        return Err(format!("dial tcp {target}: no such host"));
    }
    let mut last_err = String::new();
    for addr in addrs {
        let attempt = if timeout.is_zero() {
            TcpStream::connect(addr)
        } else {
            TcpStream::connect_timeout(&addr, timeout)
        };
        match attempt {
            Ok(conn) => return Ok(conn),
            Err(err) => last_err = format!("dial tcp {addr}: connect: {}", go_io_error(&err)),
        }
    }
    Err(last_err)
}

pub fn rpc_request_expects_response(req: &RpcRequest) -> bool {
    // Only selected PTY attachment operations use JSON-RPC notification
    // semantics; all other id-less requests still get a response.
    !rpc_request_is_pty_attachment_notification(req)
}

pub fn rpc_request_is_pty_attachment_notification(req: &RpcRequest) -> bool {
    !req.has_id && (req.method == "pty.write" || req.method == "pty.resize")
}

pub fn pty_attach_error_code(require_existing: bool) -> &'static str {
    if require_existing {
        "pty_session_not_found"
    } else {
        "pty_start_failed"
    }
}

pub fn rpc_pty_attachment_key(attachment: &PtyAttachment) -> String {
    let kind = match attachment.session_key.kind {
        SessionKind::Persistent => 0,
        SessionKind::Anonymous => 1,
    };
    format!(
        "{}:{}:{}:{}:{}",
        kind,
        attachment.session_key.session_id,
        attachment.session_key.anonymous_id,
        attachment.id,
        attachment.client_token
    )
}

pub fn missing_pty_attachment_token_response(req: &RpcRequest, method: &str) -> RpcResponse {
    RpcResponse::err(
        req.id.clone(),
        "invalid_params",
        format!("{method} requires client_attachment_token"),
    )
}

type SessionAttachmentParams = (String, String, String, i64, i64);

pub fn parse_session_attachment_params(
    req: &RpcRequest,
    method: &str,
) -> Result<SessionAttachmentParams, RpcResponse> {
    let (session_id, attachment_id, attachment_token) = parse_pty_attachment_identity(req, method)?;
    let cols = match get_int_param(req.params(), "cols") {
        Some(cols) if cols > 0 => cols,
        _ => {
            return Err(RpcResponse::err(
                req.id.clone(),
                "invalid_params",
                format!("{method} requires cols > 0"),
            ))
        }
    };
    let rows = match get_int_param(req.params(), "rows") {
        Some(rows) if rows > 0 => rows,
        _ => {
            return Err(RpcResponse::err(
                req.id.clone(),
                "invalid_params",
                format!("{method} requires rows > 0"),
            ))
        }
    };
    Ok((session_id, attachment_id, attachment_token, cols, rows))
}

pub fn parse_pty_attachment_identity(
    req: &RpcRequest,
    method: &str,
) -> Result<(String, String, String), RpcResponse> {
    let session_id = match get_string_param(req.params(), "session_id") {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return Err(RpcResponse::err(
                req.id.clone(),
                "invalid_params",
                format!("{method} requires session_id"),
            ))
        }
    };
    let attachment_id = match get_string_param(req.params(), "attachment_id") {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return Err(RpcResponse::err(
                req.id.clone(),
                "invalid_params",
                format!("{method} requires attachment_id"),
            ))
        }
    };
    let attachment_token =
        get_string_param(req.params(), "client_attachment_token").unwrap_or_default();
    Ok((
        session_id.trim().to_string(),
        attachment_id.trim().to_string(),
        attachment_token.trim().to_string(),
    ))
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
        .iter()
        .map(|id| {
            let attachment = &session.attachments[*id];
            json!({
                "attachment_id": id,
                "cols": attachment.cols,
                "rows": attachment.rows,
                "updated_at": attachment.updated_at,
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

pub fn rpc_pty_event_for_frame(attachment: &PtyAttachment, frame: &OutgoingFrame) -> RpcEvent {
    if frame.input_ack {
        return RpcEvent {
            event: "pty.input_ack".to_string(),
            session_id: attachment.session_key.session_id.clone(),
            attachment_id: attachment.id.clone(),
            attachment_token: attachment.client_token.clone(),
            seq: attachment.consume_input_ack(),
            ..Default::default()
        };
    }
    let mut event = RpcEvent {
        event: "pty.data".to_string(),
        session_id: attachment.session_key.session_id.clone(),
        attachment_id: attachment.id.clone(),
        attachment_token: attachment.client_token.clone(),
        ..Default::default()
    };
    if frame.kind == FrameKind::Text {
        if let Ok(ws_event) = serde_json::from_slice::<WsPtyEventFrame>(&frame.payload) {
            if !ws_event.kind.trim().is_empty() {
                event.event = format!("pty.{}", ws_event.kind.trim());
                event.message = ws_event.message;
                if !ws_event.session_id.trim().is_empty() {
                    event.session_id = ws_event.session_id.trim().to_string();
                }
                if !ws_event.attachment_id.trim().is_empty() {
                    event.attachment_id = ws_event.attachment_id.trim().to_string();
                }
                event.attachment_token = attachment.client_token.clone();
                return event;
            }
        }
        event.event = "pty.message".to_string();
        event.message = String::from_utf8_lossy(&frame.payload).into_owned();
        return event;
    }
    event.data_base64 = base64_encode(&frame.payload);
    event
}

pub fn rpc_pty_exit_event(attachment: &PtyAttachment) -> RpcEvent {
    RpcEvent {
        event: "pty.exit".to_string(),
        session_id: attachment.session_key.session_id.clone(),
        attachment_id: attachment.id.clone(),
        attachment_token: attachment.client_token.clone(),
        ..Default::default()
    }
}

/// Serve newline-delimited JSON-RPC from `reader` until EOF. `request_shutdown`
/// enables the persistent daemon's `daemon.shutdown` control method.
pub fn run_rpc_server_with_reader<R: BufRead>(
    reader: &mut R,
    writer: Arc<StdioFrameWriter>,
    pty_hub: Arc<PtyHub>,
    owns_pty_hub: bool,
    request_shutdown: Option<&(dyn Fn() + Send + Sync)>,
    stderr: LogSink,
) -> io::Result<()> {
    let server = RpcServer::builder()
        .pty_hub(pty_hub, owns_pty_hub)
        .frame_writer(writer.clone())
        .stderr(stderr)
        .build();
    let result = serve_loop(reader, &writer, &server, request_shutdown);
    server.close_all();
    let _ = writer.flush();
    result
}

fn serve_loop<R: BufRead>(
    reader: &mut R,
    writer: &Arc<StdioFrameWriter>,
    server: &Arc<RpcServer>,
    request_shutdown: Option<&(dyn Fn() + Send + Sync)>,
) -> io::Result<()> {
    loop {
        let (line, oversized) = match read_rpc_frame(reader, MAX_RPC_FRAME_BYTES) {
            Ok(frame) => frame,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        };
        if oversized {
            writer.write_response(&RpcResponse::err(
                None,
                "invalid_request",
                "request frame exceeds maximum size",
            ))?;
            continue;
        }
        let line = trim_frame(&line);
        if line.is_empty() {
            continue;
        }
        let req = match RpcRequest::parse(line) {
            Ok(req) => req,
            Err(_) => {
                writer.write_response(&RpcResponse::err(
                    None,
                    "invalid_request",
                    "invalid JSON request",
                ))?;
                continue;
            }
        };
        if req.method == PERSISTENT_DAEMON_SHUTDOWN_METHOD {
            if let Some(shutdown) = request_shutdown {
                writer.write_response(&RpcResponse::ok(
                    req.id.clone(),
                    json!({"shutting_down": true}),
                ))?;
                shutdown();
                return Ok(());
            }
        }
        server.handle_request_and_write_response(&req)?;
    }
}

pub fn trim_frame(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    if end > 0 && line[end - 1] == b'\n' {
        end -= 1;
    }
    if end > 0 && line[end - 1] == b'\r' {
        end -= 1;
    }
    &line[..end]
}

pub fn run_rpc_server(
    stdin: Box<dyn Read + Send>,
    stdout: Box<dyn Write + Send>,
    pty_hub: Arc<PtyHub>,
    owns_pty_hub: bool,
    stderr: LogSink,
) -> io::Result<()> {
    let writer = StdioFrameWriter::new(stdout);
    let mut reader = io::BufReader::with_capacity(64 * 1024, stdin);
    run_rpc_server_with_reader(&mut reader, writer, pty_hub, owns_pty_hub, None, stderr)
}

pub fn run_stdio_server(
    stdin: Box<dyn Read + Send>,
    stdout: Box<dyn Write + Send>,
) -> io::Result<()> {
    let hub = PtyHub::new(crate::pty_hub::PtyHubConfig::default(), None);
    run_rpc_server(stdin, stdout, hub, true, LogSink::discard())
}

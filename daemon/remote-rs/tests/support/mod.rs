#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use serde_json::{json, Map, Value};
use sha2::Sha256;

use cmuxd_remote::persistent::{
    persistent_daemon_fixed_token_verifier, serve_persistent_daemon_with_verifier,
    serve_persistent_daemon_with_verifier_config, PersistentServerConfig, TokenVerifier,
    PERSISTENT_DAEMON_AUTH_METHOD,
};
use cmuxd_remote::rpc::RpcRequest;
use cmuxd_remote::util::{LogSink, SharedBuffer, StopSignal};

// --- process-global environment serialization ---

fn env_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Holds the global environment lock and restores every variable it touched.
pub struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    pub fn new() -> Self {
        let lock = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self {
            _lock: lock,
            saved: Vec::new(),
        }
    }

    fn remember(&mut self, key: &str) {
        if !self.saved.iter().any(|(k, _)| k == key) {
            self.saved.push((key.to_string(), std::env::var(key).ok()));
        }
    }

    /// Remember a variable's current value so it is restored on drop even if
    /// code under test mutates it directly.
    pub fn track(&mut self, key: &str) {
        self.remember(key);
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.remember(key);
        std::env::set_var(key, value);
    }

    pub fn remove(&mut self, key: &str) {
        self.remember(key);
        std::env::remove_var(key);
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..) {
            match value {
                Some(value) => std::env::set_var(&key, value),
                None => std::env::remove_var(&key),
            }
        }
    }
}

pub fn temp_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create temp dir")
}

/// Short paths under /tmp keep Unix socket paths under the 104-byte limit.
pub fn short_temp_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .expect("create short temp dir")
}

pub fn path_str(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

pub fn run_daemon(args: &[&str], stdin: &str) -> (i32, String, String) {
    let out = SharedBuffer::new();
    let err = SharedBuffer::new();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let code = cmuxd_remote::daemon::run(
        &args,
        Box::new(Cursor::new(stdin.to_string())),
        Box::new(out.clone()),
        Box::new(err.clone()),
    );
    (code, out.to_string_lossy(), err.to_string_lossy())
}

pub fn lines(output: &str) -> Vec<String> {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed.split('\n').map(str::to_string).collect()
}

pub fn parse_json_map(text: &str) -> Map<String, Value> {
    serde_json::from_str::<Value>(text)
        .unwrap_or_else(|err| panic!("decode {text:?}: {err}"))
        .as_object()
        .cloned()
        .unwrap_or_else(|| panic!("expected JSON object: {text:?}"))
}

pub fn map_ok(map: &Map<String, Value>) -> bool {
    map.get("ok").and_then(|v| v.as_bool()).unwrap_or(false)
}

pub fn map_str<'a>(map: &'a Map<String, Value>, key: &str) -> &'a str {
    map.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

pub fn error_code(map: &Map<String, Value>) -> String {
    map.get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string()
}

pub fn as_int(value: Option<&Value>, field: &str) -> i64 {
    match value {
        Some(Value::Number(n)) => {
            if let Some(v) = n.as_i64() {
                v
            } else if let Some(f) = n.as_f64() {
                assert_eq!(f.trunc(), f, "{field} should be integer-valued, got {f}");
                f as i64
            } else {
                panic!("{field} has unexpected number {n}")
            }
        }
        other => panic!("{field} has unexpected value {other:?}"),
    }
}

pub fn base64_encode(data: &[u8]) -> String {
    cmuxd_remote::rpc::base64_encode(data)
}

pub fn base64_decode(data: &str) -> Vec<u8> {
    cmuxd_remote::rpc::base64_decode(data).expect("valid base64")
}

// --- RPC event capture helpers (Go's notifyingBuffer + waitForRPCEvent) ---

pub fn rpc_event_lines(buffer: &SharedBuffer) -> Vec<String> {
    lines(&buffer.to_string_lossy())
}

pub fn wait_for_rpc_event(
    buffer: &SharedBuffer,
    start_line: usize,
    matches: impl Fn(&Map<String, Value>) -> bool,
) -> Map<String, Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let all = rpc_event_lines(buffer);
        for line in all.iter().skip(start_line.min(all.len())) {
            if let Ok(Value::Object(event)) = serde_json::from_str::<Value>(line) {
                if matches(&event) {
                    return event;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for matching RPC event after line {start_line}: {:?}",
                buffer.to_string_lossy()
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// --- persistent daemon test harness ---

pub struct PersistentDaemonHandle {
    pub socket_path: String,
    pub stop: Arc<StopSignal>,
    join: Option<JoinHandle<io::Result<()>>>,
    _dir: tempfile::TempDir,
}

impl PersistentDaemonHandle {
    /// Stop the daemon (like closing the Go listener) and assert it exits cleanly.
    pub fn stop(&mut self) {
        self.stop.stop();
        self.wait_exit(Duration::from_secs(2));
    }

    pub fn wait_exit(&mut self, timeout: Duration) {
        let handle = match self.join.take() {
            Some(handle) => handle,
            None => return,
        };
        let result = join_with_timeout(handle, timeout).expect("persistent daemon did not stop");
        result.expect("persistent daemon exited with error");
    }

    pub fn exited(&mut self, timeout: Duration) -> bool {
        match self.join.take() {
            Some(handle) => match join_with_timeout(handle, timeout) {
                Some(result) => {
                    result.expect("persistent daemon exited with error");
                    true
                }
                None => false,
            },
            None => true,
        }
    }
}

impl Drop for PersistentDaemonHandle {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(handle) = self.join.take() {
            let _ = join_with_timeout(handle, Duration::from_secs(2));
        }
    }
}

pub fn join_with_timeout<T: Send + 'static>(handle: JoinHandle<T>, timeout: Duration) -> Option<T> {
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let _ = tx.send(handle.join());
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(value)) => Some(value),
        Ok(Err(_)) => panic!("thread panicked"),
        Err(_) => None,
    }
}

pub fn start_persistent_daemon_for_test(token: &str) -> PersistentDaemonHandle {
    start_persistent_daemon_with_verifier_for_test(persistent_daemon_fixed_token_verifier(token))
}

pub fn start_persistent_daemon_with_verifier_for_test(
    verifier: TokenVerifier,
) -> PersistentDaemonHandle {
    start_persistent_daemon_with_config(verifier, None)
}

pub fn start_persistent_daemon_with_config(
    verifier: TokenVerifier,
    config: Option<PersistentServerConfig>,
) -> PersistentDaemonHandle {
    let dir = short_temp_dir("cmuxd-remote-test-");
    let socket_path = path_str(&dir, "rpc.sock");
    let listener = UnixListener::bind(&socket_path).expect("listen unix");
    let stop = StopSignal::new().expect("stop signal");
    let stop_ref = Arc::clone(&stop);
    let join = std::thread::spawn(move || match config {
        Some(config) => serve_persistent_daemon_with_verifier_config(
            listener,
            verifier,
            LogSink::discard(),
            config,
            stop_ref,
        ),
        None => {
            serve_persistent_daemon_with_verifier(listener, verifier, LogSink::discard(), stop_ref)
        }
    });
    PersistentDaemonHandle {
        socket_path,
        stop,
        join: Some(join),
        _dir: dir,
    }
}

pub struct PersistentClient {
    conn: UnixStream,
    reader: BufReader<UnixStream>,
    pending: Vec<Map<String, Value>>,
}

impl PersistentClient {
    pub fn open(socket_path: &str, token: &str) -> Self {
        let conn = UnixStream::connect(socket_path).expect("dial persistent daemon");
        let reader = BufReader::new(conn.try_clone().expect("clone"));
        let mut client = Self {
            conn,
            reader,
            pending: Vec::new(),
        };
        let mut params = Map::new();
        params.insert("token".to_string(), Value::String(token.to_string()));
        client.write_frame(
            &RpcRequest::new("auth", PERSISTENT_DAEMON_AUTH_METHOD, Some(params)).to_json(),
        );
        let frame = client.read_frame();
        assert!(map_ok(&frame), "persistent daemon auth failed: {frame:?}");
        client
    }

    pub fn write_frame(&mut self, text: &str) {
        self.conn
            .write_all(format!("{text}\n").as_bytes())
            .expect("write frame");
        self.conn.flush().expect("flush frame");
    }

    pub fn write_json(&mut self, value: &Value) {
        self.write_frame(&cmuxd_remote::util::go_json(value));
    }

    pub fn read_frame(&mut self) -> Map<String, Value> {
        let _ = self.conn.set_read_timeout(Some(Duration::from_secs(5)));
        let mut line = String::new();
        let n = self
            .reader
            .read_line(&mut line)
            .expect("read persistent daemon frame");
        assert!(n > 0, "persistent daemon closed the connection");
        let _ = self.conn.set_read_timeout(None);
        parse_json_map(line.trim())
    }

    pub fn call(&mut self, req: &RpcRequest) -> Map<String, Value> {
        self.write_frame(&req.to_json());
        loop {
            let frame = self.read_frame();
            if frame.contains_key("event") {
                self.pending.push(frame);
                continue;
            }
            return frame;
        }
    }

    pub fn read_event(
        &mut self,
        matches: impl Fn(&Map<String, Value>) -> bool,
    ) -> Map<String, Value> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last: Option<Map<String, Value>> = None;
        while Instant::now() < deadline {
            if !self.pending.is_empty() {
                let frame = self.pending.remove(0);
                last = Some(frame.clone());
                if frame.contains_key("event") && matches(&frame) {
                    return frame;
                }
                continue;
            }
            let frame = self.read_frame();
            last = Some(frame.clone());
            if frame.contains_key("event") && matches(&frame) {
                return frame;
            }
        }
        panic!("timed out waiting for persistent daemon event; last={last:?}");
    }

    pub fn close(self) {
        let _ = self.conn.shutdown(std::net::Shutdown::Both);
    }
}

pub fn rpc_request(id: impl Into<Value>, method: &str, params: Value) -> RpcRequest {
    let map = params.as_object().cloned().unwrap_or_default();
    RpcRequest::new(id, method, Some(map))
}

// --- mock cmux sockets (Go cli_test.go helpers) ---

pub struct MockSocket {
    pub path: String,
    pub _dir: tempfile::TempDir,
}

pub fn make_short_unix_socket_dir() -> (tempfile::TempDir, String) {
    let dir = short_temp_dir("cmuxd-");
    let path = path_str(&dir, "cmux.sock");
    (dir, path)
}

fn spawn_accept_loop<F>(listener: UnixListener, handler: F)
where
    F: Fn(UnixStream) + Send + Sync + 'static,
{
    std::thread::spawn(move || {
        let handler = Arc::new(handler);
        for conn in listener.incoming() {
            match conn {
                Ok(conn) => {
                    let handler = Arc::clone(&handler);
                    std::thread::spawn(move || handler(conn));
                }
                Err(_) => return,
            }
        }
    });
}

fn read_request_line(conn: &mut UnixStream) -> Option<Map<String, Value>> {
    let _ = conn.set_read_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(conn.try_clone().ok()?);
    let mut line = String::new();
    let n = reader.read_line(&mut line).ok()?;
    if n == 0 {
        return None;
    }
    serde_json::from_str::<Value>(&line)
        .ok()?
        .as_object()
        .cloned()
}

/// A Unix socket that answers every connection with a canned response line.
pub fn start_mock_socket(response: &str) -> MockSocket {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    let response = response.to_string();
    spawn_accept_loop(listener, move |mut conn| {
        let mut buf = [0u8; 4096];
        let _ = conn.read(&mut buf);
        let _ = conn.write_all(format!("{response}\n").as_bytes());
    });
    MockSocket { path, _dir: dir }
}

/// Echoes the received request's method and params back as a successful
/// JSON-RPC response.
pub fn start_mock_v2_socket() -> MockSocket {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    spawn_accept_loop(listener, move |mut conn| {
        let mut buf = vec![0u8; 4096];
        let n = conn.read(&mut buf).unwrap_or(0);
        if n > 0 {
            match serde_json::from_slice::<Value>(&buf[..n]) {
                Ok(req) => {
                    let resp = json!({
                        "id": req.get("id").cloned().unwrap_or(Value::Null),
                        "ok": true,
                        "result": {"method": req.get("method").cloned(), "params": req.get("params").cloned()},
                    });
                    let _ = conn
                        .write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
                }
                Err(_) => {
                    let _ = conn.write_all(
                        b"{\"ok\":false,\"error\":{\"code\":\"parse\",\"message\":\"bad json\"}}\n",
                    );
                }
            }
        }
    });
    MockSocket { path, _dir: dir }
}

pub fn start_mock_v2_socket_with_request_capture(
) -> (MockSocket, flume::Receiver<Map<String, Value>>) {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    let (tx, rx) = flume::bounded(8);
    spawn_accept_loop(listener, move |mut conn| {
        let mut buf = vec![0u8; 4096];
        let n = conn.read(&mut buf).unwrap_or(0);
        if n == 0 {
            return;
        }
        match serde_json::from_slice::<Value>(&buf[..n]) {
            Ok(req) => {
                let map = req.as_object().cloned().unwrap_or_default();
                let _ = tx.send(map);
                let resp = json!({
                    "id": req.get("id").cloned().unwrap_or(Value::Null),
                    "ok": true,
                    "result": {"method": req.get("method").cloned(), "params": req.get("params").cloned()},
                });
                let _ =
                    conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
            }
            Err(_) => {
                let _ = conn.write_all(
                    b"{\"ok\":false,\"error\":{\"code\":\"parse\",\"message\":\"bad json\"}}\n",
                );
            }
        }
    });
    (MockSocket { path, _dir: dir }, rx)
}

pub fn receive_request(rx: &flume::Receiver<Map<String, Value>>) -> Map<String, Value> {
    rx.recv_timeout(Duration::from_secs(2))
        .expect("timed out waiting for request")
}

pub fn params_of(req: &Map<String, Value>) -> Map<String, Value> {
    req.get("params")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default()
}

pub fn start_mock_v2_tcp_socket_with_result(result: Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen tcp");
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let result = result.clone();
            std::thread::spawn(move || {
                let mut buf = vec![0u8; 4096];
                let n = conn.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    return;
                }
                match serde_json::from_slice::<Value>(&buf[..n]) {
                    Ok(req) => {
                        let resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true, "result": result});
                        let _ = conn.write_all(
                            format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes(),
                        );
                    }
                    Err(_) => {
                        let _ = conn.write_all(b"{\"ok\":false,\"error\":{\"code\":\"parse\",\"message\":\"bad json\"}}\n");
                    }
                }
            });
        }
    });
    addr
}

pub fn start_mock_tcp_socket(response: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen tcp");
    let addr = listener.local_addr().unwrap().to_string();
    let response = response.to_string();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let mut buf = [0u8; 4096];
            let _ = conn.read(&mut buf);
            let _ = conn.write_all(format!("{response}\n").as_bytes());
        }
    });
    addr
}

pub fn start_mock_authenticated_tcp_socket(
    relay_id: &str,
    relay_token: &str,
    response: &str,
) -> String {
    let relay_token_bytes = hex::decode(relay_token).expect("hex token");
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen tcp");
    let addr = listener.local_addr().unwrap().to_string();
    let relay_id = relay_id.to_string();
    let response = response.to_string();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let relay_id = relay_id.clone();
            let response = response.clone();
            let relay_token_bytes = relay_token_bytes.clone();
            std::thread::spawn(move || {
                let nonce = "testnonce";
                let challenge = json!({"protocol": "cmux-relay-auth", "version": 1, "relay_id": relay_id, "nonce": nonce});
                let _ = conn
                    .write_all(format!("{}\n", cmuxd_remote::util::go_json(&challenge)).as_bytes());
                let mut reader = BufReader::new(conn.try_clone().unwrap());
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    return;
                }
                let auth_resp: Value = match serde_json::from_str(&line) {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = conn.write_all(b"{\"ok\":false}\n");
                        return;
                    }
                };
                let mac_hex = auth_resp.get("mac").and_then(|v| v.as_str()).unwrap_or("");
                let received = match hex::decode(mac_hex) {
                    Ok(received) => received,
                    Err(_) => {
                        let _ = conn.write_all(b"{\"ok\":false}\n");
                        return;
                    }
                };
                let mut mac = Hmac::<Sha256>::new_from_slice(&relay_token_bytes).unwrap();
                mac.update(format!("relay_id={relay_id}\nnonce={nonce}\nversion=1").as_bytes());
                if mac.verify_slice(&received).is_err() {
                    let _ = conn.write_all(b"{\"ok\":false}\n");
                    return;
                }
                let _ = conn.write_all(b"{\"ok\":true}\n");
                let mut buf = [0u8; 4096];
                let n = conn.read(&mut buf).unwrap_or(0);
                let _ = conn.write_all(response.as_bytes());
                if n > 0 && !response.ends_with('\n') {
                    let _ = conn.write_all(b"\n");
                }
            });
        }
    });
    addr
}

/// A mock cmux socket that models one workspace with a leader pane/surface and
/// a lazily created split (Go's `startMockTmuxCompatSocket`).
pub fn start_mock_tmux_compat_socket() -> MockSocket {
    let (dir, path) = make_short_unix_socket_dir();
    let cwd = dir.path().to_string_lossy().into_owned();
    let listener = UnixListener::bind(&path).expect("listen");
    let split_created = Arc::new(Mutex::new(false));
    spawn_accept_loop(listener, move |mut conn| {
        let Some(req) = read_request_line(&mut conn) else {
            return;
        };
        let method = map_str(&req, "method").to_string();
        let params = params_of(&req);
        let mut resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true});
        let created = *split_created.lock().unwrap();
        match method.as_str() {
            "system.identify" => {
                resp["result"] = json!({"focused": {
                    "workspace_id": "11111111-1111-4111-8111-111111111111",
                    "workspace_ref": "workspace:1",
                    "pane_id": "pane:1",
                    "pane_ref": "pane:1",
                    "surface_ref": "surface:1",
                }});
            }
            "workspace.list" => {
                resp["result"] = json!({"workspaces": [{
                    "id": "11111111-1111-4111-8111-111111111111",
                    "ref": "workspace:1",
                    "index": 1,
                    "title": "demo",
                    "active": true,
                    "current_directory": cwd,
                }]});
            }
            "surface.list" => {
                let mut surfaces = vec![json!({
                    "id": "44444444-4444-4444-8444-444444444444",
                    "ref": "surface:1",
                    "focused": "1",
                    "selected_in_pane": "1",
                    "pane_id": "33333333-3333-4333-8333-333333333333",
                    "pane_ref": "pane:1",
                    "title": "leader",
                    "requested_working_directory": cwd,
                })];
                if created {
                    surfaces.push(json!({
                        "id": "77777777-7777-4777-8777-777777777777",
                        "ref": "surface:2",
                        "focused": "0",
                        "selected_in_pane": "1",
                        "pane_id": "66666666-6666-4666-8666-666666666666",
                        "pane_ref": "pane:2",
                        "title": "teammate",
                        "requested_working_directory": cwd,
                    }));
                }
                resp["result"] = json!({"surfaces": surfaces});
            }
            "surface.current" => {
                resp["result"] = json!({
                    "workspace_id": "11111111-1111-4111-8111-111111111111",
                    "workspace_ref": "workspace:1",
                    "pane_id": "33333333-3333-4333-8333-333333333333",
                    "pane_ref": "pane:1",
                    "surface_id": "44444444-4444-4444-8444-444444444444",
                    "surface_ref": "surface:1",
                });
            }
            "pane.list" => {
                let mut panes = vec![json!({
                    "id": "33333333-3333-4333-8333-333333333333",
                    "ref": "pane:1",
                    "index": 1,
                    "focused": "1",
                    "columns": 120,
                    "rows": 40,
                    "cell_width_px": 10,
                    "cell_height_px": 20,
                    "pixel_frame": {"x": 0, "y": 0, "width": 1200, "height": 800},
                    "surface_ids": ["44444444-4444-4444-8444-444444444444"],
                    "surface_refs": ["surface:1"],
                    "surface_count": 1,
                    "selected_surface_id": "44444444-4444-4444-8444-444444444444",
                })];
                if created {
                    panes.push(json!({
                        "id": "66666666-6666-4666-8666-666666666666",
                        "ref": "pane:2",
                        "index": 2,
                        "focused": "0",
                        "columns": 120,
                        "rows": 40,
                        "cell_width_px": 10,
                        "cell_height_px": 20,
                        "pixel_frame": {"x": 1200, "y": 0, "width": 1200, "height": 800},
                        "surface_ids": ["77777777-7777-4777-8777-777777777777"],
                        "surface_refs": ["surface:2"],
                        "surface_count": 1,
                        "selected_surface_id": "77777777-7777-4777-8777-777777777777",
                    }));
                }
                resp["result"] =
                    json!({"panes": panes, "container_frame": {"width": 1200, "height": 800}});
            }
            "pane.surfaces" => {
                let pane_id = map_str(&params, "pane_id");
                let surface = if pane_id == "66666666-6666-4666-8666-666666666666"
                    || pane_id == "pane:2"
                {
                    json!({"id": "77777777-7777-4777-8777-777777777777", "ref": "surface:2", "selected": "1", "focused": "0"})
                } else {
                    json!({"id": "44444444-4444-4444-8444-444444444444", "ref": "surface:1", "selected": "1", "focused": "1"})
                };
                resp["result"] = json!({"surfaces": [surface]});
            }
            "surface.split" => {
                if map_str(&params, "surface_id") != "44444444-4444-4444-8444-444444444444" {
                    resp["ok"] = Value::Bool(false);
                    resp["error"] = json!({"code": "not_found", "message": "Surface not found"});
                } else {
                    *split_created.lock().unwrap() = true;
                    resp["result"] = json!({
                        "surface_id": "77777777-7777-4777-8777-777777777777",
                        "pane_id": "66666666-6666-4666-8666-666666666666",
                    });
                }
            }
            "workspace.equalize_splits" => {
                resp["result"] = json!({"ok": true});
            }
            _ => {
                resp["ok"] = Value::Bool(false);
                resp["error"] = json!({"code": "unsupported", "message": method});
            }
        }
        let _ = conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
    });
    MockSocket { path, _dir: dir }
}

pub fn start_mock_tmux_selector_priority_socket() -> MockSocket {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    spawn_accept_loop(listener, move |mut conn| {
        let Some(req) = read_request_line(&mut conn) else {
            return;
        };
        let method = map_str(&req, "method").to_string();
        let mut resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true});
        match method.as_str() {
            "pane.list" => {
                resp["result"] = json!({"panes": [
                    {"id": "22222222-2222-4222-8222-222222222222", "ref": "pane:index", "index": 1},
                    {"id": "33333333-3333-4333-8333-333333333333", "ref": "1", "index": 2},
                ]});
            }
            "surface.list" => {
                resp["result"] = json!({"surfaces": [
                    {"id": "44444444-4444-4444-8444-444444444444", "ref": "surface:index", "index": 1},
                    {"id": "55555555-5555-4555-8555-555555555555", "ref": "1", "index": 2},
                ]});
            }
            _ => {
                resp["result"] = json!({});
            }
        }
        let _ = conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
    });
    MockSocket { path, _dir: dir }
}

pub fn start_slow_focused_canonicalization_socket(delay: Duration) -> MockSocket {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    spawn_accept_loop(listener, move |mut conn| {
        let Some(req) = read_request_line(&mut conn) else {
            return;
        };
        let method = map_str(&req, "method").to_string();
        let mut resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true});
        match method.as_str() {
            "system.identify" => {
                resp["result"] = json!({"focused": {
                    "workspace_id": "11111111-1111-4111-8111-111111111111",
                    "pane_id": "pane:1",
                    "pane_ref": "pane:1",
                    "surface_ref": "surface:1",
                }});
            }
            "pane.list" => {
                std::thread::sleep(delay);
                resp["result"] = json!({"panes": [{
                    "id": "33333333-3333-4333-8333-333333333333",
                    "ref": "pane:1",
                    "index": 1,
                }]});
            }
            _ => {
                resp["result"] = json!({});
            }
        }
        let _ = conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
    });
    MockSocket { path, _dir: dir }
}

// --- tmux corpus RPC recorder ---

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub params: Map<String, Value>,
}

#[derive(Default)]
struct RecorderState {
    requests: Vec<RecordedRequest>,
    workspaces: Vec<Map<String, Value>>,
    read_text: String,
}

pub struct TmuxCorpusRecorder {
    pub socket_path: String,
    state: Arc<Mutex<RecorderState>>,
    _dir: tempfile::TempDir,
}

impl TmuxCorpusRecorder {
    pub fn start() -> Self {
        Self::start_with_metric_availability(true, true, true)
    }

    pub fn start_with_pane_metrics(include_pane_metrics: bool) -> Self {
        Self::start_with_metric_availability(
            include_pane_metrics,
            include_pane_metrics,
            include_pane_metrics,
        )
    }

    pub fn start_with_metric_availability(
        include_pane_metrics: bool,
        include_point_metrics: bool,
        include_container_frame: bool,
    ) -> Self {
        let (dir, path) = make_short_unix_socket_dir();
        let listener = UnixListener::bind(&path).expect("listen");
        let state = Arc::new(Mutex::new(RecorderState {
            workspaces: vec![json!({
                "id": "11111111-1111-4111-8111-111111111111",
                "ref": "workspace:1",
                "index": 1,
                "title": "main",
            })
            .as_object()
            .cloned()
            .unwrap()],
            ..Default::default()
        }));
        let state_ref = Arc::clone(&state);
        spawn_accept_loop(listener, move |mut conn| {
            let Some(req) = read_request_line(&mut conn) else {
                return;
            };
            let method = map_str(&req, "method").to_string();
            let params = params_of(&req);
            let mut st = state_ref.lock().unwrap();
            st.requests.push(RecordedRequest {
                method: method.clone(),
                params: params.clone(),
            });
            let mut resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true});
            match method.as_str() {
                "workspace.create" => {
                    let next = st.workspaces.len() + 1;
                    let ws_id = format!("22222222-2222-4222-8222-22222222222{next}");
                    let workspace = json!({
                        "id": ws_id,
                        "ref": format!("workspace:{next}"),
                        "index": next,
                        "title": "created",
                    });
                    st.workspaces.push(workspace.as_object().cloned().unwrap());
                    resp["result"] = json!({"workspace_id": ws_id});
                }
                "workspace.rename" => {
                    let ws_id = map_str(&params, "workspace_id").to_string();
                    let title = map_str(&params, "title").to_string();
                    for workspace in st.workspaces.iter_mut() {
                        if map_str(workspace, "id") == ws_id {
                            workspace.insert("title".to_string(), Value::String(title.clone()));
                            break;
                        }
                    }
                    resp["result"] = json!({"ok": true});
                }
                "workspace.current" => {
                    resp["result"] = json!({"workspace_id": st.workspaces[0].get("id").cloned()});
                }
                "workspace.list" => {
                    resp["result"] = json!({"workspaces": st.workspaces.clone()});
                }
                "surface.list" => {
                    resp["result"] = json!({"surfaces": [{
                        "id": "44444444-4444-4444-8444-444444444444",
                        "ref": "surface:1",
                        "focused": true,
                        "pane_id": "33333333-3333-4333-8333-333333333333",
                        "title": "shell",
                    }]});
                }
                "surface.current" => {
                    resp["result"] = json!({
                        "workspace_id": st.workspaces[0].get("id").cloned(),
                        "pane_id": "33333333-3333-4333-8333-333333333333",
                        "pane_ref": "pane:1",
                        "surface_id": "44444444-4444-4444-8444-444444444444",
                        "surface_ref": "surface:1",
                    });
                }
                "surface.read_text" => {
                    let mut text = st.read_text.clone();
                    if text.is_empty() {
                        text = "\x1b[31mRED\x1b[0m\nplain\n".to_string();
                    }
                    resp["result"] = json!({"text": text});
                }
                "surface.send_text" => {
                    resp["result"] = json!({"ok": true});
                }
                "pane.list" => {
                    let mut pane = json!({
                        "id": "33333333-3333-4333-8333-333333333333",
                        "ref": "pane:1",
                        "index": 1,
                        "focused": true,
                        "columns": 80,
                        "rows": 24,
                    });
                    let mut result = json!({});
                    if include_pane_metrics {
                        pane["cell_width_px"] = json!(8);
                        pane["cell_height_px"] = json!(16);
                        if include_point_metrics {
                            pane["cell_width_points"] = json!(4);
                            pane["cell_height_points"] = json!(8);
                        }
                        pane["pixel_frame"] = json!({"x": 0, "y": 0, "width": 332, "height": 222});
                        if include_container_frame {
                            result["container_frame"] = json!({"width": 640, "height": 384});
                        }
                    }
                    result["panes"] = json!([pane]);
                    resp["result"] = result;
                }
                "pane.surfaces" => {
                    resp["result"] = json!({"surfaces": [{
                        "id": "44444444-4444-4444-8444-444444444444",
                        "ref": "surface:1",
                        "selected": true,
                    }]});
                }
                "pane.resize" => {
                    resp["result"] = json!({"ok": true});
                }
                _ => {
                    resp["ok"] = Value::Bool(false);
                    resp["error"] = json!({"code": "unsupported", "message": method});
                }
            }
            drop(st);
            let _ = conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
        });
        Self {
            socket_path: path,
            state,
            _dir: dir,
        }
    }

    pub fn methods(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .map(|r| r.method.clone())
            .collect()
    }

    pub fn requests_for(&self, method: &str) -> Vec<RecordedRequest> {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == method)
            .cloned()
            .collect()
    }

    pub fn set_read_text(&self, text: &str) {
        self.state.lock().unwrap().read_text = text.to_string();
    }
}

pub fn stress_marker() -> HashMap<String, String> {
    HashMap::new()
}

pub fn tcp_echo_upstream(response: &str) -> (String, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let addr = listener.local_addr().unwrap();
    let response = response.to_string();
    std::thread::spawn(move || {
        if let Ok((mut conn, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = conn.read(&mut buf);
            let _ = conn.write_all(response.as_bytes());
        }
    });
    (addr.to_string(), addr.port())
}

pub fn tcp_stream_read_timeout(stream: &TcpStream, timeout: Duration) {
    let _ = stream.set_read_timeout(Some(timeout));
}

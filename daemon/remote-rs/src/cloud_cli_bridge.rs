//! Cloud CLI bridge: a Unix socket on the VM that forwards `cmux` CLI requests
//! to whichever local cmux apps are attached over the WebSocket RPC transport.
//! Mirrors `cloud_cli_bridge.go`.

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::rpc::{
    base64_decode, base64_encode, get_string_param, read_rpc_frame, RpcEvent, RpcRequest,
    RpcResponse, RpcServer, MAX_RPC_FRAME_BYTES,
};
use crate::util::{accept_unix_with_stop, path_dir, LogSink, StopSignal};

pub const DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH: &str = "/tmp/cmux-cloud-cli.sock";

#[derive(Clone, Debug, Default)]
pub struct CloudCliResponse {
    pub data: Vec<u8>,
    pub err: String,
}

struct ForwardTarget {
    server: Arc<RpcServer>,
    request_id: String,
}

#[derive(Default)]
struct BridgeState {
    next_id: u64,
    servers: HashMap<usize, Arc<RpcServer>>,
    pending: HashMap<String, flume::Sender<CloudCliResponse>>,
}

#[derive(Default)]
pub struct CloudCliBridge {
    state: Mutex<BridgeState>,
    stop: Mutex<Option<Arc<StopSignal>>>,
}

/// Dropping the registration unregisters the server, like Go's returned closure.
pub struct BridgeRegistration {
    bridge: Arc<CloudCliBridge>,
    key: usize,
}

impl Drop for BridgeRegistration {
    fn drop(&mut self) {
        self.bridge.state.lock().unwrap().servers.remove(&self.key);
    }
}

pub fn default_cloud_cli_bridge_socket_if_exists() -> String {
    match fs::metadata(DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH) {
        Ok(info) if info.file_type().is_socket() => {
            DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH.to_string()
        }
        _ => String::new(),
    }
}

use std::os::unix::fs::FileTypeExt;

impl CloudCliBridge {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn start(
        self: &Arc<Self>,
        socket_path: &str,
        stderr: LogSink,
    ) -> io::Result<Arc<StopSignal>> {
        let socket_path =
            strings_trim_space_or_default(socket_path, DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH);
        fs::create_dir_all(path_dir(&socket_path))?;
        let _ = fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)?;
        if let Err(err) = fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o666)) {
            drop(listener);
            let _ = fs::remove_file(&socket_path);
            return Err(err);
        }
        listener.set_nonblocking(true)?;
        let stop = StopSignal::new()?;
        *self.stop.lock().unwrap() = Some(Arc::clone(&stop));
        stderr.write_str(&format!(
            "cmuxd-remote cloud CLI bridge listening on {socket_path}\n"
        ));
        let bridge = Arc::clone(self);
        let stop_ref = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("cmuxd-cli-bridge".to_string())
            .spawn(move || bridge.accept_loop(listener, &socket_path, &stop_ref))
            .expect("spawn cli bridge accept thread");
        Ok(stop)
    }

    pub fn stop(&self) {
        if let Some(stop) = self.stop.lock().unwrap().as_ref() {
            stop.stop();
        }
    }

    fn accept_loop(self: Arc<Self>, listener: UnixListener, socket_path: &str, stop: &StopSignal) {
        loop {
            match accept_unix_with_stop(&listener, stop, None) {
                Ok(Some(conn)) => {
                    let bridge = Arc::clone(&self);
                    std::thread::Builder::new()
                        .name("cmuxd-cli-bridge-conn".to_string())
                        .spawn(move || bridge.handle_conn(conn))
                        .expect("spawn cli bridge conn thread");
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        let _ = fs::remove_file(socket_path);
    }

    pub fn register(self: &Arc<Self>, server: &Arc<RpcServer>) -> BridgeRegistration {
        let key = Arc::as_ptr(server) as usize;
        self.state
            .lock()
            .unwrap()
            .servers
            .insert(key, Arc::clone(server));
        BridgeRegistration {
            bridge: Arc::clone(self),
            key,
        }
    }

    fn handle_conn(&self, mut conn: UnixStream) {
        let _ = conn.set_read_timeout(Some(Duration::from_secs(16)));
        let _ = conn.set_write_timeout(Some(Duration::from_secs(16)));
        let mut reader =
            BufReader::with_capacity(64 * 1024, conn.try_clone().expect("clone unix stream"));
        let (line, oversized) = match read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES) {
            Ok(frame) => frame,
            Err(_) => return,
        };
        if oversized {
            let _ = conn.write_all(
                b"{\"ok\":false,\"error\":{\"code\":\"request_too_large\",\"message\":\"cloud CLI request exceeded maximum size\"}}\n",
            );
            return;
        }
        match self.forward(&line) {
            Ok(response) => {
                let _ = conn.write_all(&response);
                if response.is_empty() || *response.last().unwrap() != b'\n' {
                    let _ = conn.write_all(b"\n");
                }
            }
            Err(err) => {
                let payload = json!({"ok": false, "error": {"code": "cloud_cli_unavailable", "message": err}});
                let _ = conn.write_all(format!("{}\n", crate::util::go_json(&payload)).as_bytes());
            }
        }
    }

    pub fn forward(&self, request: &[u8]) -> Result<Vec<u8>, String> {
        let (targets, response_rx) = self.reserve_requests();
        if targets.is_empty() {
            return Err("no cmux app is attached to this cloud VM".to_string());
        }
        let data_base64 = base64_encode(request);
        let mut sent_targets: Vec<ForwardTarget> = Vec::with_capacity(targets.len());
        let mut write_err: Option<String> = None;
        for target in targets {
            let event = RpcEvent {
                event: "cli.request".to_string(),
                request_id: target.request_id.clone(),
                data_base64: data_base64.clone(),
                ..Default::default()
            };
            if let Err(err) = target.server.frame_writer().write_event(&event) {
                self.forget_request(&target.request_id);
                write_err = Some(err.to_string());
                continue;
            }
            sent_targets.push(target);
        }
        if sent_targets.is_empty() {
            if let Some(err) = write_err {
                return Err(err);
            }
            return Err("no cmux app accepted cloud CLI request".to_string());
        }

        let result = self.await_responses(&sent_targets, &response_rx);
        self.forget_requests(&sent_targets);
        result
    }

    fn await_responses(
        &self,
        sent_targets: &[ForwardTarget],
        response_rx: &flume::Receiver<CloudCliResponse>,
    ) -> Result<Vec<u8>, String> {
        let mut first_routing_rejection: Option<Vec<u8>> = None;
        let mut first_response_err = String::new();
        let mut pending = sent_targets.len();
        let deadline = Instant::now() + Duration::from_secs(15);
        while pending > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("timed out waiting for cmux app response".to_string());
            }
            match response_rx.recv_timeout(remaining) {
                Ok(response) => {
                    pending -= 1;
                    if !response.err.is_empty() {
                        if first_response_err.is_empty() {
                            first_response_err = response.err;
                        }
                        continue;
                    }
                    if sent_targets.len() > 1
                        && is_cloud_cli_workspace_routing_rejection(&response.data)
                    {
                        if first_routing_rejection.is_none() {
                            first_routing_rejection = Some(response.data);
                        }
                        continue;
                    }
                    return Ok(response.data);
                }
                Err(_) => return Err("timed out waiting for cmux app response".to_string()),
            }
        }
        if let Some(rejection) = first_routing_rejection {
            return Ok(rejection);
        }
        if !first_response_err.is_empty() {
            return Err(first_response_err);
        }
        Err("cmux app rejected cloud CLI request".to_string())
    }

    fn reserve_requests(&self) -> (Vec<ForwardTarget>, flume::Receiver<CloudCliResponse>) {
        let mut state = self.state.lock().unwrap();
        let (tx, rx) = flume::bounded(state.servers.len().max(1));
        if state.servers.is_empty() {
            return (Vec::new(), rx);
        }
        let servers: Vec<Arc<RpcServer>> = state.servers.values().cloned().collect();
        let mut targets = Vec::with_capacity(servers.len());
        for server in servers {
            state.next_id += 1;
            let request_id = format!("cli-{}", state.next_id);
            state.pending.insert(request_id.clone(), tx.clone());
            targets.push(ForwardTarget { server, request_id });
        }
        (targets, rx)
    }

    fn forget_request(&self, request_id: &str) {
        self.state.lock().unwrap().pending.remove(request_id);
    }

    fn forget_requests(&self, targets: &[ForwardTarget]) {
        let mut state = self.state.lock().unwrap();
        for target in targets {
            state.pending.remove(&target.request_id);
        }
    }

    pub fn deliver_response(&self, request_id: &str, response: CloudCliResponse) -> bool {
        let sender = self.state.lock().unwrap().pending.remove(request_id);
        match sender {
            Some(sender) => {
                let _ = sender.send(response);
                true
            }
            None => false,
        }
    }
}

impl RpcServer {
    pub fn handle_cli_response(&self, req: &RpcRequest) -> RpcResponse {
        let bridge = match self.cli_bridge() {
            Some(bridge) => bridge,
            None => {
                return RpcResponse::err(
                    req.id.clone(),
                    "unavailable",
                    "cloud CLI bridge is not enabled",
                )
            }
        };
        let request_id = match get_string_param(req.params(), "request_id") {
            Some(id) if !id.is_empty() => id,
            _ => {
                return RpcResponse::err(
                    req.id.clone(),
                    "invalid_params",
                    "cli.response requires request_id",
                )
            }
        };
        let response_ok = match req.params().and_then(|p| p.get("ok")) {
            Some(serde_json::Value::Bool(value)) => *value,
            _ => true,
        };
        let mut response = CloudCliResponse::default();
        if response_ok {
            let data_base64 = match get_string_param(req.params(), "data_base64") {
                Some(data) => data,
                None => {
                    return RpcResponse::err(
                        req.id.clone(),
                        "invalid_params",
                        "cli.response requires data_base64",
                    )
                }
            };
            match base64_decode(&data_base64) {
                Ok(data) => response.data = data,
                Err(_) => {
                    return RpcResponse::err(
                        req.id.clone(),
                        "invalid_params",
                        "data_base64 must be valid base64",
                    )
                }
            }
        } else {
            response.err = get_string_param(req.params(), "error").unwrap_or_default();
            if response.err.is_empty() {
                response.err = "cmux app rejected cloud CLI request".to_string();
            }
        }
        if !bridge.deliver_response(&request_id, response) {
            return RpcResponse::err(req.id.clone(), "not_found", "cloud CLI request not found");
        }
        RpcResponse::ok(req.id.clone(), json!({"delivered": true}))
    }
}

pub fn is_cloud_cli_workspace_routing_rejection(data: &[u8]) -> bool {
    let envelope: serde_json::Value = match serde_json::from_slice(data) {
        Ok(value) => value,
        Err(_) => return false,
    };
    if envelope
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return false;
    }
    let code = envelope
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    code == "remote_cli_workspace_denied" || code == "remote_cli_unscoped"
}

pub fn strings_trim_space_or_default(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return fallback.to_string();
    }
    value.to_string()
}

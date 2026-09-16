//! Cloud CLI bridge: a Unix socket on the VM that forwards `cmux` CLI
//! requests to attached apps over the `/rpc` WebSocket as `cli.request`
//! events and relays their `cli.response` back.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use crossbeam_channel::{Receiver, Sender, bounded};

use crate::logger::Logger;
use crate::rpc::server::{CliBridge, CliResponse};
use crate::rpc::{FrameWriter, MAX_RPC_FRAME_BYTES, RpcEvent, RpcFrame, read_rpc_frame};
use crate::util::quote;

pub const DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH: &str = "/tmp/cmux-cloud-cli.sock";
const FORWARD_TIMEOUT: Duration = Duration::from_secs(15);
const CONN_DEADLINE: Duration = Duration::from_secs(16);

struct ForwardTarget {
    writer: Arc<dyn FrameWriter>,
    request_id: String,
}

struct BridgeState {
    next_id: u64,
    next_server_id: u64,
    servers: HashMap<u64, Arc<dyn FrameWriter>>,
    pending: HashMap<String, Sender<CliResponse>>,
}

pub struct CloudCliBridge {
    state: Mutex<BridgeState>,
}

/// Unregisters the attached app on drop.
pub struct Registration {
    bridge: Arc<CloudCliBridge>,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.bridge.lock().servers.remove(&self.id);
    }
}

impl Default for CloudCliBridge {
    fn default() -> Self {
        Self {
            state: Mutex::new(BridgeState {
                next_id: 0,
                next_server_id: 0,
                servers: HashMap::new(),
                pending: HashMap::new(),
            }),
        }
    }
}

#[must_use]
pub fn default_cloud_cli_bridge_socket_if_exists() -> String {
    use std::os::unix::fs::FileTypeExt;
    match std::fs::metadata(DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH) {
        Ok(info) if info.file_type().is_socket() => {
            DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH.to_string()
        }
        _ => String::new(),
    }
}

impl CloudCliBridge {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BridgeState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Bind the bridge socket and serve it on a background thread.
    pub fn start(self: &Arc<Self>, socket_path: &str, logger: &Arc<dyn Logger>) -> io::Result<()> {
        let socket_path = {
            let trimmed = socket_path.trim();
            if trimmed.is_empty() {
                DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH.to_string()
            } else {
                trimmed.to_string()
            }
        };
        if let Some(dir) = Path::new(&socket_path).parent() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o755).create(dir)?;
        }
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)?;
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) =
                std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666))
            {
                let _ = std::fs::remove_file(&socket_path);
                return Err(e);
            }
        }
        logger.log(&format!("cmuxd-remote cloud CLI bridge listening on {socket_path}\n"));
        let bridge = Arc::clone(self);
        std::thread::spawn(move || bridge.accept_loop(&listener, &socket_path));
        Ok(())
    }

    fn accept_loop(self: &Arc<Self>, listener: &UnixListener, socket_path: &str) {
        loop {
            match listener.accept() {
                Ok((conn, _)) => {
                    let bridge = Arc::clone(self);
                    std::thread::spawn(move || bridge.handle_conn(conn));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        let _ = std::fs::remove_file(socket_path);
    }

    /// Register an attached app's frame writer as a forwarding target.
    pub fn register(self: &Arc<Self>, writer: Arc<dyn FrameWriter>) -> Registration {
        let mut state = self.lock();
        state.next_server_id += 1;
        let id = state.next_server_id;
        state.servers.insert(id, writer);
        Registration { bridge: Arc::clone(self), id }
    }

    #[must_use]
    pub fn attached_count(&self) -> usize {
        self.lock().servers.len()
    }

    pub fn handle_conn(&self, conn: UnixStream) {
        let _ = conn.set_read_timeout(Some(CONN_DEADLINE));
        let _ = conn.set_write_timeout(Some(CONN_DEADLINE));
        let mut writer = &conn;
        let mut reader = BufReader::with_capacity(64 * 1024, &conn);
        let line = match read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES) {
            Ok(RpcFrame::Line(line)) => line,
            Ok(RpcFrame::Oversized) => {
                let _ = writer.write_all(
                    b"{\"ok\":false,\"error\":{\"code\":\"request_too_large\",\"message\":\"cloud CLI request exceeded maximum size\"}}\n",
                );
                return;
            }
            Ok(RpcFrame::Eof) | Err(_) => return,
        };
        match self.forward(&line) {
            Ok(response) => {
                let _ = writer.write_all(&response);
                if response.last() != Some(&b'\n') {
                    let _ = writer.write_all(b"\n");
                }
            }
            Err(err) => {
                let _ = writer.write_all(
                    format!("{{\"ok\":false,\"error\":{{\"code\":\"cloud_cli_unavailable\",\"message\":{}}}}}\n", quote(&err))
                        .as_bytes(),
                );
            }
        }
        let _ = writer.flush();
    }

    /// Send `request` to every attached app and return the first usable
    /// response.
    pub fn forward(&self, request: &[u8]) -> Result<Vec<u8>, String> {
        let (targets, response_rx) = self.reserve_requests();
        if targets.is_empty() {
            return Err("no cmux app is attached to this cloud VM".to_string());
        }
        let data_base64 = BASE64.encode(request);
        let mut sent = Vec::with_capacity(targets.len());
        let mut write_err: Option<String> = None;
        for target in targets {
            let event = RpcEvent {
                event: "cli.request".to_string(),
                request_id: target.request_id.clone(),
                data_base64: data_base64.clone(),
                ..RpcEvent::default()
            };
            if let Err(e) = target.writer.write_event(&event) {
                self.lock().pending.remove(&target.request_id);
                write_err = Some(crate::util::io_error_text(&e));
                continue;
            }
            sent.push(target);
        }
        if sent.is_empty() {
            return Err(
                write_err.unwrap_or_else(|| "no cmux app accepted cloud CLI request".to_string())
            );
        }
        let result = self.collect_responses(&sent, &response_rx);
        let mut state = self.lock();
        for target in &sent {
            state.pending.remove(&target.request_id);
        }
        result
    }

    fn collect_responses(
        &self,
        sent: &[ForwardTarget],
        response_rx: &Receiver<CliResponse>,
    ) -> Result<Vec<u8>, String> {
        let deadline = Instant::now() + FORWARD_TIMEOUT;
        let mut first_routing_rejection: Option<Vec<u8>> = None;
        let mut first_response_err = String::new();
        let mut pending = sent.len();
        while pending > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Ok(response) = response_rx.recv_timeout(remaining) else {
                return Err("timed out waiting for cmux app response".to_string());
            };
            pending -= 1;
            if !response.err.is_empty() {
                if first_response_err.is_empty() {
                    first_response_err = response.err;
                }
                continue;
            }
            if sent.len() > 1 && is_cloud_cli_workspace_routing_rejection(&response.data) {
                if first_routing_rejection.is_none() {
                    first_routing_rejection = Some(response.data);
                }
                continue;
            }
            return Ok(response.data);
        }
        if let Some(rejection) = first_routing_rejection {
            return Ok(rejection);
        }
        if !first_response_err.is_empty() {
            return Err(first_response_err);
        }
        Err("cmux app rejected cloud CLI request".to_string())
    }

    fn reserve_requests(&self) -> (Vec<ForwardTarget>, Receiver<CliResponse>) {
        let mut state = self.lock();
        let (tx, rx) = bounded(state.servers.len().max(1));
        if state.servers.is_empty() {
            return (Vec::new(), rx);
        }
        let mut ids: Vec<u64> = state.servers.keys().copied().collect();
        ids.sort_unstable();
        let mut targets = Vec::with_capacity(ids.len());
        for id in ids {
            state.next_id += 1;
            let request_id = format!("cli-{}", state.next_id);
            state.pending.insert(request_id.clone(), tx.clone());
            targets.push(ForwardTarget { writer: Arc::clone(&state.servers[&id]), request_id });
        }
        (targets, rx)
    }
}

impl CliBridge for CloudCliBridge {
    fn deliver_response(&self, request_id: &str, response: CliResponse) -> bool {
        let sender = self.lock().pending.remove(request_id);
        match sender {
            Some(sender) => {
                let _ = sender.send(response);
                true
            }
            None => false,
        }
    }
}

#[must_use]
pub fn is_cloud_cli_workspace_routing_rejection(data: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct Envelope {
        #[serde(default)]
        ok: bool,
        #[serde(default)]
        error: Option<EnvelopeError>,
    }
    #[derive(serde::Deserialize)]
    struct EnvelopeError {
        #[serde(default)]
        code: String,
    }
    let Ok(envelope) = serde_json::from_slice::<Envelope>(data) else {
        return false;
    };
    if envelope.ok {
        return false;
    }
    let Some(error) = envelope.error else { return false };
    error.code == "remote_cli_workspace_denied" || error.code == "remote_cli_unscoped"
}

/// Send one request line over the bridge socket and read the response line.
pub fn forward_cloud_cli_request(
    socket_path: &str,
    request: &[u8],
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    let conn = UnixStream::connect(socket_path)?;
    conn.set_read_timeout(Some(timeout))?;
    conn.set_write_timeout(Some(timeout))?;
    let mut writer = &conn;
    writer.write_all(request)?;
    if request.last() != Some(&b'\n') {
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    let mut reader = BufReader::with_capacity(64 * 1024, &conn);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::RpcResponse;

    struct Capture {
        events: Mutex<Vec<RpcEvent>>,
        fail: bool,
    }

    impl FrameWriter for Capture {
        fn write_response(&self, _resp: &RpcResponse) -> io::Result<()> {
            Ok(())
        }
        fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
            if self.fail {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "gone"));
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    #[test]
    fn routing_rejection_detection() {
        assert!(is_cloud_cli_workspace_routing_rejection(
            br#"{"ok":false,"error":{"code":"remote_cli_unscoped"}}"#
        ));
        assert!(is_cloud_cli_workspace_routing_rejection(
            br#"{"ok":false,"error":{"code":"remote_cli_workspace_denied"}}"#
        ));
        assert!(!is_cloud_cli_workspace_routing_rejection(br#"{"ok":true}"#));
        assert!(!is_cloud_cli_workspace_routing_rejection(
            br#"{"ok":false,"error":{"code":"other"}}"#
        ));
        assert!(!is_cloud_cli_workspace_routing_rejection(b"nope"));
    }

    #[test]
    fn forward_without_apps_fails_fast() {
        let bridge = CloudCliBridge::new();
        assert_eq!(bridge.forward(b"{}").unwrap_err(), "no cmux app is attached to this cloud VM");
    }

    #[test]
    fn forward_delivers_first_success_and_prefers_non_rejections() {
        let bridge = CloudCliBridge::new();
        let a = Arc::new(Capture { events: Mutex::new(Vec::new()), fail: false });
        let b = Arc::new(Capture { events: Mutex::new(Vec::new()), fail: false });
        let _ra = bridge.register(a.clone());
        let _rb = bridge.register(b.clone());
        let responder = {
            let bridge = Arc::clone(&bridge);
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline {
                    let ea = a.events.lock().unwrap().first().cloned();
                    let eb = b.events.lock().unwrap().first().cloned();
                    if let (Some(ea), Some(eb)) = (ea, eb) {
                        assert_eq!(BASE64.decode(&ea.data_base64).unwrap(), b"{\"cmd\":1}");
                        assert!(
                            bridge.deliver_response(
                                &ea.request_id,
                                CliResponse {
                                    data: br#"{"ok":false,"error":{"code":"remote_cli_unscoped"}}"#
                                        .to_vec(),
                                    err: String::new()
                                }
                            )
                        );
                        assert!(bridge.deliver_response(
                            &eb.request_id,
                            CliResponse { data: b"{\"ok\":true}".to_vec(), err: String::new() }
                        ));
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                panic!("events never arrived");
            })
        };
        let response = bridge.forward(b"{\"cmd\":1}").unwrap();
        assert_eq!(response, b"{\"ok\":true}");
        responder.join().unwrap();
        assert!(
            !bridge.deliver_response("cli-1", CliResponse::default()),
            "pending entries are cleared"
        );
    }

    #[test]
    fn forward_reports_write_failures_and_errors() {
        let bridge = CloudCliBridge::new();
        let broken = Arc::new(Capture { events: Mutex::new(Vec::new()), fail: true });
        let _r = bridge.register(broken);
        assert_eq!(bridge.forward(b"{}").unwrap_err(), "gone");

        let bridge = CloudCliBridge::new();
        let app = Arc::new(Capture { events: Mutex::new(Vec::new()), fail: false });
        let _r = bridge.register(app.clone());
        let responder = {
            let bridge = Arc::clone(&bridge);
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline {
                    if let Some(e) = app.events.lock().unwrap().first().cloned() {
                        bridge.deliver_response(
                            &e.request_id,
                            CliResponse { data: Vec::new(), err: "app said no".into() },
                        );
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };
        assert_eq!(bridge.forward(b"{}").unwrap_err(), "app said no");
        responder.join().unwrap();
    }
}

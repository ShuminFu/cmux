//! Ports of `ws_pty_test.go`, `ws_rpc_test.go` and the tmux-corpus WebSocket
//! test. Client side uses tokio-tungstenite; hub-level tests drive the PTY hub
//! directly with pipes standing in for the PTY master.

mod support;

use std::fs;
use std::io::{self, Read};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use cmuxd_remote::pty_hub::{
    normalize_pty_size, persistent_pty_session_key, InputChunk, InputWriteStatus, OutgoingFrame,
    PtyAttachment, PtyHub, PtyHubConfig, PtyMaster, PtySession, WsPtyControlFrame,
    DEFAULT_PTY_INPUT_CHUNK_BYTES, DEFAULT_PTY_INPUT_QUEUE_CAP, DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
    MAX_PTY_DIMENSION,
};
use cmuxd_remote::rpc::{rpc_pty_event_for_frame, CaptureFrameWriter, FrameWriter, RpcServer};
use cmuxd_remote::util::{LogSink, SharedBuffer};
use cmuxd_remote::ws::{
    consume_websocket_lease, hub_for_config, new_websocket_pty_handler, serve_router, WsAuthFrame,
    WsLease, WsServerConfig, WS_STATUS_NORMAL_CLOSURE, WS_STATUS_POLICY_VIOLATION,
};
use support::*;

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct TestWsServer {
    rt: tokio::runtime::Runtime,
    url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    hub: Arc<PtyHub>,
    _stderr: SharedBuffer,
}

impl TestWsServer {
    fn start(mut cfg: WsServerConfig) -> Self {
        let stderr = SharedBuffer::new();
        let sink = LogSink::new(Box::new(stderr.clone()));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let hub = hub_for_config(&cfg, &sink);
        cfg.pty_hub = Some(Arc::clone(&hub));
        let router = new_websocket_pty_handler(cfg, sink);
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        rt.spawn(serve_router(listener, router, async move {
            let _ = rx.await;
        }));
        Self {
            rt,
            url: format!("http://{addr}"),
            shutdown: Some(tx),
            hub,
            _stderr: stderr,
        }
    }

    fn ws_url(&self, path: &str) -> String {
        format!("ws{}{}", self.url.trim_start_matches("http"), path)
    }

    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }
}

impl Drop for TestWsServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.hub.close_all();
    }
}

fn new_test_websocket_pty_server(lease_path: &str) -> TestWsServer {
    TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path.to_string(),
        shell: "/bin/sh".to_string(),
        scrollback_limit: 64 * 1024,
        ..Default::default()
    })
}

fn write_test_lease(path: &str, token: &str, session_id: &str, single_use: bool, expires_at: i64) {
    let lease = WsLease {
        version: 1,
        token_sha256: hex::encode(Sha256::digest(token.as_bytes())),
        expires_at_unix: expires_at,
        session_id: session_id.to_string(),
        single_use,
    };
    fs::write(path, serde_json::to_vec(&lease).unwrap()).unwrap();
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn dial(url: String) -> Client {
    let (ws, _) = connect_async(url).await.expect("dial websocket");
    ws
}

async fn send_auth_with_attachment(
    ws: &mut Client,
    token: &str,
    session_id: &str,
    attachment_id: &str,
    cols: i64,
    rows: i64,
) {
    let auth = WsAuthFrame {
        kind: "auth".to_string(),
        token: token.to_string(),
        session_id: session_id.to_string(),
        attachment_id: attachment_id.to_string(),
        cols,
        rows,
        session_id_explicit: false,
    };
    ws.send(Message::Text(serde_json::to_string(&auth).unwrap().into()))
        .await
        .expect("write auth");
}

async fn send_auth(ws: &mut Client, token: &str, session_id: &str, cols: i64, rows: i64) {
    send_auth_with_attachment(ws, token, session_id, "", cols, rows).await;
}

async fn next_message(
    ws: &mut Client,
    timeout: Duration,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    match tokio::time::timeout(timeout, ws.next()).await {
        Ok(next) => next,
        Err(_) => panic!("timed out waiting for websocket message"),
    }
}

async fn expect_close_status(ws: &mut Client) -> u16 {
    loop {
        match next_message(ws, Duration::from_secs(5)).await {
            Some(Ok(Message::Close(Some(frame)))) => return u16::from(frame.code),
            Some(Ok(Message::Close(None))) => return 1005,
            Some(Ok(_)) => continue,
            Some(Err(err)) => panic!("read error before close frame: {err}"),
            None => panic!("connection ended without close frame"),
        }
    }
}

async fn read_ready(ws: &mut Client) -> String {
    loop {
        match next_message(ws, Duration::from_secs(5)).await {
            Some(Ok(Message::Text(text))) => {
                assert!(
                    text.contains("\"ready\""),
                    "first frame should be ready text, got {text:?}"
                );
                return text.to_string();
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            other => panic!("first frame should be ready text, got {other:?}"),
        }
    }
}

async fn wait_for_binary_contains(ws: &mut Client, needle: &str, timeout: Duration) -> String {
    let mut output = String::new();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Binary(payload)))) => {
                output.push_str(&String::from_utf8_lossy(&payload));
                if output.contains(needle) {
                    return output;
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(other) => panic!(
                "read terminal output while waiting for {needle:?}: {other:?} output={output:?}"
            ),
            Err(_) => break,
        }
    }
    panic!("timed out waiting for {needle:?}, got {output:?}");
}

async fn wait_for_normal_close(ws: &mut Client, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(frame))))) => {
                assert_eq!(
                    u16::from(frame.code),
                    WS_STATUS_NORMAL_CLOSURE,
                    "expected normal close, got {frame:?}"
                );
                return;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(err))) => panic!("expected normal close, got error {err}"),
            Ok(None) => panic!("connection ended without close frame"),
            Err(_) => break,
        }
    }
    panic!("timed out waiting for normal close");
}

async fn try_write_binary(
    ws: &mut Client,
    payload: &str,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    ws.send(Message::Binary(payload.as_bytes().to_vec().into()))
        .await
}

async fn write_binary(ws: &mut Client, payload: &str) {
    try_write_binary(ws, payload).await.expect("write binary");
}

async fn try_write_control(
    ws: &mut Client,
    kind: &str,
    cols: i64,
    rows: i64,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let frame = WsPtyControlFrame {
        kind: kind.to_string(),
        cols,
        rows,
    };
    ws.send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
}

async fn write_control(ws: &mut Client, kind: &str, cols: i64, rows: i64) {
    try_write_control(ws, kind, cols, rows)
        .await
        .expect("write control");
}

fn wait_for_hub_session_count(hub: &PtyHub, want: usize, timeout: Duration) {
    assert!(
        wait_until(timeout, || hub.active_session_count() == want),
        "hub session count = {}, want {want}",
        hub.active_session_count()
    );
}

fn wait_for_hub_session_size(
    hub: &PtyHub,
    session_id: &str,
    attachments: usize,
    cols: i64,
    rows: i64,
    timeout: Duration,
) {
    let ok = wait_until(timeout, || {
        hub.session_debug_snapshot(session_id) == Some((attachments, cols, rows))
    });
    assert!(
        ok,
        "hub session {session_id} state = {:?}, want ({attachments}, {cols}, {rows})",
        hub.session_debug_snapshot(session_id)
    );
}

fn wait_for_hub_pty_size(hub: &PtyHub, session_id: &str, cols: i64, rows: i64, timeout: Duration) {
    let ok = wait_until(
        timeout,
        || matches!(hub.session_pty_size(session_id), Ok(Some((c, r))) if c == cols && r == rows),
    );
    assert!(
        ok,
        "hub session {session_id} pty size = {:?}, want {cols}x{rows}",
        hub.session_pty_size(session_id)
    );
}

fn thread_count() -> Option<usize> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("Threads:")
            .and_then(|v| v.trim().parse().ok())
    })
}

// --- hub-level PTY input fixtures (pipe standing in for the PTY master) ---

struct InputFixture {
    hub: Arc<PtyHub>,
    session: Arc<PtySession>,
    attachment: Arc<PtyAttachment>,
    read_end: fs::File,
    stderr: SharedBuffer,
}

fn new_test_pty_input_session(
    session_id: &str,
    attachment_id: &str,
    input_seq_ack: bool,
) -> InputFixture {
    let (read_fd, write_fd) = nix::unistd::pipe().unwrap();
    let stderr = SharedBuffer::new();
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/bin/sh".to_string(),
            scrollback_limit: 4096,
            session_idle_ttl: None,
        },
        Some(LogSink::new(Box::new(stderr.clone()))),
    );
    let master = Arc::new(PtyMaster::new(write_fd).unwrap());
    let key = persistent_pty_session_key(session_id);
    let attachment = PtyAttachment::new_for_test(
        key.clone(),
        attachment_id,
        "token-1",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        input_seq_ack,
    );
    let session = PtySession::new_for_test(
        session_id,
        key,
        Some(master),
        vec![Arc::clone(&attachment)],
        80,
        24,
        true,
    );
    hub.insert_session_for_test(Arc::clone(&session));
    InputFixture {
        hub,
        session,
        attachment,
        read_end: fs::File::from(read_fd),
        stderr,
    }
}

fn spawn_input_loop(hub: &Arc<PtyHub>, session: &Arc<PtySession>) -> JoinHandle<()> {
    let hub = Arc::clone(hub);
    let session = Arc::clone(session);
    std::thread::spawn(move || hub.write_input_loop(&session))
}

fn read_exactly(file: &mut fs::File, count: usize, timeout: Duration) -> Vec<u8> {
    let mut clone = file.try_clone().unwrap();
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let mut buf = vec![0u8; count];
        let result = clone.read_exact(&mut buf).map(|_| buf);
        let _ = tx.send(result);
    });
    rx.recv_timeout(timeout)
        .unwrap_or_else(|_| panic!("timed out reading {count} PTY bytes"))
        .expect("read PTY input")
}

fn chunk(
    attachment: &Arc<PtyAttachment>,
    payload: &[u8],
    seq: u64,
    final_seq_chunk: bool,
) -> InputChunk {
    InputChunk {
        attachment_id: attachment.id.clone(),
        attachment: Some(Arc::clone(attachment)),
        payload: payload.to_vec(),
        seq,
        final_seq_chunk,
    }
}

// --- tests ---

#[test]
fn attach_rpc_surfaces_pty_allocation_failure() {
    let stderr = SharedBuffer::new();
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/bin/sh".to_string(),
            ..Default::default()
        },
        Some(LogSink::new(Box::new(stderr.clone()))),
    );
    hub.set_pty_opener(Box::new(|| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "open /dev/ptmx: permission denied",
        ))
    }));
    let err = hub
        .attach_rpc("sess-1", "att-1", 80, 24, "", "", false, false)
        .err()
        .expect("attach_rpc should fail when PTY allocation is denied");
    let lowered = err.to_lowercase();
    assert!(lowered.contains("could not allocate a remote pty"), "{err}");
    assert!(err.contains("/dev/ptmx"), "{err}");
    assert!(
        lowered.contains("ptmxmode") && lowered.contains("remount"),
        "{err}"
    );
    assert!(!stderr.is_empty(), "PTY allocation failure must be logged");
    assert!(
        stderr.to_string_lossy().contains("/dev/ptmx"),
        "{}",
        stderr.to_string_lossy()
    );
    hub.close_all();
}

#[test]
fn serve_ws_requires_explicit_lease_file() {
    let (code, _, err) = run_daemon(&["serve", "--ws", "--listen", "127.0.0.1:0"], "");
    assert_eq!(code, 2, "stderr={err:?}");
    assert!(err.contains("requires --auth-lease-file"), "stderr={err:?}");
}

#[test]
fn websocket_pty_health_is_available_when_locked() {
    let dir = temp_dir("cmux-ws-");
    let server = new_test_websocket_pty_server(&path_str(&dir, "lease.json"));
    let resp = ureq::get(&format!("{}/healthz", server.url))
        .call()
        .expect("GET /healthz");
    assert_eq!(resp.status(), 200);
    let body = parse_json_map(&resp.into_string().unwrap());
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("locked"), Some(&json!(true)));
}

fn post_status(url: &str, headers: &[(&str, &str)], body: &[u8]) -> u16 {
    let mut req = ureq::post(url);
    for (key, value) in headers {
        req = req.set(key, value);
    }
    match req.send_bytes(body) {
        Ok(resp) => resp.status(),
        Err(ureq::Error::Status(code, _)) => code,
        Err(err) => panic!("POST {url}: {err}"),
    }
}

#[test]
fn websocket_pty_admin_lease_install_requires_token() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let sum = hex::encode(Sha256::digest(b"admin-token"));
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path,
        admin_token_sha256: sum,
        shell: "/bin/sh".to_string(),
        ..Default::default()
    });
    let status = post_status(
        &format!("{}/admin/leases", server.url),
        &[],
        b"{\"pty_lease\":{}}",
    );
    assert_eq!(status, 403);
    let disabled = new_test_websocket_pty_server(&path_str(&dir, "lease2.json"));
    assert_eq!(
        post_status(&format!("{}/admin/leases", disabled.url), &[], b"{}"),
        404
    );
    let method = ureq::get(&format!("{}/admin/leases", server.url)).call();
    assert!(
        matches!(method, Err(ureq::Error::Status(405, _))),
        "{method:?}"
    );
}

#[test]
fn websocket_pty_admin_lease_install_unlocks_attach() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let admin_sum = hex::encode(Sha256::digest(b"admin-token"));
    let pty_sum = hex::encode(Sha256::digest(b"pty-token"));
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path,
        admin_token_sha256: admin_sum,
        shell: "/bin/sh".to_string(),
        ..Default::default()
    });
    let lease = WsLease {
        version: 1,
        token_sha256: pty_sum,
        expires_at_unix: now_unix() + 60,
        session_id: "sess-admin".to_string(),
        single_use: true,
    };
    let body = serde_json::to_vec(&json!({"pty_lease": lease})).unwrap();
    let status = post_status(
        &format!("{}/admin/leases", server.url),
        &[
            ("Authorization", "Bearer admin-token"),
            ("Content-Type", "application/json"),
        ],
        &body,
    );
    assert_eq!(status, 200);
    let url = server.ws_url("/terminal");
    server.block_on(async move {
        let mut ws = dial(url).await;
        send_auth(&mut ws, "pty-token", "sess-admin", 80, 24).await;
        read_ready(&mut ws).await;
        let _ = ws.close(None).await;
    });
}

#[test]
fn websocket_pty_admin_lease_install_accepts_ed25519_signature() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let secret: [u8; 32] = cmuxd_remote::util::random_bytes(32).try_into().unwrap();
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key = signing_key.verifying_key();
    let pty_sum = hex::encode(Sha256::digest(b"pty-token"));
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path,
        admin_ed25519_pub_key: cmuxd_remote::rpc::base64_encode(public_key.as_bytes()),
        shell: "/bin/sh".to_string(),
        ..Default::default()
    });
    let lease = WsLease {
        version: 1,
        token_sha256: pty_sum,
        expires_at_unix: now_unix() + 60,
        session_id: "sess-signed".to_string(),
        single_use: true,
    };
    let body = serde_json::to_vec(&json!({"pty_lease": lease})).unwrap();
    let url = format!("{}/admin/leases", server.url);
    assert_eq!(
        post_status(&url, &[("Content-Type", "application/json")], &body),
        403
    );
    let signature = cmuxd_remote::rpc::base64_encode(&signing_key.sign(&body).to_bytes());
    assert_eq!(
        post_status(
            &url,
            &[
                ("Content-Type", "application/json"),
                ("X-Cmux-Admin-Signature-Ed25519", &signature)
            ],
            &body
        ),
        200
    );
}

#[test]
fn websocket_pty_rejects_missing_and_wrong_lease() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let lease_for_async = lease_path.clone();
    server.block_on(async move {
        let mut ws = dial(url.clone()).await;
        send_auth(&mut ws, "missing", "sess-missing", 80, 24).await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION,
            "missing lease"
        );

        write_test_lease(
            &lease_for_async,
            "correct-token",
            "sess-wrong",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url.clone()).await;
        send_auth(&mut ws, "wrong-token", "sess-wrong", 80, 24).await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION,
            "wrong token"
        );
        assert!(
            fs::metadata(&lease_for_async).is_ok(),
            "wrong-token attempt should not consume lease"
        );

        write_test_lease(
            &lease_for_async,
            "expired-token",
            "sess-expired",
            true,
            now_unix() - 60,
        );
        let mut ws = dial(url.clone()).await;
        send_auth(&mut ws, "expired-token", "sess-expired", 80, 24).await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION,
            "expired token"
        );

        let mut ws = dial(url.clone()).await;
        ws.send(Message::Binary(b"not json".to_vec().into()))
            .await
            .unwrap();
        assert_eq!(
            expect_close_status(&mut ws).await,
            1003,
            "binary auth must be rejected"
        );

        let mut ws = dial(url).await;
        ws.send(Message::Text("{\"type\":\"nope\"}".into()))
            .await
            .unwrap();
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION,
            "invalid auth"
        );
    });
}

#[test]
fn websocket_pty_requires_session_match_and_consumes_lease_once() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "cmux-secret",
            "sess-good",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url.clone()).await;
        send_auth(&mut ws, "cmux-secret", "sess-other", 80, 24).await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION
        );
        assert!(
            fs::metadata(&lease_path).is_ok(),
            "wrong-session attempt should not consume lease"
        );

        let mut ws = dial(url.clone()).await;
        send_auth(&mut ws, "cmux-secret", "sess-good", 100, 30).await;
        read_ready(&mut ws).await;
        assert!(
            fs::metadata(&lease_path).is_err(),
            "successful auth should consume lease"
        );
        let _ = ws.close(None).await;

        let mut ws = dial(url).await;
        send_auth(&mut ws, "cmux-secret", "sess-good", 100, 30).await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION,
            "replay"
        );
    });
}

#[test]
fn websocket_pty_runs_shell_over_binary_frames() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "terminal-token",
            "sess-shell",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth(&mut ws, "terminal-token", "sess-shell", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(
            &mut ws,
            "printf '%b\\n' '\\103\\115\\125\\130\\137\\127\\123\\137\\117\\113'; exit\r",
        )
        .await;
        wait_for_binary_contains(&mut ws, "CMUX_WS_OK", Duration::from_secs(15)).await;
        wait_for_normal_close(&mut ws, Duration::from_secs(10)).await;
    });
}

#[test]
fn websocket_pty_reconnect_keeps_session_process() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "first-token",
            "sess-reconnect",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url.clone()).await;
        send_auth_with_attachment(&mut ws, "first-token", "sess-reconnect", "same", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(
            &mut ws,
            "CMUX_RECONNECT_MARKER=alive; export CMUX_RECONNECT_MARKER; printf 'first-ready\\n'\r",
        )
        .await;
        wait_for_binary_contains(&mut ws, "first-ready", Duration::from_secs(5)).await;
        let _ = ws.close(None).await;

        write_test_lease(
            &lease_path,
            "second-token",
            "sess-reconnect",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth_with_attachment(&mut ws, "second-token", "sess-reconnect", "same", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(&mut ws, "printf '%s\\n' \"$CMUX_RECONNECT_MARKER\"; exit\r").await;
        wait_for_binary_contains(&mut ws, "alive", Duration::from_secs(5)).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_replaced_attachment_cannot_write_input() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(&lease_path, "old-token", "sess-replace", true, now_unix() + 60);
        let mut old = dial(url.clone()).await;
        send_auth_with_attachment(&mut old, "old-token", "sess-replace", "same", 120, 40).await;
        read_ready(&mut old).await;

        write_test_lease(&lease_path, "new-token", "sess-replace", true, now_unix() + 60);
        let mut new = dial(url).await;
        send_auth_with_attachment(&mut new, "new-token", "sess-replace", "same", 90, 30).await;
        read_ready(&mut new).await;
        let hub_ref = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || wait_for_hub_session_size(&hub_ref, "sess-replace", 1, 90, 30, Duration::from_secs(5))).await.unwrap();

        // The server tears the replaced connection down on its own schedule,
        // so these writes may fail (Go ignored their errors too).
        let _ = try_write_binary(&mut old, "printf 'STALE_INPUT\\n'\r").await;
        let _ = try_write_control(&mut old, "resize", 100, 35).await;
        let _ = old.close(None).await;
        let hub_ref = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_size(&hub_ref, "sess-replace", 1, 90, 30, Duration::from_secs(5));
            wait_for_hub_pty_size(&hub_ref, "sess-replace", 90, 30, Duration::from_secs(5));
        })
        .await
        .unwrap();

        write_binary(&mut new, "printf 'SIZE:'; stty size; printf '%b\\n' '\\106\\122\\105\\123\\110\\137\\111\\116\\120\\125\\124'; exit\r").await;
        let output = wait_for_binary_contains(&mut new, "FRESH_INPUT", Duration::from_secs(5)).await;
        assert!(output.contains("SIZE:30 90"), "replaced attachment changed terminal size, output={output:?}");
        assert!(!output.contains("STALE_INPUT"), "replaced attachment wrote input, output={output:?}");
        wait_for_normal_close(&mut new, Duration::from_secs(5)).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_reattach_writes_accepted_old_input_before_new() {
    let mut fixture = new_test_pty_input_session("sess-reattach-seam", "same", false);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    let _loop = spawn_input_loop(&hub, &session);

    let writer_guard = session.lock_pty_writer();
    assert_eq!(
        hub.write_input_by_id(
            &session.id,
            &attachment.id,
            &attachment.client_token,
            b"OLD"
        ),
        InputWriteStatus::Ok
    );

    let (tx, rx) = flume::bounded(1);
    {
        let hub = Arc::clone(&hub);
        let session_id = session.id.clone();
        let attachment_id = attachment.id.clone();
        std::thread::spawn(move || {
            let _ = tx.send(hub.prepare_attachment(
                &session_id,
                &attachment_id,
                80,
                24,
                true,
                "",
                "new-token",
                true,
                false,
            ));
        });
    }
    // Reattach must complete while the PTY writer is still stalled: a wedged
    // reader must never turn reattach into an indefinite hang.
    let (new_attachment, _) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reattach blocked behind a stalled PTY writer")
        .expect("prepare replacement attachment");
    assert_eq!(
        hub.write_input_by_id(
            &session.id,
            &new_attachment.id,
            &new_attachment.client_token,
            b"NEW"
        ),
        InputWriteStatus::Ok
    );
    drop(writer_guard);
    let got = read_exactly(&mut fixture.read_end, 6, Duration::from_secs(5));
    assert_eq!(got, b"OLDNEW");
    session.done.close();
}

#[test]
fn websocket_pty_input_seq_enforcement() {
    let mut fixture = new_test_pty_input_session("sess-seq", "seq-att", true);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    let _loop = spawn_input_loop(&hub, &session);
    let tok = attachment.client_token.clone();
    assert_eq!(
        hub.write_input_by_id_with_seq(&session.id, &attachment.id, &tok, b"A", 1, true)
            .status,
        InputWriteStatus::Ok
    );
    assert_eq!(
        hub.write_input_by_id_with_seq(&session.id, &attachment.id, &tok, b"B", 2, true)
            .status,
        InputWriteStatus::Ok
    );
    let gap = hub.write_input_by_id_with_seq(&session.id, &attachment.id, &tok, b"D", 4, true);
    assert!(
        gap.status == InputWriteStatus::SeqGap && gap.got == 4 && gap.want == 3,
        "{gap:?}"
    );
    assert_eq!(
        read_exactly(&mut fixture.read_end, 2, Duration::from_secs(5)),
        b"AB"
    );

    let (replacement, _) = hub
        .prepare_attachment(
            &session.id,
            &attachment.id,
            80,
            24,
            true,
            "",
            "seq-token-2",
            true,
            true,
        )
        .unwrap();
    assert_eq!(
        hub.write_input_by_id_with_seq(
            &session.id,
            &replacement.id,
            &replacement.client_token,
            b"C",
            1,
            true
        )
        .status,
        InputWriteStatus::Ok
    );
    assert_eq!(
        read_exactly(&mut fixture.read_end, 1, Duration::from_secs(5)),
        b"C"
    );
    session.done.close();
}

#[test]
fn websocket_pty_saturated_ack_queue_drops_attachment() {
    let mut fixture = new_test_pty_input_session("sess-ack-full", "ack-att", true);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    for _ in 0..attachment.queue_capacity() {
        assert!(attachment.enqueue_frame(OutgoingFrame::binary(Vec::new())));
    }
    assert!(
        hub.write_input_chunk(&session, chunk(&attachment, b"x", 1, true)),
        "payload write should still succeed"
    );
    assert_eq!(
        read_exactly(&mut fixture.read_end, 1, Duration::from_secs(5)),
        b"x"
    );
    assert!(
        !hub.session_attachment_ids(&session.key)
            .contains(&attachment.id),
        "attachment with a saturated ack queue should be dropped"
    );
}

#[test]
fn websocket_pty_write_rejects_malformed_seq() {
    let fixture = new_test_pty_input_session("sess-seq-invalid", "seq-att", true);
    let server = RpcServer::builder()
        .pty_hub(Arc::clone(&fixture.hub), false)
        .frame_writer(CaptureFrameWriter::new())
        .build();
    for bad in [json!("not-a-number"), json!(-1), json!(1.5)] {
        let req = rpc_request(
            1,
            "pty.write",
            json!({
                "session_id": "sess-seq-invalid", "attachment_id": "seq-att", "client_attachment_token": "token-1",
                "data_base64": base64_encode(b"x"), "seq": bad,
            }),
        );
        let resp = server.handle_pty_write(&req);
        assert!(
            !resp.ok && resp.error_code() == "invalid_params",
            "seq={bad} response = {resp:?}"
        );
    }
}

#[test]
fn websocket_pty_input_seq_gap_notification_emits_pty_error() {
    let fixture = new_test_pty_input_session("sess-seq-event", "seq-att", true);
    let writer = CaptureFrameWriter::new();
    let server = RpcServer::builder()
        .pty_hub(Arc::clone(&fixture.hub), false)
        .frame_writer(writer.clone())
        .build();
    let req = cmuxd_remote::rpc::RpcRequest::notification("pty.write", json!({
        "session_id": "sess-seq-event", "attachment_id": "seq-att", "client_attachment_token": "token-1",
        "data_base64": base64_encode(b"gap"), "seq": 2,
    }).as_object().cloned());
    let resp = server.handle_pty_write(&req);
    assert!(
        !resp.ok && resp.error_code() == "pty_input_seq_gap",
        "{resp:?}"
    );
    server.handle_notification_response(&req, &resp).unwrap();
    let events = writer.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event, "pty.error");
    assert!(
        events[0].message.contains("got 2, want 1"),
        "{:?}",
        events[0]
    );
    assert!(
        fixture
            .hub
            .session_attachment_ids(&fixture.session.key)
            .is_empty(),
        "seq-gap error should detach the attachment"
    );
}

#[test]
fn websocket_pty_input_ack_emission() {
    let fixture = new_test_pty_input_session("sess-ack", "ack-att", true);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    for seq in 1..=10u64 {
        assert!(
            hub.write_input_chunk(&session, chunk(&attachment, b"x", seq, true)),
            "write seq {seq} failed"
        );
    }
    let frame = attachment
        .frames()
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    let event = rpc_pty_event_for_frame(&attachment, &frame);
    assert!(
        event.event == "pty.input_ack" && event.seq == 10,
        "ack event = {event:?}"
    );
    assert!(
        attachment.frames().try_recv().is_err(),
        "ack was not coalesced"
    );

    let legacy = PtyAttachment::new_for_test(
        session.key.clone(),
        "legacy-att",
        "legacy-token",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    {
        let hub_guard = hub.lock();
        session
            .state(&hub_guard)
            .attachments
            .insert(legacy.id.clone(), Arc::clone(&legacy));
    }
    assert!(hub.write_input_chunk(&session, chunk(&legacy, b"y", 1, true)));
    assert!(
        legacy.frames().try_recv().is_err(),
        "legacy attachment received unexpected ack frame"
    );
    let mut read_end = fixture.read_end;
    let _ = read_exactly(&mut read_end, 11, Duration::from_secs(5));
}

#[test]
fn websocket_pty_multi_attach_uses_smallest_resize() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(&lease_path, "a-token", "sess-resize", true, now_unix() + 60);
        let mut a = dial(url.clone()).await;
        send_auth_with_attachment(&mut a, "a-token", "sess-resize", "a", 120, 40).await;
        read_ready(&mut a).await;

        write_test_lease(&lease_path, "b-token", "sess-resize", true, now_unix() + 60);
        let mut b = dial(url).await;
        send_auth_with_attachment(&mut b, "b-token", "sess-resize", "b", 90, 30).await;
        read_ready(&mut b).await;
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_pty_size(&h, "sess-resize", 90, 30, Duration::from_secs(5))
        })
        .await
        .unwrap();

        write_control(&mut b, "resize", 0, 0).await;
        write_binary(&mut b, "printf 'BADSIZE:'; stty size\r").await;
        wait_for_binary_contains(&mut a, "BADSIZE:30 90", Duration::from_secs(5)).await;
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_size(&h, "sess-resize", 2, 90, 30, Duration::from_secs(5));
            wait_for_hub_pty_size(&h, "sess-resize", 90, 30, Duration::from_secs(5));
        })
        .await
        .unwrap();

        write_binary(&mut a, "stty size\r").await;
        wait_for_binary_contains(&mut a, "30 90", Duration::from_secs(5)).await;

        write_control(&mut b, "resize", 100, 35).await;
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_size(&h, "sess-resize", 2, 100, 35, Duration::from_secs(5));
            wait_for_hub_pty_size(&h, "sess-resize", 100, 35, Duration::from_secs(5));
        })
        .await
        .unwrap();
        write_binary(&mut a, "printf 'SIZE2:'; stty size\r").await;
        wait_for_binary_contains(&mut a, "SIZE2:35 100", Duration::from_secs(5)).await;

        let _ = b.close(None).await;
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_size(&h, "sess-resize", 1, 120, 40, Duration::from_secs(5));
            wait_for_hub_pty_size(&h, "sess-resize", 120, 40, Duration::from_secs(5));
        })
        .await
        .unwrap();
        write_binary(&mut a, "printf 'SIZE3:'; stty size\r").await;
        wait_for_binary_contains(&mut a, "SIZE3:40 120", Duration::from_secs(5)).await;
        write_binary(&mut a, "exit\r").await;
        wait_for_normal_close(&mut a, Duration::from_secs(5)).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_stress_session_cleanup_and_bounded_scrollback() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path.clone(),
        shell: "/bin/sh".to_string(),
        scrollback_limit: 4096,
        ..Default::default()
    });
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let base_threads = thread_count();
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        for i in 0..25 {
            let session_id = format!("stress-{i}");
            let token = format!("token-{i}");
            write_test_lease(&lease_path, &token, &session_id, true, now_unix() + 60);
            let mut ws = dial(url.clone()).await;
            send_auth(&mut ws, &token, &session_id, 80 + i, 24).await;
            read_ready(&mut ws).await;
            write_binary(&mut ws, "printf '%8192s\\n' x; printf '%b\\n' '\\103\\115\\125\\130\\137\\110\\117\\114\\104'; read line; exit\r").await;
            wait_for_binary_contains(&mut ws, "CMUX_HOLD", Duration::from_secs(10)).await;
            assert_eq!(hub_async.max_scrollback_bytes(), 4096, "scrollback bytes should be capped");
            write_binary(&mut ws, "\r").await;
            wait_for_normal_close(&mut ws, Duration::from_secs(5)).await;
            let h = Arc::clone(&hub_async);
            tokio::task::spawn_blocking(move || wait_for_hub_session_count(&h, 0, Duration::from_secs(5))).await.unwrap();
        }
    });
    if let Some(base) = base_threads {
        let ok = wait_until(Duration::from_secs(5), || {
            thread_count().map(|now| now <= base + 8).unwrap_or(true)
        });
        assert!(
            ok,
            "thread count = {:?}, want <= {}",
            thread_count(),
            base + 8
        );
    }
}

#[test]
fn websocket_pty_anonymous_detach_terminates_session() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "anon-token",
            "sess-anon",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth(&mut ws, "anon-token", "sess-anon", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(&mut ws, "printf 'ANON_READY\\n'\r").await;
        wait_for_binary_contains(&mut ws, "ANON_READY", Duration::from_secs(5)).await;
        let _ = ws.close(None).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_anonymous_attaches_are_isolated() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "anon-a-token",
            "sess-anon-shared",
            true,
            now_unix() + 60,
        );
        let mut a = dial(url.clone()).await;
        send_auth(&mut a, "anon-a-token", "sess-anon-shared", 80, 24).await;
        read_ready(&mut a).await;
        write_binary(
            &mut a,
            "CMUX_ANON_MARK=one; export CMUX_ANON_MARK; printf 'A_READY\\n'\r",
        )
        .await;
        wait_for_binary_contains(&mut a, "A_READY", Duration::from_secs(5)).await;

        write_test_lease(
            &lease_path,
            "anon-b-token",
            "sess-anon-shared",
            true,
            now_unix() + 60,
        );
        let mut b = dial(url).await;
        send_auth(&mut b, "anon-b-token", "sess-anon-shared", 80, 24).await;
        read_ready(&mut b).await;
        write_binary(
            &mut b,
            "printf 'B_MARK:%s\\n' \"${CMUX_ANON_MARK-unset}\"; exit\r",
        )
        .await;
        let output = wait_for_binary_contains(&mut b, "B_MARK:unset", Duration::from_secs(5)).await;
        assert!(
            !output.contains("B_MARK:one"),
            "anonymous attach reused another shell, output={output:?}"
        );
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_count(&h, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        let _ = a.close(None).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_anonymous_session_key_cannot_be_forged() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "anon-forge-token",
            "sess-forge",
            true,
            now_unix() + 60,
        );
        let mut anon = dial(url.clone()).await;
        send_auth(&mut anon, "anon-forge-token", "sess-forge", 80, 24).await;
        read_ready(&mut anon).await;
        write_binary(
            &mut anon,
            "CMUX_FORGE_MARK=anon; export CMUX_FORGE_MARK; printf 'ANON_FORGE_READY\\n'\r",
        )
        .await;
        wait_for_binary_contains(&mut anon, "ANON_FORGE_READY", Duration::from_secs(5)).await;

        write_test_lease(
            &lease_path,
            "persistent-forge-token",
            "sess-forge:anon-0",
            true,
            now_unix() + 60,
        );
        let mut persistent = dial(url).await;
        send_auth_with_attachment(
            &mut persistent,
            "persistent-forge-token",
            "sess-forge:anon-0",
            "persist",
            80,
            24,
        )
        .await;
        read_ready(&mut persistent).await;
        write_binary(
            &mut persistent,
            "printf 'PERSISTENT_FORGE:%s\\n' \"${CMUX_FORGE_MARK-unset}\"; exit\r",
        )
        .await;
        let output = wait_for_binary_contains(
            &mut persistent,
            "PERSISTENT_FORGE:unset",
            Duration::from_secs(5),
        )
        .await;
        assert!(
            !output.contains("PERSISTENT_FORGE:anon"),
            "persistent attach reused anonymous shell, output={output:?}"
        );
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_count(&h, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        let _ = anon.close(None).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_attachment_without_session_id_is_anonymous() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(&lease_path, "no-session-a-token", "", true, now_unix() + 60);
        let mut a = dial(url.clone()).await;
        send_auth_with_attachment(&mut a, "no-session-a-token", "", "same", 80, 24).await;
        read_ready(&mut a).await;
        write_binary(&mut a, "CMUX_NO_SESSION_MARK=one; export CMUX_NO_SESSION_MARK; printf 'NO_SESSION_A_READY\\n'\r").await;
        wait_for_binary_contains(&mut a, "NO_SESSION_A_READY", Duration::from_secs(5)).await;

        write_test_lease(&lease_path, "no-session-b-token", "", true, now_unix() + 60);
        let mut b = dial(url).await;
        send_auth_with_attachment(&mut b, "no-session-b-token", "", "same", 80, 24).await;
        read_ready(&mut b).await;
        write_binary(&mut b, "printf 'NO_SESSION_B:%s\\n' \"${CMUX_NO_SESSION_MARK-unset}\"; exit\r").await;
        let output = wait_for_binary_contains(&mut b, "NO_SESSION_B:unset", Duration::from_secs(5)).await;
        assert!(!output.contains("NO_SESSION_B:one"), "attachment without session_id reused another shell, output={output:?}");
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || wait_for_hub_session_count(&h, 1, Duration::from_secs(5))).await.unwrap();
        let _ = a.close(None).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_drops_backpressured_attachment() {
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/bin/sh".to_string(),
            scrollback_limit: 4096,
            session_idle_ttl: None,
        },
        None,
    );
    let key = persistent_pty_session_key("sess-backpressure");
    let attachment = PtyAttachment::new_for_test(key.clone(), "slow", "", 80, 24, 1, true, false);
    assert!(attachment.enqueue_frame(OutgoingFrame::binary(b"already queued".to_vec())));
    let session = PtySession::new_for_test(
        "sess-backpressure",
        key,
        None,
        vec![Arc::clone(&attachment)],
        80,
        24,
        false,
    );
    hub.insert_session_for_test(Arc::clone(&session));

    hub.record_and_broadcast(&session, b"overflow");
    assert_eq!(
        hub.session_debug_snapshot("sess-backpressure"),
        Some((0, 80, 24))
    );
    assert!(
        attachment.is_cancelled(),
        "backpressured attachment was not cancelled"
    );
}

#[test]
fn websocket_pty_input_backpressure_does_not_block_hub() {
    let fixture = new_test_pty_input_session("sess-input-backpressure", "att-input", false);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    let _loop = spawn_input_loop(&hub, &session);

    let payload = vec![b'x'; 64 * 1024];
    let (tx, rx) = flume::bounded(1);
    {
        let hub = Arc::clone(&hub);
        let session_id = session.id.clone();
        let attachment_id = attachment.id.clone();
        std::thread::spawn(move || {
            for _ in 0..DEFAULT_PTY_INPUT_QUEUE_CAP * 4 {
                let _ = hub.write_input_by_id(&session_id, &attachment_id, "", &payload);
            }
            let _ = tx.send(());
        });
    }
    rx.recv_timeout(Duration::from_secs(2))
        .expect("write_input_by_id blocked behind a full PTY writer");

    let (tx, rx) = flume::bounded(1);
    {
        let hub = Arc::clone(&hub);
        let session_id = session.id.clone();
        std::thread::spawn(move || {
            let _ = tx.send(hub.close_session_by_id(&session_id));
        });
    }
    assert!(rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close_session_by_id blocked behind a full PTY writer"));
    session.done.close();
}

#[test]
fn websocket_pty_write_failure_closes_connection_and_reaps_attachment() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path.clone(),
        shell: "/bin/sh".to_string(),
        scrollback_limit: 4096,
        session_idle_ttl: Some(Duration::from_millis(20)),
        ..Default::default()
    });
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    let hub_async = Arc::clone(&hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "write-fail-token",
            "sess-write-fail",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth_with_attachment(
            &mut ws,
            "write-fail-token",
            "sess-write-fail",
            "persist",
            80,
            24,
        )
        .await;
        read_ready(&mut ws).await;
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_size(&h, "sess-write-fail", 1, 80, 24, Duration::from_secs(5))
        })
        .await
        .unwrap();
        let attachment = hub_async
            .debug_attachment("sess-write-fail", "persist")
            .expect("attachment was not registered");
        // A failed frame write cancels the attachment; simulate the same
        // teardown path the writer takes.
        attachment.cancel();
        let h = Arc::clone(&hub_async);
        tokio::task::spawn_blocking(move || {
            wait_for_hub_session_count(&h, 0, Duration::from_secs(5))
        })
        .await
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, ws.next()).await {
                Ok(Some(Ok(_))) => continue,
                Ok(_) => break,
                Err(_) => panic!("client connection stayed open after server write failure"),
            }
        }
    });
}

#[test]
fn websocket_pty_input_backpressure_rejects_whole_payload() {
    let fixture = new_test_pty_input_session("sess-input-atomic", "att-input", false);
    let hub = Arc::clone(&fixture.hub);
    let session = Arc::clone(&fixture.session);
    let attachment = Arc::clone(&fixture.attachment);
    for _ in 0..DEFAULT_PTY_INPUT_QUEUE_CAP - 1 {
        session.push_input_for_test(chunk(&attachment, b"queued", 0, false));
    }
    let mut payload = vec![b'x'; DEFAULT_PTY_INPUT_CHUNK_BYTES];
    payload.push(b'y');
    assert_eq!(
        hub.write_input_by_id(
            &session.id,
            &attachment.id,
            &attachment.client_token,
            &payload
        ),
        InputWriteStatus::QueueFull
    );
    assert_eq!(
        session.input_len(),
        DEFAULT_PTY_INPUT_QUEUE_CAP - 1,
        "input queue length should be unchanged"
    );
    for chunk in session.drain_input_for_test() {
        assert!(
            !chunk.payload.contains(&b'x') && !chunk.payload.contains(&b'y'),
            "rejected payload chunk was partially enqueued"
        );
    }
    assert!(
        fixture
            .stderr
            .to_string_lossy()
            .contains("ws pty input queue full"),
        "{}",
        fixture.stderr.to_string_lossy()
    );
}

#[test]
fn websocket_pty_reaps_detached_idle_session() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path.clone(),
        shell: "/bin/sh".to_string(),
        scrollback_limit: 4096,
        session_idle_ttl: Some(Duration::from_millis(20)),
        ..Default::default()
    });
    let url = server.ws_url("/terminal");
    let hub = Arc::clone(&server.hub);
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "idle-token",
            "sess-idle",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth_with_attachment(&mut ws, "idle-token", "sess-idle", "persist", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(&mut ws, "printf 'IDLE_READY\\n'\r").await;
        wait_for_binary_contains(&mut ws, "IDLE_READY", Duration::from_secs(5)).await;
        let _ = ws.close(None).await;
    });
    wait_for_hub_session_count(&hub, 0, Duration::from_secs(5));
}

#[test]
fn websocket_pty_scrollback_does_not_retain_oversized_chunks() {
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/bin/sh".to_string(),
            scrollback_limit: 4096,
            session_idle_ttl: None,
        },
        None,
    );
    let session = PtySession::new_for_test(
        "scrollback",
        persistent_pty_session_key("scrollback"),
        None,
        vec![],
        80,
        24,
        false,
    );
    {
        let guard = hub.lock();
        let mut st = session.state(&guard);
        hub.append_scrollback_locked(&mut st, &vec![b'x'; 1 << 20]);
        assert_eq!(st.scrollback.len(), 4096);
        assert!(
            st.scrollback.capacity() <= 4096,
            "cap = {}",
            st.scrollback.capacity()
        );
        hub.append_scrollback_locked(&mut st, b"tail");
        assert_eq!(st.scrollback.len(), 4096);
        assert!(st.scrollback.capacity() <= 4096);
        assert!(st.scrollback.ends_with(b"tail"));
    }
}

#[test]
fn websocket_pty_seeds_utf8_locale_and_terminal_env() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = new_test_websocket_pty_server(&lease_path);
    let url = server.ws_url("/terminal");
    server.block_on(async move {
        write_test_lease(&lease_path, "env-token", "sess-env", true, now_unix() + 60);
        let mut ws = dial(url).await;
        send_auth(&mut ws, "env-token", "sess-env", 80, 24).await;
        read_ready(&mut ws).await;
        write_binary(&mut ws, "printf '%s\\n' \"$LANG|$LC_CTYPE|$LC_ALL|$TERM|$COLORTERM|$TERM_PROGRAM|$CMUX_REMOTE_TRANSPORT\"; locale charmap; exit\r").await;
        let output = wait_for_binary_contains(&mut ws, "|xterm-256color|truecolor|ghostty|ws", Duration::from_secs(5)).await;
        let output = if output.contains("UTF-8") { output } else { wait_for_binary_contains(&mut ws, "UTF-8", Duration::from_secs(5)).await };
        assert!(output.contains("UTF-8"), "{output:?}");
    });
}

// --- WebSocket RPC ---

async fn send_rpc_auth(ws: &mut Client, token: &str, session_id: &str) {
    let auth = WsAuthFrame {
        kind: "auth".to_string(),
        token: token.to_string(),
        session_id: session_id.to_string(),
        ..Default::default()
    };
    ws.send(Message::Text(serde_json::to_string(&auth).unwrap().into()))
        .await
        .expect("write auth");
}

async fn read_ws_rpc_frame(ws: &mut Client) -> Map<String, Value> {
    loop {
        match next_message(ws, Duration::from_secs(5)).await {
            Some(Ok(Message::Text(text))) => return parse_json_map(&text),
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn rpc_call(
    ws: &mut Client,
    req: &cmuxd_remote::rpc::RpcRequest,
    events: &mut Vec<Map<String, Value>>,
) -> Map<String, Value> {
    ws.send(Message::Text(req.to_json().into()))
        .await
        .expect("write request");
    loop {
        let frame = read_ws_rpc_frame(ws).await;
        if frame.contains_key("event") {
            events.push(frame);
            continue;
        }
        assert_eq!(
            frame.get("id"),
            req.id.as_ref(),
            "response id mismatch: {frame:?}"
        );
        return frame;
    }
}

fn rpc_test_server(dir: &tempfile::TempDir) -> (TestWsServer, String) {
    let lease_path = path_str(dir, "rpc-lease.json");
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: path_str(dir, "pty-lease.json"),
        rpc_auth_lease_file: lease_path.clone(),
        shell: "/bin/sh".to_string(),
        ..Default::default()
    });
    (server, lease_path)
}

#[test]
fn websocket_rpc_rejects_missing_and_wrong_lease() {
    let dir = temp_dir("cmux-ws-");
    let (server, lease_path) = rpc_test_server(&dir);
    let url = server.ws_url("/rpc");
    server.block_on(async move {
        let mut ws = dial(url.clone()).await;
        send_rpc_auth(&mut ws, "missing", "sess-missing").await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION
        );
        write_test_lease(
            &lease_path,
            "correct-token",
            "sess-good",
            false,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_rpc_auth(&mut ws, "wrong-token", "sess-good").await;
        assert_eq!(
            expect_close_status(&mut ws).await,
            WS_STATUS_POLICY_VIOLATION
        );
    });
    let disabled = new_test_websocket_pty_server(&path_str(&dir, "other.json"));
    let resp = ureq::get(&format!("{}/rpc", disabled.url)).call();
    assert!(matches!(resp, Err(ureq::Error::Status(404, _))), "{resp:?}");
}

#[test]
fn websocket_rpc_hello_and_proxy_round_trip() {
    let dir = temp_dir("cmux-ws-");
    let (server, lease_path) = rpc_test_server(&dir);
    let (_, upstream_port) =
        tcp_echo_upstream("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK");
    let url = server.ws_url("/rpc");
    server.block_on(async move {
        write_test_lease(&lease_path, "rpc-token", "sess-rpc", false, now_unix() + 60);
        let mut ws = dial(url).await;
        send_rpc_auth(&mut ws, "rpc-token", "sess-rpc").await;
        let ready = read_ws_rpc_frame(&mut ws).await;
        assert_eq!(map_str(&ready, "type"), "ready", "{ready:?}");
        assert_eq!(map_str(&ready, "session_id"), "sess-rpc");
        let mut events = Vec::new();
        let hello = rpc_call(&mut ws, &rpc_request(1, "hello", json!({})), &mut events).await;
        assert!(map_ok(&hello), "{hello:?}");
        let open = rpc_call(
            &mut ws,
            &rpc_request(
                2,
                "proxy.open",
                json!({"host": "127.0.0.1", "port": upstream_port}),
            ),
            &mut events,
        )
        .await;
        let stream_id = open
            .get("result")
            .and_then(|r| r.get("stream_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        assert!(!stream_id.trim().is_empty(), "{open:?}");
        let subscribe = rpc_call(
            &mut ws,
            &rpc_request(3, "proxy.stream.subscribe", json!({"stream_id": stream_id})),
            &mut events,
        )
        .await;
        assert!(map_ok(&subscribe));
        let request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        let write = rpc_call(
            &mut ws,
            &rpc_request(
                4,
                "proxy.write",
                json!({"stream_id": stream_id, "data_base64": base64_encode(request.as_bytes())}),
            ),
            &mut events,
        )
        .await;
        assert!(map_ok(&write), "{write:?}");
        let mut output = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let frame = if !events.is_empty() {
                events.remove(0)
            } else {
                read_ws_rpc_frame(&mut ws).await
            };
            match map_str(&frame, "event") {
                "proxy.stream.data" | "proxy.stream.eof" => {
                    let data = map_str(&frame, "data_base64");
                    if !data.is_empty() {
                        output.extend(base64_decode(data));
                    }
                    if map_str(&frame, "event") == "proxy.stream.eof" {
                        let text = String::from_utf8_lossy(&output);
                        assert!(
                            text.contains("200 OK") && text.contains("\r\n\r\nOK"),
                            "unexpected proxy output: {text:?}"
                        );
                        return;
                    }
                }
                "" => panic!("unexpected response while waiting for stream events: {frame:?}"),
                _ => {}
            }
        }
        panic!("timed out waiting for proxy stream events");
    });
}

#[test]
fn websocket_rpc_pty_notifications_do_not_emit_response() {
    let dir = temp_dir("cmux-ws-");
    let (server, lease_path) = rpc_test_server(&dir);
    let url = server.ws_url("/rpc");
    server.block_on(async move {
        for (method, extra) in [("pty.write", json!({"data_base64": base64_encode(b"a")})), ("pty.resize", json!({"cols": 100, "rows": 30}))] {
            write_test_lease(&lease_path, "rpc-token", "sess-rpc-notify", false, now_unix() + 60);
            let mut ws = dial(url.clone()).await;
            send_rpc_auth(&mut ws, "rpc-token", "sess-rpc-notify").await;
            let ready = read_ws_rpc_frame(&mut ws).await;
            assert_eq!(map_str(&ready, "type"), "ready");
            let mut params = json!({"session_id": "missing", "attachment_id": "missing", "client_attachment_token": "token"});
            for (k, v) in extra.as_object().unwrap() {
                params[k] = v.clone();
            }
            let payload = json!({"method": method, "params": params});
            ws.send(Message::Text(payload.to_string().into())).await.unwrap();
            let event = read_ws_rpc_frame(&mut ws).await;
            assert!(!event.contains_key("id"), "{event:?}");
            assert_eq!(map_str(&event, "event"), "pty.error");
            let mut events = Vec::new();
            let ping = rpc_call(&mut ws, &rpc_request(2, "ping", json!({})), &mut events).await;
            assert!(map_ok(&ping), "{ping:?}");
            ws.send(Message::Binary(b"nope".to_vec().into())).await.unwrap();
            assert_eq!(expect_close_status(&mut ws).await, 1003);
        }
    });
}

#[test]
fn tmux_corpus_websocket_pty_initial_size_and_resize_control() {
    let dir = temp_dir("cmux-ws-");
    let lease_path = path_str(&dir, "lease.json");
    let server = TestWsServer::start(WsServerConfig {
        pty_auth_lease_file: lease_path.clone(),
        shell: "/bin/sh".to_string(),
        ..Default::default()
    });
    let url = server.ws_url("/terminal");
    server.block_on(async move {
        write_test_lease(
            &lease_path,
            "size-token",
            "sess-size",
            true,
            now_unix() + 60,
        );
        let mut ws = dial(url).await;
        send_auth(&mut ws, "size-token", "sess-size", 40, 10).await;
        read_ready(&mut ws).await;
        write_binary(&mut ws, "printf 'SIZE1:'; stty size\r").await;
        let mut output =
            wait_for_binary_contains(&mut ws, "SIZE1:10 40", Duration::from_secs(5)).await;
        write_control(&mut ws, "resize", 100, 31).await;
        write_binary(&mut ws, "printf 'SIZE2:'; stty size; exit\r").await;
        output.push_str(
            &wait_for_binary_contains(&mut ws, "SIZE2:31 100", Duration::from_secs(5)).await,
        );
        assert!(
            output.contains("SIZE1:10 40") && output.contains("SIZE2:31 100"),
            "{output:?}"
        );
    });
}

#[test]
fn normalize_pty_size_and_lease_fuzz_seeds() {
    for (cols, rows) in [(80, 24), (0, 0), (-1, -100), (1_000_000, 1_000_000)] {
        let (c, r) = normalize_pty_size(cols, rows);
        assert!(c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION);
    }
    for seed in [
        "{\"type\":\"resize\",\"cols\":80,\"rows\":24}",
        "{\"type\":\"resize\",\"cols\":1000000,\"rows\":1000000}",
        "{\"type\":\"close\"}",
        "{\"type\":\"resize\",\"cols\":-1,\"rows\":24}",
    ] {
        let frame: WsPtyControlFrame = serde_json::from_str(seed).unwrap();
        if frame.kind == "resize" && frame.cols > 0 && frame.rows > 0 {
            let (c, r) = normalize_pty_size(frame.cols, frame.rows);
            assert!(c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION);
        }
    }
    let dir = temp_dir("cmux-lease-");
    let path = path_str(&dir, "lease.json");
    for (lease, token, session) in [
        ("{\"version\":1,\"token_sha256\":\"2bb80d537b1da3e38bd30361aa855686bde0ba2cf9c27ffb6b3874b764d66e16\",\"expires_at_unix\":4102444800,\"session_id\":\"sess\",\"single_use\":false}", "secret", "sess"),
        ("{\"version\":1,\"token_sha256\":\"bad\",\"expires_at_unix\":0,\"single_use\":true}", "secret", ""),
        ("not-json", "secret", "sess"),
    ] {
        fs::write(&path, lease).unwrap();
        let auth = WsAuthFrame { kind: "auth".to_string(), token: token.to_string(), session_id: session.to_string(), cols: 80, rows: 24, ..Default::default() };
        let _ = consume_websocket_lease(&path, &auth);
    }
}

#[allow(dead_code)]
fn silence_unused(_: &dyn FrameWriter) {}

#[test]
fn pump_session_finishes_on_zero_byte_read() {
    // Regression: a 0-byte read from the PTY master is EOF (macOS/BSD report
    // hangup that way; Linux uses EIO). The pump must finish the session
    // instead of spinning on a readable-but-empty descriptor.
    let (read_fd, write_fd) = nix::unistd::pipe().unwrap();
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/bin/sh".to_string(),
            scrollback_limit: 4096,
            session_idle_ttl: None,
        },
        None,
    );
    let master = Arc::new(PtyMaster::new(read_fd).unwrap());
    let key = persistent_pty_session_key("sess-eof");
    let attachment = PtyAttachment::new_for_test(
        key.clone(),
        "att-eof",
        "token-1",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let session = PtySession::new_for_test(
        "sess-eof",
        key,
        Some(master),
        vec![Arc::clone(&attachment)],
        80,
        24,
        true,
    );
    hub.insert_session_for_test(Arc::clone(&session));

    let pump = {
        let hub = Arc::clone(&hub);
        let session = Arc::clone(&session);
        std::thread::spawn(move || hub.pump_session(&session))
    };
    let mut writer = fs::File::from(write_fd);
    use std::io::Write as _;
    writer.write_all(b"last words").unwrap();
    drop(writer);

    assert!(
        join_with_timeout(pump, Duration::from_secs(3)).is_some(),
        "pump_session kept polling after the PTY master hit EOF"
    );
    assert!(
        session.done.is_closed(),
        "session.done should close once the pump finishes"
    );
    assert!(
        wait_until(Duration::from_secs(3), || hub.active_session_count() == 0),
        "session should be reaped after EOF"
    );
}

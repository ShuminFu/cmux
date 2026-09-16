//! Lease-gated WebSocket PTY and RPC transport plus the cloud CLI bridge.

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cmuxd_remote::cli_bridge::CloudCliBridge;
use cmuxd_remote::logger::DiscardLogger;
use cmuxd_remote::pty::{PtyHub, PtyHubConfig};
use cmuxd_remote::ws::frame::{WsConn, WsMessage};
use cmuxd_remote::ws::http::connect_websocket;
use cmuxd_remote::ws::lease::WsLease;
use cmuxd_remote::ws::{
    WsPtyServerConfig, run_websocket_pty_server, serve_listener, token_sha256_hex,
};
use common::*;
use serde_json::{Value, json};
use sha2::Digest as _;

struct Server {
    addr: String,
    dir: tempfile::TempDir,
    hub: Arc<PtyHub>,
    admin_token: String,
}

impl Server {
    fn pty_lease(&self) -> std::path::PathBuf {
        self.dir.path().join("pty.lease")
    }
    fn rpc_lease(&self) -> std::path::PathBuf {
        self.dir.path().join("rpc.lease")
    }
}

fn lease(token: &str, session: &str, single_use: bool) -> WsLease {
    WsLease {
        version: 1,
        token_sha256: token_sha256_hex(token),
        expires_at_unix: cmuxd_remote::ws::lease::unix_now() + 300,
        session_id: session.to_string(),
        single_use,
    }
}

fn write_lease(path: &std::path::Path, lease: &WsLease) {
    std::fs::write(path, format!("{}\n", serde_json::to_string(lease).unwrap())).unwrap();
}

fn start(
    with_rpc: bool,
    idle_ttl: Duration,
    scrollback: usize,
    bridge_socket: Option<&std::path::Path>,
) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: String::new(),
            scrollback_limit: scrollback,
            session_idle_ttl: idle_ttl,
        },
        Arc::new(DiscardLogger),
    );
    let admin_token = "admin-secret".to_string();
    let logger: Arc<dyn cmuxd_remote::logger::Logger> = Arc::new(DiscardLogger);
    let mut cfg = WsPtyServerConfig {
        pty_auth_lease_file: dir.path().join("pty.lease").to_string_lossy().into_owned(),
        rpc_auth_lease_file: if with_rpc {
            dir.path().join("rpc.lease").to_string_lossy().into_owned()
        } else {
            String::new()
        },
        admin_token_sha256: hex::encode(sha2::Sha256::digest(admin_token.as_bytes())),
        pty_hub: Some(hub.clone()),
        ..WsPtyServerConfig::default()
    };
    if let Some(path) = bridge_socket {
        let bridge = CloudCliBridge::new();
        bridge.start(&path.to_string_lossy(), &logger).unwrap();
        cfg.cli_bridge = Some(bridge);
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve_listener(listener, cfg, logger));
    Server { addr, dir, hub, admin_token }
}

fn http(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    let mut req =
        format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n", body.len());
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status: u16 = head.split(' ').nth(1).unwrap().parse().unwrap();
    let ctype =
        head.lines().find_map(|l| l.strip_prefix("Content-Type: ")).unwrap_or("").to_string();
    (status, ctype, body.to_string())
}

fn ws(addr: &str, path: &str) -> WsConn {
    let conn = connect_websocket(addr, path, 4 << 20).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    conn
}

fn auth(conn: &WsConn, frame: Value) {
    conn.write_text(frame.to_string().as_bytes()).unwrap();
}

fn expect_close(conn: &WsConn) -> (Option<u16>, String) {
    match conn.read() {
        Ok(WsMessage::Close { code, reason }) => (code, reason),
        other => panic!("expected close frame, got {other:?}"),
    }
}

fn expect_ready(conn: &WsConn) -> Frame {
    match conn.read() {
        Ok(WsMessage::Text(payload)) => {
            let frame = parse_frame(&String::from_utf8_lossy(&payload));
            assert_eq!(frame["type"], "ready", "{frame:?}");
            frame
        }
        other => panic!("expected ready frame, got {other:?}"),
    }
}

/// Collect binary output until `needle` appears.
fn read_output_until(conn: &WsConn, needle: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::new();
    while Instant::now() < deadline {
        conn.set_read_timeout(Some(
            deadline.saturating_duration_since(Instant::now()).max(Duration::from_millis(10)),
        ))
        .unwrap();
        match conn.read() {
            Ok(WsMessage::Binary(data)) => buf.extend_from_slice(&data),
            Ok(WsMessage::Text(_)) => {}
            Ok(WsMessage::Close { .. }) | Err(_) => break,
        }
        if String::from_utf8_lossy(&buf).contains(needle) {
            break;
        }
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    assert!(text.contains(needle), "output {text:?} lacks {needle:?}");
    text
}

/// Find the output line starting with `prefix`, tolerating the `\r` bash
/// emits before command output.
fn find_line<'a>(out: &'a str, prefix: &str) -> &'a str {
    out.split(['\r', '\n'])
        .find(|l| l.starts_with(prefix))
        .unwrap_or_else(|| panic!("no line starting with {prefix:?} in {out:?}"))
}

#[test]
fn serve_ws_requires_explicit_lease_file() {
    let err = run_websocket_pty_server(
        WsPtyServerConfig { listen_addr: "127.0.0.1:0".into(), ..WsPtyServerConfig::default() },
        Arc::new(DiscardLogger),
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "auth lease file is required");
}

#[test]
fn health_and_routing() {
    let server = start(false, Duration::ZERO, 0, None);
    let (status, ctype, body) = http(&server.addr, "GET", "/healthz", &[], b"");
    assert_eq!(
        (status, ctype.as_str(), body.as_str()),
        (200, "application/json", "{\"locked\":true,\"ok\":true}\n")
    );
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let (_, _, body) = http(&server.addr, "GET", "/healthz", &[], b"");
    assert_eq!(body, "{\"locked\":false,\"ok\":true}\n");
    let (status, _, body) = http(&server.addr, "GET", "/nope", &[], b"");
    assert_eq!((status, body.as_str()), (404, "404 page not found\n"));
    let (status, _, body) = http(&server.addr, "GET", "/rpc", &[], b"");
    assert_eq!(
        (status, body.as_str()),
        (404, "404 page not found\n"),
        "rpc is disabled without an rpc lease file"
    );
    let (status, _, body) = http(&server.addr, "GET", "/terminal", &[], b"");
    assert_eq!(status, 426);
    assert!(body.contains("does not contain Upgrade"), "{body}");
}

#[test]
fn rejects_missing_wrong_expired_and_mismatched_leases() {
    let server = start(false, Duration::ZERO, 0, None);
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"anything"}));
    assert_eq!(expect_close(&conn), (Some(1008), "no active lease".into()));
    assert_eq!(server.hub.active_session_count(), 0);

    write_lease(&server.pty_lease(), &lease("secret", "bound", false));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"wrong","session_id":"bound"}));
    assert_eq!(expect_close(&conn), (Some(1008), "lease rejected".into()));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"secret","session_id":"other"}));
    assert_eq!(expect_close(&conn), (Some(1008), "lease rejected".into()));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"nope","token":"secret"}));
    assert_eq!(expect_close(&conn), (Some(1008), "invalid auth".into()));
    let conn = ws(&server.addr, "/terminal");
    conn.write_binary(b"x").unwrap();
    assert_eq!(expect_close(&conn), (Some(1003), "auth must be text JSON".into()));

    let mut expired = lease("secret", "", false);
    expired.expires_at_unix = 1;
    write_lease(&server.pty_lease(), &expired);
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"secret"}));
    assert_eq!(expect_close(&conn), (Some(1008), "lease expired".into()));
    assert_eq!(server.hub.active_session_count(), 0, "no PTY is started before auth succeeds");
}

#[test]
fn single_use_lease_is_consumed_once_and_shell_runs_over_binary_frames() {
    let server = start(false, Duration::ZERO, 0, None);
    write_lease(&server.pty_lease(), &lease("once", "ws-1", true));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"once","session_id":"ws-1","cols":90,"rows":30}));
    let ready = expect_ready(&conn);
    assert_eq!(ready["session_id"], "ws-1");
    assert!(!server.pty_lease().exists(), "single-use lease consumed before the shell starts");
    conn.write_binary(b"stty size; printf 'marker:%s\\n' \"$CMUX_REMOTE_TRANSPORT\"\n").unwrap();
    let out = read_output_until(&conn, "marker:ws", Duration::from_secs(5));
    assert!(out.contains("30 90"), "{out}");
    conn.write_text(b"{\"type\":\"resize\",\"cols\":120,\"rows\":40}").unwrap();
    conn.write_binary(b"stty size; printf 'resized-%s\\n' marker\n").unwrap();
    let out = read_output_until(&conn, "resized-marker", Duration::from_secs(5));
    assert!(out.contains("40 120"), "{out}");

    let replay = ws(&server.addr, "/terminal");
    auth(&replay, json!({"type":"auth","token":"once","session_id":"ws-1"}));
    assert_eq!(expect_close(&replay), (Some(1008), "no active lease".into()));

    conn.write_text(b"{\"type\":\"close\"}").unwrap();
    assert_eq!(expect_close(&conn).0, Some(1000));
    assert!(wait_until(Duration::from_secs(3), || server.hub.active_session_count() == 0));
}

#[test]
fn pty_process_exit_closes_socket_normally() {
    let server = start(false, Duration::ZERO, 0, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"t"}));
    expect_ready(&conn);
    conn.write_binary(b"exit 0\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        match conn.read() {
            Ok(WsMessage::Close { code, reason }) => {
                assert_eq!((code, reason), (Some(1000), "pty closed".into()));
                break;
            }
            Ok(_) => {}
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
fn seeds_utf8_locale_and_terminal_env() {
    let server = start(false, Duration::ZERO, 0, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"t"}));
    expect_ready(&conn);
    conn.write_binary(b"printf 'env:%s|%s|%s|%s|%s\\n' \"$TERM\" \"$COLORTERM\" \"$LANG$LC_ALL$LC_CTYPE\" \"$TERM_PROGRAM\" \"$SHELL\"\n").unwrap();
    let out = read_output_until(&conn, "env:xterm-256color|", Duration::from_secs(5));
    let line = find_line(&out, "env:");
    assert!(
        line.to_uppercase().contains("UTF-8") || line.to_uppercase().contains("UTF8"),
        "{line}"
    );
    assert!(line.contains("|truecolor|"), "{line}");
    assert!(line.contains("ghostty") || line.contains("|/"), "{line}");
    conn.close_now();
}

#[test]
fn multi_attach_uses_smallest_size_and_reconnect_keeps_process() {
    let server = start(false, Duration::from_secs(3600), 0, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let a = ws(&server.addr, "/terminal");
    auth(
        &a,
        json!({"type":"auth","token":"t","session_id":"shared","attachment_id":"a","cols":120,"rows":40}),
    );
    expect_ready(&a);
    a.write_binary(b"stty -echo; printf 'pid=%s DO%sNE\\n' $$ ''\n").unwrap();
    let out = read_output_until(&a, "DONE", Duration::from_secs(5));
    let pid = find_line(&out, "pid=").split(' ').next().unwrap()[4..].to_string();

    let b = ws(&server.addr, "/terminal");
    auth(
        &b,
        json!({"type":"auth","token":"t","session_id":"shared","attachment_id":"b","cols":80,"rows":24}),
    );
    expect_ready(&b);
    a.write_binary(b"stty size; printf 'sized-%s\\n' 1\n").unwrap();
    let out = read_output_until(&a, "sized-1", Duration::from_secs(5));
    assert!(out.contains("24 80"), "smallest attachment wins: {out}");
    let snapshot = server.hub.session_snapshots();
    assert_eq!(snapshot[0]["effective_cols"], 80);
    assert_eq!(snapshot[0]["attachments"].as_array().unwrap().len(), 2);

    // Dropping the smaller attachment grows the PTY back.
    b.close_now();
    assert!(wait_until(
        Duration::from_secs(3),
        || server.hub.session_snapshots()[0]["attachments"].as_array().unwrap().len() == 1
    ));
    a.write_binary(b"stty size; printf 'sized-%s\\n' 2\n").unwrap();
    let out = read_output_until(&a, "sized-2", Duration::from_secs(5));
    assert!(out.contains("40 120"), "{out}");

    // Reconnecting with the same attachment id replays scrollback and keeps the process.
    a.close_now();
    assert!(wait_until(Duration::from_secs(3), || {
        server.hub.session_snapshots()[0]["attachments"].as_array().unwrap().is_empty()
    }));
    assert_eq!(server.hub.active_session_count(), 1);
    let c = ws(&server.addr, "/terminal");
    auth(
        &c,
        json!({"type":"auth","token":"t","session_id":"shared","attachment_id":"a","cols":120,"rows":40}),
    );
    expect_ready(&c);
    let replay = read_output_until(&c, &pid, Duration::from_secs(5));
    assert!(replay.contains("sized-2"), "{replay}");
    c.write_binary(b"printf 'again=%s DO%sNE\\n' $$ ''\n").unwrap();
    let out = read_output_until(&c, "DONE", Duration::from_secs(5));
    assert_eq!(
        find_line(&out, "again=").split(' ').next().unwrap()[6..].to_string(),
        pid,
        "same process after reconnect"
    );
    c.close_now();
}

#[test]
fn anonymous_attachments_are_isolated_and_terminate_on_detach() {
    let server = start(false, Duration::from_secs(3600), 0, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let a = ws(&server.addr, "/terminal");
    auth(&a, json!({"type":"auth","token":"t"}));
    expect_ready(&a);
    let b = ws(&server.addr, "/terminal");
    auth(&b, json!({"type":"auth","token":"t","attachment_id":"named-but-no-session"}));
    expect_ready(&b);
    assert_eq!(
        server.hub.active_session_count(),
        2,
        "attachments without an explicit session are anonymous"
    );
    assert!(server.hub.session_snapshots().is_empty(), "anonymous sessions are not listed");
    a.write_binary(b"printf 'only-%s\\n' a\n").unwrap();
    read_output_until(&a, "only-a", Duration::from_secs(5));
    b.write_binary(b"printf 'only-%s\\n' b\n").unwrap();
    let out_b = read_output_until(&b, "only-b", Duration::from_secs(5));
    assert!(!out_b.contains("only-a"), "{out_b}");
    a.close_now();
    assert!(
        wait_until(Duration::from_secs(3), || server.hub.active_session_count() == 1),
        "an anonymous detach kills its session"
    );
    b.close_now();
    assert!(wait_until(Duration::from_secs(3), || server.hub.active_session_count() == 0));
}

#[test]
fn reaps_detached_idle_session() {
    let server = start(false, Duration::from_millis(300), 0, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"t","session_id":"idle","attachment_id":"a"}));
    expect_ready(&conn);
    conn.close_now();
    assert!(
        wait_until(Duration::from_secs(3), || server.hub.active_session_count() == 0),
        "idle TTL reaps the detached session"
    );
}

#[test]
fn scrollback_stays_bounded() {
    let server = start(false, Duration::from_secs(3600), 4096, None);
    write_lease(&server.pty_lease(), &lease("t", "", false));
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"t","session_id":"big","attachment_id":"a"}));
    expect_ready(&conn);
    conn.write_binary(
        b"head -c 100000 /dev/zero | tr '\\0' 'y'; echo; printf 'end-%s\\n' marker\n",
    )
    .unwrap();
    read_output_until(&conn, "end-marker", Duration::from_secs(10));
    assert!(server.hub.max_scrollback_bytes() <= 4096, "{}", server.hub.max_scrollback_bytes());
    assert_eq!(
        server.hub.session_snapshots()[0]["scrollback_bytes"].as_u64().unwrap() as usize,
        server.hub.max_scrollback_bytes()
    );
    conn.close_now();
}

#[test]
fn admin_lease_install_requires_token_and_unlocks_attach() {
    let server = start(true, Duration::ZERO, 0, None);
    let (status, _, body) = http(&server.addr, "GET", "/admin/leases", &[], b"");
    assert_eq!((status, body.as_str()), (405, "method not allowed\n"));
    let (status, _, body) = http(&server.addr, "POST", "/admin/leases", &[], b"{}");
    assert_eq!((status, body.as_str()), (403, "forbidden\n"));
    let (status, _, _) =
        http(&server.addr, "POST", "/admin/leases", &[("Authorization", "Bearer wrong")], b"{}");
    assert_eq!(status, 403);
    let bearer = format!("Bearer {}", server.admin_token);
    let (status, _, body) =
        http(&server.addr, "POST", "/admin/leases", &[("Authorization", &bearer)], b"{");
    assert_eq!((status, body.as_str()), (400, "invalid JSON\n"));
    let (status, _, body) =
        http(&server.addr, "POST", "/admin/leases", &[("Authorization", &bearer)], b"{}");
    assert_eq!((status, body.as_str()), (400, "missing lease\n"));
    let install = json!({"pty_lease": lease("pty-tok", "ws-1", false), "rpc_lease": lease("rpc-tok", "", false)});
    let (status, ctype, body) = http(
        &server.addr,
        "POST",
        "/admin/leases",
        &[("Authorization", &bearer)],
        install.to_string().as_bytes(),
    );
    assert_eq!((status, ctype.as_str(), body.as_str()), (200, "application/json", "{\"ok\":true}"));
    let stored: WsLease =
        serde_json::from_str(&std::fs::read_to_string(server.pty_lease()).unwrap()).unwrap();
    assert_eq!(stored.session_id, "ws-1");
    assert!(server.rpc_lease().exists());
    let conn = ws(&server.addr, "/terminal");
    auth(&conn, json!({"type":"auth","token":"pty-tok","session_id":"ws-1"}));
    expect_ready(&conn);
    conn.close_now();
}

#[test]
fn admin_lease_install_accepts_ed25519_signature() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    let signing = SigningKey::from_bytes(&[9u8; 32]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = WsPtyServerConfig {
        pty_auth_lease_file: dir.path().join("pty.lease").to_string_lossy().into_owned(),
        admin_ed25519_pub_key: base64::engine::general_purpose::STANDARD
            .encode(signing.verifying_key().to_bytes()),
        ..WsPtyServerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve_listener(listener, cfg, Arc::new(DiscardLogger)));
    let body = json!({"pty_lease": lease("sig-tok", "", false)}).to_string();
    let signature =
        base64::engine::general_purpose::STANDARD.encode(signing.sign(body.as_bytes()).to_bytes());
    let (status, _, _) = http(
        &addr,
        "POST",
        "/admin/leases",
        &[("X-Cmux-Admin-Signature-Ed25519", &signature)],
        b"{\"pty_lease\":{}}",
    );
    assert_eq!(status, 403, "signature over a different body is rejected");
    let (status, _, _) = http(
        &addr,
        "POST",
        "/admin/leases",
        &[("Authorization", "Bearer anything")],
        body.as_bytes(),
    );
    assert_eq!(status, 403, "no bearer hash configured");
    let (status, _, _) = http(
        &addr,
        "POST",
        "/admin/leases",
        &[("X-Cmux-Admin-Signature-Ed25519", &signature)],
        body.as_bytes(),
    );
    assert_eq!(status, 200);
    assert!(dir.path().join("pty.lease").exists());
    let (status, _, body) = http(
        &addr,
        "POST",
        "/admin/leases",
        &[("X-Cmux-Admin-Signature-Ed25519", &signature)],
        json!({"rpc_lease": lease("x", "", false)}).to_string().as_bytes(),
    );
    assert_eq!(
        (status, body.as_str()),
        (403, "forbidden\n"),
        "signature does not cover the new body"
    );
}

#[test]
fn admin_lease_install_is_disabled_without_admin_config() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WsPtyServerConfig {
        pty_auth_lease_file: dir.path().join("pty.lease").to_string_lossy().into_owned(),
        ..WsPtyServerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve_listener(listener, cfg, Arc::new(DiscardLogger)));
    let (status, _, body) =
        http(&addr, "POST", "/admin/leases", &[("Authorization", "Bearer x")], b"{}");
    assert_eq!((status, body.as_str()), (404, "lease install disabled\n"));
}

fn rpc_conn(server: &Server, token: &str) -> WsConn {
    let conn = ws(&server.addr, "/rpc");
    auth(&conn, json!({"type":"auth","token":token}));
    let ready = expect_ready(&conn);
    assert_eq!(ready["session_id"], "default");
    conn
}

fn rpc_text(conn: &WsConn) -> Frame {
    match conn.read() {
        Ok(WsMessage::Text(payload)) => parse_frame(&String::from_utf8_lossy(&payload)),
        other => panic!("expected text frame, got {other:?}"),
    }
}

fn rpc_response(conn: &WsConn) -> Frame {
    loop {
        let frame = rpc_text(conn);
        if !frame.contains_key("event") {
            return frame;
        }
    }
}

fn rpc_event(conn: &WsConn, matches: impl Fn(&Frame) -> bool) -> Frame {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "timed out waiting for event");
        let frame = rpc_text(conn);
        if frame.contains_key("event") && matches(&frame) {
            return frame;
        }
    }
}

#[test]
fn rpc_hello_proxy_and_pty_over_websocket() {
    let server = start(true, Duration::from_secs(3600), 0, None);
    write_lease(&server.rpc_lease(), &lease("rpc-tok", "", false));
    let wrong = ws(&server.addr, "/rpc");
    auth(&wrong, json!({"type":"auth","token":"nope"}));
    assert_eq!(expect_close(&wrong), (Some(1008), "lease rejected".into()));

    let conn = rpc_conn(&server, "rpc-tok");
    conn.write_text(request(1, "hello", json!({})).as_bytes()).unwrap();
    let hello = rpc_response(&conn);
    assert!(is_ok(&hello));
    assert!(
        result_obj(&hello)["capabilities"].as_array().unwrap().iter().any(|c| c == "cli.bridge")
    );
    conn.write_text(b"not json").unwrap();
    let bad = rpc_response(&conn);
    assert_eq!(error_code(&bad), "invalid_request");
    conn.write_text(b"   ").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let echo = std::thread::spawn(move || {
        let (mut c, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4];
        c.read_exact(&mut buf).unwrap();
        c.write_all(&buf).unwrap();
        std::thread::sleep(Duration::from_millis(300));
    });
    conn.write_text(request(2, "proxy.open", json!({"host":"127.0.0.1","port":port})).as_bytes())
        .unwrap();
    let open = rpc_response(&conn);
    let stream_id = result_obj(&open)["stream_id"].as_str().unwrap().to_string();
    conn.write_text(
        request(3, "proxy.stream.subscribe", json!({"stream_id": stream_id})).as_bytes(),
    )
    .unwrap();
    assert!(is_ok(&rpc_response(&conn)));
    conn.write_text(
        request(4, "proxy.write", json!({"stream_id": stream_id, "data_base64": b64(b"ping")}))
            .as_bytes(),
    )
    .unwrap();
    assert!(is_ok(&rpc_response(&conn)));
    let data = rpc_event(&conn, |f| f["event"] == "proxy.stream.data");
    assert_eq!(unb64(&data), b"ping");
    echo.join().unwrap();
    rpc_event(&conn, |f| f["event"] == "proxy.stream.eof");

    conn.write_text(request(5, "pty.attach", json!({"session_id":"rpc-pty","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"command":"echo via-rpc; sleep 60"})).as_bytes()).unwrap();
    assert!(is_ok(&rpc_response(&conn)));
    rpc_event(&conn, |f| f["event"] == "pty.ready");
    rpc_event(&conn, |f| {
        f["event"] == "pty.data" && String::from_utf8_lossy(&unb64(f)).contains("via-rpc")
    });
    // Notifications over the WebSocket behave like stdio: error event, no response.
    conn.write_text(json!({"method":"pty.write","params":{"session_id":"missing","attachment_id":"m","client_attachment_token":"t","data_base64":"YQ=="}}).to_string().as_bytes()).unwrap();
    let err = rpc_event(&conn, |f| f["event"] == "pty.error");
    assert!(!err.contains_key("id"));
    conn.write_text(request(6, "ping", json!({})).as_bytes()).unwrap();
    assert_eq!(rpc_response(&conn)["id"], 6);
    // Closing the RPC socket detaches but the shared hub keeps the session.
    conn.close_now();
    assert!(wait_until(Duration::from_secs(3), || {
        server.hub.session_snapshots()[0]["attachments"].as_array().unwrap().is_empty()
    }));
    assert_eq!(server.hub.active_session_count(), 1);
    conn.close_now();
    let binary = rpc_conn(&server, "rpc-tok");
    binary.write_binary(b"x").unwrap();
    assert_eq!(expect_close(&binary), (Some(1003), "rpc frames must be text JSON".into()));
}

#[test]
fn cloud_cli_bridge_forwards_requests_through_rpc_event() {
    let dir = tempfile::Builder::new().prefix("bridge-").tempdir_in("/tmp").unwrap();
    let socket = dir.path().join("cli.sock");
    let server = start(true, Duration::ZERO, 0, Some(&socket));
    write_lease(&server.rpc_lease(), &lease("rpc-tok", "", false));
    let bridge = |request: &[u8]| -> String {
        let mut c = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        c.write_all(request).unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).unwrap();
        out
    };
    assert_eq!(
        bridge(b"{\"command\":\"ping\"}\n"),
        "{\"ok\":false,\"error\":{\"code\":\"cloud_cli_unavailable\",\"message\":\"no cmux app is attached to this cloud VM\"}}\n"
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777, 0o666);

    let conn = rpc_conn(&server, "rpc-tok");
    let socket_path = socket.clone();
    let requester = std::thread::spawn(move || {
        let mut c = std::os::unix::net::UnixStream::connect(&socket_path).unwrap();
        c.write_all(b"{\"command\":\"list-workspaces\"}\n").unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).unwrap();
        out
    });
    let event = rpc_event(&conn, |f| f["event"] == "cli.request");
    assert_eq!(unb64(&event), b"{\"command\":\"list-workspaces\"}\n");
    let request_id = str_field(&event, "request_id");
    assert!(request_id.starts_with("cli-"));
    conn.write_text(
        request(
            9,
            "cli.response",
            json!({"request_id": request_id, "data_base64": b64(b"{\"ok\":true,\"result\":[]}")}),
        )
        .as_bytes(),
    )
    .unwrap();
    let delivered = rpc_response(&conn);
    assert_eq!(result_obj(&delivered)["delivered"], true);
    assert_eq!(requester.join().unwrap(), "{\"ok\":true,\"result\":[]}\n");
    conn.write_text(
        request(10, "cli.response", json!({"request_id": "cli-999", "data_base64": ""})).as_bytes(),
    )
    .unwrap();
    assert_eq!(error_code(&rpc_response(&conn)), "not_found");

    let socket_path = socket.clone();
    let requester = std::thread::spawn(move || {
        let mut c = std::os::unix::net::UnixStream::connect(&socket_path).unwrap();
        c.write_all(b"{\"command\":\"x\"}\n").unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).unwrap();
        out
    });
    let event = rpc_event(&conn, |f| f["event"] == "cli.request");
    conn.write_text(json!({"method":"cli.response","params":{"request_id": str_field(&event, "request_id"), "ok": false, "error": "denied"}}).to_string().as_bytes()).unwrap();
    rpc_response(&conn);
    assert_eq!(
        requester.join().unwrap(),
        "{\"ok\":false,\"error\":{\"code\":\"cloud_cli_unavailable\",\"message\":\"denied\"}}\n"
    );
    conn.close_now();
}

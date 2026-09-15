//! Ports of the persistent-daemon tests spread across `main_test.go`,
//! `persistent_lifecycle_test.go` and `persistent_proxy_test.go`.

mod support;

use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use cmuxd_remote::persistent::*;
use cmuxd_remote::pty_hub::{PtyHub, PtyHubConfig};
use cmuxd_remote::rpc::RpcRequest;
use cmuxd_remote::util::{
    getuid, set_version, temp_dir as util_temp_dir, LogSink, SharedBuffer, StopSignal,
};
use support::*;

fn read_dir_mode(path: &str) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn persistent_daemon_rejects_invalid_slot() {
    let _env = EnvGuard::new();
    for slot in ["", ".", "..", "../nope", "bad/slot", &"a".repeat(129)] {
        assert!(
            persistent_daemon_paths_for_slot(slot).is_err(),
            "slot {slot:?} should be rejected"
        );
    }
}

#[test]
fn persistent_daemon_paths_use_short_socket_path() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    let root_base = base
        .path()
        .join("long-path-segment-".repeat(4))
        .join("daemon-root");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root_base.to_string_lossy());
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", "");
    let paths = persistent_daemon_paths_for_slot(&"a".repeat(128)).unwrap();
    assert!(
        !paths.socket.starts_with(&paths.root),
        "socket path should not live under long daemon root"
    );
    assert!(
        paths.socket.len() < 100,
        "socket path length = {}: {}",
        paths.socket.len(),
        paths.socket
    );
}

#[test]
fn persistent_daemon_paths_include_daemon_version() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &path_str(&base, "daemon-root"));
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", "");
    let old = cmuxd_remote::util::version();
    set_version("v1.2.3");
    let first = persistent_daemon_paths_for_slot("versioned-slot").unwrap();
    assert!(
        first.root.contains("/v1.2.3/"),
        "root {} should include daemon version",
        first.root
    );
    set_version("v1.2.4");
    let second = persistent_daemon_paths_for_slot("versioned-slot").unwrap();
    set_version(&old);
    assert_ne!(first.root, second.root);
    assert_ne!(first.socket, second.socket);
    assert_ne!(first.lock_file, second.lock_file);
}

#[test]
fn persistent_daemon_version_component_sanitizes() {
    let _env = EnvGuard::new();
    let old = cmuxd_remote::util::version();
    set_version("v1.0.0+build/with spaces");
    assert_eq!(
        persistent_daemon_version_component(),
        "v1.0.0_build_with_spaces"
    );
    set_version("");
    assert_eq!(persistent_daemon_version_component(), "dev");
    set_version(&"x".repeat(100));
    let long = persistent_daemon_version_component();
    assert_eq!(long.len(), 48 + 1 + 8);
    set_version(&old);
}

#[test]
fn persistent_daemon_socket_dir_override_uses_private_child() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    let parent_dir = temp_dir("cmuxd-socket-parent-");
    let socket_parent = path_str(&parent_dir, "caller-socket-dir");
    fs::create_dir_all(&socket_parent).unwrap();
    fs::set_permissions(&socket_parent, fs::Permissions::from_mode(0o755)).unwrap();
    env.set("CMUX_REMOTE_DAEMON_ROOT", &path_str(&base, "daemon-root"));
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", &socket_parent);

    let paths = persistent_daemon_paths_for_slot("override-slot").unwrap();
    let socket_dir = cmuxd_remote::util::path_dir(&paths.socket);
    assert_ne!(
        socket_dir, socket_parent,
        "socket dir should be a private child"
    );
    assert_eq!(cmuxd_remote::util::path_dir(&socket_dir), socket_parent);

    let paths = ensure_persistent_daemon_directory(paths).unwrap();
    assert_eq!(read_dir_mode(&socket_parent), 0o755);
    let socket_dir = cmuxd_remote::util::path_dir(&paths.socket);
    assert_eq!(read_dir_mode(&socket_dir), 0o700);
}

#[test]
fn persistent_daemon_socket_dir_falls_back_from_unsafe_symlink() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    let parent_dir = temp_dir("cmuxd-socket-parent-");
    let socket_parent = path_str(&parent_dir, "caller-socket-dir");
    fs::create_dir_all(&socket_parent).unwrap();
    let attacker = temp_dir("cmuxd-attacker-");
    let unsafe_target = path_str(&attacker, "attacker-dir");
    fs::create_dir_all(&unsafe_target).unwrap();
    let unsafe_child = format!("{socket_parent}/cmuxd-remote-{}", getuid());
    std::os::unix::fs::symlink(&unsafe_target, &unsafe_child).unwrap();
    env.set("CMUX_REMOTE_DAEMON_ROOT", &path_str(&base, "daemon-root"));
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", &socket_parent);

    let paths = persistent_daemon_paths_for_slot("unsafe-socket-slot").unwrap();
    assert_eq!(
        cmuxd_remote::util::path_dir(&paths.socket),
        unsafe_child,
        "precondition"
    );

    let paths = ensure_persistent_daemon_directory(paths).unwrap();
    let socket_dir = cmuxd_remote::util::path_dir(&paths.socket);
    assert_ne!(
        socket_dir, unsafe_child,
        "socket dir still points at unsafe child"
    );
    assert_eq!(
        cmuxd_remote::util::clean_path(&cmuxd_remote::util::path_dir(&socket_dir)),
        cmuxd_remote::util::clean_path(&util_temp_dir().to_string_lossy())
    );
    let info = fs::symlink_metadata(&socket_dir).unwrap();
    assert!(!info.file_type().is_symlink() && info.is_dir());
    assert_eq!(info.permissions().mode() & 0o777, 0o700);
    let stored = read_persistent_daemon_socket_dir(&paths.root).unwrap();
    assert_eq!(stored, socket_dir);
}

#[test]
fn persistent_daemon_socket_dir_replaces_invalid_stored_fallback() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    let parent_dir = temp_dir("cmuxd-socket-parent-");
    let socket_parent = path_str(&parent_dir, "caller-socket-dir");
    fs::create_dir_all(&socket_parent).unwrap();
    let attacker = temp_dir("cmuxd-attacker-");
    let unsafe_target = path_str(&attacker, "attacker-dir");
    fs::create_dir_all(&unsafe_target).unwrap();
    let unsafe_child = format!("{socket_parent}/cmuxd-remote-{}", getuid());
    std::os::unix::fs::symlink(&unsafe_target, &unsafe_child).unwrap();
    env.set("CMUX_REMOTE_DAEMON_ROOT", &path_str(&base, "daemon-root"));
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", &socket_parent);

    let paths = persistent_daemon_paths_for_slot("invalid-stored-fallback-slot").unwrap();
    fs::create_dir_all(&paths.root).unwrap();
    let invalid_stored = path_str(&attacker, "invalid-stored-socket-dir");
    fs::write(&invalid_stored, b"not a directory").unwrap();
    fs::write(
        format!("{}/socket-dir", paths.root),
        format!("{invalid_stored}\n"),
    )
    .unwrap();

    let paths = ensure_persistent_daemon_directory(paths).unwrap();
    let socket_dir = cmuxd_remote::util::path_dir(&paths.socket);
    assert_ne!(socket_dir, unsafe_child);
    assert_eq!(
        cmuxd_remote::util::clean_path(&cmuxd_remote::util::path_dir(&socket_dir)),
        cmuxd_remote::util::clean_path(&util_temp_dir().to_string_lossy())
    );
    assert_eq!(
        read_persistent_daemon_socket_dir(&paths.root).unwrap(),
        socket_dir
    );
}

#[test]
fn persistent_daemon_socket_dir_reuses_stored_fallback() {
    let mut env = EnvGuard::new();
    let base = temp_dir("cmuxd-root-");
    let parent_dir = temp_dir("cmuxd-socket-parent-");
    let socket_parent = path_str(&parent_dir, "caller-socket-dir");
    fs::create_dir_all(&socket_parent).unwrap();
    let unsafe_child = format!("{socket_parent}/cmuxd-remote-{}", getuid());
    fs::write(&unsafe_child, b"not a directory").unwrap();
    env.set("CMUX_REMOTE_DAEMON_ROOT", &path_str(&base, "daemon-root"));
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", &socket_parent);

    let paths = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("stored-fallback-slot").unwrap(),
    )
    .unwrap();
    let first_socket_dir = cmuxd_remote::util::path_dir(&paths.socket);
    let next = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("stored-fallback-slot").unwrap(),
    )
    .unwrap();
    assert_eq!(cmuxd_remote::util::path_dir(&next.socket), first_socket_dir);
}

#[test]
fn persistent_daemon_token_concurrent_create() {
    let root = temp_dir("cmuxd-token-");
    let paths = PersistentDaemonPaths {
        root: root.path().to_string_lossy().into_owned(),
        token_file: path_str(&root, "auth.token"),
        ..Default::default()
    };
    let paths = Arc::new(paths);
    let handles: Vec<_> = (0..12)
        .map(|_| {
            let paths = Arc::clone(&paths);
            std::thread::spawn(move || persistent_daemon_token(&paths))
        })
        .collect();
    let tokens: Vec<String> = handles
        .into_iter()
        .map(|h| h.join().unwrap().expect("persistent_daemon_token"))
        .collect();
    for token in &tokens {
        assert_eq!(token.len(), 64);
        assert_eq!(token, &tokens[0], "concurrent token mismatch");
    }
    let on_disk = fs::read_to_string(&paths.token_file).unwrap();
    assert_eq!(on_disk.trim(), tokens[0]);
}

#[test]
fn persistent_daemon_rejects_bad_token() {
    let mut daemon = start_persistent_daemon_for_test("good-token");
    let mut conn = UnixStream::connect(&daemon.socket_path).unwrap();
    let mut params = serde_json::Map::new();
    params.insert("token".to_string(), json!("bad-token"));
    conn.write_all(
        format!(
            "{}\n",
            RpcRequest::new(1, PERSISTENT_DAEMON_AUTH_METHOD, Some(params)).to_json()
        )
        .as_bytes(),
    )
    .unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut line = String::new();
    BufReader::new(conn.try_clone().unwrap())
        .read_line(&mut line)
        .unwrap();
    let frame = parse_json_map(line.trim());
    assert!(!map_ok(&frame), "bad token auth should fail: {frame:?}");
    assert_eq!(error_code(&frame), "unauthorized");
    drop(conn);
    daemon.stop();
}

#[test]
fn persistent_daemon_rejects_non_auth_first_frame() {
    let mut daemon = start_persistent_daemon_for_test("good-token");
    let mut conn = UnixStream::connect(&daemon.socket_path).unwrap();
    conn.write_all(b"{\"id\":1,\"method\":\"ping\",\"params\":{}}\n")
        .unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut line = String::new();
    BufReader::new(conn.try_clone().unwrap())
        .read_line(&mut line)
        .unwrap();
    let frame = parse_json_map(line.trim());
    assert_eq!(error_code(&frame), "unauthorized");
    drop(conn);
    daemon.stop();
}

#[test]
fn dial_persistent_daemon_bad_token_wraps_auth_failure() {
    let mut daemon = start_persistent_daemon_for_test("good-token");
    let err = dial_persistent_daemon(&daemon.socket_path, "bad-token")
        .expect_err("dial should fail with bad token");
    assert!(is_persistent_daemon_auth_failed(&err), "error = {err}");
    assert!(
        err.to_string().contains("invalid persistent daemon token"),
        "error = {err}"
    );
    daemon.stop();
}

#[test]
fn persistent_daemon_accepts_rotated_token_file() {
    let dir = temp_dir("cmuxd-token-");
    let token_file = path_str(&dir, "auth.token");
    fs::write(&token_file, b"old-token\n").unwrap();
    let mut daemon = start_persistent_daemon_with_verifier_for_test(
        persistent_daemon_file_token_verifier("old-token", &token_file),
    );
    fs::write(&token_file, b"new-token\n").unwrap();
    let client = PersistentClient::open(&daemon.socket_path, "new-token");
    client.close();
    daemon.stop();
}

#[test]
fn persistent_daemon_pty_write_notification_does_not_emit_response() {
    let mut daemon = start_persistent_daemon_for_test("good-token");
    let mut client = PersistentClient::open(&daemon.socket_path, "good-token");
    client.write_json(&json!({"method": "pty.write", "params": {
        "session_id": "missing", "attachment_id": "missing", "client_attachment_token": "token",
        "data_base64": base64_encode(b"a"),
    }}));
    let event = client.read_frame();
    assert!(!event.contains_key("id"), "{event:?}");
    assert_eq!(map_str(&event, "event"), "pty.error");
    let ping = client.call(&rpc_request(2, "ping", json!({})));
    assert_eq!(ping.get("id"), Some(&json!(2)));
    assert!(map_ok(&ping));
    client.close();
    daemon.stop();
}

#[test]
fn persistent_daemon_pty_resize_notification_does_not_emit_response() {
    let mut daemon = start_persistent_daemon_for_test("good-token");
    let mut client = PersistentClient::open(&daemon.socket_path, "good-token");
    client.write_json(&json!({"method": "pty.resize", "params": {
        "session_id": "missing", "attachment_id": "missing", "client_attachment_token": "token",
        "cols": 100, "rows": 30,
    }}));
    let event = client.read_frame();
    assert!(!event.contains_key("id"), "{event:?}");
    assert_eq!(map_str(&event, "event"), "pty.error");
    let ping = client.call(&rpc_request(2, "ping", json!({})));
    assert!(map_ok(&ping));
    client.close();
    daemon.stop();
}

#[test]
fn authenticate_persistent_daemon_client_read_deadline() {
    let (client, server) = UnixStream::pair().unwrap();
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(server).read_line(&mut line).map(|_| ());
        let _ = tx.send(result);
    });
    let start = std::time::Instant::now();
    let err = authenticate_persistent_daemon_client_with_timeout(
        &client,
        "token",
        Duration::from_millis(50),
    );
    assert!(err.is_err(), "expected timeout error");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "took {:?}",
        start.elapsed()
    );
    rx.recv_timeout(Duration::from_secs(1))
        .expect("server did not receive auth request")
        .expect("read auth request");
}

#[test]
fn authenticate_persistent_daemon_server_read_deadline() {
    let (client, server) = UnixStream::pair().unwrap();
    let hub = PtyHub::new(PtyHubConfig::default(), None);
    let (tx, rx) = flume::bounded(1);
    let hub_ref = Arc::clone(&hub);
    std::thread::spawn(move || {
        handle_persistent_daemon_conn_with_auth_timeout(
            server,
            persistent_daemon_fixed_token_verifier("token"),
            hub_ref,
            Duration::from_millis(50),
            None,
            LogSink::discard(),
        );
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(1))
        .expect("server auth handler did not return after deadline");
    drop(client);
    hub.close_all();
}

#[test]
fn persistent_stdio_proxy_returns_when_daemon_closes_first() {
    let (client, server) = UnixStream::pair().unwrap();
    let (stdin_read, stdin_write) = nix::unistd::pipe().unwrap();
    let stdin_write = fs::File::from(stdin_write);
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let _ = tx.send(proxy_persistent_daemon_conn(
            Box::new(fs::File::from(stdin_read)),
            Box::new(io::sink()),
            client,
        ));
    });
    drop(server);
    rx.recv_timeout(Duration::from_secs(1))
        .expect("proxy did not return after daemon side closed")
        .expect("proxy error");
    drop(stdin_write);
}

#[test]
fn persistent_stdio_proxy_copies_daemon_frames_then_returns_on_close() {
    let dir = short_temp_dir("cmux-proxy-test-");
    let socket_path = path_str(&dir, "proxy.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (server_done_tx, server_done_rx) = flume::bounded(1);
    std::thread::spawn(move || {
        if let Ok((mut conn, _)) = listener.accept() {
            let _ = conn.write_all(b"frame-one\n");
            let _ = conn.write_all(b"frame-two\n");
        }
        let _ = server_done_tx.send(());
    });
    let conn = UnixStream::connect(&socket_path).unwrap();
    let (stdin_read, stdin_write) = nix::unistd::pipe().unwrap();
    let stdin_write = fs::File::from(stdin_write);
    let stdout = SharedBuffer::new();
    let (tx, rx) = flume::bounded(1);
    let out = stdout.clone();
    std::thread::spawn(move || {
        let _ = tx.send(proxy_persistent_daemon_conn(
            Box::new(fs::File::from(stdin_read)),
            Box::new(out),
            conn,
        ));
    });
    rx.recv_timeout(Duration::from_secs(1))
        .expect("proxy did not return after daemon side closed")
        .expect("proxy error");
    assert_eq!(stdout.to_string_lossy(), "frame-one\nframe-two\n");
    server_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("server did not finish");
    drop(stdin_write);
}

#[test]
fn persistent_stdio_proxy_keeps_pumping_while_daemon_stays_open() {
    let dir = short_temp_dir("cmux-proxy-test-");
    let socket_path = path_str(&dir, "proxy.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (server_read_tx, server_read_rx) = flume::bounded(1);
    let (can_close_tx, can_close_rx) = flume::bounded::<()>(1);
    let (server_done_tx, server_done_rx) = flume::bounded(1);
    std::thread::spawn(move || {
        if let Ok((mut conn, _)) = listener.accept() {
            let _ = conn.write_all(b"daemon-ready\n");
            let mut buffer = [0u8; 64];
            let n = conn.read(&mut buffer).unwrap_or(0);
            let _ = server_read_tx.send(String::from_utf8_lossy(&buffer[..n]).into_owned());
            let _ = can_close_rx.recv();
        }
        let _ = server_done_tx.send(());
    });
    let conn = UnixStream::connect(&socket_path).unwrap();
    let (stdin_read, stdin_write) = nix::unistd::pipe().unwrap();
    let mut stdin_write = fs::File::from(stdin_write);
    let stdout = SharedBuffer::new();
    let (tx, rx) = flume::bounded(1);
    let out = stdout.clone();
    std::thread::spawn(move || {
        let _ = tx.send(proxy_persistent_daemon_conn(
            Box::new(fs::File::from(stdin_read)),
            Box::new(out),
            conn,
        ));
    });
    stdin_write.write_all(b"client-input\n").unwrap();
    assert_eq!(
        server_read_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server did not receive stdin data"),
        "client-input\n"
    );
    assert!(
        wait_until(Duration::from_secs(1), || stdout.to_string_lossy()
            == "daemon-ready\n"),
        "proxy did not copy daemon frame"
    );
    assert!(
        rx.try_recv().is_err(),
        "proxy returned while daemon stayed open"
    );
    drop(stdin_write);
    drop(can_close_tx);
    rx.recv_timeout(Duration::from_secs(1))
        .expect("proxy did not return after stdin and daemon closed")
        .expect("proxy error");
    server_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("server did not finish");
}

#[test]
fn persistent_daemon_pty_reattach_survives_client_disconnect() {
    let mut daemon = start_persistent_daemon_for_test("reattach-token");
    let session_id = "persistent-rpc";
    let mut client1 = PersistentClient::open(&daemon.socket_path, "reattach-token");
    let attach1 = client1.call(&rpc_request(
        1,
        "pty.attach",
        json!({
            "session_id": session_id, "attachment_id": "a1", "client_attachment_token": "token-a1",
            "cols": 80, "rows": 24, "command": "printf 'persistent-rpc-data\\n'; sleep 60",
        }),
    ));
    assert!(map_ok(&attach1), "{attach1:?}");
    client1
        .read_event(|f| map_str(f, "event") == "pty.ready" && map_str(f, "attachment_id") == "a1");
    client1.read_event(|f| {
        map_str(f, "event") == "pty.data"
            && map_str(f, "attachment_id") == "a1"
            && String::from_utf8_lossy(&base64_decode(map_str(f, "data_base64")))
                .contains("persistent-rpc-data")
    });
    client1.close();

    let mut client2 = PersistentClient::open(&daemon.socket_path, "reattach-token");
    let attach2 = client2.call(&rpc_request(2, "pty.attach", json!({
        "session_id": session_id, "attachment_id": "a2", "client_attachment_token": "token-a2",
        "cols": 100, "rows": 30, "command": "printf 'should-not-run\\n'", "require_existing": true,
    })));
    assert!(map_ok(&attach2), "{attach2:?}");
    client2
        .read_event(|f| map_str(f, "event") == "pty.ready" && map_str(f, "attachment_id") == "a2");
    client2.read_event(|f| {
        map_str(f, "event") == "pty.data"
            && map_str(f, "attachment_id") == "a2"
            && String::from_utf8_lossy(&base64_decode(map_str(f, "data_base64")))
                .contains("persistent-rpc-data")
    });
    let list = client2.call(&rpc_request(3, "pty.list", json!({})));
    assert!(map_ok(&list));
    let sessions = list
        .get("result")
        .and_then(|r| r.get("sessions"))
        .and_then(|v| v.as_array())
        .unwrap()
        .clone();
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    assert_eq!(
        sessions[0].get("session_id").and_then(|v| v.as_str()),
        Some(session_id)
    );
    let close = client2.call(&rpc_request(
        4,
        "pty.close",
        json!({"session_id": session_id}),
    ));
    assert!(map_ok(&close), "{close:?}");
    client2.close();
    daemon.stop();
}

#[test]
fn persistent_daemon_ready_signal_allows_immediate_dial() {
    let mut env = EnvGuard::new();
    let (ready_read, ready_write) = nix::unistd::pipe().unwrap();
    let ready_fd = nix::unistd::dup(ready_write.as_raw_fd()).unwrap();
    env.set(PERSISTENT_DAEMON_READY_FD_ENV, &ready_fd.to_string());
    signal_persistent_daemon_ready();
    drop(ready_write);
    let mut line = String::new();
    BufReader::new(fs::File::from(ready_read))
        .read_line(&mut line)
        .unwrap();
    assert_eq!(line.trim(), "ready");
    drop(env);

    let mut daemon = start_persistent_daemon_for_test("ready-token");
    let conn = dial_persistent_daemon(&daemon.socket_path, "ready-token")
        .expect("dial after ready signal");
    drop(conn);
    daemon.stop();
}

#[test]
fn persistent_daemon_server_exits_after_empty_slot_idle_timeout() {
    let config = PersistentServerConfig {
        empty_idle_timeout: Duration::from_millis(500),
        accept_poll_step: Duration::from_millis(25),
        ..Default::default()
    };
    let mut daemon = start_persistent_daemon_with_config(
        persistent_daemon_fixed_token_verifier("idle-token"),
        Some(config),
    );
    let mut client = PersistentClient::open(&daemon.socket_path, "idle-token");
    let attach = client.call(&rpc_request(1, "pty.attach", json!({
        "session_id": "idle-session", "attachment_id": "idle-attachment", "client_attachment_token": "idle-attachment-token",
        "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(map_ok(&attach), "{attach:?}");
    client.read_event(|f| {
        map_str(f, "event") == "pty.ready" && map_str(f, "attachment_id") == "idle-attachment"
    });
    let close = client.call(&rpc_request(
        2,
        "pty.close",
        json!({"session_id": "idle-session"}),
    ));
    assert!(map_ok(&close), "{close:?}");
    client.close();
    assert!(
        daemon.exited(Duration::from_secs(2)),
        "persistent daemon did not stop after empty idle timeout"
    );
}

#[test]
fn persistent_daemon_shutdown_stops_slot_with_active_pty() {
    let mut daemon = start_persistent_daemon_for_test("shutdown-token");
    let mut client = PersistentClient::open(&daemon.socket_path, "shutdown-token");
    let attach = client.call(&rpc_request(1, "pty.attach", json!({
        "session_id": "shutdown-session", "attachment_id": "shutdown-attachment",
        "client_attachment_token": "shutdown-attachment-token", "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(map_ok(&attach), "{attach:?}");
    client.read_event(|f| {
        map_str(f, "event") == "pty.ready" && map_str(f, "attachment_id") == "shutdown-attachment"
    });
    let shutdown = client.call(&rpc_request(2, "daemon.shutdown", json!({})));
    assert!(map_ok(&shutdown), "{shutdown:?}");
    assert!(
        daemon.exited(Duration::from_secs(2)),
        "persistent daemon did not stop after daemon.shutdown"
    );
    client.close();
}

#[test]
fn run_persistent_stop_uses_slot_control_plane() {
    let mut env = EnvGuard::new();
    let root = temp_dir("cmuxd-root-");
    let socket_base = short_temp_dir("cmuxd-remote-stop-command-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root.path().to_string_lossy());
    env.set(
        "CMUX_REMOTE_DAEMON_SOCKET_DIR",
        &socket_base.path().to_string_lossy(),
    );
    let paths = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("stop-command-slot").unwrap(),
    )
    .unwrap();
    let token = persistent_daemon_token(&paths).unwrap();
    let listener = UnixListener::bind(&paths.socket).unwrap();
    let stop = StopSignal::new().unwrap();
    let stop_ref = Arc::clone(&stop);
    let verifier = persistent_daemon_fixed_token_verifier(&token);
    let join = std::thread::spawn(move || {
        serve_persistent_daemon_with_verifier(listener, verifier, LogSink::discard(), stop_ref)
    });

    let mut client = PersistentClient::open(&paths.socket, &token);
    let attach = client.call(&rpc_request(1, "pty.attach", json!({
        "session_id": "stop-command-session", "attachment_id": "stop-command-attachment",
        "client_attachment_token": "stop-command-attachment-token", "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(map_ok(&attach), "{attach:?}");
    client.read_event(|f| {
        map_str(f, "event") == "pty.ready"
            && map_str(f, "attachment_id") == "stop-command-attachment"
    });
    client.close();

    let (code, _, err) = run_daemon(
        &["serve", "--persistent-stop", "--slot", "stop-command-slot"],
        "",
    );
    assert_eq!(code, 0, "stderr = {err:?}");
    let result = join_with_timeout(join, Duration::from_secs(2))
        .expect("persistent daemon did not stop after serve --persistent-stop");
    result.expect("persistent daemon exited with error");
    stop.stop();
}

#[test]
fn persistent_daemon_shutdown_rejection_does_not_expose_remote_message() {
    let (client, mut server) = UnixStream::pair().unwrap();
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let mut reader = BufReader::new(server.try_clone().unwrap());
        if reader.read_line(&mut line).is_err() {
            let _ = tx.send(false);
            return;
        }
        let _ = server.write_all(b"{\"id\":\"shutdown\",\"ok\":false,\"error\":{\"code\":\"internal\",\"message\":\"private remote detail\"}}\n");
        let _ = tx.send(true);
    });
    let err = request_persistent_daemon_shutdown(&client).expect_err("shutdown should be rejected");
    assert_eq!(err.to_string(), "persistent daemon shutdown rejected");
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
}

#[test]
fn run_persistent_lease_port_validation() {
    let _env = EnvGuard::new();
    for args in [
        vec![
            "serve",
            "--stdio",
            "--persistent",
            "--slot",
            "slot",
            "--persistent-lease-port",
            "70000",
        ],
        vec![
            "serve",
            "--persistent-stop",
            "--slot",
            "slot",
            "--persistent-lease-port",
            "64008",
        ],
        vec!["serve", "--stdio", "--persistent-lease-port", "64008"],
    ] {
        let (code, _, err) = run_daemon(&args, "");
        assert_eq!(code, 2, "args={args:?} stderr={err:?}");
    }
}

#[test]
fn stop_persistent_daemon_waits_for_slot_lock_when_socket_is_absent() {
    let mut env = EnvGuard::new();
    let root = temp_dir("cmuxd-root-");
    let socket_base = short_temp_dir("cmuxd-remote-stop-lock-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root.path().to_string_lossy());
    env.set(
        "CMUX_REMOTE_DAEMON_SOCKET_DIR",
        &socket_base.path().to_string_lossy(),
    );
    let paths = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("stop-lock-slot").unwrap(),
    )
    .unwrap();
    let lock = SlotLockGuard::acquire(&paths.lock_file).unwrap();
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let _ = tx.send(stop_persistent_daemon("stop-lock-slot"));
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "persistent stop returned before slot ownership released"
    );
    lock.release();
    rx.recv_timeout(Duration::from_secs(2))
        .expect("persistent stop did not finish after slot ownership released")
        .expect("persistent stop failed");
}

#[test]
fn stop_persistent_daemon_handles_missing_stored_socket_directory() {
    let mut env = EnvGuard::new();
    let root = temp_dir("cmuxd-root-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root.path().to_string_lossy());
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", "");
    let paths = persistent_daemon_paths_for_slot("missing-socket-directory").unwrap();
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&paths.root)
        .unwrap();
    let stored = short_temp_dir("cmuxd-remote-missing-socket-");
    let stored_path = stored.path().to_string_lossy().into_owned();
    write_persistent_daemon_socket_dir(&paths.root, &stored_path).unwrap();
    persistent_daemon_token(&paths).unwrap();
    drop(stored);
    stop_persistent_daemon(&paths.slot).expect("stop with missing stored socket directory");
}

#[test]
fn stop_persistent_daemon_removes_stale_socket_only_after_lock_released() {
    let mut env = EnvGuard::new();
    let root = temp_dir("cmuxd-root-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root.path().to_string_lossy());
    env.set("CMUX_REMOTE_DAEMON_SOCKET_DIR", "");
    let mut paths = persistent_daemon_paths_for_slot("missing-token").unwrap();
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&paths.root)
        .unwrap();
    let stored = temp_dir("cmuxd-stored-");
    let stored_path = stored.path().to_string_lossy().into_owned();
    write_persistent_daemon_socket_dir(&paths.root, &stored_path).unwrap();
    paths.socket = format!(
        "{stored_path}/{}",
        cmuxd_remote::util::path_base(&paths.socket)
    );
    fs::write(&paths.socket, b"stale").unwrap();
    stop_persistent_daemon(&paths.slot).expect("stop with missing token");
    assert!(
        fs::symlink_metadata(&paths.socket).is_err(),
        "stale socket still exists"
    );
}

#[test]
fn wait_for_persistent_daemon_stop_times_out_when_ownership_does_not_release() {
    let dir = temp_dir("cmuxd-lock-");
    let lock_path = path_str(&dir, "daemon.lock");
    let lock = SlotLockGuard::acquire(&lock_path).unwrap();
    let err = wait_for_persistent_daemon_stop_with_timeout(
        &lock_path,
        Duration::from_millis(50),
        Duration::from_millis(5),
    )
    .expect_err("wait should time out");
    assert!(err.to_string().contains("timed out waiting"), "{err}");
    lock.release();
}

#[test]
fn persistent_daemon_reaps_active_pty_after_observed_slot_lease_disappears() {
    let lease_present = Arc::new(AtomicBool::new(true));
    let (checked_tx, checked_rx) = flume::bounded::<()>(1);
    let present = Arc::clone(&lease_present);
    let config = PersistentServerConfig {
        accept_poll_step: Duration::from_millis(10),
        slot_lease_present: Some(Box::new(move || {
            let _ = checked_tx.try_send(());
            Ok(present.load(Ordering::SeqCst))
        })),
        ..Default::default()
    };
    let mut daemon = start_persistent_daemon_with_config(
        persistent_daemon_fixed_token_verifier("lease-token"),
        Some(config),
    );
    checked_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("persistent daemon did not inspect the slot lease");
    let mut client = PersistentClient::open(&daemon.socket_path, "lease-token");
    let attach = client.call(&rpc_request(1, "pty.attach", json!({
        "session_id": "lease-session", "attachment_id": "lease-attachment", "client_attachment_token": "lease-attachment-token",
        "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(map_ok(&attach), "{attach:?}");
    client.read_event(|f| {
        map_str(f, "event") == "pty.ready" && map_str(f, "attachment_id") == "lease-attachment"
    });
    client.close();
    lease_present.store(false, Ordering::SeqCst);
    assert!(
        daemon.exited(Duration::from_secs(2)),
        "persistent daemon did not stop after its observed slot lease disappeared"
    );
}

#[test]
fn persistent_daemon_slot_lease_present_matches_exact_relay_port_and_slot() {
    let mut env = EnvGuard::new();
    let home = temp_dir("cmuxd-home-");
    env.set("HOME", &home.path().to_string_lossy());
    assert!(!persistent_daemon_slot_lease_present("target-slot", 64008).unwrap());
    let relay = home.path().join(".cmux/relay");
    fs::create_dir_all(&relay).unwrap();
    fs::write(relay.join("64008.slot"), b"other-slot\n").unwrap();
    fs::write(relay.join("64009.slot"), b"target-slot\n").unwrap();
    assert!(!persistent_daemon_slot_lease_present("target-slot", 64008).unwrap());
    fs::write(relay.join("64008.slot"), b"target-slot\n").unwrap();
    assert!(persistent_daemon_slot_lease_present("target-slot", 64008).unwrap());
    assert!(persistent_daemon_slot_lease_present("target-slot", 0).is_err());
}

#[test]
fn persistent_daemon_server_arguments_carry_validated_lease_port() {
    let with_lease = persistent_daemon_server_arguments("target-slot", 64008);
    assert_eq!(
        with_lease,
        vec![
            "serve",
            "--persistent-server",
            "--slot",
            "target-slot",
            "--persistent-lease-port",
            "64008"
        ]
    );
    let without = persistent_daemon_server_arguments("target-slot", 0);
    assert!(!without.join(" ").contains("persistent-lease-port"));
}

#[test]
fn persistent_daemon_relay_shell_cleanup_preserves_replacement_lease() {
    let mut env = EnvGuard::new();
    let home = temp_dir("cmuxd-home-");
    env.set("HOME", &home.path().to_string_lossy());
    let relay = home.path().join(".cmux/relay");
    let shell_dir = relay.join("64008.shell");
    fs::create_dir_all(&shell_dir).unwrap();
    let lease_path = relay.join("64008.slot");
    fs::write(&lease_path, b"replacement-slot\n").unwrap();
    remove_persistent_daemon_relay_shell_directory_if_unleased(64008).unwrap();
    assert!(
        shell_dir.exists(),
        "replacement owner's shell directory was removed"
    );
    fs::remove_file(&lease_path).unwrap();
    remove_persistent_daemon_relay_shell_directory_if_unleased(64008).unwrap();
    assert!(!shell_dir.exists(), "unleased shell directory still exists");
}

#[test]
fn persistent_stdio_proxy_spawns_daemon_and_round_trips() {
    // End-to-end: `serve --stdio --persistent --slot` must spawn the real
    // per-slot daemon (using this crate's binary), authenticate, and proxy
    // frames. Uses a private root/socket dir so nothing touches $HOME.
    let mut env = EnvGuard::new();
    let root = temp_dir("cmuxd-root-");
    let socket_base = short_temp_dir("cmuxd-remote-e2e-");
    env.set("CMUX_REMOTE_DAEMON_ROOT", &root.path().to_string_lossy());
    env.set(
        "CMUX_REMOTE_DAEMON_SOCKET_DIR",
        &socket_base.path().to_string_lossy(),
    );
    let slot = format!("e2e-{}", cmuxd_remote::util::random_hex(4));
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_cmuxd-remote"))
        .args(["serve", "--stdio", "--persistent", "--slot", &slot])
        .env("CMUX_REMOTE_DAEMON_ROOT", root.path())
        .env("CMUX_REMOTE_DAEMON_SOCKET_DIR", socket_base.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn proxy");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    stdin
        .write_all(b"{\"id\":1,\"method\":\"hello\",\"params\":{}}\n")
        .unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let _ = reader.read_line(&mut line);
        let _ = tx.send(line);
    });
    let line = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("no hello response from persistent proxy");
    let frame = parse_json_map(line.trim());
    assert!(map_ok(&frame), "{frame:?}");
    let caps = frame
        .get("result")
        .and_then(|r| r.get("capabilities"))
        .and_then(|c| c.as_array())
        .unwrap();
    assert!(caps.iter().any(|c| c == "pty.session.persistent_daemon"));
    drop(stdin);
    let status = child.wait().expect("wait proxy");
    assert!(status.success(), "proxy exit status {status:?}");
    let (code, _, err) = run_daemon(&["serve", "--persistent-stop", "--slot", &slot], "");
    assert_eq!(code, 0, "stop stderr = {err:?}");
}

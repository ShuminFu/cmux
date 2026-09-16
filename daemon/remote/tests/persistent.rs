//! Persistent per-slot daemon: paths, private directories, tokens, auth,
//! reattach across client disconnects, idle and lease-driven shutdown.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::IntoRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cmuxd_remote::logger::DiscardLogger;
use cmuxd_remote::persistent::*;
use cmuxd_remote::pty::{PtyHub, PtyHubConfig};
use common::*;
use serde_json::json;

struct TestDaemon {
    socket: PathBuf,
    _dir: tempfile::TempDir,
    handle: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}

impl TestDaemon {
    fn start(verifier: TokenVerifier, config: PersistentDaemonServerConfig) -> Self {
        let dir = temp_socket_dir();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let handle = std::thread::spawn(move || {
            serve_persistent_daemon_with_verifier_config(
                listener,
                verifier,
                Arc::new(DiscardLogger),
                config,
            )
        });
        Self { socket, _dir: dir, handle: Some(handle) }
    }

    fn start_fixed(token: &str) -> Self {
        Self::start(
            persistent_daemon_fixed_token_verifier(token.to_string()),
            PersistentDaemonServerConfig::default(),
        )
    }

    fn client(&self, token: &str) -> UnixClient {
        UnixClient::connect_and_auth(&self.socket, token)
    }

    /// Ask the daemon to shut down through the control plane and wait for it.
    fn stop(mut self, token: &str) {
        let mut client = self.client(token);
        let resp = client.call("shutdown", PERSISTENT_DAEMON_SHUTDOWN_METHOD, json!({}));
        assert!(is_ok(&resp), "{resp:?}");
        assert_eq!(result_obj(&resp)["shutting_down"], true);
        drop(client);
        self.join();
    }

    fn join(&mut self) {
        let handle = self.handle.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !handle.is_finished() {
            assert!(Instant::now() < deadline, "persistent daemon did not stop");
            std::thread::sleep(Duration::from_millis(10));
        }
        handle.join().unwrap().expect("persistent daemon exited with error");
    }
}

#[test]
fn rejects_invalid_slots() {
    for slot in ["", ".", "..", "../nope", "bad/slot", &"a".repeat(129)] {
        assert!(persistent_daemon_paths_for_slot(slot).is_err(), "{slot:?} should be rejected");
    }
}

#[test]
fn paths_use_short_socket_path_and_include_version() {
    let temp = tempfile::tempdir().unwrap();
    let root_base = temp.path().join("long-path-segment-".repeat(4)).join("daemon-root");
    let _env = EnvGuard::set(&[
        ("CMUX_REMOTE_DAEMON_ROOT", Some(root_base.to_str().unwrap())),
        ("CMUX_REMOTE_DAEMON_SOCKET_DIR", None),
    ]);
    let paths = persistent_daemon_paths_for_slot(&"a".repeat(128)).unwrap();
    assert!(!paths.socket.starts_with(&paths.root), "socket must not live under the long root");
    assert!(paths.socket.to_string_lossy().len() < 100, "{}", paths.socket.display());
    let version = persistent_daemon_version_component();
    assert!(
        paths.root.to_string_lossy().contains(&format!("/{version}/")),
        "{}",
        paths.root.display()
    );
    assert_eq!(paths.token_file, paths.root.join("auth.token"));
    assert_eq!(paths.lock_file, paths.root.join("daemon.lock"));
    assert_eq!(paths.log_file, paths.root.join("daemon.log"));
    let other = persistent_daemon_paths_for_slot("other").unwrap();
    assert_ne!(other.socket, paths.socket);
}

#[test]
fn socket_dir_override_uses_private_child() {
    let temp = tempfile::tempdir().unwrap();
    let root_base = temp.path().join("daemon-root");
    let socket_parent = temp.path().join("caller-socket-dir");
    std::fs::create_dir_all(&socket_parent).unwrap();
    std::fs::set_permissions(&socket_parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_REMOTE_DAEMON_ROOT", Some(root_base.to_str().unwrap())),
        ("CMUX_REMOTE_DAEMON_SOCKET_DIR", Some(socket_parent.to_str().unwrap())),
    ]);
    let paths = persistent_daemon_paths_for_slot("override-slot").unwrap();
    let socket_dir = paths.socket.parent().unwrap().to_path_buf();
    assert_ne!(socket_dir, socket_parent);
    assert_eq!(socket_dir.parent().unwrap(), socket_parent);
    let paths = ensure_persistent_daemon_directory(paths).unwrap();
    assert_eq!(std::fs::metadata(&socket_parent).unwrap().permissions().mode() & 0o777, 0o755);
    assert_eq!(
        std::fs::metadata(paths.socket.parent().unwrap()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(std::fs::metadata(&paths.root).unwrap().permissions().mode() & 0o777, 0o700);
}

#[test]
fn socket_dir_falls_back_from_unsafe_symlink_and_reuses_stored_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let root_base = temp.path().join("daemon-root");
    let socket_parent = temp.path().join("caller-socket-dir");
    std::fs::create_dir_all(&socket_parent).unwrap();
    let unsafe_target = temp.path().join("attacker-dir");
    std::fs::create_dir_all(&unsafe_target).unwrap();
    let unsafe_child = socket_parent.join(format!("cmuxd-remote-{}", cmuxd_remote::util::getuid()));
    std::os::unix::fs::symlink(&unsafe_target, &unsafe_child).unwrap();
    let tmp = temp.path().join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_REMOTE_DAEMON_ROOT", Some(root_base.to_str().unwrap())),
        ("CMUX_REMOTE_DAEMON_SOCKET_DIR", Some(socket_parent.to_str().unwrap())),
        ("TMPDIR", Some(tmp.to_str().unwrap())),
    ]);
    let paths = persistent_daemon_paths_for_slot("unsafe-socket-slot").unwrap();
    assert_eq!(paths.socket.parent().unwrap(), unsafe_child, "precondition");
    let paths = ensure_persistent_daemon_directory(paths).unwrap();
    let socket_dir = paths.socket.parent().unwrap().to_path_buf();
    assert_ne!(socket_dir, unsafe_child);
    assert_eq!(socket_dir.parent().unwrap(), tmp, "fallback lives under the temp dir");
    let info = std::fs::symlink_metadata(&socket_dir).unwrap();
    assert!(info.is_dir() && !info.file_type().is_symlink());
    assert_eq!(info.permissions().mode() & 0o777, 0o700);
    let stored = std::fs::read_to_string(paths.root.join("socket-dir")).unwrap();
    assert_eq!(stored.trim(), socket_dir.to_str().unwrap());

    // A second run reuses the stored fallback instead of creating another.
    let again = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("unsafe-socket-slot").unwrap(),
    )
    .unwrap();
    assert_eq!(again.socket.parent().unwrap(), socket_dir);

    // An invalid stored fallback is replaced.
    std::fs::write(
        paths.root.join("socket-dir"),
        format!("{}\n", temp.path().join("not-a-dir").display()),
    )
    .unwrap();
    std::fs::write(temp.path().join("not-a-dir"), "x").unwrap();
    let replaced = ensure_persistent_daemon_directory(
        persistent_daemon_paths_for_slot("unsafe-socket-slot").unwrap(),
    )
    .unwrap();
    let replaced_dir = replaced.socket.parent().unwrap().to_path_buf();
    assert_ne!(replaced_dir, unsafe_child);
    assert_eq!(replaced_dir.parent().unwrap(), tmp);
    assert_eq!(
        std::fs::read_to_string(paths.root.join("socket-dir")).unwrap().trim(),
        replaced_dir.to_str().unwrap()
    );
}

#[test]
fn token_concurrent_create_yields_one_token() {
    let temp = tempfile::tempdir().unwrap();
    let paths = PersistentDaemonPaths {
        slot: "s".into(),
        root: temp.path().to_path_buf(),
        socket: temp.path().join("s.sock"),
        token_file: temp.path().join("auth.token"),
        log_file: temp.path().join("daemon.log"),
        lock_file: temp.path().join("daemon.lock"),
    };
    let tokens: Vec<String> = std::thread::scope(|scope| {
        let handles: Vec<_> =
            (0..12).map(|_| scope.spawn(|| persistent_daemon_token(&paths).unwrap())).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(tokens[0].len(), 64);
    assert!(tokens.iter().all(|t| *t == tokens[0]), "{tokens:?}");
    let on_disk = std::fs::read_to_string(&paths.token_file).unwrap();
    assert_eq!(on_disk, format!("{}\n", tokens[0]));
    assert_eq!(std::fs::metadata(&paths.token_file).unwrap().permissions().mode() & 0o777, 0o600);
    assert!(
        std::fs::read_dir(temp.path()).unwrap().all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".auth.token")),
        "temp files cleaned up"
    );
    assert_eq!(read_persistent_daemon_token_file(&paths.token_file).unwrap(), tokens[0]);
    std::fs::write(&paths.token_file, " \n").unwrap();
    assert_eq!(
        read_persistent_daemon_token_file(&paths.token_file).unwrap_err().to_string(),
        "persistent daemon token file is empty"
    );
}

#[test]
fn rejects_bad_token_and_wraps_dial_failure() {
    let daemon = TestDaemon::start_fixed("good-token");
    let mut client = UnixClient::connect(&daemon.socket);
    let frame = client.call(1, PERSISTENT_DAEMON_AUTH_METHOD, json!({"token":"bad-token"}));
    assert!(!is_ok(&frame));
    assert_eq!(error_code(&frame), "unauthorized");
    assert_eq!(error_message(&frame), "invalid persistent daemon token");
    assert_eq!(frame["id"], 1);
    drop(client);

    let err = dial_persistent_daemon(&daemon.socket, "bad-token").expect_err("bad token must fail");
    assert_eq!(err.to_string(), format!("{AUTH_FAILED_PREFIX}: invalid persistent daemon token"));

    // A non-auth first request is rejected before anything else runs.
    let mut client = UnixClient::connect(&daemon.socket);
    let frame = client.call(7, "ping", json!({}));
    assert_eq!(error_code(&frame), "unauthorized");
    assert_eq!(error_message(&frame), "persistent daemon authentication required");
    drop(client);
    let mut client = UnixClient::connect(&daemon.socket);
    client.send_line("not json");
    let frame = client.frames.expect_next();
    assert_eq!(error_code(&frame), "invalid_request");
    drop(client);
    daemon.stop("good-token");
}

#[test]
fn accepts_rotated_token_file() {
    let temp = tempfile::tempdir().unwrap();
    let token_file = temp.path().join("auth.token");
    std::fs::write(&token_file, "old-token\n").unwrap();
    let daemon = TestDaemon::start(
        persistent_daemon_file_token_verifier("old-token".into(), token_file.clone()),
        PersistentDaemonServerConfig::default(),
    );
    std::fs::write(&token_file, "new-token\n").unwrap();
    let client = daemon.client("new-token");
    drop(client);
    assert!(dial_persistent_daemon(&daemon.socket, "old-token").is_err());
    daemon.stop("new-token");
}

#[test]
fn pty_notifications_do_not_emit_responses_over_socket() {
    let daemon = TestDaemon::start_fixed("good-token");
    let mut client = daemon.client("good-token");
    client.send_line(&json!({"method":"pty.write","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","data_base64":b64(b"a")}}).to_string());
    let event = client.frames.expect_next();
    assert!(!event.contains_key("id"));
    assert_eq!(event["event"], "pty.error");
    client.send_line(&json!({"method":"pty.resize","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","cols":100,"rows":30}}).to_string());
    let event = client.frames.expect_next();
    assert_eq!(event["event"], "pty.error");
    let ping = client.call(2, "ping", json!({}));
    assert_eq!(ping["id"], 2);
    assert!(is_ok(&ping));
    drop(client);
    daemon.stop("good-token");
}

#[test]
fn client_auth_read_deadline_is_bounded() {
    let (client, server) = UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(server).read_line(&mut line).map(|_| line)
    });
    let start = Instant::now();
    let err = authenticate_persistent_daemon_client_with_timeout(
        &client,
        "token",
        Duration::from_millis(50),
    )
    .unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(1), "took {:?}: {err}", start.elapsed());
    let line = reader.join().unwrap().unwrap();
    let frame = parse_frame(&line);
    assert_eq!(frame["method"], PERSISTENT_DAEMON_AUTH_METHOD);
    assert_eq!(frame["params"]["token"], "token");
    assert_eq!(frame["id"], "auth");
}

#[test]
fn server_auth_read_deadline_is_bounded() {
    let (_client, server) = UnixStream::pair().unwrap();
    let hub = PtyHub::new(PtyHubConfig::default(), Arc::new(DiscardLogger));
    let verifier = persistent_daemon_fixed_token_verifier("token".into());
    let start = Instant::now();
    let done = std::thread::spawn(move || {
        handle_persistent_daemon_conn_with_auth_timeout(
            server,
            &verifier,
            &hub,
            Duration::from_millis(50),
            &|| {},
        );
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while !done.is_finished() {
        assert!(Instant::now() < deadline, "server auth handler did not return after deadline");
        std::thread::sleep(Duration::from_millis(5));
    }
    done.join().unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn stdio_proxy_returns_when_daemon_closes_first() {
    let (client, server) = UnixStream::pair().unwrap();
    let (stdin_r, stdin_w) = std::io::pipe().unwrap();
    let done =
        std::thread::spawn(move || proxy_persistent_daemon_conn(stdin_r, std::io::sink(), client));
    drop(server);
    let deadline = Instant::now() + Duration::from_secs(1);
    while !done.is_finished() {
        assert!(Instant::now() < deadline, "proxy did not return after daemon side closed");
        std::thread::sleep(Duration::from_millis(5));
    }
    done.join().unwrap().unwrap();
    drop(stdin_w);
}

#[test]
fn stdio_proxy_copies_frames_until_stdin_closes() {
    let (client, server) = UnixStream::pair().unwrap();
    let (stdin_r, mut stdin_w) = std::io::pipe().unwrap();
    let (mut stdout_r, stdout_w) = std::io::pipe().unwrap();
    let done = std::thread::spawn(move || proxy_persistent_daemon_conn(stdin_r, stdout_w, client));
    let echo = std::thread::spawn(move || {
        let mut server = server;
        let mut reader = BufReader::new(server.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        server.write_all(format!("echo:{line}").as_bytes()).unwrap();
        // Wait for the client's write half to close before finishing.
        let mut rest = String::new();
        reader.read_to_string(&mut rest).unwrap();
        server.write_all(b"bye\n").unwrap();
    });
    stdin_w.write_all(b"hello\n").unwrap();
    stdin_w.flush().unwrap();
    let mut out_reader = BufReader::new(&mut stdout_r);
    let mut line = String::new();
    out_reader.read_line(&mut line).unwrap();
    assert_eq!(line, "echo:hello\n");
    drop(stdin_w);
    echo.join().unwrap();
    line.clear();
    out_reader.read_line(&mut line).unwrap();
    assert_eq!(line, "bye\n");
    done.join().unwrap().unwrap();
}

#[test]
fn pty_reattach_survives_client_disconnect() {
    let daemon = TestDaemon::start_fixed("reattach-token");
    let mut c1 = daemon.client("reattach-token");
    let attach = c1.call(1, "pty.attach", json!({"session_id":"persistent-rpc","attachment_id":"a1","client_attachment_token":"token-a1","cols":80,"rows":24,"command":"printf 'persistent-rpc-data\\n'; sleep 60"}));
    assert!(is_ok(&attach), "{attach:?}");
    c1.frames.event(|f| f["event"] == "pty.ready" && f["attachment_id"] == "a1");
    c1.frames.event(|f| {
        f["event"] == "pty.data"
            && String::from_utf8_lossy(&unb64(f)).contains("persistent-rpc-data")
    });
    drop(c1);

    let mut c2 = daemon.client("reattach-token");
    let attach2 = c2.call(2, "pty.attach", json!({"session_id":"persistent-rpc","attachment_id":"a2","client_attachment_token":"token-a2","cols":100,"rows":30,"command":"printf 'should-not-run\\n'","require_existing":true}));
    assert!(is_ok(&attach2), "{attach2:?}");
    c2.frames.event(|f| f["event"] == "pty.ready" && f["attachment_id"] == "a2");
    c2.frames.event(|f| {
        f["event"] == "pty.data"
            && f["attachment_id"] == "a2"
            && String::from_utf8_lossy(&unb64(f)).contains("persistent-rpc-data")
    });
    let list = c2.call(3, "pty.list", json!({}));
    let sessions = result_obj(&list)["sessions"].as_array().unwrap().clone();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], "persistent-rpc");
    assert_eq!(
        sessions[0]["attachments"].as_array().unwrap().len(),
        1,
        "the disconnected attachment was dropped"
    );
    assert_eq!(sessions[0]["effective_cols"], 100);
    let close = c2.call(4, "pty.close", json!({"session_id":"persistent-rpc"}));
    assert!(is_ok(&close));
    c2.frames.event(|f| f["event"] == "pty.exit" && f["attachment_id"] == "a2");
    drop(c2);
    daemon.stop("reattach-token");
}

#[test]
fn ready_signal_writes_ready_line_to_inherited_fd() {
    let (reader, writer) = rustix::pipe::pipe().unwrap();
    let raw = writer.into_raw_fd();
    let _env = EnvGuard::set(&[(PERSISTENT_DAEMON_READY_FD_ENV, Some(&raw.to_string()))]);
    signal_persistent_daemon_ready();
    let mut buf = [0u8; 16];
    let n = rustix::io::read(&reader, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"ready\n");
    // The descriptor was closed by the signal: reading again hits EOF.
    assert_eq!(rustix::io::read(&reader, &mut buf).unwrap(), 0);
    drop(_env);
    let _env = EnvGuard::set(&[(PERSISTENT_DAEMON_READY_FD_ENV, Some("2"))]);
    signal_persistent_daemon_ready(); // fd < 3 is ignored
}

#[test]
fn server_exits_after_empty_slot_idle_timeout() {
    let mut daemon = TestDaemon::start(
        persistent_daemon_fixed_token_verifier("idle-token".into()),
        PersistentDaemonServerConfig {
            empty_idle_timeout: Duration::from_millis(500),
            accept_poll_step: Duration::from_millis(25),
            ..PersistentDaemonServerConfig::default()
        },
    );
    let mut client = daemon.client("idle-token");
    let attach = client.call(1, "pty.attach", json!({"session_id":"idle-session","attachment_id":"idle-attachment","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"}));
    assert!(is_ok(&attach));
    client.frames.event(|f| f["event"] == "pty.ready");
    // An active session keeps the daemon alive past the idle timeout.
    std::thread::sleep(Duration::from_millis(800));
    assert!(
        !daemon.handle.as_ref().unwrap().is_finished(),
        "daemon must not exit while a session is active"
    );
    let close = client.call(2, "pty.close", json!({"session_id":"idle-session"}));
    assert!(is_ok(&close));
    drop(client);
    daemon.join();
}

#[test]
fn server_reaps_after_observed_slot_lease_disappears() {
    let present = Arc::new(AtomicBool::new(true));
    let removed = Arc::new(AtomicBool::new(false));
    let present_probe = Arc::clone(&present);
    let removed_flag = Arc::clone(&removed);
    let mut daemon = TestDaemon::start(
        persistent_daemon_fixed_token_verifier("lease-token".into()),
        PersistentDaemonServerConfig {
            accept_poll_step: Duration::from_millis(25),
            slot_lease_present: Some(Box::new(move || Ok(present_probe.load(Ordering::SeqCst)))),
            slot_lease_removed: Some(Box::new(move || removed_flag.store(true, Ordering::SeqCst))),
            ..PersistentDaemonServerConfig::default()
        },
    );
    let mut client = daemon.client("lease-token");
    let attach = client.call(1, "pty.attach", json!({"session_id":"leased","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"}));
    assert!(is_ok(&attach));
    client.frames.event(|f| f["event"] == "pty.ready");
    present.store(false, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !daemon.handle.as_ref().unwrap().is_finished(),
        "an active connection defers the lease-driven exit"
    );
    drop(client);
    daemon.join();
    assert!(removed.load(Ordering::SeqCst));
}

#[test]
fn shutdown_stops_slot_with_active_pty() {
    let daemon = TestDaemon::start_fixed("stop-token");
    let mut client = daemon.client("stop-token");
    let attach = client.call(1, "pty.attach", json!({"session_id":"active","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"}));
    assert!(is_ok(&attach));
    client.frames.event(|f| f["event"] == "pty.ready");
    daemon.stop("stop-token");
    // The first client's stream ends once the daemon is gone.
    assert!(wait_until(Duration::from_secs(3), || client
        .frames
        .next(Duration::from_millis(50))
        .is_none()
        && client.conn.set_nonblocking(false).is_ok()));
}

#[test]
fn request_shutdown_rejection_does_not_expose_remote_message() {
    let (client, mut server) = UnixStream::pair().unwrap();
    let responder = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(server.try_clone().unwrap()).read_line(&mut line).unwrap();
        server.write_all(b"{\"id\":\"shutdown\",\"ok\":false,\"error\":{\"code\":\"x\",\"message\":\"secret remote detail\"}}\n").unwrap();
        line
    });
    let err = request_persistent_daemon_shutdown(&client).unwrap_err();
    assert_eq!(err.to_string(), "persistent daemon shutdown rejected");
    let line = responder.join().unwrap();
    assert_eq!(parse_frame(&line)["method"], PERSISTENT_DAEMON_SHUTDOWN_METHOD);
}

fn full_daemon_env(temp: &Path) -> EnvGuard {
    let root = temp.join("root");
    let sock = temp.join("sock");
    let home = temp.join("home");
    std::fs::create_dir_all(&home).unwrap();
    EnvGuard::set(&[
        ("CMUX_REMOTE_DAEMON_ROOT", Some(root.to_str().unwrap())),
        ("CMUX_REMOTE_DAEMON_SOCKET_DIR", Some(sock.to_str().unwrap())),
        ("HOME", Some(home.to_str().unwrap())),
    ])
}

#[test]
fn run_persistent_daemon_server_and_stop_via_control_plane() {
    let temp = temp_socket_dir();
    let _env = full_daemon_env(temp.path());
    let slot = "slot-a";
    let server =
        std::thread::spawn(move || run_persistent_daemon_server(slot, 0, Arc::new(DiscardLogger)));
    let paths = persistent_daemon_paths_for_slot(slot).unwrap();
    assert!(wait_until(Duration::from_secs(5), || std::fs::metadata(&paths.token_file).is_ok()
        && existing_persistent_daemon_paths_for_slot(slot)
            .map(|(p, exists)| exists && p.socket.exists())
            .unwrap_or(false)));
    let (paths, _) = existing_persistent_daemon_paths_for_slot(slot).unwrap();
    let token = read_persistent_daemon_token_file(&paths.token_file).unwrap();
    assert_eq!(std::fs::metadata(&paths.socket).unwrap().permissions().mode() & 0o777, 0o600);
    let conn = dial_persistent_daemon(&paths.socket, &token).unwrap();
    drop(conn);
    // A second server for the same slot is refused by the lock.
    let err = run_persistent_daemon_server(slot, 0, Arc::new(DiscardLogger)).unwrap_err();
    assert_eq!(err.to_string(), "persistent daemon slot \"slot-a\" is already running");
    stop_persistent_daemon(slot).unwrap();
    server.join().unwrap().unwrap();
    assert!(!paths.socket.exists(), "socket removed after stop");
    // Stopping again is a no-op, as is stopping a slot that never existed.
    stop_persistent_daemon(slot).unwrap();
    stop_persistent_daemon("never-started").unwrap();
    assert!(stop_persistent_daemon("bad/slot").is_err());
}

#[test]
fn stop_waits_for_lock_release_and_times_out() {
    let temp = tempfile::tempdir().unwrap();
    let lock_path = temp.path().join("daemon.lock");
    wait_for_persistent_daemon_stop_with_timeout(
        &lock_path,
        Duration::from_millis(200),
        Duration::from_millis(10),
    )
    .unwrap();
    let holder = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    rustix::fs::flock(&holder, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    let start = Instant::now();
    let err = wait_for_persistent_daemon_stop_with_timeout(
        &lock_path,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "timed out waiting for persistent daemon ownership release after 100ms"
    );
    assert!(start.elapsed() >= Duration::from_millis(100));
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        drop(holder);
    });
    wait_for_persistent_daemon_stop_with_timeout(
        &lock_path,
        Duration::from_secs(5),
        Duration::from_millis(10),
    )
    .unwrap();
    releaser.join().unwrap();
    assert!(
        wait_for_persistent_daemon_stop_with_timeout(
            &lock_path,
            Duration::ZERO,
            Duration::from_millis(10)
        )
        .is_err()
    );
}

#[test]
fn stop_removes_stale_socket_only_after_lock_released() {
    let temp = temp_socket_dir();
    let _env = full_daemon_env(temp.path());
    let slot = "stale-slot";
    let paths = ensure_persistent_daemon_directory(persistent_daemon_paths_for_slot(slot).unwrap())
        .unwrap();
    let _token = persistent_daemon_token(&paths).unwrap();
    // A stale socket with no listener behind it.
    std::fs::write(&paths.socket, b"").unwrap();
    let holder = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&paths.lock_file)
        .unwrap();
    rustix::fs::flock(&holder, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        drop(holder);
    });
    stop_persistent_daemon(slot).unwrap();
    releaser.join().unwrap();
    assert!(!paths.socket.exists());

    // Missing token file: still waits on the lock and clears the socket.
    std::fs::remove_file(&paths.token_file).unwrap();
    std::fs::write(&paths.socket, b"").unwrap();
    stop_persistent_daemon(slot).unwrap();
    assert!(!paths.socket.exists());
}

#[test]
fn stop_handles_missing_stored_socket_directory() {
    let temp = temp_socket_dir();
    let _env = full_daemon_env(temp.path());
    let slot = "missing-socket-dir";
    let paths = ensure_persistent_daemon_directory(persistent_daemon_paths_for_slot(slot).unwrap())
        .unwrap();
    let gone = temp.path().join("gone-socket-dir");
    std::fs::write(paths.root.join("socket-dir"), format!("{}\n", gone.display())).unwrap();
    let (resolved, exists) = existing_persistent_daemon_paths_for_slot(slot).unwrap();
    assert!(exists);
    assert_eq!(resolved.socket.parent().unwrap(), gone);
    stop_persistent_daemon(slot).unwrap();
}

#[test]
fn relay_lease_helpers() {
    let temp = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(&[("HOME", Some(temp.path().to_str().unwrap()))]);
    assert!(persistent_daemon_relay_path(0, ".slot").is_err());
    let lease = persistent_daemon_relay_path(4242, ".slot").unwrap();
    assert_eq!(lease, temp.path().join(".cmux").join("relay").join("4242.slot"));
    assert!(!persistent_daemon_slot_lease_present("slot-a", 4242).unwrap());
    std::fs::create_dir_all(lease.parent().unwrap()).unwrap();
    std::fs::write(&lease, "slot-a\n").unwrap();
    assert!(persistent_daemon_slot_lease_present("slot-a", 4242).unwrap());
    assert!(!persistent_daemon_slot_lease_present("slot-b", 4242).unwrap());
    assert!(!persistent_daemon_slot_lease_present("slot-a", 4243).unwrap());
    std::fs::remove_file(&lease).unwrap();
    std::fs::create_dir_all(&lease).unwrap();
    let err = persistent_daemon_slot_lease_present("slot-a", 4242).unwrap_err();
    assert!(err.to_string().contains("is not a private regular file"), "{err}");
    std::fs::remove_dir(&lease).unwrap();

    let shell = persistent_daemon_relay_path(4242, ".shell").unwrap();
    std::fs::create_dir_all(shell.join("nested")).unwrap();
    std::fs::write(&lease, "replacement\n").unwrap();
    remove_persistent_daemon_relay_shell_directory_if_unleased(4242).unwrap();
    assert!(shell.exists(), "a replacement lease preserves the shell dir");
    std::fs::remove_file(&lease).unwrap();
    remove_persistent_daemon_relay_shell_directory_if_unleased(4242).unwrap();
    assert!(!shell.exists());
    remove_persistent_daemon_relay_shell_directory_if_unleased(4242).unwrap();
}

#[test]
fn server_arguments_carry_lease_port() {
    assert_eq!(
        persistent_daemon_server_arguments("s", 0),
        vec!["serve", "--persistent-server", "--slot", "s"]
    );
    assert_eq!(
        persistent_daemon_server_arguments("s", 7),
        vec!["serve", "--persistent-server", "--slot", "s", "--persistent-lease-port", "7"]
    );
}

//! Ports of the stdio/RPC-level tests in `main_test.go`.

mod support;

use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};

use cmuxd_remote::pty_hub::{
    enqueue_pty_replay, persistent_pty_session_key, PtyAttachment, PtyHub, PtyHubConfig,
    PtySession, DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES, DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
};
use cmuxd_remote::rpc::{
    get_int_param, rpc_pty_attachment_key, rpc_pty_event_for_frame, ProxyStream, RpcResponse,
    RpcServer, StdioFrameWriter, MAX_RPC_FRAME_BYTES,
};
use cmuxd_remote::util::{LogSink, SharedBuffer};
use support::*;

fn assert_effective_size(resp: &RpcResponse, want_cols: i64, want_rows: i64) {
    assert!(resp.ok, "expected ok response, got error: {resp:?}");
    let result = resp.result_object().expect("response missing result map");
    let cols = as_int(result.get("effective_cols"), "effective_cols");
    let rows = as_int(result.get("effective_rows"), "effective_rows");
    assert!(
        cols == want_cols && rows == want_rows,
        "effective size = {cols}x{rows}, want {want_cols}x{want_rows} payload={result:?}"
    );
}

fn assert_attachment_count(resp: &RpcResponse, want: usize) {
    let result = resp.result_object().expect("response missing result map");
    let attachments = result
        .get("attachments")
        .and_then(|v| v.as_array())
        .expect("attachments array");
    assert_eq!(attachments.len(), want, "payload={result:?}");
}

fn server_with_output(hub: Option<Arc<PtyHub>>) -> (Arc<RpcServer>, SharedBuffer) {
    let output = SharedBuffer::new();
    let writer = StdioFrameWriter::new(Box::new(output.clone()));
    let mut builder = RpcServer::builder()
        .frame_writer(writer)
        .stderr(LogSink::discard());
    if let Some(hub) = hub {
        builder = builder.pty_hub(hub, true);
    }
    (builder.build(), output)
}

fn pty_hub_for_test() -> Arc<PtyHub> {
    PtyHub::new(
        PtyHubConfig {
            scrollback_limit: 4096,
            session_idle_ttl: Some(Duration::from_secs(3600)),
            ..Default::default()
        },
        None,
    )
}

#[test]
fn run_version() {
    let (code, out, _) = run_daemon(&["version"], "");
    assert_eq!(code, 0);
    assert!(!out.trim().is_empty(), "version output should not be empty");
}

#[test]
fn run_without_args_prints_usage() {
    let (code, _, err) = run_daemon(&[], "");
    assert_eq!(code, 2);
    assert!(err.contains("Usage:"), "stderr = {err:?}");
}

#[test]
fn wrapper_binary_dispatches_into_cli() {
    let mock = start_mock_socket("PONG");
    let dir = temp_dir("cmuxd-wrapper-");
    let wrapper = dir.path().join("cmuxd-remote-current");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_cmuxd-remote"), &wrapper)
        .expect("symlink wrapper");
    let output = std::process::Command::new(&wrapper)
        .args(["--socket", &mock.path, "ping"])
        .output()
        .expect("wrapper invocation");
    assert!(
        output.status.success(),
        "wrapper invocation failed: {:?}",
        output
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "PONG");

    let cmux_link = dir.path().join("cmux");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_cmuxd-remote"), &cmux_link)
        .expect("symlink cmux");
    let output = std::process::Command::new(&cmux_link)
        .args(["--socket", &mock.path, "ping"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "PONG");
}

#[test]
fn stdio_hello_and_ping() {
    let input = "{\"id\":1,\"method\":\"hello\",\"params\":{}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let first = parse_json_map(&lines[0]);
    assert!(map_ok(&first), "{first:?}");
    let result = first
        .get("result")
        .and_then(|v| v.as_object())
        .expect("result");
    let capabilities: Vec<&str> = result
        .get("capabilities")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .filter_map(|c| c.as_str())
        .collect();
    assert!(capabilities.len() >= 2);
    for expected in [
        "proxy.stream.push",
        "pty.session.persistent_daemon",
        "pty.write.notification",
        "pty.resize.notification",
        "pty.input.seq_ack",
    ] {
        assert!(
            capabilities.contains(&expected),
            "hello should advertise {expected}: {result:?}"
        );
    }
    assert_eq!(map_str(result, "name"), "cmuxd-remote");
    let second = parse_json_map(&lines[1]);
    assert!(map_ok(&second));
}

#[test]
fn stdio_pty_write_notification_does_not_emit_response() {
    let input = "{\"method\":\"pty.write\",\"params\":{\"session_id\":\"missing\",\"attachment_id\":\"missing\",\"client_attachment_token\":\"token\",\"data_base64\":\"YQ==\"}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let event = parse_json_map(&lines[0]);
    assert!(
        !event.contains_key("id"),
        "notification should not emit an RPC response id: {event:?}"
    );
    assert_eq!(map_str(&event, "event"), "pty.error");
    let response = parse_json_map(&lines[1]);
    assert_eq!(response.get("id"), Some(&json!(2)));
    assert!(map_ok(&response));
}

#[test]
fn stdio_pty_resize_notification_does_not_emit_response() {
    let input = "{\"method\":\"pty.resize\",\"params\":{\"session_id\":\"missing\",\"attachment_id\":\"missing\",\"client_attachment_token\":\"token\",\"cols\":100,\"rows\":30}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let event = parse_json_map(&lines[0]);
    assert!(!event.contains_key("id"));
    assert_eq!(map_str(&event, "event"), "pty.error");
    let response = parse_json_map(&lines[1]);
    assert_eq!(response.get("id"), Some(&json!(2)));
    assert!(map_ok(&response));
}

#[test]
fn stdio_no_id_non_pty_request_still_emits_response() {
    let (code, out, _) = run_daemon(
        &["serve", "--stdio"],
        "{\"method\":\"ping\",\"params\":{}}\n",
    );
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 1);
    assert!(map_ok(&parse_json_map(&lines[0])));
}

#[test]
fn stdio_null_id_pty_write_still_emits_response() {
    let input = "{\"id\":null,\"method\":\"pty.write\",\"params\":{\"session_id\":\"missing\",\"attachment_id\":\"missing\",\"client_attachment_token\":\"token\",\"data_base64\":\"YQ==\"}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let response = parse_json_map(&lines[0]);
    assert!(
        !response.contains_key("event"),
        "id:null pty.write should emit an RPC response, got event: {response:?}"
    );
    assert!(!map_ok(&response));
    let ping = parse_json_map(&lines[1]);
    assert_eq!(ping.get("id"), Some(&json!(2)));
    assert!(map_ok(&ping));
}

#[test]
fn stdio_null_id_pty_resize_still_emits_response() {
    let input = "{\"id\":null,\"method\":\"pty.resize\",\"params\":{\"session_id\":\"missing\",\"attachment_id\":\"missing\",\"client_attachment_token\":\"token\",\"cols\":100,\"rows\":30}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let response = parse_json_map(&lines[0]);
    assert!(!response.contains_key("event"));
    assert!(!map_ok(&response));
    let ping = parse_json_map(&lines[1]);
    assert!(map_ok(&ping));
}

#[test]
fn stdio_slot_requires_persistent() {
    let (code, _, err) = run_daemon(
        &["serve", "--stdio", "--slot", "slot-without-persistent"],
        "",
    );
    assert_eq!(code, 2);
    assert!(
        err.contains("serve --slot requires --persistent"),
        "stderr = {err:?}"
    );
}

#[test]
fn serve_flag_validation_matrix() {
    let cases: Vec<(&[&str], &str)> = vec![
        (&["serve"], "serve requires exactly one of --stdio or --ws"),
        (
            &["serve", "--stdio", "--ws"],
            "serve requires exactly one of --stdio or --ws",
        ),
        (
            &["serve", "--ws", "--persistent"],
            "serve --persistent requires --stdio",
        ),
        (
            &["serve", "--stdio", "--persistent-lease-port", "5"],
            "serve --persistent-lease-port requires --persistent",
        ),
        (
            &["serve", "--stdio", "--persistent"],
            "serve --persistent requires --slot",
        ),
        (
            &["serve", "--persistent-server"],
            "serve --persistent-server requires --slot",
        ),
        (
            &["serve", "--persistent-server", "--stdio", "--slot", "x"],
            "serve --persistent-server cannot be combined",
        ),
        (
            &["serve", "--persistent-stop", "--stdio", "--slot", "x"],
            "serve --persistent-stop cannot be combined",
        ),
        (
            &["serve", "--persistent-stop"],
            "serve --persistent-stop requires --slot",
        ),
        (&["serve", "--bogus"], "flag provided but not defined"),
        (&["bogus"], "Usage:"),
    ];
    for (args, want) in cases {
        let (code, _, err) = run_daemon(args, "");
        assert_eq!(code, 2, "args={args:?} stderr={err:?}");
        assert!(
            err.contains(want),
            "args={args:?} stderr={err:?} want={want:?}"
        );
    }
}

#[test]
fn stdio_invalid_json_and_unknown_method() {
    let input = "{\"id\":1,\"method\":\"hello\",\"params\":{}\n{\"id\":2,\"method\":\"unknown\",\"params\":{}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let first = parse_json_map(&lines[0]);
    assert!(!map_ok(&first));
    assert_eq!(error_code(&first), "invalid_request");
    let second = parse_json_map(&lines[1]);
    assert!(!map_ok(&second));
    assert_eq!(error_code(&second), "method_not_found");
}

#[test]
fn stdio_session_resize_flow() {
    let input = "{\"id\":1,\"method\":\"session.open\",\"params\":{\"session_id\":\"sess-stdio\"}}\n{\"id\":2,\"method\":\"session.attach\",\"params\":{\"session_id\":\"sess-stdio\",\"attachment_id\":\"a1\",\"cols\":120,\"rows\":40}}\n{\"id\":3,\"method\":\"session.attach\",\"params\":{\"session_id\":\"sess-stdio\",\"attachment_id\":\"a2\",\"cols\":90,\"rows\":30}}\n{\"id\":4,\"method\":\"session.status\",\"params\":{\"session_id\":\"sess-stdio\"}}\n";
    let (code, out, _) = run_daemon(&["serve", "--stdio"], input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 4, "{out:?}");
    let status = parse_json_map(&lines[3]);
    assert!(map_ok(&status));
    let result = status.get("result").and_then(|v| v.as_object()).unwrap();
    assert_eq!(as_int(result.get("effective_cols"), "effective_cols"), 90);
    assert_eq!(as_int(result.get("effective_rows"), "effective_rows"), 30);
}

#[test]
fn stdio_oversized_frame_continues_serving() {
    let oversized = format!(
        "{{\"id\":1,\"method\":\"ping\",\"params\":{{\"blob\":\"{}\"}}}}",
        "a".repeat(MAX_RPC_FRAME_BYTES)
    );
    let input = format!("{oversized}\n{{\"id\":2,\"method\":\"ping\",\"params\":{{}}}}\n");
    let (code, out, _) = run_daemon(&["serve", "--stdio"], &input);
    assert_eq!(code, 0);
    let lines = lines(&out);
    assert_eq!(lines.len(), 2, "{out:?}");
    let first = parse_json_map(&lines[0]);
    assert!(!map_ok(&first));
    assert_eq!(error_code(&first), "invalid_request");
    let second = parse_json_map(&lines[1]);
    assert!(
        map_ok(&second),
        "second response should still be handled after oversized frame: {second:?}"
    );
}

#[test]
fn proxy_stream_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (done_tx, done_rx) = flume::bounded(1);
    std::thread::spawn(move || {
        if let Ok((mut conn, _)) = listener.accept() {
            let mut buffer = [0u8; 4];
            if conn.read_exact(&mut buffer).is_ok() && &buffer == b"ping" {
                let _ = conn.write_all(b"pong");
            }
        }
        let _ = done_tx.send(());
    });

    let (server, output) = server_with_output(None);
    let open = server.handle_request(&rpc_request(
        1,
        "proxy.open",
        json!({"host": "127.0.0.1", "port": port, "timeout_ms": 1000}),
    ));
    assert!(open.ok, "proxy.open failed: {open:?}");
    let stream_id = open
        .result_object()
        .and_then(|r| r.get("stream_id"))
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    assert!(!stream_id.is_empty());

    let write = server.handle_request(&rpc_request(
        2,
        "proxy.write",
        json!({"stream_id": stream_id, "data_base64": base64_encode(b"ping")}),
    ));
    assert!(write.ok, "proxy.write failed: {write:?}");
    let subscribe = server.handle_request(&rpc_request(
        3,
        "proxy.stream.subscribe",
        json!({"stream_id": stream_id}),
    ));
    assert!(subscribe.ok);
    let event = wait_for_rpc_event(&output, 0, |event| {
        map_str(event, "event") == "proxy.stream.data"
    });
    assert_eq!(base64_decode(map_str(&event, "data_base64")), b"pong");

    let close = server.handle_request(&rpc_request(
        4,
        "proxy.close",
        json!({"stream_id": stream_id}),
    ));
    assert!(close.ok);
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("proxy test server did not finish");
    server.close_all();
}

struct EofWithPayload {
    payload: Vec<u8>,
    read_once: AtomicBool,
}

impl ProxyStream for EofWithPayload {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if self.read_once.swap(true, Ordering::SeqCst) {
            return Ok(0);
        }
        let n = self.payload.len().min(buf.len());
        buf[..n].copy_from_slice(&self.payload[..n]);
        Ok(n)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn set_write_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    fn shutdown(&self) {}
}

#[test]
fn proxy_stream_eof_payload_is_not_duplicated_across_data_and_eof_events() {
    let (server, output) = server_with_output(None);
    server.insert_stream_for_test(
        "stream-1",
        Box::new(EofWithPayload {
            payload: b"tail".to_vec(),
            read_once: AtomicBool::new(false),
        }),
    );
    let resp = server.handle_request(&rpc_request(
        1,
        "proxy.stream.subscribe",
        json!({"stream_id": "stream-1"}),
    ));
    assert!(resp.ok);
    assert!(
        wait_until(Duration::from_secs(2), || rpc_event_lines(&output).len()
            >= 2),
        "events: {:?}",
        output.to_string_lossy()
    );
    let lines = rpc_event_lines(&output);
    assert_eq!(
        lines.len(),
        2,
        "expected exactly 2 stream events: {lines:?}"
    );
    let first = parse_json_map(&lines[0]);
    let second = parse_json_map(&lines[1]);
    assert_eq!(map_str(&first, "event"), "proxy.stream.data");
    assert_eq!(map_str(&second, "event"), "proxy.stream.eof");
    assert_eq!(base64_decode(map_str(&first, "data_base64")), b"tail");
    assert!(
        map_str(&second, "data_base64").is_empty(),
        "eof payload should be empty: {second:?}"
    );
    server.close_all();
}

#[test]
fn pty_rpc_session_reattach_list_and_close() {
    let (server, output) = server_with_output(Some(pty_hub_for_test()));
    let attach = server.handle_request(&rpc_request(
        1,
        "pty.attach",
        json!({
            "session_id": "pty-rpc", "attachment_id": "a1", "client_attachment_token": "token-a1",
            "cols": 80, "rows": 24, "command": "printf 'hello-rpc\\n'; sleep 60",
        }),
    ));
    assert!(attach.ok, "pty.attach failed: {attach:?}");
    let ready = wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.ready" && map_str(e, "attachment_id") == "a1"
    });
    assert_eq!(map_str(&ready, "session_id"), "pty-rpc");
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.data"
            && map_str(e, "attachment_id") == "a1"
            && String::from_utf8_lossy(&base64_decode(map_str(e, "data_base64")))
                .contains("hello-rpc")
    });

    let list = server.handle_request(&rpc_request(2, "pty.list", json!({})));
    assert!(list.ok);
    let sessions = list
        .result_object()
        .unwrap()
        .get("sessions")
        .and_then(|v| v.as_array())
        .unwrap()
        .clone();
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    assert_eq!(
        sessions[0].get("session_id").and_then(|v| v.as_str()),
        Some("pty-rpc")
    );

    let detach = server.handle_request(&rpc_request(
        3,
        "pty.detach",
        json!({
            "session_id": "pty-rpc", "attachment_id": "a1", "client_attachment_token": "token-a1",
        }),
    ));
    assert!(detach.ok, "{detach:?}");

    let before_reattach = rpc_event_lines(&output).len();
    let reattach = server.handle_request(&rpc_request(
        4,
        "pty.attach",
        json!({
            "session_id": "pty-rpc", "attachment_id": "a2", "client_attachment_token": "token-a2",
            "cols": 100, "rows": 30, "command": "printf 'should-not-run\\n'",
        }),
    ));
    assert!(reattach.ok, "{reattach:?}");
    wait_for_rpc_event(&output, before_reattach, |e| {
        map_str(e, "event") == "pty.data"
            && map_str(e, "attachment_id") == "a2"
            && String::from_utf8_lossy(&base64_decode(map_str(e, "data_base64")))
                .contains("hello-rpc")
    });

    let before_close = rpc_event_lines(&output).len();
    let close = server.handle_request(&rpc_request(
        5,
        "pty.close",
        json!({"session_id": "pty-rpc"}),
    ));
    assert!(close.ok, "{close:?}");
    wait_for_rpc_event(&output, before_close, |e| {
        map_str(e, "event") == "pty.exit" && map_str(e, "attachment_id") == "a2"
    });
    let empty = server.handle_request(&rpc_request(6, "pty.list", json!({})));
    assert!(empty.ok);
    let sessions = empty
        .result_object()
        .unwrap()
        .get("sessions")
        .and_then(|v| v.as_array())
        .unwrap()
        .clone();
    assert!(sessions.is_empty(), "{sessions:?}");
    server.close_all();
}

#[test]
fn pty_rpc_command_uses_posix_shell_for_configured_login_shell() {
    if std::fs::metadata("/usr/bin/false").is_err() {
        eprintln!("skipping: /usr/bin/false is not available");
        return;
    }
    let hub = PtyHub::new(
        PtyHubConfig {
            shell: "/usr/bin/false".to_string(),
            scrollback_limit: 4096,
            session_idle_ttl: Some(Duration::from_secs(3600)),
        },
        None,
    );
    let (server, output) = server_with_output(Some(hub));
    let attach = server.handle_request(&rpc_request(1, "pty.attach", json!({
        "session_id": "pty-posix-shell", "attachment_id": "a1", "client_attachment_token": "token-a1",
        "cols": 80, "rows": 24, "command": "printf 'posix-shell-ok\\n'; sleep 60",
    })));
    assert!(attach.ok, "{attach:?}");
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.data"
            && String::from_utf8_lossy(&base64_decode(map_str(e, "data_base64")))
                .contains("posix-shell-ok")
    });
    server.close_all();
}

#[test]
fn pty_rpc_require_existing_fails_missing_session() {
    let hub = PtyHub::new(PtyHubConfig::default(), None);
    let (server, _) = server_with_output(Some(Arc::clone(&hub)));
    let resp = server.handle_request(&rpc_request(1, "pty.attach", json!({
        "session_id": "missing-session", "attachment_id": "a1", "client_attachment_token": "token-a1",
        "cols": 80, "rows": 24, "require_existing": true,
    })));
    assert!(!resp.ok, "{resp:?}");
    assert_eq!(resp.error_code(), "pty_session_not_found");
    assert!(hub.session_snapshots().is_empty());
    server.close_all();
}

#[test]
fn rpc_server_close_all_leaves_shared_pty_hub_alive() {
    let hub = PtyHub::new(PtyHubConfig::default(), None);
    let key = persistent_pty_session_key("shared");
    let attachment = PtyAttachment::new_for_test(
        key.clone(),
        "a1",
        "",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let session = PtySession::new_for_test(
        "shared",
        key.clone(),
        None,
        vec![Arc::clone(&attachment)],
        80,
        24,
        false,
    );
    hub.insert_session_for_test(Arc::clone(&session));
    let server = RpcServer::builder()
        .pty_hub(Arc::clone(&hub), false)
        .frame_writer(StdioFrameWriter::new(Box::new(io::sink())))
        .build();
    server.track_pty_attachment(&attachment);

    server.close_all();
    assert_eq!(
        hub.active_session_count(),
        1,
        "shared PTY hub session count"
    );
    assert!(
        hub.session_attachment_ids(&key).is_empty(),
        "shared PTY hub attachment count"
    );
    assert!(
        attachment.is_cancelled(),
        "shared PTY attachment was not cancelled"
    );
    hub.remove_session_for_test(&key);
}

#[test]
fn rpc_server_untrack_pty_attachment_keeps_newer_reattach() {
    let key = persistent_pty_session_key("reattach");
    let old = PtyAttachment::new_for_test(
        key.clone(),
        "same",
        "old-token",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let new = PtyAttachment::new_for_test(
        key.clone(),
        "same",
        "new-token",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let server = RpcServer::builder().build();
    server.track_pty_attachment(&old);
    server.track_pty_attachment(&new);
    server.untrack_pty_attachment(&old);
    let tracked = server
        .tracked_pty_attachment(&rpc_pty_attachment_key(&new))
        .expect("newer attachment tracked");
    assert!(Arc::ptr_eq(&tracked, &new));
    server.untrack_pty_attachment(&new);
    assert!(server
        .tracked_pty_attachment(&rpc_pty_attachment_key(&new))
        .is_none());
}

#[test]
fn pty_rpc_pump_emits_exit_when_attachment_cancelled() {
    let (server, output) = server_with_output(None);
    let key = persistent_pty_session_key("replaced");
    let attachment = PtyAttachment::new_for_test(
        key,
        "same",
        "old-token",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let session_done = cmuxd_remote::util::DoneSignal::new();
    let server_ref = Arc::clone(&server);
    let attachment_ref = Arc::clone(&attachment);
    let done_ref = session_done.clone();
    let pump =
        std::thread::spawn(move || server_ref.pty_attachment_pump(&attachment_ref, &done_ref));
    attachment.cancel();
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.exit"
            && map_str(e, "session_id") == "replaced"
            && map_str(e, "attachment_id") == "same"
            && map_str(e, "attachment_token") == "old-token"
    });
    assert!(
        join_with_timeout(pump, Duration::from_secs(2)).is_some(),
        "PTY attachment pump did not stop after cancellation"
    );
}

#[test]
fn pty_rpc_token_rejects_stale_attachment_control() {
    let (server, output) = server_with_output(Some(pty_hub_for_test()));
    let attach_old = server.handle_request(&rpc_request(1, "pty.attach", json!({
        "session_id": "token-race", "attachment_id": "same", "client_attachment_token": "old-token",
        "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(attach_old.ok, "{attach_old:?}");
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.ready"
            && map_str(e, "attachment_id") == "same"
            && map_str(e, "attachment_token") == "old-token"
    });

    let attach_new = server.handle_request(&rpc_request(2, "pty.attach", json!({
        "session_id": "token-race", "attachment_id": "same", "client_attachment_token": "new-token",
        "cols": 100, "rows": 30, "require_existing": true,
    })));
    assert!(attach_new.ok, "{attach_new:?}");
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.exit"
            && map_str(e, "attachment_id") == "same"
            && map_str(e, "attachment_token") == "old-token"
    });
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.ready"
            && map_str(e, "attachment_id") == "same"
            && map_str(e, "attachment_token") == "new-token"
    });

    let stale_write = server.handle_request(&rpc_request(3, "pty.write", json!({
        "session_id": "token-race", "attachment_id": "same", "client_attachment_token": "old-token",
        "data_base64": base64_encode(b"stale"),
    })));
    assert!(
        !stale_write.ok && stale_write.error_code() == "not_found",
        "{stale_write:?}"
    );
    let stale_detach = server.handle_request(&rpc_request(4, "pty.detach", json!({
        "session_id": "token-race", "attachment_id": "same", "client_attachment_token": "old-token",
    })));
    assert!(
        !stale_detach.ok && stale_detach.error_code() == "not_found",
        "{stale_detach:?}"
    );
    let fresh_detach = server.handle_request(&rpc_request(5, "pty.detach", json!({
        "session_id": "token-race", "attachment_id": "same", "client_attachment_token": "new-token",
    })));
    assert!(fresh_detach.ok, "{fresh_detach:?}");
    server.close_all();
}

#[test]
fn pty_rpc_requires_attachment_token() {
    let (server, output) = server_with_output(Some(pty_hub_for_test()));
    let expect_missing = |method: &str, resp: RpcResponse| {
        assert!(
            !resp.ok && resp.error_code() == "invalid_params",
            "{method} response = {resp:?}"
        );
        assert!(
            resp.error_message()
                .contains(&format!("{method} requires client_attachment_token")),
            "{method} message = {:?}",
            resp.error_message()
        );
    };
    expect_missing("pty.attach", server.handle_request(&rpc_request(1, "pty.attach", json!({
        "session_id": "token-required", "attachment_id": "same", "cols": 80, "rows": 24, "command": "sleep 60",
    }))));
    let attach = server.handle_request(&rpc_request(2, "pty.attach", json!({
        "session_id": "token-required", "attachment_id": "same", "client_attachment_token": "fresh-token",
        "cols": 80, "rows": 24, "command": "sleep 60",
    })));
    assert!(attach.ok, "{attach:?}");
    wait_for_rpc_event(&output, 0, |e| {
        map_str(e, "event") == "pty.ready"
            && map_str(e, "session_id") == "token-required"
            && map_str(e, "attachment_id") == "same"
            && map_str(e, "attachment_token") == "fresh-token"
    });
    expect_missing("pty.write", server.handle_request(&rpc_request(3, "pty.write", json!({
        "session_id": "token-required", "attachment_id": "same", "data_base64": base64_encode(b"missing token"),
    }))));
    expect_missing("pty.resize", server.handle_request(&rpc_request(4, "pty.resize", json!({
        "session_id": "token-required", "attachment_id": "same", "client_attachment_token": "   ", "cols": 100, "rows": 30,
    }))));
    expect_missing(
        "pty.detach",
        server.handle_request(&rpc_request(
            5,
            "pty.detach",
            json!({
                "session_id": "token-required", "attachment_id": "same",
            }),
        )),
    );
    let detach = server.handle_request(&rpc_request(6, "pty.detach", json!({
        "session_id": "token-required", "attachment_id": "same", "client_attachment_token": "fresh-token",
    })));
    assert!(detach.ok, "{detach:?}");
    server.close_all();
}

#[test]
fn pty_replay_is_chunked_below_rpc_frame_buffer() {
    const SWIFT_RPC_MAX_FRAME_BYTES: usize = 256 * 1024;
    let key = persistent_pty_session_key("chunked");
    let attachment = PtyAttachment::new_for_test(
        key,
        "att-chunked",
        "",
        80,
        24,
        DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP,
        true,
        false,
    );
    let replay = vec![b'x'; DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES * 2 + 17];
    assert!(enqueue_pty_replay(&attachment, &replay));

    let mut joined = Vec::new();
    let mut frame_count = 0;
    let mut first_two = 0;
    while let Ok(frame) = attachment.frames().try_recv() {
        frame_count += 1;
        assert!(frame.payload.len() <= DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES);
        let event = rpc_pty_event_for_frame(&attachment, &frame);
        assert_eq!(event.event, "pty.data");
        let line = event.to_json();
        assert!(line.len() + 1 < SWIFT_RPC_MAX_FRAME_BYTES);
        if frame_count <= 2 {
            first_two += line.len() + 1;
            assert!(first_two < SWIFT_RPC_MAX_FRAME_BYTES);
        }
        assert_eq!(base64_decode(&event.data_base64), frame.payload);
        joined.extend_from_slice(&frame.payload);
    }
    assert!(
        frame_count >= 2,
        "frame count = {frame_count}, want multiple replay chunks"
    );
    assert_eq!(joined, replay);
}

#[test]
fn get_int_param_rejects_fractional_float() {
    let mut params = Map::new();
    params.insert("port".to_string(), json!(80.9));
    params.insert("timeout_ms".to_string(), json!(100.0));
    assert!(
        get_int_param(Some(&params), "port").is_none(),
        "fractional float should be rejected"
    );
    assert_eq!(get_int_param(Some(&params), "timeout_ms"), Some(100));
    let mut params = Map::new();
    params.insert("n".to_string(), json!("7"));
    assert!(get_int_param(Some(&params), "n").is_none());
}

#[test]
fn proxy_open_invalid_params() {
    let (server, _) = server_with_output(None);
    let resp = server.handle_request(&rpc_request(
        1,
        "proxy.open",
        json!({"host": "127.0.0.1", "port": "8080"}),
    ));
    assert!(!resp.ok, "{resp:?}");
    assert_eq!(resp.error_code(), "invalid_params");
    let refused = server.handle_request(&rpc_request(
        2,
        "proxy.open",
        json!({"host": "127.0.0.1", "port": 1, "timeout_ms": 500}),
    ));
    assert!(!refused.ok);
    assert_eq!(refused.error_code(), "open_failed");
    let missing = server.handle_request(&rpc_request(
        3,
        "proxy.write",
        json!({"stream_id": "nope", "data_base64": "AA=="}),
    ));
    assert_eq!(missing.error_code(), "not_found");
    let bad_b64 = server.handle_request(&rpc_request(
        4,
        "proxy.write",
        json!({"stream_id": "nope", "data_base64": "***"}),
    ));
    assert_eq!(bad_b64.error_code(), "invalid_params");
    let closed =
        server.handle_request(&rpc_request(5, "proxy.close", json!({"stream_id": "nope"})));
    assert!(closed.ok);
    let no_method = server.handle_request(&rpc_request(6, "", json!({})));
    assert_eq!(no_method.error_code(), "invalid_request");
    server.close_all();
}

#[test]
fn session_resize_coordinator() {
    let (server, _) = server_with_output(None);
    let open = server.handle_request(&rpc_request(
        1,
        "session.open",
        json!({"session_id": "sess-rz"}),
    ));
    assert!(open.ok);
    let attach_small = server.handle_request(&rpc_request(
        2,
        "session.attach",
        json!({"session_id": "sess-rz", "attachment_id": "a-small", "cols": 90, "rows": 30}),
    ));
    assert_effective_size(&attach_small, 90, 30);
    let attach_large = server.handle_request(&rpc_request(
        3,
        "session.attach",
        json!({"session_id": "sess-rz", "attachment_id": "a-large", "cols": 120, "rows": 40}),
    ));
    assert_effective_size(&attach_large, 90, 30);
    let resize_large = server.handle_request(&rpc_request(
        4,
        "session.resize",
        json!({"session_id": "sess-rz", "attachment_id": "a-large", "cols": 200, "rows": 60}),
    ));
    assert_effective_size(&resize_large, 90, 30);
    let detach_small = server.handle_request(&rpc_request(
        5,
        "session.detach",
        json!({"session_id": "sess-rz", "attachment_id": "a-small"}),
    ));
    assert_effective_size(&detach_small, 200, 60);
    let detach_large = server.handle_request(&rpc_request(
        6,
        "session.detach",
        json!({"session_id": "sess-rz", "attachment_id": "a-large"}),
    ));
    assert_effective_size(&detach_large, 200, 60);
    assert_attachment_count(&detach_large, 0);
    let reattach = server.handle_request(&rpc_request(
        7,
        "session.attach",
        json!({"session_id": "sess-rz", "attachment_id": "a-reconnect", "cols": 110, "rows": 50}),
    ));
    assert_effective_size(&reattach, 110, 50);
    let anon = server.handle_request(&rpc_request(8, "session.open", json!({})));
    assert_eq!(
        anon.result_object()
            .and_then(|r| r.get("session_id"))
            .and_then(|v| v.as_str()),
        Some("sess-1")
    );
    let close = server.handle_request(&rpc_request(
        9,
        "session.close",
        json!({"session_id": "sess-rz"}),
    ));
    assert!(close.ok);
    let gone = server.handle_request(&rpc_request(
        10,
        "session.close",
        json!({"session_id": "sess-rz"}),
    ));
    assert_eq!(gone.error_code(), "not_found");
    server.close_all();
}

#[test]
fn session_invalid_params_and_not_found() {
    let (server, _) = server_with_output(None);
    let missing = server.handle_request(&rpc_request(
        1,
        "session.attach",
        json!({"session_id": "missing", "attachment_id": "a1", "cols": 80, "rows": 24}),
    ));
    assert!(
        !missing.ok && missing.error_code() == "not_found",
        "{missing:?}"
    );
    let bad_size = server.handle_request(&rpc_request(
        2,
        "session.attach",
        json!({"session_id": "missing", "attachment_id": "a1", "cols": 0, "rows": 24}),
    ));
    assert!(
        !bad_size.ok && bad_size.error_code() == "invalid_params",
        "{bad_size:?}"
    );
    let no_attachment = server.handle_request(&rpc_request(
        3,
        "session.detach",
        json!({"session_id": "missing"}),
    ));
    assert_eq!(no_attachment.error_code(), "invalid_params");
    let status = server.handle_request(&rpc_request(
        4,
        "session.status",
        json!({"session_id": "missing"}),
    ));
    assert_eq!(status.error_code(), "not_found");
    server.close_all();
}

#[test]
fn stdio_server_writes_frames_in_go_field_order() {
    let input = "{\"id\":7,\"method\":\"ping\",\"params\":{}}\n{\"id\":8,\"method\":\"nope\"}\n";
    let (_, out, _) = run_daemon(&["serve", "--stdio"], input);
    let lines = lines(&out);
    assert_eq!(
        lines[0],
        "{\"id\":7,\"ok\":true,\"result\":{\"pong\":true}}"
    );
    assert_eq!(lines[1], "{\"id\":8,\"ok\":false,\"error\":{\"code\":\"method_not_found\",\"message\":\"unknown method \\\"nope\\\"\"}}");
}

#[test]
fn stdio_pty_attach_streams_output_and_exit() {
    let input = "{\"id\":1,\"method\":\"pty.attach\",\"params\":{\"session_id\":\"s\",\"attachment_id\":\"a\",\"client_attachment_token\":\"t\",\"cols\":80,\"rows\":24,\"command\":\"printf marker-ok; exit 0\"}}\n";
    let output = SharedBuffer::new();
    let (reader, mut writer) = os_pipe();
    let out_clone = output.clone();
    let handle = std::thread::spawn(move || {
        cmuxd_remote::daemon::run(
            &["serve".to_string(), "--stdio".to_string()],
            Box::new(reader),
            Box::new(out_clone),
            Box::new(io::sink()),
        )
    });
    writer.write_all(input.as_bytes()).unwrap();
    wait_for_rpc_event(&output, 0, |e| map_str(e, "event") == "pty.exit");
    let events: Vec<String> = rpc_event_lines(&output)
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| v.get("event").and_then(|e| e.as_str()).map(str::to_string))
        .collect();
    assert_eq!(
        events.first().map(String::as_str),
        Some("pty.ready"),
        "{events:?}"
    );
    assert!(events.contains(&"pty.data".to_string()), "{events:?}");
    assert_eq!(
        events.last().map(String::as_str),
        Some("pty.exit"),
        "{events:?}"
    );
    drop(writer);
    assert_eq!(join_with_timeout(handle, Duration::from_secs(5)), Some(0));
}

fn os_pipe() -> (std::fs::File, std::fs::File) {
    let (read, write) = nix::unistd::pipe().expect("pipe");
    (std::fs::File::from(read), std::fs::File::from(write))
}

#[allow(dead_code)]
fn unused_mutex_guard() -> Mutex<()> {
    Mutex::new(())
}

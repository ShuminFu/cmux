//! stdio RPC server: framing, legacy session methods, proxy streams, and
//! PTY sessions over `serve --stdio`.

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cmuxd_remote::logger::DiscardLogger;
use cmuxd_remote::pty::{PtyHub, PtyHubConfig};
use cmuxd_remote::rpc::server::StreamConn;
use cmuxd_remote::rpc::{RpcRequest, RpcServer};
use common::*;
use serde_json::{Map, Value, json};

fn req(id: impl Into<Value>, method: &str, params: Value) -> RpcRequest {
    let params = match params {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    RpcRequest::new(id, method, params)
}

#[test]
fn run_version_prints_version() {
    let (code, out, _) = run_args(&["version"]);
    assert_eq!(code, 0);
    assert!(!out.trim().is_empty());
}

#[test]
fn run_without_args_prints_usage() {
    let (code, _, err) = run_args(&[]);
    assert_eq!(code, 2);
    assert!(err.starts_with("Usage:\n  cmuxd-remote version\n"), "{err}");
    let (code, _, err) = run_args(&["bogus-subcommand-zzz"]);
    assert_eq!(code, 2);
    assert!(err.contains("cmuxd-remote serve --stdio"));
}

#[test]
fn serve_flag_validation_matches_go_messages() {
    let cases: &[(&[&str], &str)] = &[
        (&["serve"], "serve requires exactly one of --stdio or --ws"),
        (&["serve", "--stdio", "--ws"], "serve requires exactly one of --stdio or --ws"),
        (
            &["serve", "--stdio", "--slot", "slot-without-persistent"],
            "serve --slot requires --persistent",
        ),
        (&["serve", "--ws", "--persistent"], "serve --persistent requires --stdio"),
        (&["serve", "--stdio", "--persistent"], "serve --persistent requires --slot"),
        (
            &["serve", "--stdio", "--persistent-lease-port", "5"],
            "serve --persistent-lease-port requires --persistent",
        ),
        (
            &["serve", "--stdio", "--persistent-lease-port", "70000"],
            "serve --persistent-lease-port must be 0 or between 1 and 65535",
        ),
        (
            &["serve", "--stdio", "--persistent-lease-port", "-1"],
            "serve --persistent-lease-port must be 0 or between 1 and 65535",
        ),
        (&["serve", "--ws"], "serve --ws requires --auth-lease-file"),
        (&["serve", "--persistent-server"], "serve --persistent-server requires --slot"),
        (
            &["serve", "--persistent-server", "--slot", "a", "--stdio"],
            "serve --persistent-server cannot be combined with --stdio, --ws, --persistent, or --persistent-stop",
        ),
        (&["serve", "--persistent-stop"], "serve --persistent-stop requires --slot"),
        (
            &["serve", "--persistent-stop", "--slot", "a", "--persistent-lease-port", "1"],
            "serve --persistent-stop cannot be combined with --stdio, --ws, --persistent, or --persistent-lease-port",
        ),
        (&["serve", "--nope"], "flag provided but not defined: -nope"),
    ];
    for (args, want) in cases {
        let (code, _, err) = run_args(args);
        assert_eq!(code, 2, "{args:?}: {err}");
        assert!(err.contains(want), "{args:?}: stderr {err:?} should contain {want:?}");
    }
    let (code, _, err) = run_args(&["serve", "-h"]);
    assert_eq!(code, 2);
    assert!(err.contains("-stdio"), "{err}");
}

#[test]
fn hello_and_ping() {
    let (code, frames, _) = run_stdio(
        "{\"id\":1,\"method\":\"hello\",\"params\":{}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n",
    );
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 2);
    assert!(is_ok(&frames[0]));
    let result = result_obj(&frames[0]);
    let caps: Vec<String> = result["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect();
    for want in [
        "proxy.stream.push",
        "pty.session.persistent_daemon",
        "pty.write.notification",
        "pty.resize.notification",
        "pty.input.seq_ack",
        "cli.bridge",
    ] {
        assert!(caps.iter().any(|c| c == want), "missing capability {want}: {caps:?}");
    }
    assert_eq!(result["name"], "cmuxd-remote");
    assert!(is_ok(&frames[1]));
    assert_eq!(frames[1]["result"]["pong"], true);
}

#[test]
fn invalid_json_and_unknown_method() {
    let (code, frames, _) = run_stdio(
        "{\"id\":1,\"method\":\"hello\",\"params\":{}\n{\"id\":2,\"method\":\"unknown\",\"params\":{}}\n{\"id\":3}\n",
    );
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 3);
    assert!(!is_ok(&frames[0]));
    assert_eq!(error_code(&frames[0]), "invalid_request");
    assert_eq!(error_message(&frames[0]), "invalid JSON request");
    assert_eq!(error_code(&frames[1]), "method_not_found");
    assert_eq!(error_message(&frames[1]), "unknown method \"unknown\"");
    assert_eq!(frames[1]["id"], 2);
    assert_eq!(error_code(&frames[2]), "invalid_request");
    assert_eq!(error_message(&frames[2]), "method is required");
}

#[test]
fn oversized_frame_continues_serving() {
    let mut input = String::from("{\"id\":1,\"method\":\"ping\",\"params\":{\"pad\":\"");
    input.push_str(&"x".repeat(4 * 1024 * 1024));
    input.push_str("\"}}\n{\"id\":2,\"method\":\"ping\",\"params\":{}}\n");
    let (code, frames, _) = run_stdio(&input);
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert_eq!(error_code(&frames[0]), "invalid_request");
    assert_eq!(error_message(&frames[0]), "request frame exceeds maximum size");
    assert!(!frames[0].contains_key("id"));
    assert!(is_ok(&frames[1]));
    assert_eq!(frames[1]["id"], 2);
}

#[test]
fn pty_write_notification_emits_error_event_not_response() {
    let (code, frames, _) = run_stdio(&format!(
        "{}\n{}\n",
        json!({"method":"pty.write","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","data_base64":"YQ=="}}),
        json!({"id":2,"method":"ping","params":{}})
    ));
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert!(!frames[0].contains_key("id"));
    assert_eq!(frames[0]["event"], "pty.error");
    assert_eq!(frames[0]["attachment_token"], "token");
    assert_eq!(frames[0]["message"], "PTY attachment not found");
    assert_eq!(frames[1]["id"], 2);
    assert!(is_ok(&frames[1]));
}

#[test]
fn pty_resize_notification_emits_error_event_not_response() {
    let (code, frames, _) = run_stdio(&format!(
        "{}\n{}\n",
        json!({"method":"pty.resize","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","cols":100,"rows":30}}),
        json!({"id":2,"method":"ping","params":{}})
    ));
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert_eq!(frames[0]["event"], "pty.error");
    assert!(!frames[0].contains_key("id"));
    assert!(is_ok(&frames[1]));
}

#[test]
fn notification_without_token_stays_silent() {
    let (_, frames, _) = run_stdio(&format!(
        "{}\n{}\n",
        json!({"method":"pty.write","params":{"session_id":"missing","attachment_id":"missing","data_base64":"YQ=="}}),
        json!({"id":2,"method":"ping","params":{}})
    ));
    assert_eq!(frames.len(), 1, "no token means no pty.error event: {frames:?}");
    assert_eq!(frames[0]["id"], 2);
}

#[test]
fn no_id_non_pty_request_still_emits_response() {
    let (_, frames, _) = run_stdio("{\"method\":\"ping\",\"params\":{}}\n");
    assert_eq!(frames.len(), 1);
    assert!(is_ok(&frames[0]));
    assert!(!frames[0].contains_key("id"));
}

#[test]
fn null_id_pty_write_and_resize_still_emit_responses() {
    for line in [
        json!({"id":null,"method":"pty.write","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","data_base64":"YQ=="}}),
        json!({"id":null,"method":"pty.resize","params":{"session_id":"missing","attachment_id":"missing","client_attachment_token":"token","cols":100,"rows":30}}),
    ] {
        let (_, frames, _) =
            run_stdio(&format!("{line}\n{}\n", json!({"id":2,"method":"ping","params":{}})));
        assert_eq!(frames.len(), 2, "{frames:?}");
        assert!(!frames[0].contains_key("event"));
        assert!(!is_ok(&frames[0]));
        assert_eq!(error_code(&frames[0]), "not_found");
        assert_eq!(frames[1]["id"], 2);
    }
}

#[test]
fn session_resize_flow_uses_smallest_size() {
    let (code, frames, _) = run_stdio(&format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        json!({"id":1,"method":"session.open","params":{"session_id":"sess-stdio"}}),
        json!({"id":2,"method":"session.attach","params":{"session_id":"sess-stdio","attachment_id":"a1","cols":120,"rows":40}}),
        json!({"id":3,"method":"session.attach","params":{"session_id":"sess-stdio","attachment_id":"a2","cols":90,"rows":30}}),
        json!({"id":4,"method":"session.status","params":{"session_id":"sess-stdio"}}),
        json!({"id":5,"method":"session.detach","params":{"session_id":"sess-stdio","attachment_id":"a2"}}),
        json!({"id":6,"method":"session.close","params":{"session_id":"sess-stdio"}}),
    ));
    assert_eq!(code, 0);
    assert_eq!(frames.len(), 6);
    let status = result_obj(&frames[3]);
    assert_eq!(status["effective_cols"], 90);
    assert_eq!(status["effective_rows"], 30);
    assert_eq!(status["attachments"].as_array().unwrap().len(), 2);
    assert!(status["attachments"][0]["updated_at"].as_str().unwrap().ends_with('Z'));
    let detached = result_obj(&frames[4]);
    assert_eq!(detached["effective_cols"], 120);
    assert_eq!(frames[5]["result"]["closed"], true);
}

#[test]
fn session_invalid_params_and_not_found() {
    let (_, frames, _) = run_stdio(&format!(
        "{}\n{}\n{}\n{}\n",
        json!({"id":1,"method":"session.attach","params":{"session_id":"nope","attachment_id":"a","cols":1,"rows":1}}),
        json!({"id":2,"method":"session.attach","params":{"session_id":"nope","attachment_id":"a","cols":0,"rows":1}}),
        json!({"id":3,"method":"session.close","params":{}}),
        json!({"id":4,"method":"session.open","params":{}}),
    ));
    assert_eq!(error_code(&frames[0]), "not_found");
    assert_eq!(error_code(&frames[1]), "invalid_params");
    assert_eq!(error_message(&frames[1]), "session.attach requires cols > 0");
    assert_eq!(error_message(&frames[2]), "session.close requires session_id");
    assert_eq!(result_obj(&frames[3])["session_id"], "sess-1");
}

#[test]
fn proxy_open_invalid_params() {
    let (_, frames, _) = run_stdio(&format!(
        "{}\n{}\n{}\n",
        json!({"id":1,"method":"proxy.open","params":{"port":80}}),
        json!({"id":2,"method":"proxy.open","params":{"host":"127.0.0.1","port":0}}),
        json!({"id":3,"method":"proxy.open","params":{"host":"127.0.0.1","port":80.5}}),
    ));
    assert_eq!(error_message(&frames[0]), "proxy.open requires host");
    assert_eq!(error_message(&frames[1]), "proxy.open requires port in range 1-65535");
    assert_eq!(error_message(&frames[2]), "proxy.open requires port in range 1-65535");
}

#[test]
fn proxy_stream_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_thread = std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");
        conn.write_all(b"pong").unwrap();
        // Keep the peer open so the second subscribe still finds the stream.
        std::thread::sleep(Duration::from_millis(500));
    });
    let mut session = StdioSession::start(&["serve", "--stdio"]);
    let open =
        session.call(1, "proxy.open", json!({"host":"127.0.0.1","port":port,"timeout_ms":1000}));
    assert!(is_ok(&open), "{open:?}");
    let stream_id = result_obj(&open)["stream_id"].as_str().unwrap().to_string();
    assert_eq!(stream_id, "s-1");
    let write = session.call(
        2,
        "proxy.write",
        json!({"stream_id": stream_id, "data_base64": b64(b"ping")}),
    );
    assert_eq!(result_obj(&write)["written"], 4);
    let sub = session.call(3, "proxy.stream.subscribe", json!({"stream_id": stream_id}));
    assert_eq!(result_obj(&sub)["already_subscribed"], false);
    let again = session.call(4, "proxy.stream.subscribe", json!({"stream_id": stream_id}));
    assert_eq!(result_obj(&again)["already_subscribed"], true);
    let data = session.frames.event(|f| f["event"] == "proxy.stream.data");
    assert_eq!(unb64(&data), b"pong");
    assert_eq!(data["stream_id"], "s-1");
    server_thread.join().unwrap();
    let eof = session.frames.event(|f| f["event"] == "proxy.stream.eof");
    assert_eq!(eof["stream_id"], "s-1");
    let close = session.call(5, "proxy.close", json!({"stream_id": stream_id}));
    assert_eq!(result_obj(&close)["closed"], true);
    let write_after =
        session.call(6, "proxy.write", json!({"stream_id": stream_id, "data_base64": ""}));
    assert_eq!(error_code(&write_after), "not_found");
    let (code, _) = session.finish();
    assert_eq!(code, 0);
}

/// A stream whose read returns a payload followed by EOF.
struct EofWithPayloadConn {
    payload: Vec<u8>,
    read_once: AtomicBool,
}

impl StreamConn for EofWithPayloadConn {
    fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.read_once.swap(true, Ordering::SeqCst) {
            return Ok(0);
        }
        let n = self.payload.len().min(buf.len());
        buf[..n].copy_from_slice(&self.payload[..n]);
        Ok(n)
    }
    fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }
    fn set_write_timeout(&self, _: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
    fn close(&self) {}
}

#[test]
fn proxy_stream_eof_payload_is_not_duplicated() {
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer.clone(), None, false, None);
    server.insert_stream_for_test(
        "stream-1",
        Box::new(EofWithPayloadConn {
            payload: b"tail".to_vec(),
            read_once: AtomicBool::new(false),
        }),
    );
    let resp =
        server.handle_request(&req(1, "proxy.stream.subscribe", json!({"stream_id":"stream-1"})));
    assert!(resp.ok);
    let first = writer.frames.expect_next();
    let second = writer.frames.expect_next();
    assert_eq!(first["event"], "proxy.stream.data");
    assert_eq!(unb64(&first), b"tail");
    assert_eq!(second["event"], "proxy.stream.eof");
    assert!(!second.contains_key("data_base64"));
    assert!(writer.frames.next(Duration::from_millis(200)).is_none());
    assert_eq!(server.stream_count(), 0);
    server.close_all();
}

#[test]
fn proxy_close_is_silent_and_idempotent() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hold = std::thread::spawn(move || {
        let (conn, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(600));
        drop(conn);
    });
    let mut session = StdioSession::start(&["serve", "--stdio"]);
    let open = session.call(1, "proxy.open", json!({"host":"127.0.0.1","port":port}));
    let stream_id = result_obj(&open)["stream_id"].as_str().unwrap().to_string();
    session.call(2, "proxy.stream.subscribe", json!({"stream_id": stream_id}));
    let close = session.call(3, "proxy.close", json!({"stream_id": stream_id}));
    assert!(is_ok(&close));
    let close_again = session.call(4, "proxy.close", json!({"stream_id": stream_id}));
    assert!(is_ok(&close_again), "closing an unknown stream is still ok");
    assert!(
        session
            .frames
            .event_within(Duration::from_millis(300), |f| f["event"] == "proxy.stream.eof")
            .is_none(),
        "a local close must not emit eof"
    );
    hold.join().unwrap();
    session.finish();
}

fn pty_hub(scrollback: usize, idle: Duration) -> Arc<PtyHub> {
    PtyHub::new(
        PtyHubConfig { shell: String::new(), scrollback_limit: scrollback, session_idle_ttl: idle },
        Arc::new(DiscardLogger),
    )
}

#[test]
fn pty_rpc_session_reattach_list_and_close() {
    let writer = CaptureWriter::new();
    let server =
        RpcServer::new(writer.clone(), Some(pty_hub(4096, Duration::from_secs(3600))), true, None);
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"pty-rpc","attachment_id":"a1","client_attachment_token":"token-a1","cols":80,"rows":24,"command":"printf 'hello-rpc\\n'; sleep 60"})));
    assert!(attach.ok, "{attach:?}");
    let ready = writer.frames.event(|f| f["event"] == "pty.ready" && f["attachment_id"] == "a1");
    assert_eq!(ready["session_id"], "pty-rpc");
    assert_eq!(ready["attachment_token"], "token-a1");
    writer.frames.event(|f| {
        f["event"] == "pty.data"
            && f["attachment_id"] == "a1"
            && String::from_utf8_lossy(&unb64(f)).contains("hello-rpc")
    });

    let list = server.handle_request(&req(2, "pty.list", json!({})));
    let sessions = list.result.unwrap()["sessions"].as_array().unwrap().clone();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], "pty-rpc");
    assert_eq!(sessions[0]["attachments"][0]["attachment_id"], "a1");

    let detach = server.handle_request(&req(
        3,
        "pty.detach",
        json!({"session_id":"pty-rpc","attachment_id":"a1","client_attachment_token":"token-a1"}),
    ));
    assert!(detach.ok);
    writer.frames.event(|f| f["event"] == "pty.exit" && f["attachment_id"] == "a1");

    let reattach = server.handle_request(&req(4, "pty.attach", json!({"session_id":"pty-rpc","attachment_id":"a2","client_attachment_token":"token-a2","cols":100,"rows":30,"command":"printf 'should-not-run\\n'"})));
    assert!(reattach.ok, "{reattach:?}");
    let replay = writer.frames.event(|f| f["event"] == "pty.data" && f["attachment_id"] == "a2");
    assert!(
        String::from_utf8_lossy(&unb64(&replay)).contains("hello-rpc"),
        "replay should carry scrollback"
    );
    assert!(
        writer
            .frames
            .event_within(Duration::from_millis(300), |f| String::from_utf8_lossy(&unb64(f))
                .contains("should-not-run"))
            .is_none()
    );

    let close = server.handle_request(&req(5, "pty.close", json!({"session_id":"pty-rpc"})));
    assert!(close.ok);
    writer.frames.event(|f| f["event"] == "pty.exit" && f["attachment_id"] == "a2");
    let empty = server.handle_request(&req(6, "pty.list", json!({})));
    assert_eq!(empty.result.unwrap()["sessions"].as_array().unwrap().len(), 0);
    let close_again = server.handle_request(&req(7, "pty.close", json!({"session_id":"pty-rpc"})));
    assert_eq!(close_again.error_code(), "not_found");
    assert!(wait_until(Duration::from_secs(2), || server.pty_attachment_count() == 0));
    server.close_all();
}

#[test]
fn pty_rpc_require_existing_fails_for_missing_session() {
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer, Some(pty_hub(0, Duration::ZERO)), true, None);
    let resp = server.handle_request(&req(1, "pty.attach", json!({"session_id":"missing","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"require_existing":true})));
    assert!(!resp.ok);
    assert_eq!(resp.error_code(), "pty_session_not_found");
    assert_eq!(resp.error_message(), "persistent PTY session \"missing\" is not running");
    assert_eq!(
        server.handle_request(&req(2, "pty.list", json!({}))).result.unwrap()["sessions"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    server.close_all();
}

#[test]
fn pty_rpc_requires_attachment_token() {
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer, Some(pty_hub(0, Duration::ZERO)), true, None);
    let resp =
        server.handle_request(&req(1, "pty.attach", json!({"session_id":"s","cols":80,"rows":24})));
    assert_eq!(resp.error_code(), "invalid_params");
    assert_eq!(resp.error_message(), "pty.attach requires client_attachment_token");
    for method in ["pty.write", "pty.detach"] {
        let resp =
            server.handle_request(&req(1, method, json!({"session_id":"s","attachment_id":"a"})));
        assert_eq!(resp.error_message(), format!("{method} requires client_attachment_token"));
    }
    let resp = server.handle_request(&req(
        1,
        "pty.resize",
        json!({"session_id":"s","attachment_id":"a","cols":1,"rows":1}),
    ));
    assert_eq!(resp.error_message(), "pty.resize requires client_attachment_token");
    let resp = server.handle_request(&req(
        1,
        "pty.attach",
        json!({"session_id":"s","client_attachment_token":"t","cols":0,"rows":24}),
    ));
    assert_eq!(resp.error_message(), "pty.attach requires cols > 0");
    server.close_all();
}

#[test]
fn pty_rpc_token_rejects_stale_attachment_control() {
    let writer = CaptureWriter::new();
    let server =
        RpcServer::new(writer.clone(), Some(pty_hub(0, Duration::from_secs(3600))), true, None);
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"first","cols":80,"rows":24,"command":"sleep 60"})));
    assert!(attach.ok);
    writer.frames.event(|f| f["event"] == "pty.ready");
    let wrong = server.handle_request(&req(2, "pty.write", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"wrong","data_base64":b64(b"x")})));
    assert_eq!(wrong.error_code(), "not_found");
    let wrong_resize = server.handle_request(&req(3, "pty.resize", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"wrong","cols":10,"rows":10})));
    assert_eq!(wrong_resize.error_code(), "not_found");
    // Superseding the attachment id with a new token invalidates the old one.
    let replace = server.handle_request(&req(4, "pty.attach", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"second","cols":80,"rows":24})));
    assert!(replace.ok);
    let stale = server.handle_request(&req(5, "pty.write", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"first","data_base64":b64(b"x")})));
    assert_eq!(stale.error_code(), "not_found");
    let fresh = server.handle_request(&req(6, "pty.write", json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"second","data_base64":b64(b"x")})));
    assert!(fresh.ok, "{fresh:?}");
    let stale_detach = server.handle_request(&req(
        7,
        "pty.detach",
        json!({"session_id":"tok","attachment_id":"a1","client_attachment_token":"first"}),
    ));
    assert_eq!(stale_detach.error_code(), "not_found");
    server.close_all();
}

#[test]
fn pty_rpc_pump_emits_exit_when_attachment_is_canceled() {
    let writer = CaptureWriter::new();
    let hub = pty_hub(0, Duration::from_secs(3600));
    let server = RpcServer::new(writer.clone(), Some(hub.clone()), true, None);
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"cancel","attachment_id":"a1","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"})));
    assert!(attach.ok);
    writer.frames.event(|f| f["event"] == "pty.ready");
    assert!(hub.detach_by_id("cancel", "a1", "t"));
    let exit = writer.frames.event(|f| f["event"] == "pty.exit");
    assert_eq!(exit["attachment_id"], "a1");
    assert!(wait_until(Duration::from_secs(2), || server.pty_attachment_count() == 0));
    assert_eq!(hub.active_session_count(), 1, "detach keeps the persistent session alive");
    server.close_all();
    assert_eq!(hub.active_session_count(), 0, "an owning server kills sessions on close_all");
}

#[test]
fn pty_rpc_input_seq_ack_and_gap() {
    let writer = CaptureWriter::new();
    let server =
        RpcServer::new(writer.clone(), Some(pty_hub(0, Duration::from_secs(3600))), true, None);
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"seq","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"input_seq_ack":true,"command":"stty -echo; cat"})));
    assert!(attach.ok);
    writer.frames.event(|f| f["event"] == "pty.ready");
    let no_seq = server.handle_request(&req(2, "pty.write", json!({"session_id":"seq","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"one\n")})));
    assert_eq!(no_seq.error_code(), "pty_input_seq_gap");
    assert_eq!(no_seq.error_message(), "PTY input sequence gap: got 0, want 1");
    let bad = server.handle_request(&req(3, "pty.write", json!({"session_id":"seq","attachment_id":"a","client_attachment_token":"t","data_base64":"","seq":-1})));
    assert_eq!(bad.error_message(), "seq must be a non-negative integer");
    let first = server.handle_request(&req(4, "pty.write", json!({"session_id":"seq","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"one\n"),"seq":1})));
    assert!(first.ok, "{first:?}");
    let ack = writer.frames.event(|f| f["event"] == "pty.input_ack");
    assert_eq!(ack["seq"], 1);
    let echoed = writer
        .frames
        .event(|f| f["event"] == "pty.data" && String::from_utf8_lossy(&unb64(f)).contains("one"));
    assert_eq!(echoed["attachment_token"], "t");
    let gap = server.handle_request(&req(5, "pty.write", json!({"session_id":"seq","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"three\n"),"seq":3})));
    assert_eq!(gap.error_message(), "PTY input sequence gap: got 3, want 2");
    server.close_all();
}

#[test]
fn seq_gap_notification_emits_pty_error_and_detaches() {
    let mut session = StdioSession::start(&["serve", "--stdio"]);
    let attach = session.call(1, "pty.attach", json!({"session_id":"gap","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"input_seq_ack":true,"command":"cat"}));
    assert!(is_ok(&attach));
    session.frames.event(|f| f["event"] == "pty.ready");
    session.send_line(&json!({"method":"pty.write","params":{"session_id":"gap","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"x"),"seq":5}}).to_string());
    let error = session.frames.event(|f| f["event"] == "pty.error");
    assert_eq!(error["error"], "PTY input sequence gap: got 5, want 1");
    assert_eq!(error["message"], "PTY input sequence gap: got 5, want 1");
    session.frames.event(|f| f["event"] == "pty.exit" && f["attachment_id"] == "a");
    let list = session.call(2, "pty.list", json!({}));
    let sessions = result_obj(&list)["sessions"].as_array().unwrap().clone();
    assert_eq!(
        sessions[0]["attachments"].as_array().unwrap().len(),
        0,
        "gap detaches the attachment but keeps the session"
    );
    session.finish();
}

#[test]
fn rpc_server_close_all_leaves_shared_hub_alive() {
    let hub = pty_hub(0, Duration::from_secs(3600));
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer.clone(), Some(hub.clone()), false, None);
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"shared","attachment_id":"a1","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"})));
    assert!(attach.ok);
    writer.frames.event(|f| f["event"] == "pty.ready");
    server.close_all();
    assert_eq!(hub.active_session_count(), 1);
    let snapshots = hub.session_snapshots();
    assert_eq!(snapshots[0]["attachments"].as_array().unwrap().len(), 0);
    hub.close_all();
    assert_eq!(hub.active_session_count(), 0);
}

#[test]
fn rpc_server_untrack_keeps_newer_reattach() {
    let hub = pty_hub(0, Duration::from_secs(3600));
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer.clone(), Some(hub.clone()), false, None);
    let first = server.handle_request(&req(1, "pty.attach", json!({"session_id":"re","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"command":"sleep 60"})));
    assert!(first.ok);
    writer.frames.event(|f| f["event"] == "pty.ready");
    let second = server.handle_request(&req(2, "pty.attach", json!({"session_id":"re","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24})));
    assert!(second.ok);
    // The superseded pump exits and untracks only its own attachment.
    writer.frames.event(|f| f["event"] == "pty.exit");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(server.pty_attachment_count(), 1);
    let write = server.handle_request(&req(3, "pty.write", json!({"session_id":"re","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"x")})));
    assert!(write.ok);
    server.close_all();
    hub.close_all();
}

#[test]
fn attach_surfaces_pty_allocation_failure() {
    let hub = pty_hub(0, Duration::ZERO);
    hub.set_pty_opener(Arc::new(|| {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "open /dev/ptmx: permission denied",
        ))
    }));
    let writer = CaptureWriter::new();
    let server = RpcServer::new(writer, Some(hub), true, None);
    let resp = server.handle_request(&req(1, "pty.attach", json!({"session_id":"denied","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24})));
    assert!(!resp.ok);
    assert_eq!(resp.error_code(), "pty_start_failed");
    assert!(
        resp.error_message()
            .starts_with("could not allocate a remote PTY: open /dev/ptmx: permission denied"),
        "{}",
        resp.error_message()
    );
    assert!(resp.error_message().contains("ptmxmode=0666"), "{}", resp.error_message());
    server.close_all();
}

#[test]
fn pty_replay_is_chunked_below_rpc_frame_buffer() {
    let writer = CaptureWriter::new();
    let server = RpcServer::new(
        writer.clone(),
        Some(pty_hub(1 << 20, Duration::from_secs(3600))),
        true,
        None,
    );
    let attach = server.handle_request(&req(1, "pty.attach", json!({"session_id":"big","attachment_id":"a1","client_attachment_token":"t","cols":80,"rows":24,"command":"head -c 200000 /dev/zero | tr '\\0' 'x'; echo DONE; sleep 60"})));
    assert!(attach.ok);
    writer
        .frames
        .event(|f| f["event"] == "pty.data" && String::from_utf8_lossy(&unb64(f)).contains("DONE"));
    assert!(
        server
            .handle_request(&req(
                2,
                "pty.detach",
                json!({"session_id":"big","attachment_id":"a1","client_attachment_token":"t"})
            ))
            .ok
    );
    writer.frames.event(|f| f["event"] == "pty.exit");
    writer.frames.drain();
    let reattach = server.handle_request(&req(3, "pty.attach", json!({"session_id":"big","attachment_id":"a2","client_attachment_token":"t2","cols":80,"rows":24})));
    assert!(reattach.ok);
    let mut total = 0;
    let mut frames = 0;
    loop {
        let Some(frame) = writer.frames.event_within(Duration::from_secs(2), |f| {
            f["event"] == "pty.data" && f["attachment_id"] == "a2"
        }) else {
            break;
        };
        let payload = unb64(&frame);
        assert!(payload.len() <= 48 * 1024, "replay chunk {} exceeds cap", payload.len());
        total += payload.len();
        frames += 1;
        if String::from_utf8_lossy(&payload).contains("DONE") {
            break;
        }
    }
    assert!(frames >= 4, "replay should be split into several frames: {frames}");
    assert!(total >= 200_000, "replay total {total}");
    server.close_all();
}

#[test]
fn stdio_pty_session_end_to_end() {
    let mut session = StdioSession::start(&["serve", "--stdio"]);
    let attach = session.call(1, "pty.attach", json!({"session_id":"e2e","attachment_id":"a","client_attachment_token":"t","cols":80,"rows":24,"command":"stty -echo; read line; printf 'got:%s\\n' \"$line\"; stty size; exit 3"}));
    assert!(is_ok(&attach), "{attach:?}");
    assert_eq!(result_obj(&attach)["attached"], true);
    session.frames.event(|f| f["event"] == "pty.ready");
    let write = session.call(2, "pty.write", json!({"session_id":"e2e","attachment_id":"a","client_attachment_token":"t","data_base64":b64(b"first\n")}));
    assert_eq!(result_obj(&write)["written"], 6);
    session.frames.event(|f| {
        f["event"] == "pty.data" && String::from_utf8_lossy(&unb64(f)).contains("got:first")
    });
    session.frames.event(|f| {
        f["event"] == "pty.data" && String::from_utf8_lossy(&unb64(f)).contains("24 80")
    });
    session.frames.event(|f| f["event"] == "pty.exit");
    let list = session.call(3, "pty.list", json!({}));
    assert_eq!(result_obj(&list)["sessions"].as_array().unwrap().len(), 0);
    let (code, _) = session.finish();
    assert_eq!(code, 0);
}

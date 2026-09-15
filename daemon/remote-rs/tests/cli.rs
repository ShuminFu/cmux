//! Ports of `cli_test.go`, `cli_relay_test.go`, `cloud_cli_bridge_test.go`,
//! `agent_launch_test.go`, `tmux_compat_test.go`, `tmux_split_ref_test.go`
//! and `tmux_corpus_behavior_test.go`.
//!
//! Every test that touches process environment (the relay reads
//! `CMUX_SOCKET_PATH`, `CMUX_WORKSPACE_ID`, `HOME`, ...) holds the shared
//! `EnvGuard`, which serializes those tests and restores the variables.

mod support;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use cmuxd_remote::agent_launch::{
    claude_teams_launch_args, configure_agent_environment, create_omo_shim_dir,
    create_tmux_shim_dir, get_focused_context, get_focused_context_with_timeout,
    merge_node_options, omo_ensure_plugin, AgentConfig, FocusedContext, CLAUDE_TEAMS_SHIM_SCRIPT,
};
use cmuxd_remote::cli::{
    dial_socket, flag_to_param_key, parse_flags, socket_round_trip_v2, CliIo, RelayAuthState,
    RpcContext,
};
use cmuxd_remote::cloud_cli_bridge::CloudCliBridge;
use cmuxd_remote::daemon::should_run_cli_for_invocation;
use cmuxd_remote::rpc::{FnFrameWriter, RpcRequest, RpcServer};
use cmuxd_remote::tmux_compat::{
    dispatch_tmux_command, load_tmux_compat_store, parse_tmux_args, save_tmux_compat_store,
    split_tmux_cmd, tmux_canonical_pane_id, tmux_canonical_surface_id, tmux_pane_selector,
    tmux_render_format, tmux_resolve_workspace_id, tmux_send_keys_text, tmux_shell_command_text,
    tmux_show_buffer, tmux_stable_numeric_id, tmux_wait_for_signal_path, tmux_window_selector,
    MainVerticalState, TmuxCompatStore,
};
use cmuxd_remote::util::{is_uuidish, random_hex};

use support::*;

// --- helpers ---

/// Serialized, scrubbed environment: no cmux caller context, no relay auth,
/// and `HOME` pointed at a fresh temp dir.
struct TestEnv {
    guard: EnvGuard,
    home: tempfile::TempDir,
}

impl TestEnv {
    fn set(&mut self, key: &str, value: &str) {
        self.guard.set(key, value);
    }

    fn track(&mut self, key: &str) {
        self.guard.track(key);
    }

    fn home(&self) -> String {
        self.home.path().to_string_lossy().into_owned()
    }
}

fn clean_env() -> TestEnv {
    let mut guard = EnvGuard::new();
    for key in [
        "CMUX_SOCKET_PATH",
        "CMUX_WORKSPACE_ID",
        "CMUX_SURFACE_ID",
        "CMUX_TAB_ID",
        "CMUX_PANEL_ID",
        "CMUX_PANE_ID",
        "CMUX_RELAY_ID",
        "CMUX_RELAY_TOKEN",
        "TMUX",
        "TMUX_PANE",
    ] {
        guard.remove(key);
    }
    let home = temp_dir("cmux-cli-home-");
    guard.set("HOME", &home.path().to_string_lossy());
    TestEnv { guard, home }
}

fn run_cli(args: &[&str]) -> (i32, String, String) {
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let mut stdout: Vec<u8> = Vec::new();
    let mut stderr: Vec<u8> = Vec::new();
    let code = {
        let mut io = CliIo {
            stdout: &mut stdout,
            stderr: &mut stderr,
        };
        cmuxd_remote::cli::run_cli(&args, &mut io)
    };
    (
        code,
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

fn run_cli_code(args: &[&str]) -> i32 {
    run_cli(args).0
}

fn dispatch(rc: &RpcContext, command: &str, args: &[&str]) -> (Result<(), String>, String) {
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let mut out: Vec<u8> = Vec::new();
    let result = dispatch_tmux_command(rc, command, &args, &mut out);
    (result, String::from_utf8(out).expect("utf8 output"))
}

fn no_rc() -> RpcContext {
    RpcContext::new("")
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn expect_request(
    rx: &flume::Receiver<Map<String, Value>>,
    want_method: &str,
) -> Map<String, Value> {
    let req = receive_request(rx);
    assert_eq!(map_str(&req, "method"), want_method, "request {:?}", req);
    params_of(&req)
}

fn expect_no_request(rx: &flume::Receiver<Map<String, Value>>) {
    if let Ok(req) = rx.recv_timeout(Duration::from_millis(100)) {
        panic!("expected no request to be sent, got: {:?}", req);
    }
}

fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn assert_sigil_digits(value: &str, sigil: char, field: &str) {
    let rest = value
        .strip_prefix(sigil)
        .unwrap_or_else(|| panic!("{field} = {value:?}, want {sigil} prefix"));
    assert!(
        is_digits(rest),
        "{field} = {value:?}, want {sigil} followed by digits"
    );
}

// --- dial / transport ---

#[test]
fn dial_socket_refreshes_to_updated_tcp_address_without_polling() {
    let stale = TcpListener::bind("127.0.0.1:0").unwrap();
    let stale_addr = stale.local_addr().unwrap().to_string();
    drop(stale);

    let ready = TcpListener::bind("127.0.0.1:0").unwrap();
    let ready_addr = ready.local_addr().unwrap().to_string();
    let accepted = std::thread::spawn(move || {
        if let Ok((conn, _)) = ready.accept() {
            drop(conn);
        }
    });

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_ref = Arc::clone(&calls);
    let refresh_target = ready_addr.clone();
    let refresh: Arc<dyn Fn() -> String + Send + Sync> = Arc::new(move || {
        calls_ref.fetch_add(1, Ordering::SeqCst);
        refresh_target.clone()
    });
    let start = Instant::now();
    let conn = dial_socket(&stale_addr, Some(&refresh))
        .expect("dial_socket should refresh to updated address");
    let elapsed = start.elapsed();
    drop(conn);
    let _ = accepted.join();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "refresh_addr should be called once"
    );
    assert!(
        elapsed <= Duration::from_millis(500),
        "dial_socket should fail over without polling, took {elapsed:?}"
    );
}

#[test]
fn dial_socket_fails_fast_when_tcp_address_stays_stale() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_ref = Arc::clone(&calls);
    let stale = addr.clone();
    let refresh: Arc<dyn Fn() -> String + Send + Sync> = Arc::new(move || {
        calls_ref.fetch_add(1, Ordering::SeqCst);
        stale.clone()
    });
    let start = Instant::now();
    let result = dial_socket(&addr, Some(&refresh));
    let elapsed = start.elapsed();
    assert!(
        result.is_err(),
        "dial_socket should fail when the relay address stays stale"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "refresh_addr should be called once on stale TCP failure"
    );
    assert!(
        elapsed <= Duration::from_millis(500),
        "dial_socket should fail fast without polling, took {elapsed:?}"
    );
}

#[test]
fn dial_socket_detection() {
    for path in [
        "/tmp/cmux-nonexistent-test-99999.sock",
        "/var/run/cmux-nonexistent.sock",
    ] {
        assert!(
            dial_socket(path, None).is_err(),
            "dial_socket({path:?}) should fail for non-existent path"
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        if let Ok((conn, _)) = listener.accept() {
            drop(conn);
        }
    });
    let conn = dial_socket(&addr, None).expect("dial_socket should succeed for TCP");
    drop(conn);
}

#[test]
fn socket_round_trip_v2_list_result() {
    let windows = json!([
        {"id": "alpha", "ref": "@1"},
        {"id": "beta", "ref": "@2"},
        {"id": "gamma", "ref": "@3"},
    ]);
    let addr = start_mock_v2_tcp_socket_with_result(json!({"windows": windows}));
    let resp = socket_round_trip_v2(&addr, "window.list", None, None)
        .expect("socket_round_trip_v2 should succeed");
    assert!(
        resp.contains("alpha") && resp.contains("beta") && resp.contains("gamma"),
        "socket_round_trip_v2 response missing window IDs: {resp:?}"
    );
}

// --- basic CLI commands ---

#[test]
fn cli_ping() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(run_cli_code(&["--socket", &sock.path, "ping"]), 0);
    expect_request(&rx, "system.ping");
}

#[test]
fn cli_ping_over_tcp() {
    let _env = clean_env();
    let addr = start_mock_v2_tcp_socket_with_result(json!({}));
    assert_eq!(run_cli_code(&["--socket", &addr, "ping"]), 0);
}

#[test]
fn cli_ping_over_authenticated_tcp_with_env() {
    let mut env = clean_env();
    let relay_id = "relay-1";
    let relay_token = "a1".repeat(32);
    let ping_resp = cmuxd_remote::util::go_json(&json!({"id": 1, "ok": true, "result": {}}));
    let addr = start_mock_authenticated_tcp_socket(relay_id, &relay_token, &ping_resp);
    env.set("CMUX_RELAY_ID", relay_id);
    env.set("CMUX_RELAY_TOKEN", &relay_token);
    assert_eq!(
        run_cli_code(&["--socket", &addr, "ping"]),
        0,
        "ping over authenticated TCP should return 0"
    );
}

#[test]
fn cli_ping_over_authenticated_tcp_with_relay_file() {
    let env = clean_env();
    let relay_id = "relay-2";
    let relay_token = "b2".repeat(32);
    let ping_resp = cmuxd_remote::util::go_json(&json!({"id": 1, "ok": true, "result": {}}));
    let addr = start_mock_authenticated_tcp_socket(relay_id, &relay_token, &ping_resp);
    let port = addr.rsplit(':').next().unwrap().to_string();

    let relay_dir = format!("{}/.cmux/relay", env.home());
    std::fs::create_dir_all(&relay_dir).unwrap();
    let auth = RelayAuthState {
        relay_id: relay_id.to_string(),
        relay_token: relay_token.clone(),
    };
    std::fs::write(
        format!("{relay_dir}/{port}.auth"),
        serde_json::to_vec(&auth).unwrap(),
    )
    .unwrap();

    assert_eq!(
        run_cli_code(&["--socket", &addr, "ping"]),
        0,
        "ping over authenticated TCP file relay should return 0"
    );
}

#[test]
fn cli_new_window() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(run_cli_code(&["--socket", &sock.path, "new-window"]), 0);
    expect_request(&rx, "window.create");
}

#[test]
fn cli_close_window() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "close-window", "--window", "win-42"]),
        0
    );
    let params = expect_request(&rx, "window.close");
    assert_eq!(map_str(&params, "window_id"), "win-42");
}

#[test]
fn cli_list_workspaces_v2() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "--json", "list-workspaces"]),
        0
    );
}

#[test]
fn cli_list_workspaces_v2_default_output_shows_result() {
    let _env = clean_env();
    let addr =
        start_mock_v2_tcp_socket_with_result(json!({"method": "workspace.list", "params": {}}));
    let (code, stdout, _) = run_cli(&["--socket", &addr, "list-workspaces"]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("\"method\": \"workspace.list\""),
        "expected default output to include result payload, got {stdout:?}"
    );
}

#[test]
fn cli_notify_default_output_prints_ok_for_empty_result() {
    let _env = clean_env();
    let addr = start_mock_v2_tcp_socket_with_result(json!({}));
    let (code, stdout, _) = run_cli(&["--socket", &addr, "notify", "--body", "hi"]);
    assert_eq!(code, 0);
    assert_eq!(
        stdout.trim(),
        "OK",
        "expected empty-result command to print OK"
    );
}

#[test]
fn cli_rpc_passthrough() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "rpc", "system.capabilities"]),
        0
    );
}

#[test]
fn cli_rpc_with_params() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "rpc",
            "workspace.create",
            "{\"title\":\"test\"}"
        ]),
        0
    );
}

#[test]
fn cli_unknown_command() {
    let _env = clean_env();
    assert_eq!(
        run_cli_code(&["--socket", "/dev/null", "does-not-exist"]),
        2
    );
}

#[test]
fn cli_no_socket() {
    let _env = clean_env();
    assert_eq!(run_cli_code(&["ping"]), 1, "missing socket should return 1");
}

#[test]
fn cli_socket_env_var() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SOCKET_PATH", &sock.path);
    assert_eq!(run_cli_code(&["ping"]), 0);
    expect_request(&rx, "system.ping");
}

#[test]
fn cli_v2_flag_mapping() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "close-workspace",
            "--workspace",
            "ws-abc"
        ]),
        0
    );
    let params = expect_request(&rx, "workspace.close");
    assert_eq!(map_str(&params, "workspace_id"), "ws-abc");
}

#[test]
fn busybox_argv0_detection() {
    assert!(should_run_cli_for_invocation("cmux", &[]));
    assert!(should_run_cli_for_invocation(
        "/home/user/.cmux/bin/cmux",
        &[]
    ));
    assert!(!should_run_cli_for_invocation("cmuxd-remote", &[]));
    assert!(!should_run_cli_for_invocation(
        "/usr/local/bin/cmuxd-remote",
        &strings(&["serve", "--stdio"])
    ));
}

#[test]
fn cli_browser_subcommand() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "browser",
            "open",
            "--url",
            "https://example.com"
        ]),
        0
    );
}

#[test]
fn cli_new_pane_defaults_direction_and_forwards_extra_flags() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "new-pane",
        "--workspace",
        "ws-1",
        "--type",
        "browser",
        "--url",
        "https://example.com",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "pane.create");
    assert_eq!(map_str(&params, "workspace_id"), "ws-1");
    assert_eq!(map_str(&params, "direction"), "right");
    assert_eq!(map_str(&params, "type"), "browser");
    assert_eq!(map_str(&params, "url"), "https://example.com");
}

#[test]
fn cli_list_panels_uses_surface_list() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "list-panels",
            "--workspace",
            "ws-1"
        ]),
        0
    );
    let params = expect_request(&rx, "surface.list");
    assert_eq!(map_str(&params, "workspace_id"), "ws-1");
}

#[test]
fn cli_focus_panel_uses_surface_focus() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "focus-panel",
        "--workspace",
        "ws-1",
        "--panel",
        "surface-1",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "surface.focus");
    assert_eq!(map_str(&params, "workspace_id"), "ws-1");
    assert_eq!(map_str(&params, "surface_id"), "surface-1");
    assert!(
        !params.contains_key("panel_id"),
        "did not expect panel_id in params: {params:?}"
    );
}

#[test]
fn cli_browser_open_uses_open_split_and_workspace_env() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_WORKSPACE_ID", "env-ws");
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "browser",
            "open",
            "https://example.com"
        ]),
        0
    );
    let params = expect_request(&rx, "browser.open_split");
    assert_eq!(map_str(&params, "workspace_id"), "env-ws");
    assert_eq!(map_str(&params, "url"), "https://example.com");
}

#[test]
fn cli_browser_get_url_uses_current_method_and_surface_env() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "--json", "browser", "get-url"]),
        0
    );
    let params = expect_request(&rx, "browser.url.get");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
}

#[test]
fn cli_browser_snapshot_uses_surface_env_and_forwards_options() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "browser",
        "snapshot",
        "--selector",
        "main",
        "--max-depth",
        "4",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "browser.snapshot");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
    assert_eq!(map_str(&params, "selector"), "main");
    assert_eq!(map_str(&params, "max_depth"), "4");
}

#[test]
fn cli_browser_wait_uses_surface_env_and_forwards_options() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "browser",
        "wait",
        "--timeout-ms",
        "1500",
        "--url-contains",
        "/cloud",
        "--load-state",
        "networkidle",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "browser.wait");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
    assert_eq!(map_str(&params, "timeout_ms"), "1500");
    assert_eq!(map_str(&params, "url_contains"), "/cloud");
    assert_eq!(map_str(&params, "load_state"), "networkidle");
}

#[test]
fn cli_browser_automation_positionals() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "browser",
        "fill",
        "input[name=email]",
        "dev@example.com",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "browser.fill");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
    assert_eq!(map_str(&params, "selector"), "input[name=email]");
    assert_eq!(map_str(&params, "text"), "dev@example.com");
    assert!(
        !params.contains_key("value"),
        "browser.fill should not send value param: {params:?}"
    );
}

#[test]
fn cli_browser_select_does_not_mirror_value_to_text() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "browser",
        "select",
        "select[name=plan]",
        "free",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "browser.select");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
    assert_eq!(map_str(&params, "selector"), "select[name=plan]");
    assert_eq!(map_str(&params, "value"), "free");
    assert!(
        !params.contains_key("text"),
        "browser.select should not send text param: {params:?}"
    );
}

#[test]
fn cli_browser_eval_uses_positional_script() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_SURFACE_ID", "env-sf");
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "browser",
            "eval",
            "document.title"
        ]),
        0
    );
    let params = expect_request(&rx, "browser.eval");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
    assert_eq!(map_str(&params, "script"), "document.title");
}

#[test]
fn cli_no_args() {
    let _env = clean_env();
    assert_eq!(run_cli_code(&[]), 2);
}

#[test]
fn cli_help_flag_and_command() {
    let _env = clean_env();
    assert_eq!(run_cli_code(&["--help"]), 0);
    assert_eq!(run_cli_code(&["help"]), 0);
}

// --- flag parsing ---

#[test]
fn parse_flags_rejects_missing_flag_value() {
    let err = parse_flags(
        &strings(&["--timeout-ms"]),
        &["timeout-ms", "url-contains"],
        None,
    )
    .expect_err("parse_flags should reject missing flag values");
    assert_eq!(err, "flag --timeout-ms requires a value");
}

#[test]
fn parse_flags_allows_single_dash_flag_value() {
    let parsed = parse_flags(
        &strings(&["--text", "-n", "--command", "-lc echo hi"]),
        &["text", "command"],
        None,
    )
    .expect("parse_flags should allow single-dash values");
    assert_eq!(parsed.flags["text"], "-n");
    assert_eq!(parsed.flags["command"], "-lc echo hi");
}

#[test]
fn parse_flags_allows_double_dash_flag_value() {
    let parsed = parse_flags(
        &strings(&["--text", "--some-content", "--body", "--flag-like text"]),
        &["text", "body"],
        None,
    )
    .expect("parse_flags should allow double-dash values");
    assert_eq!(parsed.flags["text"], "--some-content");
    assert_eq!(parsed.flags["body"], "--flag-like text");
}

#[test]
fn flag_to_param_key_mapping() {
    let cases = [
        ("workspace", "workspace_id"),
        ("surface", "surface_id"),
        ("panel", "panel_id"),
        ("pane", "pane_id"),
        ("window", "window_id"),
        ("command", "initial_command"),
        ("name", "title"),
        ("working-directory", "working_directory"),
        ("title", "title"),
        ("url", "url"),
        ("direction", "direction"),
    ];
    for (input, expected) in cases {
        assert_eq!(
            flag_to_param_key(input),
            expected,
            "flag_to_param_key({input:?})"
        );
    }
}

#[test]
fn parse_flags_rejects_unknown_flags() {
    let args = strings(&[
        "positional-cmd",
        "--workspace",
        "ws-1",
        "--surface",
        "sf-2",
        "--unknown",
        "val",
    ]);
    assert!(
        parse_flags(&args, &["workspace", "surface"], None).is_err(),
        "parse_flags should reject unknown flags"
    );
}

#[test]
fn parse_flags_collects_known_flags_and_positional_args() {
    let args = strings(&["positional-cmd", "--workspace", "ws-1", "--surface", "sf-2"]);
    let result =
        parse_flags(&args, &["workspace", "surface"], None).expect("parse_flags should succeed");
    assert_eq!(result.flags["workspace"], "ws-1");
    assert_eq!(result.flags["surface"], "sf-2");
    assert_eq!(
        result.positional.first().map(String::as_str),
        Some("positional-cmd")
    );
}

#[test]
fn cli_env_var_defaults() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_WORKSPACE_ID", "env-ws-id");
    env.set("CMUX_SURFACE_ID", "env-sf-id");
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "--json", "close-surface"]),
        0
    );
    let params = expect_request(&rx, "surface.close");
    assert_eq!(map_str(&params, "workspace_id"), "env-ws-id");
    assert_eq!(map_str(&params, "surface_id"), "env-sf-id");
}

// --- workspace groups ---

#[test]
fn cli_workspace_group_list() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "workspace",
            "group",
            "list"
        ]),
        0
    );
    expect_request(&rx, "workspace.group.list");
}

#[test]
fn cli_workspace_group_dash_alias() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "workspace-group",
            "collapse",
            "workspace_group:1"
        ]),
        0
    );
    let params = expect_request(&rx, "workspace.group.collapse");
    assert_eq!(map_str(&params, "group_id"), "workspace_group:1");
}

#[test]
fn cli_workspace_group_create_maps_flags() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "create",
        "--name",
        "My Group",
        "--cwd",
        "/repo/path",
        "--from",
        "workspace:1, workspace:2",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.create");
    assert_eq!(map_str(&params, "name"), "My Group");
    assert_eq!(map_str(&params, "cwd"), "/repo/path");
    assert_eq!(
        params.get("child_workspace_ids"),
        Some(&json!(["workspace:1", "workspace:2"]))
    );
}

#[test]
fn cli_workspace_group_add_requires_group_and_workspace() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "workspace",
            "group",
            "add",
            "--group",
            "g1"
        ]),
        2
    );
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "add",
        "--group",
        "g1",
        "--workspace",
        "ws1",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.add");
    assert_eq!(map_str(&params, "group_id"), "g1");
    assert_eq!(map_str(&params, "workspace_id"), "ws1");
}

#[test]
fn cli_workspace_group_rename_positional_name() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "rename",
        "workspace_group:2",
        "New Name",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.rename");
    assert_eq!(map_str(&params, "group_id"), "workspace_group:2");
    assert_eq!(map_str(&params, "name"), "New Name");
}

#[test]
fn cli_workspace_group_new_workspace_uses_underscore_method() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "new-workspace",
        "workspace_group:3",
        "--placement",
        "top",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.new_workspace");
    assert_eq!(map_str(&params, "group_id"), "workspace_group:3");
    assert_eq!(map_str(&params, "placement"), "top");
}

#[test]
fn cli_workspace_group_set_color_omitted_hex_clears() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "set-color",
        "workspace_group:4",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.set_color");
    assert_eq!(
        params.get("hex"),
        Some(&json!("")),
        "expected empty hex (clear), got {params:?}"
    );
}

#[test]
fn cli_workspace_group_move_validates_position() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "workspace", "group", "move", "g1"]),
        2
    );
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "workspace",
            "group",
            "move",
            "g1",
            "--to-index",
            "abc"
        ]),
        2
    );
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "workspace",
        "group",
        "move",
        "g1",
        "--to-index",
        "2",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.group.move");
    assert_eq!(
        params.get("to_index"),
        Some(&json!(2)),
        "expected integer to_index 2, got {params:?}"
    );
    assert_eq!(map_str(&params, "group_id"), "g1");
}

#[test]
fn cli_workspace_group_unknown_subcommand() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "workspace", "group", "explode"]),
        2
    );
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "workspace", "group"]),
        2
    );
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "workspace", "rename"]),
        2
    );
}

#[test]
fn cli_workspace_group_list_forwards_caller_env_context() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_WORKSPACE_ID", "env-ws");
    env.set("CMUX_SURFACE_ID", "env-sf");
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "--json",
            "workspace",
            "group",
            "list"
        ]),
        0
    );
    let params = expect_request(&rx, "workspace.group.list");
    assert_eq!(map_str(&params, "workspace_id"), "env-ws");
    assert_eq!(map_str(&params, "surface_id"), "env-sf");
}

#[test]
fn cli_workspace_group_remove_still_requires_explicit_workspace_with_env() {
    let mut env = clean_env();
    let sock = start_mock_v2_socket();
    env.set("CMUX_WORKSPACE_ID", "env-ws");
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "workspace", "group", "remove"]),
        2
    );
}

#[test]
fn cli_notify_uses_caller_env_for_cloud_bridge() {
    let mut env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    env.set("CMUX_WORKSPACE_ID", "env-ws");
    env.set("CMUX_SURFACE_ID", "env-sf");
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "--json",
        "notify",
        "--title",
        "Done",
        "--body",
        "Build finished",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "notification.create_for_caller");
    assert_eq!(map_str(&params, "preferred_workspace_id"), "env-ws");
    assert_eq!(map_str(&params, "preferred_surface_id"), "env-sf");
    assert!(
        !params.contains_key("workspace_id"),
        "workspace_id should be rewritten: {params:?}"
    );
    assert!(
        !params.contains_key("surface_id"),
        "surface_id should be rewritten: {params:?}"
    );
}

// --- cli_relay_test.go ---

#[test]
fn bool_flag_coercion_focus_true() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--focus", "true"]),
        0
    );
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(
        params.get("focus"),
        Some(&Value::Bool(true)),
        "expected focus=true (bool)"
    );
}

#[test]
fn bool_flag_coercion_focus_false() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--focus", "false"]),
        0
    );
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(
        params.get("focus"),
        Some(&Value::Bool(false)),
        "expected focus=false (bool)"
    );
}

#[test]
fn bool_flag_coercion_invalid_value() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_ne!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--focus", "maybe"]),
        0
    );
    expect_no_request(&rx);
}

#[test]
fn new_workspace_param_names() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "new-workspace",
        "--name",
        "My WS",
        "--cwd",
        "/home/dev/code",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(map_str(&params, "title"), "My WS");
    assert!(
        !params.contains_key("name"),
        "unexpected 'name' param: {params:?}"
    );
    assert_eq!(map_str(&params, "cwd"), "/home/dev/code");
    assert!(
        !params.contains_key("working_directory"),
        "unexpected 'working_directory' param: {params:?}"
    );
}

#[test]
fn rename_workspace() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "rename-workspace",
            "--title",
            "devbox"
        ]),
        0
    );
    let params = expect_request(&rx, "workspace.rename");
    assert_eq!(map_str(&params, "title"), "devbox");
}

#[test]
fn join_pane_target_pane_param() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "join-pane",
        "--pane",
        "pane-1",
        "--target-pane",
        "pane-2",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "pane.join");
    assert_eq!(map_str(&params, "target_pane_id"), "pane-2");
    assert!(
        !params.contains_key("target-pane"),
        "unexpected 'target-pane' param: {params:?}"
    );
}

#[test]
fn new_workspace_removed_flags() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_ne!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "new-workspace",
            "--working-directory",
            "/home/dev"
        ]),
        0
    );
}

#[test]
fn new_workspace_layout() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let layout = "{\"splits\":[{\"direction\":\"vertical\",\"ratio\":0.5}]}";
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--layout", layout]),
        0
    );
    let params = expect_request(&rx, "workspace.create");
    assert!(
        params.get("layout").map(Value::is_object).unwrap_or(false),
        "expected layout to be a JSON object: {params:?}"
    );
}

#[test]
fn new_workspace_layout_invalid() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_ne!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "new-workspace",
            "--layout",
            "not-json"
        ]),
        0
    );
}

#[test]
fn new_workspace_env() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "new-workspace",
        "--env",
        "FOO=bar",
        "--env",
        "BAZ=qux",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(
        params.get("env"),
        Some(&json!({"FOO": "bar", "BAZ": "qux"}))
    );
}

#[test]
fn new_workspace_env_bad_format() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_ne!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--env", "NOEQUALS"]),
        0
    );
}

#[test]
fn new_workspace_env_file() {
    let _env = clean_env();
    let dir = temp_dir("cmux-env-");
    let file = path_str(&dir, "vars.env");
    std::fs::write(&file, "# comment\nHOST=localhost\nPORT=5432\n\n").unwrap();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    assert_eq!(
        run_cli_code(&["--socket", &sock.path, "new-workspace", "--env-file", &file]),
        0
    );
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(
        params.get("env"),
        Some(&json!({"HOST": "localhost", "PORT": "5432"}))
    );
}

#[test]
fn new_workspace_window_group_flags() {
    let _env = clean_env();
    let (sock, rx) = start_mock_v2_socket_with_request_capture();
    let code = run_cli_code(&[
        "--socket",
        &sock.path,
        "new-workspace",
        "--window",
        "win-1",
        "--group",
        "grp-1",
        "--group-placement",
        "before",
        "--group-reference",
        "ws-ref-1",
    ]);
    assert_eq!(code, 0);
    let params = expect_request(&rx, "workspace.create");
    assert_eq!(map_str(&params, "window_id"), "win-1");
    assert_eq!(map_str(&params, "group_id"), "grp-1");
    assert_eq!(map_str(&params, "placement"), "before");
    assert_eq!(map_str(&params, "group_reference_workspace_id"), "ws-ref-1");
}

/// Mock where `workspace.create` returns a surface id and everything else
/// acknowledges (`TestNewWorkspaceCommand`).
fn start_mock_create_socket() -> (MockSocket, flume::Receiver<Map<String, Value>>) {
    let (dir, path) = make_short_unix_socket_dir();
    let listener = UnixListener::bind(&path).expect("listen");
    let (tx, rx) = flume::bounded(8);
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let tx = tx.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(conn.try_clone().unwrap());
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                let req: Map<String, Value> = match serde_json::from_str::<Value>(&line) {
                    Ok(Value::Object(map)) => map,
                    _ => {
                        let _ = conn.write_all(b"{\"ok\":false,\"error\":{\"code\":\"parse\",\"message\":\"bad json\"}}\n");
                        return;
                    }
                };
                let _ = tx.send(req.clone());
                let result = if map_str(&req, "method") == "workspace.create" {
                    json!({"workspace_id": "ws-1", "surface_id": "surf-1"})
                } else {
                    json!({"ok": true})
                };
                let resp = json!({"id": req.get("id").cloned().unwrap_or(Value::Null), "ok": true, "result": result});
                let _ =
                    conn.write_all(format!("{}\n", cmuxd_remote::util::go_json(&resp)).as_bytes());
            });
        }
    });
    (MockSocket { path, _dir: dir }, rx)
}

#[test]
fn new_workspace_command() {
    let _env = clean_env();
    let (sock, rx) = start_mock_create_socket();
    assert_eq!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "new-workspace",
            "--command",
            "claude ."
        ]),
        0
    );

    expect_request(&rx, "workspace.create");
    let send_text = expect_request(&rx, "surface.send_text");
    assert_eq!(map_str(&send_text, "surface_id"), "surf-1");
    assert_eq!(map_str(&send_text, "text"), "claude .");
    let send_key = expect_request(&rx, "surface.send_key");
    assert_eq!(map_str(&send_key, "surface_id"), "surf-1");
    assert_eq!(map_str(&send_key, "key"), "return");
}

#[test]
fn send_positional() {
    let _env = clean_env();
    {
        let (sock, rx) = start_mock_v2_socket_with_request_capture();
        assert_eq!(
            run_cli_code(&["--socket", &sock.path, "send", "hello world"]),
            0
        );
        let params = expect_request(&rx, "surface.send_text");
        assert_eq!(map_str(&params, "text"), "hello world");
    }
    {
        let (sock, rx) = start_mock_v2_socket_with_request_capture();
        assert_eq!(
            run_cli_code(&["--socket", &sock.path, "send-key", "ctrl+c"]),
            0
        );
        let params = expect_request(&rx, "surface.send_key");
        assert_eq!(map_str(&params, "key"), "ctrl+c");
    }
    {
        let sock = start_mock_v2_socket();
        assert_ne!(
            run_cli_code(&["--socket", &sock.path, "send", "--text", "hello"]),
            0,
            "--text is not a flag"
        );
    }
}

#[test]
fn positional_rejected_on_flag_only_commands() {
    let _env = clean_env();
    let sock = start_mock_v2_socket();
    assert_ne!(
        run_cli_code(&[
            "--socket",
            &sock.path,
            "new-workspace",
            "unexpected-positional"
        ]),
        0
    );
}

#[test]
fn new_commands_method() {
    let _env = clean_env();
    let cases: &[(&[&str], &str)] = &[
        (&["next-workspace"], "workspace.next"),
        (&["previous-workspace"], "workspace.previous"),
        (&["last-workspace"], "workspace.last"),
        (&["equalize-splits"], "workspace.equalize_splits"),
        (&["last-pane"], "pane.last"),
        (&["swap-pane", "--pane", "p1"], "pane.swap"),
        (&["break-pane", "--pane", "p1"], "pane.break"),
        (&["read-screen"], "surface.read_text"),
        (&["clear-history"], "surface.clear_history"),
        (&["jump-to-unread"], "notification.jump_to_unread"),
        (
            &["dismiss-notification", "--id", "n1"],
            "notification.dismiss",
        ),
        (
            &["mark-notification-read", "--id", "n1"],
            "notification.mark_read",
        ),
        (&["open-notification", "--id", "n1"], "notification.open"),
    ];
    for (args, method) in cases {
        let (sock, rx) = start_mock_v2_socket_with_request_capture();
        let mut full = vec!["--socket", sock.path.as_str()];
        full.extend_from_slice(args);
        assert_eq!(run_cli_code(&full), 0, "{}: exit", args[0]);
        expect_request(&rx, method);
    }
}

// --- cloud_cli_bridge_test.go ---

type ServerSlot = Arc<Mutex<Option<Arc<RpcServer>>>>;

fn bridge_server(
    bridge: &Arc<CloudCliBridge>,
    on_event: impl Fn(&Arc<RpcServer>, &str) + Send + Sync + 'static,
) -> Arc<RpcServer> {
    let slot: ServerSlot = Arc::new(Mutex::new(None));
    let slot_ref = Arc::clone(&slot);
    let writer = FnFrameWriter::new(
        |_| Ok(()),
        move |event| {
            let server = slot_ref.lock().unwrap().clone().expect("server registered");
            on_event(&server, &event.request_id);
            Ok(())
        },
    );
    let server = RpcServer::builder()
        .cli_bridge(Arc::clone(bridge))
        .frame_writer(writer)
        .build();
    *slot.lock().unwrap() = Some(Arc::clone(&server));
    server
}

fn cli_response(server: &Arc<RpcServer>, id: &str, request_id: &str, payload: &[u8]) {
    let params =
        json!({"request_id": request_id, "ok": true, "data_base64": base64_encode(payload)});
    let req = RpcRequest::new(id, "cli.response", params.as_object().cloned());
    let resp = server.handle_cli_response(&req);
    assert!(resp.ok, "cli.response failed: {}", resp.to_json());
}

#[test]
fn cloud_cli_bridge_forwards_request_through_rpc_event() {
    let bridge = CloudCliBridge::new();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_ref = Arc::clone(&seen);
    let bridge_ref = Arc::clone(&bridge);
    let writer_slot: ServerSlot = Arc::new(Mutex::new(None));
    let slot_ref = Arc::clone(&writer_slot);
    let writer = FnFrameWriter::new(
        |_| Ok(()),
        move |event| {
            assert_eq!(event.event, "cli.request");
            assert!(!event.request_id.is_empty(), "request_id was empty");
            let request = base64_decode(&event.data_base64);
            seen_ref
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&request).into_owned());
            let server = slot_ref.lock().unwrap().clone().unwrap();
            cli_response(&server, "response", &event.request_id, b"pong\n");
            Ok(())
        },
    );
    let server = RpcServer::builder()
        .cli_bridge(Arc::clone(&bridge_ref))
        .frame_writer(writer)
        .build();
    *writer_slot.lock().unwrap() = Some(Arc::clone(&server));
    let _registration = bridge.register(&server);

    let response = bridge.forward(b"ping\n").expect("forward failed");
    assert_eq!(response, b"pong\n");
    assert_eq!(seen.lock().unwrap().as_slice(), &["ping\n".to_string()]);
}

#[test]
fn cloud_cli_bridge_skips_wrong_workspace_responses() {
    let bridge = CloudCliBridge::new();
    let denied = bridge_server(&bridge, |server, request_id| {
        cli_response(
            server,
            "denied-response",
            request_id,
            b"{\"ok\":false,\"error\":{\"code\":\"remote_cli_workspace_denied\",\"message\":\"wrong workspace\"}}\n",
        );
    });
    let accepted = bridge_server(&bridge, |server, request_id| {
        cli_response(
            server,
            "accepted-response",
            request_id,
            b"{\"ok\":true,\"result\":{\"delivered\":true}}\n",
        );
    });
    let _denied_registration = bridge.register(&denied);
    let _accepted_registration = bridge.register(&accepted);

    let response = bridge.forward(b"notify\n").expect("forward failed");
    assert_eq!(
        String::from_utf8_lossy(&response),
        "{\"ok\":true,\"result\":{\"delivered\":true}}\n"
    );
}

// --- agent_launch_test.go ---

#[test]
fn omo_ensure_plugin_invalid_json_error_does_not_expose_user_path() {
    let env = clean_env();
    let home = env.home();
    let user_dir = format!("{home}/.config/opencode");
    std::fs::create_dir_all(&user_dir).unwrap();
    let user_json_path = format!("{user_dir}/opencode.json");
    std::fs::write(&user_json_path, "{").unwrap();

    let mut stderr: Vec<u8> = Vec::new();
    let err = omo_ensure_plugin(&std::env::var("PATH").unwrap_or_default(), &mut stderr)
        .expect_err("omo_ensure_plugin returned Ok for invalid opencode.json");
    assert!(
        !err.contains(&home) && !err.contains(&user_json_path),
        "error {err:?} exposes user config path"
    );
    assert!(
        err.contains("invalid opencode.json"),
        "error = {err:?}, want generic invalid opencode.json message"
    );
}

// --- tmux_compat_test.go ---

#[test]
fn split_tmux_cmd_cases() {
    let cases: &[(&[&str], &str, usize)] = &[
        (&["list-panes", "-t", "%abc"], "list-panes", 2),
        (&["-V"], "-V", 0),
        (&["-L", "foo", "split-window", "-h"], "split-window", 1),
        (&["Display-Message", "-p"], "display-message", 1),
    ];
    for (args, want_cmd, want_n) in cases {
        let (cmd, rest) = split_tmux_cmd(&strings(args)).expect("unexpected error");
        assert_eq!(cmd, *want_cmd);
        assert_eq!(rest.len(), *want_n, "args count for {args:?}");
    }
}

#[test]
fn parse_tmux_args_cases() {
    let p = parse_tmux_args(
        &strings(&["-dP", "-t", "%abc", "-F", "#{pane_id}", "shell", "cmd"]),
        &["-t", "-F"],
        &["-d", "-P"],
    );
    assert!(p.has_flag("-d"));
    assert!(p.has_flag("-P"));
    assert_eq!(p.value("-t"), "%abc");
    assert_eq!(p.value("-F"), "#{pane_id}");
    assert_eq!(p.positional, strings(&["shell", "cmd"]));
}

#[test]
fn parse_tmux_args_clustered_value_flag() {
    let p = parse_tmux_args(&strings(&["-t%abc"]), &["-t"], &[]);
    assert_eq!(p.value("-t"), "%abc");
}

#[test]
fn tmux_render_format_cases() {
    let ctx: HashMap<String, String> = [
        ("pane_id", "%abc123"),
        ("pane_width", "80"),
        ("window_id", "@ws1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let cases = [
        ("#{pane_id}", "fallback", "%abc123"),
        ("#{pane_id}:#{pane_width}", "", "%abc123:80"),
        ("#{unknown_var}", "fallback", "fallback"),
        ("", "fallback", "fallback"),
        (
            "#{pane_id} #{pane_width} #{window_id}",
            "",
            "%abc123 80 @ws1",
        ),
    ];
    for (format, fallback, want) in cases {
        assert_eq!(
            tmux_render_format(format, &ctx, fallback),
            want,
            "tmux_render_format({format:?})"
        );
    }
}

#[test]
fn tmux_send_keys_text_cases() {
    let cases: &[(&[&str], bool, &str)] = &[
        (&["hello", "world"], true, "hello world"),
        (&["echo", "hello", "Enter"], false, "echo hello\r"),
        (&["C-c"], false, "\x03"),
        (&["ls", "-la", "Enter"], false, "ls -la\r"),
    ];
    for (tokens, literal, want) in cases {
        assert_eq!(
            tmux_send_keys_text(&strings(tokens), *literal),
            *want,
            "tokens {tokens:?}"
        );
    }
}

#[test]
fn tmux_shell_command_text_cases() {
    let cases: &[(&[&str], &str, &str)] = &[
        (&["echo hi"], "", "echo hi\r"),
        (&[], "/tmp", "cd -- '/tmp'\r"),
        (&["make"], "/home/user", "cd -- '/home/user' && make\r"),
        (&[], "", ""),
    ];
    for (positional, cwd, want) in cases {
        assert_eq!(
            tmux_shell_command_text(&strings(positional), cwd),
            *want,
            "({positional:?}, {cwd:?})"
        );
    }
}

#[test]
fn tmux_wait_for_signal_path_shape() {
    let path = tmux_wait_for_signal_path("test-signal");
    assert!(
        path.starts_with("/tmp/cmux-wait-for-"),
        "unexpected path prefix: {path}"
    );
    assert!(path.ends_with(".sig"), "unexpected path suffix: {path}");
}

#[test]
fn tmux_compat_store_round_trip() {
    let _env = clean_env();
    let mut store = load_tmux_compat_store();
    store
        .buffers
        .insert("test".to_string(), "captured text".to_string());
    store.main_vertical_layouts.insert(
        "ws1".to_string(),
        MainVerticalState {
            main_surface_id: "surface-main".to_string(),
            last_column_surface_id: "surface-col".to_string(),
        },
    );
    save_tmux_compat_store(&store).expect("save");

    let loaded = load_tmux_compat_store();
    assert_eq!(
        loaded.buffers.get("test").map(String::as_str),
        Some("captured text")
    );
    let mvs = loaded
        .main_vertical_layouts
        .get("ws1")
        .expect("missing main vertical layout for ws1");
    assert_eq!(mvs.last_column_surface_id, "surface-col");
}

#[test]
fn tmux_version() {
    let _env = clean_env();
    let (_, output) = dispatch(&no_rc(), "-v", &[]);
    assert_eq!(output.trim(), "tmux 3.4");
}

#[test]
fn tmux_display_reporter_format_fields() {
    let mut env = clean_env();
    env.set("CMUX_WORKSPACE_ID", "workspace:1");
    env.set("CMUX_SURFACE_ID", "surface:1");
    let leader_pane_token = format!(
        "%{}",
        tmux_stable_numeric_id("33333333-3333-4333-8333-333333333333")
    );
    env.set("TMUX_PANE", &leader_pane_token);

    let sock = start_mock_tmux_compat_socket();
    let rc = RpcContext::new(&sock.path);
    let fields = [
        "session_id",
        "session_name",
        "window_index",
        "window_id",
        "pane_id",
        "pane_width",
        "pane_height",
        "window_width",
        "window_height",
        "pane_current_path",
        "pane_active",
        "window_active",
        "session_attached",
    ];
    let format = fields
        .iter()
        .map(|f| format!("{f}=#{{{f}}}"))
        .collect::<Vec<_>>()
        .join("\t");

    let (result, output) = dispatch(
        &rc,
        "display-message",
        &["-p", "-F", &format, "-t", &leader_pane_token],
    );
    result.expect("display-message");

    let mut values: HashMap<String, String> = HashMap::new();
    for part in output.trim().split('\t') {
        let (key, value) = part
            .split_once('=')
            .unwrap_or_else(|| panic!("malformed field {part:?} in output {output:?}"));
        values.insert(key.to_string(), value.to_string());
    }
    for field in fields {
        assert!(
            values.contains_key(field),
            "missing field {field:?} in output {output:?}"
        );
    }

    assert_sigil_digits(&values["session_id"], '$', "session_id");
    assert_eq!(values["session_name"], "cmux");
    assert!(
        is_digits(&values["window_index"]),
        "window_index = {:?}",
        values["window_index"]
    );
    assert_sigil_digits(&values["window_id"], '@', "window_id");
    assert_sigil_digits(&values["pane_id"], '%', "pane_id");
    for field in ["pane_width", "pane_height", "window_width", "window_height"] {
        assert!(
            is_digits(&values[field]),
            "{field} = {:?}, want digits",
            values[field]
        );
    }
    assert!(
        values["pane_current_path"].starts_with('/'),
        "pane_current_path = {:?}",
        values["pane_current_path"]
    );
    assert_eq!(
        values["pane_active"], "1",
        "pane_active for stringy focused metadata"
    );
    for field in ["window_active", "session_attached"] {
        assert!(
            values[field] == "0" || values[field] == "1",
            "{field} = {:?}",
            values[field]
        );
    }
}

#[test]
fn tmux_no_ops() {
    let _env = clean_env();
    let no_ops = [
        "set-option",
        "set",
        "set-window-option",
        "setw",
        "source-file",
        "refresh-client",
        "attach-session",
        "detach-client",
        "last-window",
        "next-window",
        "previous-window",
        "set-hook",
        "set-buffer",
        "list-buffers",
    ];
    for cmd in no_ops {
        let (result, _) = dispatch(&no_rc(), cmd, &[]);
        result.unwrap_or_else(|err| panic!("no-op {cmd:?} returned error: {err}"));
    }
}

#[test]
fn tmux_unsupported_command() {
    let _env = clean_env();
    let (result, _) = dispatch(&no_rc(), "some-unknown-cmd", &[]);
    let err = result.expect_err("expected error for unknown command");
    assert!(
        err.contains("unsupported"),
        "error = {err:?}, want to contain 'unsupported'"
    );
}

#[test]
fn is_uuidish_cases() {
    assert!(is_uuidish("D88CE676-0A95-4DDA-AD94-E535B0D966DF"));
    assert!(is_uuidish("d88ce676-0a95-4dda-ad94-e535b0d966df"));
    assert!(!is_uuidish("not-a-uuid"));
}

#[test]
fn tmux_pane_selector_cases() {
    let cases = [
        ("%abc123", "%abc123"),
        ("pane:test", "pane:test"),
        ("@ws1.%pane2", "%pane2"),
        ("@ws1", ""),
        ("", ""),
    ];
    for (input, want) in cases {
        assert_eq!(
            tmux_pane_selector(input),
            want,
            "tmux_pane_selector({input:?})"
        );
    }
}

#[test]
fn tmux_window_selector_cases() {
    let cases = [
        ("%abc123", ""),
        ("pane:test", ""),
        ("@ws1.%pane2", "@ws1"),
        ("@ws1", "@ws1"),
        ("", ""),
    ];
    for (input, want) in cases {
        assert_eq!(
            tmux_window_selector(input),
            want,
            "tmux_window_selector({input:?})"
        );
    }
}

#[test]
fn create_tmux_shim_dir_writes_executable_shim() {
    let _env = clean_env();
    let dir = create_tmux_shim_dir("test-shim-bin", CLAUDE_TEAMS_SHIM_SCRIPT)
        .expect("create_tmux_shim_dir");
    let tmux_path = format!("{dir}/tmux");
    let metadata = std::fs::metadata(&tmux_path).expect("tmux shim not found");
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        metadata.permissions().mode() & 0o111,
        0,
        "tmux shim is not executable"
    );
    let content = std::fs::read_to_string(&tmux_path).unwrap();
    assert!(
        content.contains("__tmux-compat"),
        "shim script should reference __tmux-compat"
    );
}

#[test]
fn create_omo_shim_dir_writes_both_shims() {
    let _env = clean_env();
    let dir = create_omo_shim_dir().expect("create_omo_shim_dir");
    assert!(
        std::fs::metadata(format!("{dir}/tmux")).is_ok(),
        "tmux shim not found"
    );
    assert!(
        std::fs::metadata(format!("{dir}/terminal-notifier")).is_ok(),
        "terminal-notifier shim not found"
    );
}

#[test]
fn configure_agent_environment_sets_expected_vars() {
    let mut env = clean_env();
    for key in [
        "CMUX_CLAUDE_TEAMS_CMUX_BIN",
        "PATH",
        "TMUX",
        "TMUX_PANE",
        "TERM",
        "CMUX_SOCKET_PATH",
        "TERM_PROGRAM",
        "CMUX_WORKSPACE_ID",
        "CMUX_SURFACE_ID",
        "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS",
        "COLORTERM",
        "CMUX_CLAUDE_TEAMS_TERM",
    ] {
        env.track(key);
    }
    env.set("TERM_PROGRAM", "should-be-removed");

    configure_agent_environment(&AgentConfig {
        shim_dir: "/tmp/test-shim".to_string(),
        socket_path: "127.0.0.1:54321".to_string(),
        focused: Some(FocusedContext {
            workspace_id: "ws-abc".to_string(),
            window_id: "win-123".to_string(),
            pane_handle: "pane:456".to_string(),
            pane_id: "pane-456".to_string(),
            surface_id: "surf-789".to_string(),
        }),
        tmux_path_prefix: "cmux-claude-teams".to_string(),
        cmux_bin_env_var: "CMUX_CLAUDE_TEAMS_CMUX_BIN".to_string(),
        term_env_var: "CMUX_CLAUDE_TEAMS_TERM".to_string(),
        extra_env: [(
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS".to_string(),
            "1".to_string(),
        )]
        .into_iter()
        .collect(),
    });

    let getenv = |key: &str| std::env::var(key).unwrap_or_default();
    assert!(
        getenv("PATH").starts_with("/tmp/test-shim:"),
        "PATH should start with shim dir"
    );
    assert!(
        getenv("TMUX").contains("ws-abc"),
        "TMUX = {:?}, should contain workspace ID",
        getenv("TMUX")
    );
    assert_eq!(
        getenv("TMUX_PANE"),
        format!("%{}", tmux_stable_numeric_id("pane-456"))
    );
    assert_eq!(getenv("CMUX_SOCKET_PATH"), "127.0.0.1:54321");
    assert_eq!(getenv("COLORTERM"), "truecolor");
    assert_eq!(getenv("CMUX_WORKSPACE_ID"), "ws-abc");
    assert_eq!(getenv("CMUX_SURFACE_ID"), "surf-789");
    assert_eq!(getenv("CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS"), "1");
    assert!(
        std::env::var("TERM_PROGRAM").is_err(),
        "TERM_PROGRAM should be removed"
    );
}

#[test]
fn get_focused_context_canonicalizes_pane_ref() {
    let _env = clean_env();
    let sock = start_mock_tmux_compat_socket();
    let rc = RpcContext::new(&sock.path);
    let focused = get_focused_context(&rc).expect("get_focused_context returned None");
    assert_eq!(focused.pane_handle, "pane:1");
    assert_eq!(focused.pane_id, "33333333-3333-4333-8333-333333333333");
}

#[test]
fn get_focused_context_keeps_base_context_when_canonicalization_times_out() {
    let _env = clean_env();
    let sock = start_slow_focused_canonicalization_socket(Duration::from_millis(200));
    let rc = RpcContext::new(&sock.path);
    let focused =
        get_focused_context_with_timeout(&rc, Duration::from_millis(50)).expect("returned None");
    assert_eq!(focused.workspace_id, "11111111-1111-4111-8111-111111111111");
    assert_eq!(focused.pane_handle, "pane:1");
    assert_eq!(
        focused.pane_id, "pane:1",
        "want base pane id when canonicalization times out"
    );
}

#[test]
fn tmux_sigiled_selectors_skip_refs_and_indexes() {
    let _env = clean_env();
    let sock = start_mock_tmux_compat_socket();
    let rc = RpcContext::new(&sock.path);
    let workspace_id = "11111111-1111-4111-8111-111111111111";
    let pane_id = "33333333-3333-4333-8333-333333333333";

    assert_eq!(
        tmux_resolve_workspace_id(&rc, "1").as_deref(),
        Ok(workspace_id)
    );
    assert_eq!(
        tmux_canonical_pane_id(&rc, "1", workspace_id).as_deref(),
        Ok(pane_id)
    );
    assert!(
        tmux_resolve_workspace_id(&rc, "$1").is_err(),
        "sigiled workspace selector $1 resolved by index"
    );
    assert!(
        tmux_canonical_pane_id(&rc, "%1", workspace_id).is_err(),
        "sigiled pane selector %1 resolved by index"
    );
    let ws_token = format!("${}", tmux_stable_numeric_id(workspace_id));
    assert_eq!(
        tmux_resolve_workspace_id(&rc, &ws_token).as_deref(),
        Ok(workspace_id)
    );
    let pane_token = format!("%{}", tmux_stable_numeric_id(pane_id));
    assert_eq!(
        tmux_canonical_pane_id(&rc, &pane_token, workspace_id).as_deref(),
        Ok(pane_id)
    );
}

#[test]
fn tmux_resolve_workspace_id_accepts_sigiled_uuid_without_list() {
    let _env = clean_env();
    let workspace_id = "11111111-1111-4111-8111-111111111111";
    let dir = temp_dir("cmux-missing-");
    let rc = RpcContext::new(&path_str(&dir, "missing.sock"));
    for raw in [format!("${workspace_id}"), format!("@{workspace_id}")] {
        assert_eq!(
            tmux_resolve_workspace_id(&rc, &raw).as_deref(),
            Ok(workspace_id),
            "raw {raw:?}"
        );
    }
}

#[test]
fn tmux_canonical_selectors_prefer_refs_before_index_fallback() {
    let _env = clean_env();
    let sock = start_mock_tmux_selector_priority_socket();
    let rc = RpcContext::new(&sock.path);
    let workspace_id = "11111111-1111-4111-8111-111111111111";
    assert_eq!(
        tmux_canonical_pane_id(&rc, "1", workspace_id).as_deref(),
        Ok("33333333-3333-4333-8333-333333333333")
    );
    assert_eq!(
        tmux_canonical_surface_id(&rc, "1", workspace_id).as_deref(),
        Ok("55555555-5555-4555-8555-555555555555")
    );
}

#[test]
fn claude_teams_launch_args_cases() {
    let args = claude_teams_launch_args(&strings(&["--verbose"]));
    assert_eq!(args, strings(&["--teammate-mode", "auto", "--verbose"]));
    let args = claude_teams_launch_args(&strings(&["--teammate-mode", "off"]));
    assert_eq!(args, strings(&["--teammate-mode", "off"]));
}

#[test]
fn merge_node_options_cases() {
    let restore = "/tmp/restore-node-options.cjs";
    let want = "--require=/tmp/restore-node-options.cjs --max-old-space-size=4096";
    assert_eq!(merge_node_options("", restore), want);
    assert_eq!(
        merge_node_options("--trace-warnings", restore),
        format!("{want} --trace-warnings")
    );
    assert_eq!(
        merge_node_options("--max-old-space-size=2048 --trace-warnings", restore),
        format!("{want} --trace-warnings")
    );
    assert_eq!(
        merge_node_options("--max-old-space-size 2048 --trace-warnings", restore),
        format!("{want} --trace-warnings")
    );
}

#[test]
fn tmux_wait_for_signal_round_trip() {
    let _env = clean_env();
    let name = format!("test-roundtrip-{}", random_hex(4));
    let path = tmux_wait_for_signal_path(&name);
    let _ = dispatch(&no_rc(), "wait-for", &["-S", &name]);
    assert!(std::fs::metadata(&path).is_ok(), "signal file not created");
    let (result, _) = dispatch(&no_rc(), "wait-for", &[&name]);
    result.expect("wait-for should succeed");
    assert!(
        std::fs::metadata(&path).is_err(),
        "signal file should be removed after wait"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn tmux_show_buffer_prints_default_buffer() {
    let _env = clean_env();
    let mut store = load_tmux_compat_store();
    store
        .buffers
        .insert("default".to_string(), "hello world".to_string());
    save_tmux_compat_store(&store).unwrap();
    let mut out: Vec<u8> = Vec::new();
    tmux_show_buffer(&[], &mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap().trim(), "hello world");
}

// --- tmux_split_ref_test.go ---

fn split_env() -> TestEnv {
    let mut env = clean_env();
    env.set("CMUX_WORKSPACE_ID", "workspace:1");
    env.set("CMUX_SURFACE_ID", "surface:1");
    let token = format!(
        "%{}",
        tmux_stable_numeric_id("33333333-3333-4333-8333-333333333333")
    );
    env.set("TMUX_PANE", &token);
    env
}

#[test]
fn tmux_split_window_canonicalizes_caller_surface_refs() {
    let _env = split_env();
    let sock = start_mock_tmux_compat_socket();
    let rc = RpcContext::new(&sock.path);
    let (result, output) = dispatch(&rc, "split-window", &["-h", "-P", "-F", "#{pane_id}"]);
    result.expect("split-window");
    let want = format!(
        "%{}\n",
        tmux_stable_numeric_id("66666666-6666-4666-8666-666666666666")
    );
    assert_eq!(output, want);
}

#[test]
fn tmux_split_window_ignores_stale_uuid_column_surface() {
    let env = split_env();
    let store_dir = format!("{}/.cmuxterm", env.home());
    std::fs::create_dir_all(&store_dir).unwrap();
    let mut store = TmuxCompatStore::default();
    store.main_vertical_layouts.insert(
        "11111111-1111-4111-8111-111111111111".to_string(),
        MainVerticalState {
            main_surface_id: "44444444-4444-4444-8444-444444444444".to_string(),
            last_column_surface_id: "77777777-7777-4777-8777-777777777777".to_string(),
        },
    );
    store.last_split_surface.insert(
        "11111111-1111-4111-8111-111111111111".to_string(),
        "77777777-7777-4777-8777-777777777777".to_string(),
    );
    std::fs::write(
        format!("{store_dir}/tmux-compat-store.json"),
        serde_json::to_vec(&store).unwrap(),
    )
    .unwrap();

    let sock = start_mock_tmux_compat_socket();
    let rc = RpcContext::new(&sock.path);
    let (result, output) = dispatch(&rc, "split-window", &["-h", "-P", "-F", "#{pane_id}"]);
    result.expect("split-window");
    let want = format!(
        "%{}\n",
        tmux_stable_numeric_id("66666666-6666-4666-8666-666666666666")
    );
    assert_eq!(output, want);
}

// --- tmux_corpus_behavior_test.go ---

#[test]
fn tmux_corpus_new_session_and_new_window_commands_dispatch_shell_text() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start();
    let rc = RpcContext::new(&recorder.socket_path);

    dispatch(
        &rc,
        "new-session",
        &["-d", "-s", "build", "-c", "/tmp", "echo one"],
    )
    .0
    .expect("new-session");
    dispatch(&rc, "new-window", &["-d", "-n", "test", "echo two"])
        .0
        .expect("new-window");

    let want_order = strings(&[
        "workspace.create",
        "workspace.rename",
        "surface.list",
        "surface.send_text",
        "workspace.create",
        "workspace.rename",
        "surface.list",
        "surface.send_text",
    ]);
    assert_eq!(recorder.methods(), want_order);

    let sends = recorder.requests_for("surface.send_text");
    assert_eq!(sends.len(), 2);
    assert_eq!(
        map_str(&sends[0].params, "text"),
        "cd -- '/tmp' && echo one\r"
    );
    assert_eq!(map_str(&sends[1].params, "text"), "echo two\r");

    let creates = recorder.requests_for("workspace.create");
    assert_eq!(creates.len(), 2);
    for req in creates {
        assert_eq!(
            req.params.get("focus"),
            Some(&Value::Bool(false)),
            "detached creation should use focus=false: {:?}",
            req.params
        );
    }
}

#[test]
fn tmux_corpus_has_session_return_semantics() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start();
    let rc = RpcContext::new(&recorder.socket_path);
    dispatch(&rc, "has-session", &["-t", "main"])
        .0
        .expect("has-session existing workspace");
    let err = dispatch(&rc, "has-session", &["-t", "missing"])
        .0
        .expect_err("has-session should fail for a missing workspace");
    assert!(
        err.contains("workspace not found"),
        "has-session error = {err:?}"
    );
}

#[test]
fn tmux_corpus_send_keys_and_tty_key_tokens() {
    let cases: &[(&[&str], bool, &str)] = &[
        (&["printf", "ok", "Enter"], false, "printf ok\r"),
        (&["C-c", "C-d", "C-z", "C-l"], false, "\x03\x04\x1a\x0c"),
        (&["Escape", "Tab", "BSpace"], false, "\x1b\t\x7f"),
        (&["Enter", "C-c", "plain"], true, "Enter C-c plain"),
    ];
    for (tokens, literal, want) in cases {
        assert_eq!(
            tmux_send_keys_text(&strings(tokens), *literal),
            *want,
            "tokens {tokens:?} literal={literal}"
        );
    }
}

#[test]
fn tmux_corpus_format_strings_supported_subset() {
    let ctx: HashMap<String, String> = [
        ("session_name", "cmux"),
        ("window_id", "@workspace"),
        ("window_name", "Build"),
        ("pane_id", "%pane"),
        ("pane_width", "120"),
        ("pane_height", "40"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let cases = [
        (
            "#{session_name}:#{window_name}:#{pane_id}",
            "",
            "cmux:Build:%pane",
        ),
        (
            "#{window_id} #{pane_width}x#{pane_height}",
            "",
            "@workspace 120x40",
        ),
        ("#{unknown}#{also_unknown}", "fallback", "fallback"),
    ];
    for (format, fallback, want) in cases {
        assert_eq!(
            tmux_render_format(format, &ctx, fallback),
            want,
            "tmux_render_format({format:?})"
        );
    }
}

#[test]
fn tmux_corpus_capture_pane_preserves_truecolor_escape_bytes_in_buffers() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start();
    let rc = RpcContext::new(&recorder.socket_path);

    let (result, printed) = dispatch(&rc, "capture-pane", &["-p"]);
    result.expect("capture-pane -p");
    assert_eq!(printed, "\x1b[31mRED\x1b[0m\nplain\n");

    dispatch(&rc, "capture-pane", &[])
        .0
        .expect("capture-pane buffer");
    let (result, output) = dispatch(&no_rc(), "show-buffer", &[]);
    result.expect("show-buffer");
    assert_eq!(output, "\x1b[31mRED\x1b[0m\nplain\n");
}

#[test]
fn tmux_corpus_capture_pane_preserves_terminal_byte_fixtures() {
    let _env = clean_env();
    let cases = [
        ("capture-pane-sgr0", "\x1b[38;2;255;0;0mred\x1b[0mplain\n"),
        (
            "capture-pane-hyperlink",
            "\x1b]8;;https://example.test\x1b\\linked\x1b]8;;\x1b\\\n",
        ),
        (
            "osc-11colours-truecolor",
            "\x1b]11;rgb:12/34/56\x1b\\background\n\x1b]111\x1b\\reset\n",
        ),
        (
            "utf8-combining-and-width-bytes",
            "e\u{0301} cafe\u{0301} \u{26A1}\u{FE0F}\n",
        ),
        ("decrqm-sync-response-bytes", "\x1b[?2026;1$ysync-enabled\n"),
    ];
    for (name, text) in cases {
        let recorder = TmuxCorpusRecorder::start();
        recorder.set_read_text(text);
        let rc = RpcContext::new(&recorder.socket_path);

        let (result, printed) = dispatch(&rc, "capture-pane", &["-p"]);
        result.unwrap_or_else(|err| panic!("{name}: capture-pane -p: {err}"));
        assert_eq!(printed, text, "{name}: capture-pane -p output");

        dispatch(&rc, "capture-pane", &[])
            .0
            .unwrap_or_else(|err| panic!("{name}: capture-pane buffer: {err}"));
        let (result, buffered) = dispatch(&no_rc(), "show-buffer", &[]);
        result.unwrap_or_else(|err| panic!("{name}: show-buffer: {err}"));
        assert_eq!(buffered, text, "{name}: show-buffer output");
    }
}

#[test]
fn tmux_corpus_resize_pane_dispatches_absolute_and_directional_resize() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start();
    let rc = RpcContext::new(&recorder.socket_path);

    for args in [
        vec!["-t", "pane:1", "-x", "100"],
        vec!["-t", "pane:1", "-x", "50%"],
        vec!["-t", "pane:1", "-y", "20"],
        vec!["-t", "pane:1", "-L", "7"],
        vec!["-t", "pane:1", "-R"],
        vec!["-t", "pane:1", "-L7"],
    ] {
        dispatch(&rc, "resize-pane", &args)
            .0
            .unwrap_or_else(|err| panic!("resize-pane {args:?}: {err}"));
    }

    let resizes = recorder.requests_for("pane.resize");
    assert_eq!(resizes.len(), 5, "pane.resize requests");
    let p = |i: usize| &resizes[i].params;
    assert_eq!(map_str(p(0), "absolute_axis"), "horizontal");
    assert_eq!(
        as_int(p(0).get("target_pixels"), "absolute resize pixels"),
        412
    );
    assert_eq!(
        as_int(p(0).get("target_cells"), "absolute resize cells"),
        100
    );
    assert_eq!(p(0).get("tmux_compat"), Some(&Value::Bool(true)));
    assert_eq!(map_str(p(1), "absolute_axis"), "horizontal");
    assert_eq!(
        as_int(p(1).get("target_percentage"), "percentage resize target"),
        50
    );
    assert_eq!(
        as_int(p(1).get("target_pixels"), "percentage resize pixels"),
        320
    );
    assert!(
        !p(1).contains_key("target_cells"),
        "percentage resize unexpectedly sent target_cells"
    );
    assert_eq!(map_str(p(2), "direction"), "left");
    assert_eq!(as_int(p(2).get("amount"), "directional resize amount"), 28);
    assert_eq!(
        as_int(p(2).get("amount_cells"), "directional resize cells"),
        7
    );
    assert_eq!(p(2).get("tmux_compat"), Some(&Value::Bool(true)));
    assert_eq!(map_str(p(3), "direction"), "right");
    assert_eq!(
        as_int(p(3).get("amount_cells"), "default directional resize cells"),
        1
    );
    assert_eq!(
        as_int(p(3).get("amount"), "default directional resize amount"),
        4
    );
    assert_eq!(map_str(p(4), "direction"), "left");
    assert_eq!(
        as_int(
            p(4).get("amount_cells"),
            "attached directional resize cells"
        ),
        7
    );
    assert_eq!(
        as_int(p(4).get("amount"), "attached directional resize amount"),
        28
    );
}

#[test]
fn tmux_corpus_exact_absolute_resize_does_not_require_pane_metrics() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start_with_pane_metrics(false);
    let rc = RpcContext::new(&recorder.socket_path);
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-x", "3"])
        .0
        .expect("resize-pane cells without metrics");
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-x", "50%"])
        .0
        .expect("resize-pane percentage without metrics");
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-L7"])
        .0
        .expect("resize-pane relative without metrics");

    let requests = recorder.requests_for("pane.resize");
    assert_eq!(requests.len(), 3);
    assert_eq!(
        as_int(requests[0].params.get("target_cells"), "cell target"),
        3
    );
    assert!(
        !requests[0].params.contains_key("target_pixels"),
        "cell resize unexpectedly sent target_pixels"
    );
    assert_eq!(
        as_int(
            requests[1].params.get("target_percentage"),
            "percentage target"
        ),
        50
    );
    assert!(
        !requests[1].params.contains_key("target_pixels"),
        "percentage resize unexpectedly sent target_pixels"
    );
    assert_eq!(
        as_int(requests[2].params.get("amount_cells"), "relative cells"),
        7
    );
    assert!(
        !requests[2].params.contains_key("amount"),
        "relative resize unexpectedly sent amount"
    );
}

#[test]
fn tmux_corpus_pixel_metrics_do_not_become_point_fallbacks() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start_with_metric_availability(true, false, true);
    let rc = RpcContext::new(&recorder.socket_path);
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-x", "3"])
        .0
        .expect("cells with pixel-only metrics");
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-x", "50%"])
        .0
        .expect("percentage with pixel-only metrics");
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-L7"])
        .0
        .expect("relative with pixel-only metrics");

    let requests = recorder.requests_for("pane.resize");
    assert_eq!(requests.len(), 3);
    assert_eq!(
        as_int(requests[0].params.get("target_cells"), "cell target"),
        3
    );
    assert!(
        !requests[0].params.contains_key("target_pixels"),
        "cell resize used pixel metrics as point fallback"
    );
    assert_eq!(
        as_int(
            requests[1].params.get("target_percentage"),
            "percentage target"
        ),
        50
    );
    assert_eq!(
        as_int(requests[1].params.get("target_pixels"), "percentage points"),
        320
    );
    assert_eq!(
        as_int(requests[2].params.get("amount_cells"), "relative cells"),
        7
    );
    assert!(
        !requests[2].params.contains_key("amount"),
        "relative resize used pixel metrics as point fallback"
    );
}

#[test]
fn tmux_corpus_percentage_without_container_frame_omits_point_fallback() {
    let _env = clean_env();
    let recorder = TmuxCorpusRecorder::start_with_metric_availability(true, true, false);
    let rc = RpcContext::new(&recorder.socket_path);
    dispatch(&rc, "resize-pane", &["-t", "pane:1", "-x", "50%"])
        .0
        .expect("percentage without container frame");

    let requests = recorder.requests_for("pane.resize");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        as_int(
            requests[0].params.get("target_percentage"),
            "percentage target"
        ),
        50
    );
    assert!(
        !requests[0].params.contains_key("target_pixels"),
        "sent target_pixels without container frame"
    );
}

#[test]
fn tmux_corpus_tmux_only_features_fail_explicitly_or_no_op_deliberately() {
    let _env = clean_env();
    for command in [
        "copy-mode",
        "if-shell",
        "run-shell",
        "choose-tree",
        "display-popup",
    ] {
        let err = dispatch(&no_rc(), command, &[])
            .0
            .expect_err("should not be silently treated as supported");
        assert!(
            err.contains("unsupported"),
            "{command} error = {err:?}, want unsupported"
        );
    }
    for command in [
        "source-file",
        "set-option",
        "set-window-option",
        "refresh-client",
    ] {
        dispatch(&no_rc(), command, &["ignored"])
            .0
            .unwrap_or_else(|err| panic!("{command} should be a no-op: {err}"));
    }
}

// --- fuzz seeds (tmux_corpus_fuzz_test.go), run as deterministic smoke checks ---

#[test]
fn tmux_compat_arg_parser_seeds_do_not_panic() {
    let seeds = [
        "new-session -d -s build -c /tmp echo ok",
        "split-window -h -P -F #{pane_id}",
        "capture-pane -p -S -2000",
        "display-message -p -F #{session_name}:#{window_name}:#{pane_id}",
        "-L cmux has-session -t main",
        "-- send-keys -l C-c Enter",
        "",
        "-",
        "--",
        "-t",
        "-dP -t",
    ];
    for seed in seeds {
        let fields: Vec<String> = seed.split_whitespace().map(String::from).collect();
        let _ = split_tmux_cmd(&fields);
        let _ = parse_tmux_args(
            &fields,
            &["-c", "-F", "-n", "-s", "-t", "-x", "-y", "-S", "-E"],
            &[
                "-A", "-b", "-d", "-D", "-h", "-J", "-L", "-N", "-p", "-P", "-R", "-U", "-v",
            ],
        );
    }
}

#[test]
fn tmux_render_format_seeds_do_not_panic() {
    let ctx: HashMap<String, String> = [
        ("session_name", "cmux"),
        ("window_id", "@workspace"),
        ("window_index", "1"),
        ("window_name", "main"),
        ("pane_id", "%pane"),
        ("pane_width", "120"),
        ("pane_height", "40"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for seed in [
        "#{session_name}",
        "#{window_id}:#{window_index}:#{window_name}",
        "#{pane_id} #{pane_width}x#{pane_height}",
        "#{unknown} #{session_name} #{also_unknown}",
        "#[fg=#ff0000]#{pane_id}",
        "#{",
        "#{}",
        "}#{pane_id",
    ] {
        let _ = tmux_render_format(seed, &ctx, "fallback");
    }
}

#[test]
fn tmux_send_keys_seeds_do_not_panic() {
    for seed in [
        "Enter",
        "C-c C-d C-z C-l",
        "Escape Tab BSpace",
        "printf hello Enter",
        "38;2;255;0;0m OSC 11 truecolor",
        "C-",
        "M-x",
    ] {
        let tokens: Vec<String> = seed.split_whitespace().map(String::from).collect();
        let _ = tmux_send_keys_text(&tokens, false);
        let _ = tmux_send_keys_text(&tokens, true);
    }
}

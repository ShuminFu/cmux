//! CLI relay against a fake relay socket that records every request.

mod common;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::sync::{Arc, Mutex};

use cmuxd_remote::cli::{CliIo, run_cli_with_io};
use common::*;
use serde_json::{Value, json};

const WS: &str = "11111111-1111-1111-1111-111111111111";
const PANE: &str = "22222222-2222-2222-2222-222222222222";
const SURF: &str = "33333333-3333-3333-3333-333333333333";

fn canned(method: &str) -> Option<String> {
    let resp = match method {
        "system.ping" => json!({"ok": true, "result": {"pong": true}}),
        "workspace.list" => {
            json!({"ok": true, "result": {"workspaces": [{"id": WS, "ref": "workspace:1", "index": 0, "title": "main", "active": true}]}})
        }
        "workspace.current" => json!({"ok": true, "result": {"workspace_id": WS}}),
        "workspace.create" => {
            json!({"ok": true, "result": {"workspace_id": WS, "surface_id": SURF}})
        }
        "surface.current" => {
            json!({"ok": true, "result": {"workspace_id": WS, "pane_id": PANE, "surface_id": SURF}})
        }
        "pane.list" => {
            json!({"ok": true, "result": {"panes": [{"id": PANE, "ref": "pane:1", "index": 0, "focused": true, "columns": 120, "rows": 40, "cell_width_px": 8, "cell_height_px": 16, "cell_width_points": 7.5, "pixel_frame": {"x": 16, "y": 32, "width": 900, "height": 640}}], "container_frame": {"width": 1800, "height": 960}}})
        }
        "surface.list" => {
            json!({"ok": true, "result": {"surfaces": [{"id": SURF, "ref": "surface:1", "index": 0, "title": "zsh", "focused": true}]}})
        }
        "pane.surfaces" => {
            json!({"ok": true, "result": {"surfaces": [{"id": SURF, "selected": true}]}})
        }
        "surface.split" => {
            json!({"ok": true, "result": {"surface_id": "55555555-5555-5555-5555-555555555555", "pane_id": PANE}})
        }
        "surface.read_text" => json!({"ok": true, "result": {"text": "line1\nline2\n"}}),
        "system.capabilities" => {
            json!({"ok": true, "result": {"caps": ["a"], "html": "<b>&</b>", "empty": {}}})
        }
        "workspace.group.list" => json!({"ok": true, "result": "plain string"}),
        "fail.method" => json!({"ok": false, "error": {"code": "boom", "message": "it broke"}}),
        "raw.text" => return Some("not json at all\n".to_string()),
        "hang.eof" => return None,
        _ => json!({"ok": true, "result": {}}),
    };
    Some(format!("{resp}\n"))
}

struct FakeRelay {
    path: std::path::PathBuf,
    requests: Arc<Mutex<Vec<Frame>>>,
    _dir: tempfile::TempDir,
}

impl FakeRelay {
    fn start() -> Self {
        let dir = temp_socket_dir();
        let path = dir.path().join("relay.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { break };
                let recorded = Arc::clone(&recorded);
                std::thread::spawn(move || {
                    let mut line = String::new();
                    if BufReader::new(conn.try_clone().unwrap()).read_line(&mut line).unwrap_or(0)
                        == 0
                    {
                        return;
                    }
                    let frame = parse_frame(&line);
                    let method = str_field(&frame, "method");
                    recorded.lock().unwrap().push(frame);
                    if let Some(resp) = canned(&method) {
                        let _ = conn.write_all(resp.as_bytes());
                    }
                });
            }
        });
        Self { path, requests, _dir: dir }
    }

    fn take(&self) -> Vec<Frame> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }

    fn run(&self, args: &[&str]) -> (i32, String, String) {
        let mut full = vec!["--socket".to_string(), self.path.to_string_lossy().into_owned()];
        full.extend(args.iter().map(|s| (*s).to_string()));
        run(&full)
    }
}

fn run(args: &[String]) -> (i32, String, String) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let code = {
        let mut io = CliIo { stdout: &mut stdout, stderr: &mut stderr };
        run_cli_with_io(args, &mut io)
    };
    (
        code,
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_string()).collect()
}

fn clean_env() -> EnvGuard {
    EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("CMUX_WORKSPACE_ID", None),
        ("CMUX_SURFACE_ID", None),
        ("TMUX_PANE", None),
        ("CMUX_PANE_ID", None),
        ("CMUX_RELAY_ID", None),
        ("CMUX_RELAY_TOKEN", None),
    ])
}

#[test]
fn usage_and_dispatch_errors() {
    let _env = clean_env();
    let (code, _, err) = run(&[]);
    assert_eq!(code, 2);
    assert!(err.starts_with("Usage: cmux [--socket <path>] [--json] <command> [args...]\n"));
    for flag in ["--help", "-h", "help"] {
        let (code, _, err) = run(&args(&[flag]));
        assert_eq!(code, 0, "{flag}");
        assert!(err.contains("  rpc <method> [json-params] Send arbitrary JSON-RPC\n"));
    }
    let (code, _, err) = run(&args(&["--socket"]));
    assert_eq!((code, err.as_str()), (2, "cmux: --socket requires a path\n"));
    let (code, _, err) = run(&args(&["--socket", "/tmp/x.sock", "bogus"]));
    assert_eq!((code, err.as_str()), (2, "cmux: unknown command \"bogus\"\n"));
    let (code, _, err) = run(&args(&["--socket", "/tmp/x.sock", "workspace", "list"]));
    assert_eq!(code, 2);
    assert!(err.starts_with("cmux workspace: only the \"group\" subcommand is supported here."));
}

#[test]
fn no_socket_reports_missing_configuration() {
    let home = tempfile::tempdir().unwrap();
    let _env =
        EnvGuard::set(&[("CMUX_SOCKET_PATH", None), ("HOME", Some(home.path().to_str().unwrap()))]);
    let (code, _, err) = run(&args(&["ping"]));
    assert_eq!(code, 1);
    if std::path::Path::new(cmuxd_remote::cli_bridge::DEFAULT_CLOUD_CLI_BRIDGE_SOCKET_PATH).exists()
    {
        // A cloud CLI bridge socket on this host is the last discovery fallback.
        assert!(err.contains("failed to connect to /tmp/cmux-cloud-cli.sock"), "{err}");
    } else {
        assert_eq!(err, "cmux: CMUX_SOCKET_PATH not set and --socket not provided\n");
    }
    let (code, _, err) = run(&args(&["--socket", "/nonexistent/x.sock", "ping"]));
    assert_eq!(code, 1);
    assert_eq!(
        err,
        "cmux: failed to connect to /nonexistent/x.sock: dial unix /nonexistent/x.sock: connect: no such file or directory\n"
    );
}

#[test]
fn ping_and_json_flag_and_default_output() {
    let _env = clean_env();
    let relay = FakeRelay::start();
    let (code, out, _) = relay.run(&["ping"]);
    assert_eq!((code, out.as_str()), (0, "{\n  \"pong\": true\n}\n"));
    let (code, out, _) = relay.run(&["--json", "ping"]);
    assert_eq!((code, out.as_str()), (0, "{\"pong\":true}\n"));
    let reqs = relay.take();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0]["method"], "system.ping");
    assert_eq!(reqs[0]["params"], json!({}));
    assert_eq!(reqs[0]["id"].as_str().unwrap().len(), 16);
    let (_, out, _) = relay.run(&["--json", "capabilities"]);
    assert_eq!(
        out,
        "{\"caps\":[\"a\"],\"empty\":{},\"html\":\"\\u003cb\\u003e\\u0026\\u003c/b\\u003e\"}\n"
    );
    let (_, out, _) = relay.run(&["new-window"]);
    assert_eq!(out, "OK\n", "empty results print OK");
    let (_, out, _) = relay.run(&["workspace", "group", "list"]);
    assert_eq!(out, "plain string\n");
    let (code, out, err) = relay.run(&["rpc", "raw.text"]);
    assert_eq!((code, out.as_str(), err.as_str()), (0, "not json at all\n", ""));
    let (code, _, err) = relay.run(&["rpc", "fail.method"]);
    assert_eq!((code, err.as_str()), (1, "cmux: server error [boom]: it broke\n"));
    let (code, _, err) = relay.run(&["rpc", "hang.eof"]);
    assert_eq!((code, err.as_str()), (1, "cmux: failed to read response: EOF\n"));
}

#[test]
fn v2_flag_mapping_and_bool_coercion() {
    let _env = clean_env();
    let relay = FakeRelay::start();
    assert_eq!(relay.run(&["list-workspaces", "--window", "w1"]).0, 0);
    assert_eq!(relay.run(&["focus-panel", "--panel", "p9"]).0, 0);
    assert_eq!(
        relay.run(&["join-pane", "--target-pane", "t", "--focus", "yes", "--no-focus", "0"]).0,
        0
    );
    assert_eq!(relay.run(&["new-pane", "--type", "browser"]).0, 0);
    assert_eq!(relay.run(&["send", "--surface", "s1", "hello", "world"]).0, 0);
    assert_eq!(relay.run(&["new-surface", "--working-directory", "/w", "--focus", "true"]).0, 0);
    let reqs = relay.take();
    assert_eq!(reqs[0]["method"], "workspace.list");
    assert_eq!(reqs[0]["params"], json!({"window_id": "w1"}));
    assert_eq!(reqs[1]["method"], "surface.focus");
    assert_eq!(reqs[1]["params"], json!({"surface_id": "p9"}));
    assert_eq!(reqs[2]["params"], json!({"target_pane_id": "t", "focus": true, "no_focus": false}));
    assert_eq!(reqs[3]["method"], "pane.create");
    assert_eq!(reqs[3]["params"], json!({"type": "browser", "direction": "right"}));
    assert_eq!(reqs[4]["method"], "surface.send_text");
    assert_eq!(reqs[4]["params"], json!({"surface_id": "s1", "text": "hello world"}));
    assert_eq!(reqs[5]["params"], json!({"working_directory": "/w", "focus": true}));

    let (code, _, err) = relay.run(&["join-pane", "--focus", "maybe"]);
    assert_eq!((code, err.as_str()), (2, "cmux: --focus must be true or false\n"));
    let (code, _, err) = relay.run(&["list-workspaces", "--nope", "x"]);
    assert_eq!((code, err.as_str()), (2, "cmux: unknown flag --nope\n"));
    let (code, _, err) = relay.run(&["list-workspaces", "--window"]);
    assert_eq!((code, err.as_str()), (2, "cmux: flag --window requires a value\n"));
    let (code, _, err) = relay.run(&["list-workspaces", "extra"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux: list-workspaces does not accept positional arguments\n")
    );
    let (code, _, err) = relay.run(&["rename-workspace", "New Name"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux: rename-workspace does not accept positional arguments\n")
    );
    assert!(relay.take().is_empty(), "rejected invocations never hit the socket");
}

#[test]
fn env_fallbacks_and_notify_caller_routing() {
    let relay = FakeRelay::start();
    {
        let _env = EnvGuard::set(&[
            ("CMUX_SOCKET_PATH", None),
            ("CMUX_WORKSPACE_ID", Some("env-ws")),
            ("CMUX_SURFACE_ID", Some("env-surface")),
            ("TMUX_PANE", None),
        ]);
        assert_eq!(relay.run(&["send", "hi"]).0, 0);
        assert_eq!(relay.run(&["send", "--surface", "explicit", "hi"]).0, 0);
        assert_eq!(relay.run(&["notify", "--title", "t"]).0, 0);
        assert_eq!(relay.run(&["list-windows"]).0, 0);
    }
    let reqs = relay.take();
    assert_eq!(
        reqs[0]["params"],
        json!({"surface_id": "env-surface", "text": "hi", "workspace_id": "env-ws"})
    );
    assert_eq!(reqs[1]["params"]["surface_id"], "explicit");
    assert_eq!(reqs[2]["method"], "notification.create_for_caller");
    assert_eq!(
        reqs[2]["params"],
        json!({"title": "t", "preferred_workspace_id": "env-ws", "preferred_surface_id": "env-surface"})
    );
    assert_eq!(reqs[3]["params"], json!({}), "noParams commands ignore env fallbacks");
    let _env = clean_env();
    assert_eq!(relay.run(&["notify", "--title", "t", "--body", "b"]).0, 0);
    assert_eq!(relay.take()[0]["method"], "notification.create");
}

#[test]
fn new_workspace_relay_flags() {
    let _env = clean_env();
    let relay = FakeRelay::start();
    let envfile = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(envfile.path(), "# comment\n\nFROM_FILE=1\nQUOTED=a=b\n").unwrap();
    let (code, out, _) = relay.run(&[
        "new-workspace",
        "--name",
        "n",
        "--cwd",
        "/c",
        "--focus",
        "false",
        "--group-placement",
        "after",
        "--group-reference",
        "ref",
        "--env",
        "A=1",
        "--env",
        "B=2",
        "--env-file",
        envfile.path().to_str().unwrap(),
        "--layout",
        "{\"a\":[1,2]}",
        "--command",
        "ls -la",
    ]);
    assert_eq!(code, 0);
    assert!(out.contains("surface_id"), "{out}");
    let reqs = relay.take();
    assert_eq!(reqs.len(), 3, "{reqs:?}");
    assert_eq!(reqs[0]["method"], "workspace.create");
    assert_eq!(
        reqs[0]["params"],
        json!({"title": "n", "cwd": "/c", "focus": false, "placement": "after", "group_reference_workspace_id": "ref", "env": {"A": "1", "B": "2", "FROM_FILE": "1", "QUOTED": "a=b"}, "layout": {"a": [1, 2]}})
    );
    assert_eq!(reqs[1]["method"], "surface.send_text");
    assert_eq!(reqs[1]["params"], json!({"surface_id": SURF, "text": "ls -la"}));
    assert_eq!(reqs[2]["method"], "surface.send_key");
    assert_eq!(reqs[2]["params"], json!({"surface_id": SURF, "key": "return"}));

    let (code, _, err) = relay.run(&["new-workspace", "--layout", "{"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux new-workspace: --layout must be valid JSON: unexpected end of JSON input\n")
    );
    let (code, _, err) = relay.run(&["new-workspace", "--env", "NOEQ"]);
    assert_eq!((code, err.as_str()), (2, "cmux new-workspace: --env \"NOEQ\" must be KEY=VALUE\n"));
    let (code, _, err) = relay.run(&["new-workspace", "positional"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux: new-workspace does not accept positional arguments\n")
    );
    let (code, _, err) = relay.run(&["new-workspace", "--working-directory", "/x"]);
    assert_eq!((code, err.as_str()), (2, "cmux new-workspace: unknown flag --working-directory\n"));
    let (code, _, err) = relay.run(&["new-workspace", "--env-file", "/nonexistent/env"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux new-workspace: --env-file: open /nonexistent/env: no such file or directory\n")
    );
}

#[test]
fn rpc_passthrough() {
    let _env = clean_env();
    let relay = FakeRelay::start();
    let (code, out, _) = relay.run(&["rpc", "custom.method", "{\"x\":1,\"y\":[true,null]}"]);
    assert_eq!((code, out.as_str()), (0, "{}\n"));
    assert_eq!(relay.take()[0]["params"], json!({"x": 1, "y": [true, null]}));
    let (code, _, err) = relay.run(&["rpc"]);
    assert_eq!((code, err.as_str()), (2, "cmux rpc: requires a method name\n"));
    let (code, _, err) = relay.run(&["rpc", "m", "[1]"]);
    assert_eq!(
        (code, err.as_str()),
        (
            2,
            "cmux rpc: invalid JSON params: json: cannot unmarshal array into Go value of type map[string]interface {}\n"
        )
    );
    let (code, _, err) = relay.run(&["rpc", "m", "{"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux rpc: invalid JSON params: unexpected end of JSON input\n")
    );
}

#[test]
fn workspace_group_relay() {
    let _env = clean_env();
    let relay = FakeRelay::start();
    let (code, _, err) = relay.run(&["workspace", "group"]);
    assert_eq!(code, 2);
    assert!(err.starts_with("cmux workspace group: requires a subcommand (list, create,"));
    let (code, _, err) = relay.run(&["workspace-group", "zzz"]);
    assert_eq!(code, 2);
    assert!(err.starts_with("cmux workspace group: unknown subcommand \"zzz\""));
    assert_eq!(relay.run(&["workspace", "group", "create", "Name", "--from", "a, b,,c"]).0, 0);
    assert_eq!(relay.run(&["workspace-group", "rename", "g1", "newname"]).0, 0);
    assert_eq!(relay.run(&["workspace", "group", "move", "--group", "g", "--to-index", "3"]).0, 0);
    assert_eq!(relay.run(&["workspace", "group", "set-color", "g"]).0, 0);
    assert_eq!(relay.run(&["workspace", "group", "remove", "wsx"]).0, 0);
    assert_eq!(relay.run(&["workspace", "group", "new-workspace", "g", "--placement", "end"]).0, 0);
    let reqs = relay.take();
    assert_eq!(reqs[0]["method"], "workspace.group.create");
    assert_eq!(reqs[0]["params"], json!({"name": "Name", "child_workspace_ids": ["a", "b", "c"]}));
    assert_eq!(reqs[1]["method"], "workspace.group.rename");
    assert_eq!(reqs[1]["params"], json!({"group_id": "g1", "name": "newname"}));
    assert_eq!(reqs[2]["params"], json!({"group_id": "g", "to_index": 3}));
    assert_eq!(reqs[3]["params"], json!({"group_id": "g", "hex": ""}));
    assert_eq!(reqs[4]["params"], json!({"workspace_id": "wsx"}));
    assert_eq!(reqs[5]["method"], "workspace.group.new_workspace");
    let (code, _, err) = relay.run(&["workspace", "group", "add", "--group", "g"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux workspace group add: requires --group <id> --workspace <id>\n")
    );
    let (code, _, err) = relay.run(&["workspace", "group", "move", "g", "--to-index", "x"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux workspace group move: --to-index must be an integer\n")
    );
    let (code, _, err) = relay.run(&["workspace", "group", "rename"]);
    assert_eq!(
        (code, err.as_str()),
        (2, "cmux workspace group rename: requires a group id or --group <id>\n")
    );
    {
        let _env = EnvGuard::set(&[("CMUX_WORKSPACE_ID", Some("env-ws"))]);
        let (code, _, err) = relay.run(&["workspace", "group", "remove"]);
        assert_eq!(
            (code, err.as_str()),
            (2, "cmux workspace group remove: requires --workspace <id>\n"),
            "env never satisfies a required flag"
        );
        assert_eq!(relay.run(&["workspace", "group", "list"]).0, 0);
    }
    assert_eq!(relay.take()[0]["params"], json!({"workspace_id": "env-ws"}));
}

#[test]
fn browser_relay() {
    let relay = FakeRelay::start();
    let _env = EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("CMUX_WORKSPACE_ID", Some("env-ws")),
        ("CMUX_SURFACE_ID", Some("env-surface")),
    ]);
    assert_eq!(relay.run(&["browser", "open", "https://x.y", "z"]).0, 0);
    assert_eq!(relay.run(&["browser", "type", "#in", "some", "text"]).0, 0);
    assert_eq!(relay.run(&["browser", "select", "#s", "--value", "v"]).0, 0);
    assert_eq!(relay.run(&["browser", "press", "Enter"]).0, 0);
    assert_eq!(relay.run(&["browser", "eval", "document.title"]).0, 0);
    assert_eq!(relay.run(&["browser", "wait", "--timeout-ms", "5", "--url-contains", "x"]).0, 0);
    assert_eq!(relay.run(&["browser", "get-url"]).0, 0);
    let reqs = relay.take();
    assert_eq!(reqs[0]["method"], "browser.open_split");
    assert_eq!(reqs[0]["params"], json!({"url": "https://x.y z", "workspace_id": "env-ws"}));
    assert_eq!(reqs[1]["method"], "browser.type");
    assert_eq!(
        reqs[1]["params"],
        json!({"selector": "#in", "text": "some text", "surface_id": "env-surface"})
    );
    assert_eq!(
        reqs[2]["params"],
        json!({"selector": "#s", "value": "v", "surface_id": "env-surface"}),
        "select does not mirror value into text"
    );
    assert_eq!(reqs[3]["params"], json!({"key": "Enter", "surface_id": "env-surface"}));
    assert_eq!(reqs[4]["params"], json!({"script": "document.title", "surface_id": "env-surface"}));
    assert_eq!(
        reqs[5]["params"],
        json!({"timeout_ms": "5", "url_contains": "x", "surface_id": "env-surface"})
    );
    assert_eq!(reqs[6]["method"], "browser.url.get");
    let (code, _, err) = relay.run(&["browser"]);
    assert_eq!(code, 2);
    assert!(err.starts_with("cmux browser: requires a subcommand (back, check, click,"), "{err}");
    let (code, _, err) = relay.run(&["browser", "zzz"]);
    assert_eq!((code, err.as_str()), (2, "cmux browser: unknown subcommand \"zzz\"\n"));
}

#[test]
fn socket_addr_file_and_relay_auth_over_tcp() {
    use std::net::TcpListener;
    let home = tempfile::tempdir().unwrap();
    let relay_id = "relay-1";
    let relay_token = "ab".repeat(32);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let port = listener.local_addr().unwrap().port();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_tx = Arc::clone(&seen);
    let token_bytes = hex::decode(&relay_token).unwrap();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let seen = Arc::clone(&seen_tx);
            let token_bytes = token_bytes.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(conn.try_clone().unwrap());
                conn.write_all(b"{\"protocol\":\"cmux-relay-auth\",\"version\":1,\"relay_id\":\"relay-1\",\"nonce\":\"n0nce\"}\n").unwrap();
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let resp = parse_frame(&line);
                let mac = hex::encode(cmuxd_remote::cli::compute_relay_mac(
                    &token_bytes,
                    "relay-1",
                    "n0nce",
                    1,
                ));
                let ok = resp["relay_id"] == "relay-1" && resp["mac"] == mac;
                conn.write_all(format!("{{\"ok\":{ok}}}\n").as_bytes()).unwrap();
                if !ok {
                    return;
                }
                line.clear();
                reader.read_line(&mut line).unwrap();
                seen.lock().unwrap().push(line.clone());
                conn.write_all(b"{\"ok\":true,\"result\":{\"pong\":true}}\n").unwrap();
            });
        }
    });
    std::fs::create_dir_all(home.path().join(".cmux/relay")).unwrap();
    std::fs::write(home.path().join(".cmux/socket_addr"), format!("{addr}\n")).unwrap();
    std::fs::write(
        home.path().join(format!(".cmux/relay/{port}.auth")),
        json!({"relay_id": relay_id, "relay_token": relay_token}).to_string(),
    )
    .unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("HOME", Some(home.path().to_str().unwrap())),
        ("CMUX_RELAY_ID", None),
        ("CMUX_RELAY_TOKEN", None),
    ]);
    let (code, out, err) = run(&args(&["ping"]));
    assert_eq!((code, out.as_str(), err.as_str()), (0, "{\n  \"pong\": true\n}\n", ""));
    assert_eq!(seen.lock().unwrap().len(), 1);
    {
        let _env = EnvGuard::set(&[
            ("CMUX_RELAY_ID", Some("relay-1")),
            ("CMUX_RELAY_TOKEN", Some(&"cd".repeat(32))),
        ]);
        let (code, _, err) = run(&args(&["ping"]));
        assert_eq!(
            (code, err.as_str()),
            (1, format!("cmux: failed to connect to {addr}: relay auth rejected\n").as_str())
        );
    }
    std::fs::write(
        home.path().join(format!(".cmux/relay/{port}.auth")),
        json!({"relay_id": "wrong", "relay_token": relay_token}).to_string(),
    )
    .unwrap();
    let (code, _, err) = run(&args(&["ping"]));
    assert_eq!(
        (code, err.as_str()),
        (1, format!("cmux: failed to connect to {addr}: relay auth challenge mismatch\n").as_str())
    );
    // A refused TCP address from the environment is not refreshed.
    let dead = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);
    let (code, _, err) = run(&args(&["--socket", &dead_addr, "ping"]));
    assert_eq!(code, 1);
    assert_eq!(
        err,
        format!(
            "cmux: failed to connect to {dead_addr}: dial tcp {dead_addr}: connect: connection refused\n"
        )
    );
    // A stale socket_addr file is refreshed once after a refused connection.
    std::fs::write(
        home.path().join(format!(".cmux/relay/{port}.auth")),
        json!({"relay_id": relay_id, "relay_token": relay_token}).to_string(),
    )
    .unwrap();
    std::fs::write(home.path().join(".cmux/socket_addr"), format!("{dead_addr}\n")).unwrap();
    let (code, _, err) = run(&args(&["ping"]));
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("connection refused"), "{err}");
}

#[test]
fn tmux_compat_dispatch_and_formatting() {
    let relay = FakeRelay::start();
    let home = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("HOME", Some(home.path().to_str().unwrap())),
        ("CMUX_WORKSPACE_ID", None),
        ("CMUX_SURFACE_ID", None),
        ("TMUX_PANE", None),
        ("PWD", Some("/tmp")),
    ]);
    let (code, out, _) = relay.run(&["__tmux-compat", "-V"]);
    assert_eq!((code, out.as_str()), (0, "tmux 3.4\n"));
    let (code, _, err) = relay.run(&["__tmux-compat"]);
    assert_eq!((code, err.as_str()), (1, "cmux __tmux-compat: tmux shim requires a command\n"));
    let (code, _, err) = relay.run(&["__tmux-compat", "frobnicate"]);
    assert_eq!(
        (code, err.as_str()),
        (1, "cmux __tmux-compat: unsupported tmux command: frobnicate\n")
    );
    assert_eq!(
        relay.run(&["__tmux-compat", "set-option", "-g", "x"]),
        (0, String::new(), String::new())
    );
    let (code, _, err) = relay.run(&["__tmux-compat", "has-session", "-t", "nope"]);
    assert_eq!((code, err.as_str()), (1, "cmux __tmux-compat: workspace not found: nope\n"));
    assert_eq!(relay.run(&["__tmux-compat", "has-session", "-t", "main"]).0, 0);
    relay.take();

    let (code, out, _) = relay.run(&["__tmux-compat", "display-message", "-p", "#{session_name} #{pane_width}x#{pane_height} #{window_width}x#{window_height} #{pane_left},#{pane_top} #{window_name} #{pane_title} #{window_active}#{window_flags} #{nope}"]);
    assert_eq!((code, out.as_str()), (0, "cmux 120x40 225x60 2,2 main zsh 1*\n"));
    let (_, out, _) = relay.run(&[
        "__tmux-compat",
        "list-windows",
        "-F",
        "#{window_index}:#{window_name}:#{window_id}",
    ]);
    assert_eq!(out, format!("0:main:@{}\n", cmuxd_remote::cli::tmux::tmux_stable_numeric_id(WS)));
    relay.take();

    assert_eq!(
        relay.run(&["__tmux-compat", "send-keys", "-t", "main.0", "echo", "hi", "Enter"]).0,
        0
    );
    let (code, _, err) = relay.run(&["__tmux-compat", "send-keys", "-t", "main.7", "x"]);
    assert_eq!((code, err.as_str()), (1, "cmux __tmux-compat: pane not found: 7\n"));
    assert_eq!(relay.run(&["__tmux-compat", "send-keys", "-l", "C-c"]).0, 0);
    assert_eq!(
        relay.run(&["__tmux-compat", "new-window", "-P", "-n", "title", "-c", "/tmp/d", "make"]).2,
        ""
    );
    let reqs = relay.take();
    let sends: Vec<&Frame> = reqs.iter().filter(|r| r["method"] == "surface.send_text").collect();
    assert_eq!(sends[0]["params"]["text"], "echo hi\r");
    assert_eq!(sends[1]["params"]["text"], "C-c");
    let create = reqs.iter().find(|r| r["method"] == "workspace.create").unwrap();
    assert_eq!(create["params"], json!({"focus": false, "cwd": "/tmp/d"}));
    let rename = reqs.iter().find(|r| r["method"] == "workspace.rename").unwrap();
    assert_eq!(rename["params"]["title"], "title");
    assert_eq!(sends[2]["params"]["text"], "cd -- '/tmp/d' && make\r");

    let (code, out, _) = relay.run(&["__tmux-compat", "capture-pane", "-p", "-S", "-100"]);
    assert_eq!((code, out.as_str()), (0, "line1\nline2\n"));
    let read = relay.take().into_iter().find(|r| r["method"] == "surface.read_text").unwrap();
    assert_eq!(
        read["params"],
        json!({"workspace_id": WS, "surface_id": SURF, "scrollback": true, "lines": 100})
    );
    assert_eq!(relay.run(&["__tmux-compat", "capture-pane"]).0, 0);
    let (_, out, _) = relay.run(&["__tmux-compat", "show-buffer"]);
    assert_eq!(out, "line1\nline2\n");
    let store =
        std::fs::read_to_string(home.path().join(".cmuxterm/tmux-compat-store.json")).unwrap();
    assert_eq!(store, "{\"buffers\":{\"default\":\"line1\\nline2\\n\"}}");
    let (code, _, err) = relay.run(&["__tmux-compat", "save-buffer", "-b", "nope"]);
    assert_eq!((code, err.as_str()), (1, "cmux __tmux-compat: buffer not found: nope\n"));
    relay.take();

    assert_eq!(relay.run(&["__tmux-compat", "resize-pane", "-t", "main.0", "-x", "50%"]).0, 0);
    assert_eq!(relay.run(&["__tmux-compat", "resize-pane", "-L", "5"]).0, 0);
    assert_eq!(relay.run(&["__tmux-compat", "resize-pane", "-x", "40"]).0, 0);
    assert_eq!(relay.run(&["__tmux-compat", "resize-pane", "-y", "10"]).0, 0);
    let resizes: Vec<Frame> =
        relay.take().into_iter().filter(|r| r["method"] == "pane.resize").collect();
    assert_eq!(resizes.len(), 3, "height-only resize is a deliberate no-op");
    assert_eq!(
        resizes[0]["params"],
        json!({"workspace_id": WS, "pane_id": PANE, "absolute_axis": "horizontal", "tmux_compat": true, "target_percentage": 50, "target_pixels": 900})
    );
    assert_eq!(
        resizes[1]["params"],
        json!({"workspace_id": WS, "pane_id": PANE, "direction": "left", "amount_cells": 5, "amount": 38, "tmux_compat": true})
    );
    assert_eq!(
        resizes[2]["params"],
        json!({"workspace_id": WS, "pane_id": PANE, "absolute_axis": "horizontal", "tmux_compat": true, "target_cells": 40, "target_pixels": 300})
    );

    let (code, out, _) = relay.run(&["__tmux-compat", "wait-for", "-S", "chan/1"]);
    assert_eq!((code, out.as_str()), (0, "OK\n"));
    assert!(std::path::Path::new("/tmp/cmux-wait-for-chan_1.sig").exists());
    assert_eq!(relay.run(&["__tmux-compat", "wait-for", "chan/1"]).0, 0);
    assert!(!std::path::Path::new("/tmp/cmux-wait-for-chan_1.sig").exists());
}

#[test]
fn tmux_split_window_anchors_to_caller_surface() {
    let relay = FakeRelay::start();
    let home = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("HOME", Some(home.path().to_str().unwrap())),
        ("CMUX_WORKSPACE_ID", Some(WS)),
        ("CMUX_SURFACE_ID", Some("surface:1")),
        ("TMUX_PANE", None),
    ]);
    let (code, out, err) =
        relay.run(&["__tmux-compat", "split-window", "-hP", "-F", "#{pane_id}", "-c", "/x", "vim"]);
    assert_eq!((code, err.as_str()), (0, ""));
    assert_eq!(out, format!("%{}\n", cmuxd_remote::cli::tmux::tmux_stable_numeric_id(PANE)));
    let reqs = relay.take();
    let split = reqs.iter().find(|r| r["method"] == "surface.split").unwrap();
    assert_eq!(
        split["params"],
        json!({"workspace_id": WS, "surface_id": SURF, "direction": "right", "focus": true}),
        "caller surface ref is canonicalized"
    );
    let equalize = reqs.iter().find(|r| r["method"] == "workspace.equalize_splits").unwrap();
    assert_eq!(equalize["params"], json!({"workspace_id": WS, "orientation": "vertical"}));
    let store: Value = serde_json::from_str(
        &std::fs::read_to_string(home.path().join(".cmuxterm/tmux-compat-store.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(store["mainVerticalLayouts"][WS]["mainSurfaceId"], SURF);
    assert_eq!(
        store["mainVerticalLayouts"][WS]["lastColumnSurfaceId"],
        "55555555-5555-5555-5555-555555555555"
    );
    assert_eq!(store["lastSplitSurface"][WS], "55555555-5555-5555-5555-555555555555");
    // With a stale last-column surface the anchor falls back to the caller.
    std::fs::write(home.path().join(".cmuxterm/tmux-compat-store.json"), json!({"mainVerticalLayouts": {WS: {"mainSurfaceId": SURF, "lastColumnSurfaceId": "99999999-9999-9999-9999-999999999999"}}}).to_string()).unwrap();
    assert_eq!(relay.run(&["__tmux-compat", "split-window", "-v"]).0, 0);
    let split = relay.take().into_iter().find(|r| r["method"] == "surface.split").unwrap();
    assert_eq!(split["params"]["surface_id"], SURF);
    assert_eq!(split["params"]["direction"], "right");
    assert_eq!(relay.run(&["__tmux-compat", "kill-window"]).0, 0);
    let store: Value = serde_json::from_str(
        &std::fs::read_to_string(home.path().join(".cmuxterm/tmux-compat-store.json")).unwrap(),
    )
    .unwrap();
    assert!(
        store.get("mainVerticalLayouts").is_none(),
        "kill-window prunes workspace state: {store}"
    );
}

#[test]
fn agent_launchers_report_missing_binaries_and_create_shims() {
    use std::os::unix::fs::PermissionsExt;
    let relay = FakeRelay::start();
    let home = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(&[
        ("CMUX_SOCKET_PATH", None),
        ("HOME", Some(home.path().to_str().unwrap())),
        ("PATH", Some("/nonexistent")),
    ]);
    let (code, _, err) = relay.run(&["omx"]);
    assert_eq!(code, 1);
    assert_eq!(
        err,
        "cmux omx: omx not found in PATH\nInstall it first:\n  npm install -g oh-my-codex\n"
    );
    let shim = home.path().join(".cmuxterm/omx-bin/tmux");
    assert_eq!(std::fs::read_to_string(&shim).unwrap(), cmuxd_remote::cli::agents::OMX_SHIM_SCRIPT);
    assert_eq!(std::fs::metadata(&shim).unwrap().permissions().mode() & 0o777, 0o755);
    let (code, _, err) = relay.run(&["claude-teams"]);
    assert_eq!((code, err.as_str()), (1, "cmux claude-teams: claude not found in PATH\n"));
    assert!(home.path().join(".cmuxterm/claude-teams-bin/tmux").exists());
    let (code, _, err) = relay.run(&["omo"]);
    assert_eq!(code, 1);
    assert!(err.starts_with("cmux omo: opencode not found in PATH\n"));
    assert!(home.path().join(".cmuxterm/omo-bin/terminal-notifier").exists());
    let (code, _, err) = relay.run(&["omc"]);
    assert_eq!(code, 1);
    assert!(err.starts_with("cmux omc: omc not found in PATH\n"));
    let _ = relay.take();
    let map: HashMap<String, String> = HashMap::new();
    assert!(map.is_empty());
}

#[test]
fn omo_ensure_plugin_rejects_invalid_user_json() {
    let home = tempfile::tempdir().unwrap();
    let _env = EnvGuard::set(&[("HOME", Some(home.path().to_str().unwrap()))]);
    std::fs::create_dir_all(home.path().join(".config/opencode")).unwrap();
    std::fs::write(home.path().join(".config/opencode/opencode.json"), "{ not json").unwrap();
    let mut stderr = Vec::new();
    let err =
        cmuxd_remote::cli::agents::omo_ensure_plugin("/nonexistent", &mut stderr).unwrap_err();
    assert_eq!(err, "invalid opencode.json: fix the JSON syntax and retry");
}

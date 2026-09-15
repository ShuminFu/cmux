//! The `cmux` CLI relay that runs on the remote host: it maps CLI arguments to
//! v2 JSON-RPC calls against the local app through the reverse SSH forward.
//! Mirrors `cli.go`.

use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::Sha256;

use crate::agent_launch::{run_claude_teams_relay, run_omc_relay, run_omo_relay, run_omx_relay};
use crate::cloud_cli_bridge::default_cloud_cli_bridge_socket_if_exists;
use crate::commands::{command_index, command_overrides, CommandSpec};
use crate::tmux_compat::run_tmux_compat;
use crate::util::{go_io_error, go_json, go_json_pretty, home_dir, path_join, random_hex};

/// Output streams for the relay (Go wrote straight to `os.Stdout`/`os.Stderr`;
/// tests capture these).
pub struct CliIo<'a> {
    pub stdout: &'a mut dyn Write,
    pub stderr: &'a mut dyn Write,
}

pub type RefreshAddr = Option<Arc<dyn Fn() -> String + Send + Sync>>;

/// Connection info for making JSON-RPC calls through the relay.
#[derive(Clone)]
pub struct RpcContext {
    pub socket_path: String,
    pub refresh_addr: RefreshAddr,
}

impl RpcContext {
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
            refresh_addr: None,
        }
    }

    /// Make a JSON-RPC call and return the parsed result object. Bare (non
    /// object) results yield an empty map, as the Go relay did.
    pub fn call(
        &self,
        method: &str,
        params: Option<Map<String, Value>>,
    ) -> Result<Map<String, Value>, String> {
        let resp = socket_round_trip_v2(
            &self.socket_path,
            method,
            params.as_ref(),
            self.refresh_addr.as_ref(),
        )?;
        match serde_json::from_str::<Value>(&resp) {
            Ok(Value::Object(map)) => Ok(map),
            _ => Ok(Map::new()),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RelayAuthState {
    #[serde(default)]
    pub relay_id: String,
    #[serde(default)]
    pub relay_token: String,
}

#[derive(Clone, Debug)]
pub struct BrowserCommandSpec {
    pub method: &'static str,
    pub flag_keys: Vec<&'static str>,
    pub allow_positional_url: bool,
    pub allow_positional_script: bool,
    pub allow_positional_key: bool,
    pub allow_positional_query: bool,
    pub allow_positional_value: bool,
    pub use_workspace_env: bool,
    pub use_surface_env: bool,
}

fn browser(method: &'static str, keys: &[&'static str]) -> BrowserCommandSpec {
    BrowserCommandSpec {
        method,
        flag_keys: keys.to_vec(),
        allow_positional_url: false,
        allow_positional_script: false,
        allow_positional_key: false,
        allow_positional_query: false,
        allow_positional_value: false,
        use_workspace_env: false,
        use_surface_env: false,
    }
}

pub fn browser_commands() -> HashMap<&'static str, BrowserCommandSpec> {
    let mut m = HashMap::new();
    let open = || BrowserCommandSpec {
        allow_positional_url: true,
        use_workspace_env: true,
        ..browser("browser.open_split", &["url", "workspace", "surface"])
    };
    m.insert("open", open());
    m.insert("open-split", open());
    m.insert("new", open());
    let navigate = || BrowserCommandSpec {
        allow_positional_url: true,
        use_surface_env: true,
        ..browser("browser.navigate", &["url", "surface"])
    };
    m.insert("navigate", navigate());
    m.insert("goto", navigate());
    let surface_only = |method: &'static str| BrowserCommandSpec {
        use_surface_env: true,
        ..browser(method, &["surface"])
    };
    m.insert("back", surface_only("browser.back"));
    m.insert("forward", surface_only("browser.forward"));
    m.insert("reload", surface_only("browser.reload"));
    m.insert("get-url", surface_only("browser.url.get"));
    m.insert("url", surface_only("browser.url.get"));
    m.insert(
        "snapshot",
        BrowserCommandSpec {
            use_surface_env: true,
            ..browser("browser.snapshot", &["surface", "selector", "max-depth"])
        },
    );
    m.insert(
        "eval",
        BrowserCommandSpec {
            allow_positional_script: true,
            use_surface_env: true,
            ..browser("browser.eval", &["surface", "script"])
        },
    );
    m.insert(
        "wait",
        BrowserCommandSpec {
            use_surface_env: true,
            ..browser(
                "browser.wait",
                &[
                    "surface",
                    "selector",
                    "text",
                    "url-contains",
                    "load-state",
                    "function",
                    "timeout-ms",
                ],
            )
        },
    );
    let query = |method: &'static str| BrowserCommandSpec {
        allow_positional_query: true,
        use_surface_env: true,
        ..browser(method, &["surface", "selector"])
    };
    m.insert("click", query("browser.click"));
    m.insert("dblclick", query("browser.dblclick"));
    m.insert("hover", query("browser.hover"));
    m.insert("focus", query("browser.focus"));
    m.insert("check", query("browser.check"));
    m.insert("uncheck", query("browser.uncheck"));
    let value_text = |method: &'static str| BrowserCommandSpec {
        allow_positional_value: true,
        use_surface_env: true,
        ..browser(method, &["surface", "selector", "text"])
    };
    m.insert("type", value_text("browser.type"));
    m.insert("fill", value_text("browser.fill"));
    let key = |method: &'static str| BrowserCommandSpec {
        allow_positional_key: true,
        use_surface_env: true,
        ..browser(method, &["surface", "key"])
    };
    m.insert("press", key("browser.press"));
    m.insert("key", key("browser.press"));
    m.insert("keydown", key("browser.keydown"));
    m.insert("keyup", key("browser.keyup"));
    m.insert(
        "select",
        BrowserCommandSpec {
            allow_positional_value: true,
            use_surface_env: true,
            ..browser("browser.select", &["surface", "selector", "value"])
        },
    );
    m.insert("screenshot", surface_only("browser.screenshot"));
    m
}

/// Entry point for the `cli` subcommand (or busybox `cmux` invocation).
pub fn run_cli(args: &[String], io: &mut CliIo<'_>) -> i32 {
    let mut socket_path = std::env::var("CMUX_SOCKET_PATH").unwrap_or_default();
    let mut json_output = false;
    let mut remaining: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                if i + 1 >= args.len() {
                    let _ = writeln!(io.stderr, "cmux: --socket requires a path");
                    return 2;
                }
                socket_path = args[i + 1].clone();
                i += 1;
            }
            "--json" => json_output = true,
            "--help" | "-h" => {
                cli_usage(io.stderr);
                return 0;
            }
            _ => {
                remaining = args[i..].to_vec();
                break;
            }
        }
        i += 1;
    }

    if remaining.is_empty() {
        cli_usage(io.stderr);
        return 2;
    }
    let cmd_name = remaining[0].clone();
    let cmd_args = &remaining[1..];
    if cmd_name == "help" {
        cli_usage(io.stderr);
        return 0;
    }

    // refresh_addr is set when the address came from the socket_addr file (not
    // env/flag), allowing one stale-address refresh if another workspace has
    // replaced socket_addr.
    let mut refresh_addr: RefreshAddr = None;
    if socket_path.is_empty() {
        socket_path = read_socket_addr_file();
        refresh_addr = Some(Arc::new(read_socket_addr_file));
    }
    if socket_path.is_empty() {
        socket_path = default_cloud_cli_bridge_socket_if_exists();
    }
    if socket_path.is_empty() {
        let _ = writeln!(
            io.stderr,
            "cmux: CMUX_SOCKET_PATH not set and --socket not provided"
        );
        return 1;
    }

    if cmd_name == "rpc" {
        return run_rpc(
            &socket_path,
            cmd_args,
            json_output,
            refresh_addr.as_ref(),
            io,
        );
    }

    let overrides = command_overrides();
    if overrides
        .get(cmd_name.as_str())
        .map(|ov| ov.special_dispatch)
        .unwrap_or(false)
        && cmd_name == "new-workspace"
    {
        return run_new_workspace_relay(
            &socket_path,
            cmd_args,
            json_output,
            refresh_addr.as_ref(),
            io,
        );
    }

    if cmd_name == "browser" {
        return run_browser_relay(
            &socket_path,
            cmd_args,
            json_output,
            refresh_addr.as_ref(),
            io,
        );
    }

    // Workspace group subcommands: "workspace-group <sub>" and the canonical
    // two-word "workspace group <sub>" both map to workspace.group.* methods.
    if cmd_name == "workspace-group" {
        return run_workspace_group_relay(
            &socket_path,
            cmd_args,
            json_output,
            refresh_addr.as_ref(),
            io,
        );
    }
    if cmd_name == "workspace" {
        if !cmd_args.is_empty() && cmd_args[0] == "group" {
            return run_workspace_group_relay(
                &socket_path,
                &cmd_args[1..],
                json_output,
                refresh_addr.as_ref(),
                io,
            );
        }
        let _ = writeln!(
            io.stderr,
            "cmux workspace: only the \"group\" subcommand is supported here. Use list-workspaces, new-workspace, close-workspace, or select-workspace for workspace operations."
        );
        return 2;
    }

    match cmd_name.as_str() {
        "claude-teams" => {
            return run_claude_teams_relay(&socket_path, cmd_args, refresh_addr.clone(), io)
        }
        "omo" => return run_omo_relay(&socket_path, cmd_args, refresh_addr.clone(), io),
        "omx" => return run_omx_relay(&socket_path, cmd_args, refresh_addr.clone(), io),
        "omc" => return run_omc_relay(&socket_path, cmd_args, refresh_addr.clone(), io),
        "__tmux-compat" => {
            return run_tmux_compat(&socket_path, cmd_args, refresh_addr.clone(), io)
        }
        _ => {}
    }

    let index = command_index();
    let spec = match index.get(cmd_name.as_str()) {
        Some(spec) => spec,
        None => {
            let _ = writeln!(io.stderr, "cmux: unknown command {cmd_name:?}");
            return 2;
        }
    };
    exec_v2(
        &socket_path,
        spec,
        cmd_args,
        json_output,
        refresh_addr.as_ref(),
        io,
    )
}

fn exec_v2(
    socket_path: &str,
    spec: &CommandSpec,
    args: &[String],
    json_output: bool,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
    io: &mut CliIo<'_>,
) -> i32 {
    let mut params = Map::new();
    for (key, value) in &spec.default_params {
        params.insert((*key).to_string(), value.clone());
    }

    if !spec.no_params {
        let parsed = match parse_flags(args, &spec.flag_keys, Some(&spec.repeat_keys)) {
            Ok(parsed) => parsed,
            Err(err) => {
                let _ = writeln!(io.stderr, "cmux: {err}");
                return 2;
            }
        };
        let bool_flag_set: HashSet<&str> = spec.bool_flags.iter().copied().collect();
        let overrides = command_overrides();
        let client_only: HashSet<&str> = overrides
            .get(spec.name)
            .map(|ov| ov.client_only_flags.iter().copied().collect())
            .unwrap_or_default();

        for key in &spec.flag_keys {
            if client_only.contains(key) {
                continue;
            }
            if let Some(val) = parsed.flags.get(*key) {
                let mut param_key = flag_to_param_key(key);
                if let Some(override_key) = spec.param_key_overrides.get(key) {
                    param_key = (*override_key).to_string();
                }
                if bool_flag_set.contains(key) {
                    match val.to_lowercase().as_str() {
                        "true" | "1" | "yes" => {
                            params.insert(param_key, Value::Bool(true));
                        }
                        "false" | "0" | "no" => {
                            params.insert(param_key, Value::Bool(false));
                        }
                        _ => {
                            let _ = writeln!(io.stderr, "cmux: --{key} must be true or false");
                            return 2;
                        }
                    }
                } else {
                    params.insert(param_key, Value::String(val.clone()));
                }
            }
        }

        for key in &spec.repeat_keys {
            if client_only.contains(key) {
                continue;
            }
            if let Some(vals) = parsed.repeated.get(*key) {
                let mut param_key = flag_to_param_key(key);
                if let Some(override_key) = spec.param_key_overrides.get(key) {
                    param_key = (*override_key).to_string();
                }
                params.insert(
                    param_key,
                    Value::Array(vals.iter().map(|v| Value::String(v.clone())).collect()),
                );
            }
        }

        if !parsed.positional.is_empty() {
            if !spec.positional_key.is_empty() {
                params.insert(
                    spec.positional_key.to_string(),
                    Value::String(parsed.positional.join(" ")),
                );
            } else {
                let _ = writeln!(
                    io.stderr,
                    "cmux: {} does not accept positional arguments",
                    spec.name
                );
                return 2;
            }
        }

        if spec_uses_param(spec, "workspace_id") {
            apply_workspace_env_fallback(&mut params);
        }
        if spec_uses_param(spec, "surface_id") {
            apply_surface_env_fallback(&mut params);
        }
    }

    let mut method = spec.v2_method.to_string();
    if spec.name == "notify" {
        method = apply_notify_caller_env(&method, &mut params);
    }
    let resp = match socket_round_trip_v2(socket_path, &method, Some(&params), refresh_addr) {
        Ok(resp) => resp,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            return 1;
        }
    };
    if json_output {
        let _ = writeln!(io.stdout, "{resp}");
    } else {
        let _ = writeln!(io.stdout, "{}", default_relay_output(&resp));
    }
    0
}

fn run_new_workspace_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
    io: &mut CliIo<'_>,
) -> i32 {
    let flag_keys = [
        "name",
        "cwd",
        "description",
        "focus",
        "window",
        "group",
        "group-placement",
        "group-reference",
        "layout",
        "env-file",
        "command",
    ];
    let repeat_keys = ["env"];
    let parsed = match parse_flags(args, &flag_keys, Some(&repeat_keys)) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux new-workspace: {err}");
            return 2;
        }
    };
    if !parsed.positional.is_empty() {
        let _ = writeln!(
            io.stderr,
            "cmux: new-workspace does not accept positional arguments"
        );
        return 2;
    }

    let mut params = Map::new();
    for key in [
        "name",
        "cwd",
        "description",
        "window",
        "group",
        "group-placement",
        "group-reference",
        "command",
    ] {
        if let Some(val) = parsed.flags.get(key) {
            let param_key = match key {
                "name" => "title".to_string(),
                "group-placement" => "placement".to_string(),
                "group-reference" => "group_reference_workspace_id".to_string(),
                // handled post-create; do not send to workspace.create
                "command" => continue,
                other => flag_to_param_key(other),
            };
            params.insert(param_key, Value::String(val.clone()));
        }
    }

    if let Some(val) = parsed.flags.get("focus") {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => {
                params.insert("focus".to_string(), Value::Bool(true));
            }
            "false" | "0" | "no" => {
                params.insert("focus".to_string(), Value::Bool(false));
            }
            _ => {
                let _ = writeln!(io.stderr, "cmux: --focus must be true or false");
                return 2;
            }
        }
    }

    if let Some(val) = parsed.flags.get("layout") {
        match serde_json::from_str::<Value>(val) {
            Ok(layout) => {
                params.insert("layout".to_string(), layout);
            }
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --layout must be valid JSON: {err}"
                );
                return 2;
            }
        }
    }

    // Build env dict from --env KEY=VALUE pairs and --env-file lines.
    let mut env: Map<String, Value> = Map::new();
    if let Some(pairs) = parsed.repeated.get("env") {
        for kv in pairs {
            match kv.split_once('=') {
                Some((k, v)) => {
                    env.insert(k.to_string(), Value::String(v.to_string()));
                }
                None => {
                    let _ = writeln!(
                        io.stderr,
                        "cmux new-workspace: --env {kv:?} must be KEY=VALUE"
                    );
                    return 2;
                }
            }
        }
    }
    if let Some(env_file) = parsed.flags.get("env-file") {
        let data = match std::fs::read_to_string(env_file) {
            Ok(data) => data,
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --env-file: open {env_file}: {}",
                    go_io_error(&err)
                );
                return 2;
            }
        };
        for line in data.split('\n') {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            match line.split_once('=') {
                Some((k, v)) => {
                    env.insert(k.to_string(), Value::String(v.to_string()));
                }
                None => {
                    let _ = writeln!(
                        io.stderr,
                        "cmux new-workspace: --env-file line {line:?} must be KEY=VALUE"
                    );
                    return 2;
                }
            }
        }
    }
    if !env.is_empty() {
        params.insert("env".to_string(), Value::Object(env));
    }

    let resp =
        match socket_round_trip_v2(socket_path, "workspace.create", Some(&params), refresh_addr) {
            Ok(resp) => resp,
            Err(err) => {
                let _ = writeln!(io.stderr, "cmux: {err}");
                return 1;
            }
        };

    // --command: send text + Enter to the new workspace's surface.
    if let Some(cmd) = parsed.flags.get("command") {
        let result: Map<String, Value> = match serde_json::from_str::<Value>(&resp) {
            Ok(Value::Object(map)) => map,
            Ok(_) => Map::new(),
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --command skipped: could not parse create response: {err}"
                );
                return 1;
            }
        };
        let surface_id = result
            .get("surface_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if surface_id.is_empty() {
            let _ = writeln!(io.stderr, "cmux new-workspace: --command skipped: workspace.create response missing surface_id");
            return 1;
        }
        let mut send_params = Map::new();
        send_params.insert(
            "surface_id".to_string(),
            Value::String(surface_id.to_string()),
        );
        send_params.insert("text".to_string(), Value::String(cmd.clone()));
        if let Err(err) = socket_round_trip_v2(
            socket_path,
            "surface.send_text",
            Some(&send_params),
            refresh_addr,
        ) {
            let _ = writeln!(
                io.stderr,
                "cmux new-workspace: --command send failed: {err}"
            );
            return 1;
        }
        let mut key_params = Map::new();
        key_params.insert(
            "surface_id".to_string(),
            Value::String(surface_id.to_string()),
        );
        key_params.insert("key".to_string(), Value::String("return".to_string()));
        if let Err(err) = socket_round_trip_v2(
            socket_path,
            "surface.send_key",
            Some(&key_params),
            refresh_addr,
        ) {
            let _ = writeln!(
                io.stderr,
                "cmux new-workspace: --command send-key failed: {err}"
            );
            return 1;
        }
    }

    if json_output {
        let _ = writeln!(io.stdout, "{resp}");
    } else {
        let _ = writeln!(io.stdout, "{}", default_relay_output(&resp));
    }
    0
}

fn run_rpc(
    socket_path: &str,
    args: &[String],
    _json_output: bool,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
    io: &mut CliIo<'_>,
) -> i32 {
    if args.is_empty() {
        let _ = writeln!(io.stderr, "cmux rpc: requires a method name");
        return 2;
    }
    let method = &args[0];
    let mut params: Option<Map<String, Value>> = None;
    if args.len() > 1 {
        match serde_json::from_str::<Value>(&args[1]) {
            Ok(Value::Object(map)) => params = Some(map),
            Ok(Value::Null) => {}
            Ok(_) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux rpc: invalid JSON params: cannot unmarshal into map"
                );
                return 2;
            }
            Err(err) => {
                let _ = writeln!(io.stderr, "cmux rpc: invalid JSON params: {err}");
                return 2;
            }
        }
    }
    match socket_round_trip_v2(socket_path, method, params.as_ref(), refresh_addr) {
        Ok(resp) => {
            let _ = writeln!(io.stdout, "{resp}");
            0
        }
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            1
        }
    }
}

fn workspace_group_flag_keys() -> HashMap<&'static str, Vec<&'static str>> {
    HashMap::from([
        ("list", vec!["window"]),
        ("create", vec!["name", "cwd", "from", "window"]),
        ("ungroup", vec!["group", "window"]),
        ("delete", vec!["group", "window"]),
        ("rename", vec!["group", "name", "window"]),
        ("collapse", vec!["group", "window"]),
        ("expand", vec!["group", "window"]),
        ("pin", vec!["group", "window"]),
        ("unpin", vec!["group", "window"]),
        ("add", vec!["group", "workspace", "window"]),
        ("remove", vec!["workspace", "window"]),
        ("set-anchor", vec!["group", "workspace", "window"]),
        ("new-workspace", vec!["group", "placement", "window"]),
        ("set-color", vec!["group", "hex", "window"]),
        ("set-icon", vec!["group", "symbol", "window"]),
        (
            "move",
            vec!["group", "to-index", "before", "after", "window"],
        ),
        ("focus", vec!["group", "window"]),
    ])
}

fn run_workspace_group_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
    io: &mut CliIo<'_>,
) -> i32 {
    const SUBCOMMAND_HINT: &str = "list, create, ungroup, delete, rename, collapse, expand, pin, unpin, add, remove, set-anchor, new-workspace, set-color, set-icon, move, focus";
    if args.is_empty() {
        let _ = writeln!(
            io.stderr,
            "cmux workspace group: requires a subcommand ({SUBCOMMAND_HINT})"
        );
        return 2;
    }
    let sub = args[0].as_str();
    let keys = workspace_group_flag_keys();
    let flag_keys = match keys.get(sub) {
        Some(keys) => keys,
        None => {
            let _ = writeln!(
                io.stderr,
                "cmux workspace group: unknown subcommand {sub:?} ({SUBCOMMAND_HINT})"
            );
            return 2;
        }
    };
    let parsed = match parse_flags(&args[1..], flag_keys, None) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux workspace group {sub}: {err}");
            return 2;
        }
    };

    let mut params = Map::new();
    if let Some(win) = parsed.flags.get("window") {
        params.insert("window_id".to_string(), Value::String(win.clone()));
    }

    // The group id comes from --group or the first positional argument.
    let mut positional: &[String] = &parsed.positional;
    let take_group_id = |params: &mut Map<String, Value>, positional: &mut &[String]| -> bool {
        if let Some(gid) = parsed.flags.get("group") {
            params.insert("group_id".to_string(), Value::String(gid.clone()));
            return true;
        }
        if !positional.is_empty() {
            params.insert("group_id".to_string(), Value::String(positional[0].clone()));
            *positional = &positional[1..];
            return true;
        }
        false
    };

    macro_rules! fail {
        ($($arg:tt)*) => {{
            let _ = writeln!(io.stderr, "cmux workspace group {}: {}", sub, format!($($arg)*));
            return 2;
        }};
    }

    match sub {
        "list" => {}
        "create" => {
            let mut name = parsed.flags.get("name").cloned();
            if name.is_none() && !positional.is_empty() {
                name = Some(positional[0].clone());
            }
            params.insert("name".to_string(), Value::String(name.unwrap_or_default()));
            if let Some(cwd) = parsed.flags.get("cwd") {
                params.insert("cwd".to_string(), Value::String(cwd.clone()));
            }
            if let Some(from) = parsed.flags.get("from") {
                let ids: Vec<Value> = from
                    .split(',')
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(|id| Value::String(id.to_string()))
                    .collect();
                params.insert("child_workspace_ids".to_string(), Value::Array(ids));
            }
        }
        "ungroup" | "delete" | "collapse" | "expand" | "pin" | "unpin" | "focus" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
        }
        "rename" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
            let name = match parsed.flags.get("name") {
                Some(name) => name.clone(),
                None if !positional.is_empty() => positional[0].clone(),
                None => fail!("requires --name <name>"),
            };
            params.insert("name".to_string(), Value::String(name));
        }
        "add" | "set-anchor" => {
            let (gid, ws) = match (parsed.flags.get("group"), parsed.flags.get("workspace")) {
                (Some(gid), Some(ws)) => (gid.clone(), ws.clone()),
                _ => fail!("requires --group <id> --workspace <id>"),
            };
            params.insert("group_id".to_string(), Value::String(gid));
            params.insert("workspace_id".to_string(), Value::String(ws));
        }
        "remove" => {
            let ws = match parsed.flags.get("workspace") {
                Some(ws) => ws.clone(),
                None if !positional.is_empty() => positional[0].clone(),
                None => fail!("requires --workspace <id>"),
            };
            params.insert("workspace_id".to_string(), Value::String(ws));
        }
        "new-workspace" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
            if let Some(placement) = parsed.flags.get("placement") {
                params.insert("placement".to_string(), Value::String(placement.clone()));
            }
        }
        "set-color" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
            // Omitting --hex clears the color, matching the macOS CLI.
            params.insert(
                "hex".to_string(),
                Value::String(parsed.flags.get("hex").cloned().unwrap_or_default()),
            );
        }
        "set-icon" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
            // Omitting --symbol clears the icon, matching the macOS CLI.
            params.insert(
                "symbol".to_string(),
                Value::String(parsed.flags.get("symbol").cloned().unwrap_or_default()),
            );
        }
        "move" => {
            if !take_group_id(&mut params, &mut positional) {
                fail!("requires a group id or --group <id>");
            }
            if let Some(v) = parsed.flags.get("to-index") {
                match v.parse::<i64>() {
                    Ok(n) => {
                        params.insert("to_index".to_string(), Value::from(n));
                    }
                    Err(_) => fail!("--to-index must be an integer"),
                }
            } else if let Some(v) = parsed.flags.get("before") {
                params.insert("before_group_id".to_string(), Value::String(v.clone()));
            } else if let Some(v) = parsed.flags.get("after") {
                params.insert("after_group_id".to_string(), Value::String(v.clone()));
            } else {
                fail!("requires --to-index <n>, --before <group>, or --after <group>");
            }
        }
        _ => {}
    }

    // Forward the SSH caller's workspace/surface context so methods without a
    // group id (list, create) resolve the caller's window instead of whichever
    // local window is focused.
    apply_workspace_env_fallback(&mut params);
    apply_surface_env_fallback(&mut params);

    let method = format!("workspace.group.{}", sub.replace('-', "_"));
    let resp = match socket_round_trip_v2(socket_path, &method, Some(&params), refresh_addr) {
        Ok(resp) => resp,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            return 1;
        }
    };
    if json_output {
        let _ = writeln!(io.stdout, "{resp}");
    } else {
        let _ = writeln!(io.stdout, "{}", default_relay_output(&resp));
    }
    0
}

fn run_browser_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
    io: &mut CliIo<'_>,
) -> i32 {
    if args.is_empty() {
        let _ = writeln!(
            io.stderr,
            "cmux browser: requires a subcommand ({})",
            browser_subcommand_hint()
        );
        return 2;
    }
    let sub = args[0].as_str();
    let sub_args = &args[1..];
    let commands = browser_commands();
    let spec = match commands.get(sub) {
        Some(spec) => spec,
        None => {
            let _ = writeln!(io.stderr, "cmux browser: unknown subcommand {sub:?}");
            return 2;
        }
    };

    let mut params = Map::new();
    let parsed = match parse_flags(sub_args, &spec.flag_keys, None) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux browser: {err}");
            return 2;
        }
    };
    for key in &spec.flag_keys {
        if let Some(val) = parsed.flags.get(*key) {
            params.insert(flag_to_param_key(key), Value::String(val.clone()));
        }
    }
    if spec.allow_positional_url && !params.contains_key("url") && !parsed.positional.is_empty() {
        params.insert(
            "url".to_string(),
            Value::String(parsed.positional.join(" ")),
        );
    }
    if spec.allow_positional_script
        && !params.contains_key("script")
        && !parsed.positional.is_empty()
    {
        params.insert(
            "script".to_string(),
            Value::String(parsed.positional.join(" ")),
        );
    }
    if spec.allow_positional_key && !params.contains_key("key") && !parsed.positional.is_empty() {
        params.insert(
            "key".to_string(),
            Value::String(parsed.positional.join(" ")),
        );
    }
    if spec.allow_positional_query
        && !params.contains_key("selector")
        && !parsed.positional.is_empty()
    {
        params.insert(
            "selector".to_string(),
            Value::String(parsed.positional.join(" ")),
        );
    }
    if spec.allow_positional_value {
        apply_browser_value_positionals(
            &mut params,
            &parsed.positional,
            browser_spec_supports_param(spec, "value"),
            browser_spec_supports_param(spec, "text"),
        );
    }
    if spec.use_workspace_env {
        apply_workspace_env_fallback(&mut params);
    }
    if spec.use_surface_env {
        apply_surface_env_fallback(&mut params);
    }

    let resp = match socket_round_trip_v2(socket_path, spec.method, Some(&params), refresh_addr) {
        Ok(resp) => resp,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            return 1;
        }
    };
    if json_output {
        let _ = writeln!(io.stdout, "{resp}");
    } else {
        let _ = writeln!(io.stdout, "{}", default_relay_output(&resp));
    }
    0
}

pub fn browser_subcommand_hint() -> String {
    let mut names: Vec<&str> = browser_commands().keys().copied().collect();
    names.sort_unstable();
    names.join(", ")
}

fn browser_spec_supports_param(spec: &BrowserCommandSpec, param_key: &str) -> bool {
    spec.flag_keys
        .iter()
        .any(|key| flag_to_param_key(key) == param_key)
}

pub fn apply_browser_value_positionals(
    params: &mut Map<String, Value>,
    positionals: &[String],
    allow_value: bool,
    allow_text: bool,
) {
    if positionals.is_empty() {
        return;
    }
    let mut positionals = positionals;
    if !params.contains_key("selector") {
        params.insert(
            "selector".to_string(),
            Value::String(positionals[0].clone()),
        );
        positionals = &positionals[1..];
    }
    let joined = positionals.join(" ");
    if allow_value && !params.contains_key("value") {
        if !joined.is_empty() {
            params.insert("value".to_string(), Value::String(joined.clone()));
        } else if let Some(text) = params.get("text").cloned() {
            params.insert("value".to_string(), text);
        }
    }
    if allow_text && !params.contains_key("text") {
        if !joined.is_empty() {
            params.insert("text".to_string(), Value::String(joined));
        } else if let Some(value) = params.get("value").cloned() {
            params.insert("text".to_string(), value);
        }
    }
}

fn spec_uses_param(spec: &CommandSpec, param_key: &str) -> bool {
    spec.flag_keys.iter().any(|k| {
        let mut resolved = flag_to_param_key(k);
        if let Some(override_key) = spec.param_key_overrides.get(k) {
            resolved = (*override_key).to_string();
        }
        resolved == param_key
    })
}

pub fn apply_workspace_env_fallback(params: &mut Map<String, Value>) {
    if params.contains_key("workspace_id") {
        return;
    }
    if let Ok(env_ws) = std::env::var("CMUX_WORKSPACE_ID") {
        if !env_ws.is_empty() {
            params.insert("workspace_id".to_string(), Value::String(env_ws));
        }
    }
}

pub fn apply_surface_env_fallback(params: &mut Map<String, Value>) {
    if params.contains_key("surface_id") {
        return;
    }
    if let Ok(env_sf) = std::env::var("CMUX_SURFACE_ID") {
        if !env_sf.is_empty() {
            params.insert("surface_id".to_string(), Value::String(env_sf));
        }
    }
}

pub fn apply_notify_caller_env(method: &str, params: &mut Map<String, Value>) -> String {
    if method != "notification.create" {
        return method.to_string();
    }
    let workspace_id = params
        .get("workspace_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let surface_id = params
        .get("surface_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if workspace_id.is_empty() || surface_id.is_empty() {
        return method.to_string();
    }
    params.insert(
        "preferred_workspace_id".to_string(),
        Value::String(workspace_id),
    );
    params.insert(
        "preferred_surface_id".to_string(),
        Value::String(surface_id),
    );
    params.remove("workspace_id");
    params.remove("surface_id");
    "notification.create_for_caller".to_string()
}

pub fn default_relay_output(resp: &str) -> String {
    let result: Value = match serde_json::from_str(resp) {
        Ok(value) => value,
        Err(_) => {
            let trimmed = resp.trim();
            if trimmed.is_empty() {
                return "OK".to_string();
            }
            return trimmed.to_string();
        }
    };
    if relay_result_is_empty(&result) {
        return "OK".to_string();
    }
    match result {
        Value::String(text) => text,
        other => go_json_pretty(&other),
    }
}

fn relay_result_is_empty(result: &Value) -> bool {
    match result {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::String(text) => text.is_empty(),
        _ => false,
    }
}

/// Map a CLI flag name to its JSON-RPC param key.
pub fn flag_to_param_key(key: &str) -> String {
    match key {
        "workspace" => "workspace_id".to_string(),
        "surface" => "surface_id".to_string(),
        "panel" => "panel_id".to_string(),
        "pane" => "pane_id".to_string(),
        "window" => "window_id".to_string(),
        "group" => "group_id".to_string(),
        "command" => "initial_command".to_string(),
        "name" => "title".to_string(),
        "working-directory" => "working_directory".to_string(),
        "max-depth" => "max_depth".to_string(),
        "timeout-ms" => "timeout_ms".to_string(),
        "url-contains" => "url_contains".to_string(),
        "load-state" => "load_state".to_string(),
        // Hyphenated flag names map to underscore param keys by convention.
        other => other.replace('-', "_"),
    }
}

#[derive(Clone, Debug, Default)]
pub struct ParsedFlags {
    pub flags: HashMap<String, String>,
    pub repeated: HashMap<String, Vec<String>>,
    pub positional: Vec<String>,
}

/// Extract `--key value` pairs for the allowed keys. Keys listed in
/// `repeat_keys` may appear more than once; non-flag arguments are positional.
pub fn parse_flags(
    args: &[String],
    keys: &[&str],
    repeat_keys: Option<&[&str]>,
) -> Result<ParsedFlags, String> {
    let mut allowed: HashSet<&str> = keys.iter().copied().collect();
    let mut repeat: HashSet<&str> = HashSet::new();
    if let Some(repeat_keys) = repeat_keys {
        for key in repeat_keys {
            repeat.insert(key);
            allowed.insert(key);
        }
    }
    let mut result = ParsedFlags::default();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--" {
            result.positional.extend(args[i + 1..].iter().cloned());
            break;
        }
        if !args[i].starts_with("--") {
            result.positional.push(args[i].clone());
            i += 1;
            continue;
        }
        let key = args[i].trim_start_matches("--");
        if !allowed.contains(key) {
            return Err(format!("unknown flag --{key}"));
        }
        if i + 1 >= args.len() {
            return Err(format!("flag --{key} requires a value"));
        }
        let val = args[i + 1].clone();
        i += 1;
        if repeat.contains(key) {
            result
                .repeated
                .entry(key.to_string())
                .or_default()
                .push(val);
        } else {
            result.flags.insert(key.to_string(), val);
        }
        i += 1;
    }
    Ok(result)
}

/// Read the socket address from `~/.cmux/socket_addr` as a fallback when
/// `CMUX_SOCKET_PATH` is not set.
pub fn read_socket_addr_file() -> String {
    let Some(home) = home_dir() else {
        return String::new();
    };
    std::fs::read_to_string(path_join(&home, ".cmux/socket_addr"))
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

pub fn read_relay_auth_file(socket_path: &str) -> Option<RelayAuthState> {
    if socket_path.contains(':') && !socket_path.starts_with('/') {
        let port = socket_path.rsplit_once(':').map(|(_, port)| port)?;
        if port.is_empty() {
            return None;
        }
        let home = home_dir()?;
        let data = std::fs::read(path_join(&home, &format!(".cmux/relay/{port}.auth"))).ok()?;
        let state: RelayAuthState = serde_json::from_slice(&data).ok()?;
        if state.relay_id.is_empty() || state.relay_token.is_empty() {
            return None;
        }
        return Some(state);
    }
    None
}

pub fn current_relay_auth(socket_path: &str) -> Option<RelayAuthState> {
    let relay_id = std::env::var("CMUX_RELAY_ID")
        .unwrap_or_default()
        .trim()
        .to_string();
    let relay_token = std::env::var("CMUX_RELAY_TOKEN")
        .unwrap_or_default()
        .trim()
        .to_string();
    if !relay_id.is_empty() && !relay_token.is_empty() {
        return Some(RelayAuthState {
            relay_id,
            relay_token,
        });
    }
    read_relay_auth_file(socket_path)
}

/// Either a Unix or TCP connection to the local cmux socket/relay.
pub enum SocketConn {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl SocketConn {
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            SocketConn::Tcp(stream) => stream.set_read_timeout(timeout),
            SocketConn::Unix(stream) => stream.set_read_timeout(timeout),
        }
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            SocketConn::Tcp(stream) => stream.set_write_timeout(timeout),
            SocketConn::Unix(stream) => stream.set_write_timeout(timeout),
        }
    }

    pub fn try_clone(&self) -> io::Result<SocketConn> {
        Ok(match self {
            SocketConn::Tcp(stream) => SocketConn::Tcp(stream.try_clone()?),
            SocketConn::Unix(stream) => SocketConn::Unix(stream.try_clone()?),
        })
    }
}

impl Read for SocketConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SocketConn::Tcp(stream) => stream.read(buf),
            SocketConn::Unix(stream) => stream.read(buf),
        }
    }
}

impl Write for SocketConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SocketConn::Tcp(stream) => stream.write(buf),
            SocketConn::Unix(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SocketConn::Tcp(stream) => stream.flush(),
            SocketConn::Unix(stream) => stream.flush(),
        }
    }
}

/// Connect to the cmux socket. Addresses containing a colon that do not start
/// with `/` are TCP (host:port); everything else is a Unix socket path. For TCP
/// connections, `refresh_addr` recovers once from a stale socket_addr rewrite.
pub fn dial_socket(
    addr: &str,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
) -> io::Result<SocketConn> {
    if addr.contains(':') && !addr.starts_with('/') {
        let mut addr = addr.to_string();
        let mut attempt = dial_tcp(&addr);
        if let Err(err) = &attempt {
            if let Some(refresh) = refresh_addr {
                if is_connection_refused(err) {
                    let refreshed = refresh().trim().to_string();
                    if !refreshed.is_empty() && refreshed != addr {
                        addr = refreshed;
                        attempt = dial_tcp(&addr);
                    }
                }
            }
        }
        let conn = attempt.map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("dial tcp {addr}: connect: {}", go_io_error(&err)),
            )
        })?;
        if let Some(auth) = current_relay_auth(&addr) {
            if let Err(err) = authenticate_relay_conn(&conn, &auth) {
                drop(conn);
                return Err(err);
            }
        }
        return Ok(SocketConn::Tcp(conn));
    }
    match UnixStream::connect(addr) {
        Ok(conn) => Ok(SocketConn::Unix(conn)),
        Err(err) => Err(io::Error::new(
            err.kind(),
            format!("dial unix {addr}: connect: {}", go_io_error(&err)),
        )),
    }
}

fn dial_tcp(addr: &str) -> io::Result<TcpStream> {
    let mut last_err = io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid address {addr}"),
    );
    for socket_addr in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2)) {
            Ok(conn) => {
                let _ = conn.set_nodelay(true);
                return Ok(conn);
            }
            Err(err) => last_err = err,
        }
    }
    Err(last_err)
}

pub fn is_connection_refused(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::ConnectionRefused || err.to_string().contains("connection refused")
}

#[derive(Deserialize)]
struct RelayChallenge {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    version: i64,
    #[serde(default)]
    relay_id: String,
    #[serde(default)]
    nonce: String,
}

pub fn authenticate_relay_conn(conn: &TcpStream, auth: &RelayAuthState) -> io::Result<()> {
    let _ = conn.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = conn.set_write_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut line = String::new();
    if let Err(err) = reader.read_line(&mut line) {
        return Err(io::Error::other(format!(
            "failed to read relay auth challenge: {err}"
        )));
    }
    if line.is_empty() {
        return Err(io::Error::other("failed to read relay auth challenge: EOF"));
    }
    let challenge: RelayChallenge = serde_json::from_str(&line)
        .map_err(|_| io::Error::other("invalid relay auth challenge"))?;
    if challenge.protocol != "cmux-relay-auth"
        || challenge.version != 1
        || challenge.relay_id != auth.relay_id
        || challenge.nonce.is_empty()
    {
        return Err(io::Error::other("relay auth challenge mismatch"));
    }
    let token_bytes =
        hex::decode(&auth.relay_token).map_err(|_| io::Error::other("invalid relay auth token"))?;
    let mac = compute_relay_mac(
        &token_bytes,
        &auth.relay_id,
        &challenge.nonce,
        challenge.version,
    );
    let payload = go_json(&json!({"relay_id": auth.relay_id, "mac": hex::encode(mac)}));
    let mut writer = conn;
    writer
        .write_all(format!("{payload}\n").as_bytes())
        .map_err(|err| io::Error::other(format!("failed to send relay auth response: {err}")))?;
    line.clear();
    if let Err(err) = reader.read_line(&mut line) {
        return Err(io::Error::other(format!(
            "failed to read relay auth result: {err}"
        )));
    }
    if line.is_empty() {
        return Err(io::Error::other("failed to read relay auth result: EOF"));
    }
    let result: Value =
        serde_json::from_str(&line).map_err(|_| io::Error::other("invalid relay auth result"))?;
    if !result.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
        return Err(io::Error::other("relay auth rejected"));
    }
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    Ok(())
}

pub fn compute_relay_mac(token: &[u8], relay_id: &str, nonce: &str, version: i64) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(token).expect("HMAC accepts any key length");
    mac.update(format!("relay_id={relay_id}\nnonce={nonce}\nversion={version}").as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Send a JSON-RPC request and return the result JSON.
pub fn socket_round_trip_v2(
    socket_path: &str,
    method: &str,
    params: Option<&Map<String, Value>>,
    refresh_addr: Option<&Arc<dyn Fn() -> String + Send + Sync>>,
) -> Result<String, String> {
    let mut conn = dial_socket(socket_path, refresh_addr)
        .map_err(|err| format!("failed to connect to {socket_path}: {err}"))?;
    let id = random_hex(8);
    let mut req = Map::new();
    req.insert("id".to_string(), Value::String(id));
    req.insert("method".to_string(), Value::String(method.to_string()));
    req.insert(
        "params".to_string(),
        Value::Object(params.cloned().unwrap_or_default()),
    );
    let payload = go_json(&Value::Object(req));
    conn.write_all(format!("{payload}\n").as_bytes())
        .map_err(|err| format!("failed to send request: {err}"))?;
    conn.flush()
        .map_err(|err| format!("failed to send request: {err}"))?;

    let _ = conn.set_read_timeout(Some(Duration::from_secs(15)));
    let mut reader = BufReader::new(conn);
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => return Err("failed to read response: EOF".to_string()),
        Ok(_) => {}
        Err(err) => return Err(format!("failed to read response: {err}")),
    }
    if !line.ends_with('\n') {
        return Err("failed to read response: EOF".to_string());
    }

    let resp: Map<String, Value> = match serde_json::from_str::<Value>(&line) {
        Ok(Value::Object(map)) => map,
        _ => return Ok(line.trim_end_matches('\n').to_string()),
    };
    if !resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
        if let Some(Value::Object(err_obj)) = resp.get("error") {
            let code = err_obj.get("code").and_then(|v| v.as_str()).unwrap_or("");
            let msg = err_obj
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            return Err(format!("server error [{code}]: {msg}"));
        }
        return Err("server returned error response".to_string());
    }
    if let Some(result) = resp.get("result") {
        return Ok(go_json(result));
    }
    Ok("{}".to_string())
}

pub fn cli_usage(stderr: &mut dyn Write) {
    let lines = [
        "Usage: cmux [--socket <path>] [--json] <command> [args...]",
        "",
        "Commands:",
        "  ping                      Check connectivity",
        "  capabilities              List server capabilities",
        "  list-workspaces           List all workspaces",
        "  new-workspace             Create a new workspace",
        "    --name <title>          Workspace title",
        "    --cwd <dir>             Working directory",
        "    --description <text>    Workspace description",
        "    --focus true|false      Focus the workspace after creation",
        "    --window <id>           Target window",
        "    --group <id>            Workspace group to place into",
        "    --group-placement <p>   Placement within the group (before|after|...)",
        "    --group-reference <id>  Reference workspace for placement",
        "    --layout <json>         Pane layout JSON object",
        "    --env KEY=VALUE         Environment variable (repeatable)",
        "    --env-file <path>       File of KEY=VALUE environment variables",
        "    --command <cmd>         Command to send to the new workspace after creation",
        "  rename-workspace          Rename a workspace",
        "  close-workspace           Close a workspace",
        "  select-workspace          Select a workspace",
        "  next-workspace            Switch to next workspace",
        "  previous-workspace        Switch to previous workspace",
        "  last-workspace            Switch to last-used workspace",
        "  current-workspace         Show the active workspace ID",
        "  move-workspace-to-window  Move workspace to another window",
        "  equalize-splits           Equalize pane splits in a workspace",
        "  list-panes                List panes in a workspace",
        "  new-pane                  Create a new pane",
        "  last-pane                 Switch to last-used pane",
        "  join-pane                 Join a pane into another",
        "  swap-pane                 Swap two panes",
        "  break-pane                Break a pane into its own workspace",
        "  resize-pane               Resize a pane",
        "  list-panels               List surfaces in a workspace",
        "  list-pane-surfaces        List surfaces in a pane",
        "  new-surface               Create a new surface",
        "  new-split                 Split an existing surface",
        "  close-surface             Close a surface",
        "  focus-panel               Focus a surface",
        "  refresh-surfaces          Refresh all surfaces",
        "  send                      Send text to a surface",
        "  send-key                  Send a key to a surface",
        "  read-screen               Read terminal output from a surface",
        "  clear-history             Clear scrollback history for a surface",
        "  list-windows              List all windows",
        "  new-window                Create a new window",
        "  close-window              Close a window",
        "  current-window            Show the active window ID",
        "  focus-window              Focus a window",
        "  notify                    Create a notification",
        "  jump-to-unread            Jump to first unread notification",
        "  dismiss-notification      Dismiss a notification",
        "  mark-notification-read    Mark a notification as read",
        "  open-notification         Open a notification",
        "  workspace group <sub>     Manage sidebar workspace groups (list, create, ungroup,",
        "                            delete, rename, collapse, expand, pin, unpin, add, remove,",
        "                            set-anchor, new-workspace, set-color, set-icon, move, focus)",
        "  browser <sub>             Browser commands through the local cmux browser relay",
        "  claude-teams [args...]    Launch Claude Code in teammate mode",
        "  omo [args...]             Launch OpenCode with cmux integration",
        "  omx [args...]             Launch Oh My Codex with cmux integration",
        "  omc [args...]             Launch Oh My Claude Code with cmux integration",
        "  rpc <method> [json-params] Send arbitrary JSON-RPC",
    ];
    for line in lines {
        let _ = writeln!(stderr, "{line}");
    }
}

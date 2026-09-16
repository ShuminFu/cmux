//! The remote `cmux` CLI relay: translates `cmux <command>` invocations on
//! the SSH host into v2 JSON-RPC calls over the relay socket that the app
//! forwards back to the local cmux instance.

pub mod agents;
pub mod commands;
pub mod tmux;

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::Sha256;

use crate::cli_bridge::default_cloud_cli_bridge_socket_if_exists;
use crate::util::{go_json, go_json_indent, io_error_text, quote, random_hex, user_home_dir};
use commands::{
    BrowserCommandSpec, CommandSpec, browser_command, browser_subcommand_hint, command_override,
    command_spec,
};

pub type Params = Map<String, Value>;

/// Where a socket address came from decides whether a stale-address refresh
/// is attempted after a refused connection.
pub type RefreshAddr = Option<fn() -> String>;

/// Output sink for the CLI so tests can capture stdout/stderr.
pub struct CliIo<'a> {
    pub stdout: &'a mut dyn Write,
    pub stderr: &'a mut dyn Write,
}

/// `cmux` entrypoint for the `cli` subcommand and the busybox invocation.
pub fn run_cli(args: &[String]) -> i32 {
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut out = stdout.lock();
    let mut err = stderr.lock();
    let mut io = CliIo { stdout: &mut out, stderr: &mut err };
    run_cli_with_io(args, &mut io)
}

pub fn run_cli_with_io(args: &[String], io: &mut CliIo<'_>) -> i32 {
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

    // refresh_addr is set when the address came from the socket_addr file
    // (not env/flag), allowing one stale-address refresh if another
    // workspace has replaced socket_addr.
    let mut refresh_addr: RefreshAddr = None;
    if socket_path.is_empty() {
        socket_path = read_socket_addr_file();
        refresh_addr = Some(read_socket_addr_file);
    }
    if socket_path.is_empty() {
        socket_path = default_cloud_cli_bridge_socket_if_exists();
    }
    if socket_path.is_empty() {
        let _ = writeln!(io.stderr, "cmux: CMUX_SOCKET_PATH not set and --socket not provided");
        return 1;
    }

    if cmd_name == "rpc" {
        return run_rpc(&socket_path, cmd_args, json_output, refresh_addr, io);
    }
    if command_override(&cmd_name).special_dispatch && cmd_name == "new-workspace" {
        return run_new_workspace_relay(&socket_path, cmd_args, json_output, refresh_addr, io);
    }
    if cmd_name == "browser" {
        return run_browser_relay(&socket_path, cmd_args, json_output, refresh_addr, io);
    }
    if cmd_name == "workspace-group" {
        return run_workspace_group_relay(&socket_path, cmd_args, json_output, refresh_addr, io);
    }
    if cmd_name == "workspace" {
        if cmd_args.first().map(String::as_str) == Some("group") {
            return run_workspace_group_relay(
                &socket_path,
                &cmd_args[1..],
                json_output,
                refresh_addr,
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
            return agents::run_claude_teams_relay(&socket_path, cmd_args, refresh_addr, io);
        }
        "omo" => return agents::run_omo_relay(&socket_path, cmd_args, refresh_addr, io),
        "omx" => return agents::run_omx_relay(&socket_path, cmd_args, refresh_addr, io),
        "omc" => return agents::run_omc_relay(&socket_path, cmd_args, refresh_addr, io),
        "__tmux-compat" => return tmux::run_tmux_compat(&socket_path, cmd_args, refresh_addr, io),
        _ => {}
    }
    let Some(spec) = command_spec(&cmd_name) else {
        let _ = writeln!(io.stderr, "cmux: unknown command {}", quote(&cmd_name));
        return 2;
    };
    exec_v2(&socket_path, &spec, cmd_args, json_output, refresh_addr, io)
}

fn parse_bool_flag(value: &str) -> Option<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn print_relay_output(io: &mut CliIo<'_>, resp: &str, json_output: bool) {
    if json_output {
        let _ = writeln!(io.stdout, "{resp}");
    } else {
        let _ = writeln!(io.stdout, "{}", default_relay_output(resp));
    }
}

/// Send a v2 JSON-RPC request built from the command spec.
pub fn exec_v2(
    socket_path: &str,
    spec: &CommandSpec,
    args: &[String],
    json_output: bool,
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let mut params: Params = spec.default_params.clone();
    if !spec.no_params {
        let parsed = match parse_flags(args, spec.flag_keys, Some(spec.repeat_keys)) {
            Ok(parsed) => parsed,
            Err(err) => {
                let _ = writeln!(io.stderr, "cmux: {err}");
                return 2;
            }
        };
        let ov = command_override(spec.name);
        let client_only = ov.client_only_flags;
        for key in spec.flag_keys {
            if client_only.contains(key) {
                continue;
            }
            let Some(val) = parsed.flags.get(*key) else { continue };
            let param_key = spec
                .param_key_overrides
                .get(key)
                .map_or_else(|| flag_to_param_key(key), |k| (*k).to_string());
            if spec.bool_flags.contains(key) {
                match parse_bool_flag(val) {
                    Some(b) => {
                        params.insert(param_key, Value::Bool(b));
                    }
                    None => {
                        let _ = writeln!(io.stderr, "cmux: --{key} must be true or false");
                        return 2;
                    }
                }
            } else {
                params.insert(param_key, Value::from(val.as_str()));
            }
        }
        for key in spec.repeat_keys {
            if client_only.contains(key) {
                continue;
            }
            if let Some(vals) = parsed.repeated.get(*key) {
                let param_key = spec
                    .param_key_overrides
                    .get(key)
                    .map_or_else(|| flag_to_param_key(key), |k| (*k).to_string());
                params.insert(
                    param_key,
                    Value::Array(vals.iter().map(|v| Value::from(v.as_str())).collect()),
                );
            }
        }
        if !parsed.positional.is_empty() {
            if spec.positional_key.is_empty() {
                let _ =
                    writeln!(io.stderr, "cmux: {} does not accept positional arguments", spec.name);
                return 2;
            }
            params
                .insert(spec.positional_key.to_string(), Value::from(parsed.positional.join(" ")));
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
    match socket_round_trip_v2(socket_path, &method, Some(params), refresh_addr) {
        Ok(resp) => {
            print_relay_output(io, &resp, json_output);
            0
        }
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            1
        }
    }
}

/// `cmux new-workspace` with full flag parity to the macOS CLI: --layout
/// (JSON object), --env (repeatable KEY=VALUE), --env-file, and --command.
pub fn run_new_workspace_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: RefreshAddr,
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
    let parsed = match parse_flags(args, &flag_keys, Some(&["env"])) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux new-workspace: {err}");
            return 2;
        }
    };
    if !parsed.positional.is_empty() {
        let _ = writeln!(io.stderr, "cmux: new-workspace does not accept positional arguments");
        return 2;
    }
    let mut params = Params::new();
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
        let Some(val) = parsed.flags.get(key) else { continue };
        let param_key = match key {
            "name" => "title".to_string(),
            "group-placement" => "placement".to_string(),
            "group-reference" => "group_reference_workspace_id".to_string(),
            "command" => continue,
            _ => flag_to_param_key(key),
        };
        params.insert(param_key, Value::from(val.as_str()));
    }
    if let Some(val) = parsed.flags.get("focus") {
        match parse_bool_flag(val) {
            Some(b) => {
                params.insert("focus".to_string(), Value::Bool(b));
            }
            None => {
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
                    "cmux new-workspace: --layout must be valid JSON: {}",
                    go_json_error(&err, val)
                );
                return 2;
            }
        }
    }
    let mut env = Params::new();
    if let Some(vals) = parsed.repeated.get("env") {
        for kv in vals {
            let Some((k, v)) = kv.split_once('=') else {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --env {} must be KEY=VALUE",
                    quote(kv)
                );
                return 2;
            };
            env.insert(k.to_string(), Value::from(v));
        }
    }
    if let Some(env_file) = parsed.flags.get("env-file") {
        let data = match std::fs::read_to_string(env_file) {
            Ok(data) => data,
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --env-file: {}",
                    crate::util::path_error("open", env_file, &err)
                );
                return 2;
            }
        };
        for line in data.split('\n') {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --env-file line {} must be KEY=VALUE",
                    quote(line)
                );
                return 2;
            };
            env.insert(k.to_string(), Value::from(v));
        }
    }
    if !env.is_empty() {
        params.insert("env".to_string(), Value::Object(env));
    }
    let resp =
        match socket_round_trip_v2(socket_path, "workspace.create", Some(params), refresh_addr) {
            Ok(resp) => resp,
            Err(err) => {
                let _ = writeln!(io.stderr, "cmux: {err}");
                return 1;
            }
        };
    if let Some(cmd) = parsed.flags.get("command") {
        let result: Map<String, Value> = match serde_json::from_str::<Value>(&resp) {
            Ok(Value::Object(map)) => map,
            Ok(other) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --command skipped: could not parse create response: json: cannot unmarshal {} into Go value of type map[string]interface {{}}",
                    go_json_type_name(&other)
                );
                return 1;
            }
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux new-workspace: --command skipped: could not parse create response: {}",
                    go_json_error(&err, &resp)
                );
                return 1;
            }
        };
        let surface_id = result.get("surface_id").and_then(Value::as_str).unwrap_or("");
        if surface_id.is_empty() {
            let _ = writeln!(
                io.stderr,
                "cmux new-workspace: --command skipped: workspace.create response missing surface_id"
            );
            return 1;
        }
        let mut send_params = Params::new();
        send_params.insert("surface_id".to_string(), Value::from(surface_id));
        send_params.insert("text".to_string(), Value::from(cmd.as_str()));
        if let Err(err) =
            socket_round_trip_v2(socket_path, "surface.send_text", Some(send_params), refresh_addr)
        {
            let _ = writeln!(io.stderr, "cmux new-workspace: --command send failed: {err}");
            return 1;
        }
        let mut key_params = Params::new();
        key_params.insert("surface_id".to_string(), Value::from(surface_id));
        key_params.insert("key".to_string(), Value::from("return"));
        if let Err(err) =
            socket_round_trip_v2(socket_path, "surface.send_key", Some(key_params), refresh_addr)
        {
            let _ = writeln!(io.stderr, "cmux new-workspace: --command send-key failed: {err}");
            return 1;
        }
    }
    print_relay_output(io, &resp, json_output);
    0
}

/// Approximate Go's `encoding/json` error text for a decode failure.
fn go_json_error(err: &serde_json::Error, input: &str) -> String {
    if err.is_eof() {
        return "unexpected end of JSON input".to_string();
    }
    if err.is_syntax() {
        let offending = input
            .lines()
            .nth(err.line().saturating_sub(1))
            .and_then(|line| line.chars().nth(err.column().saturating_sub(1)));
        if let Some(c) = offending {
            return format!(
                "invalid character {} in JSON input",
                quote(&c.to_string()).replace('"', "'")
            );
        }
    }
    err.to_string()
}

/// Go's type name for a JSON value, as used in `cannot unmarshal <type>`.
fn go_json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `cmux rpc <method> [json-params]`.
pub fn run_rpc(
    socket_path: &str,
    args: &[String],
    _json_output: bool,
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    if args.is_empty() {
        let _ = writeln!(io.stderr, "cmux rpc: requires a method name");
        return 2;
    }
    let method = &args[0];
    let mut params: Option<Params> = None;
    if args.len() > 1 {
        match serde_json::from_str::<Value>(&args[1]) {
            Ok(Value::Object(map)) => params = Some(map),
            Ok(Value::Null) => params = None,
            Ok(other) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux rpc: invalid JSON params: json: cannot unmarshal {} into Go value of type map[string]interface {{}}",
                    go_json_type_name(&other)
                );
                return 2;
            }
            Err(err) => {
                let _ = writeln!(
                    io.stderr,
                    "cmux rpc: invalid JSON params: {}",
                    go_json_error(&err, &args[1])
                );
                return 2;
            }
        }
    }
    match socket_round_trip_v2(socket_path, method, params, refresh_addr) {
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

const WORKSPACE_GROUP_FLAG_KEYS: &[(&str, &[&str])] = &[
    ("list", &["window"]),
    ("create", &["name", "cwd", "from", "window"]),
    ("ungroup", &["group", "window"]),
    ("delete", &["group", "window"]),
    ("rename", &["group", "name", "window"]),
    ("collapse", &["group", "window"]),
    ("expand", &["group", "window"]),
    ("pin", &["group", "window"]),
    ("unpin", &["group", "window"]),
    ("add", &["group", "workspace", "window"]),
    ("remove", &["workspace", "window"]),
    ("set-anchor", &["group", "workspace", "window"]),
    ("new-workspace", &["group", "placement", "window"]),
    ("set-color", &["group", "hex", "window"]),
    ("set-icon", &["group", "symbol", "window"]),
    ("move", &["group", "to-index", "before", "after", "window"]),
    ("focus", &["group", "window"]),
];

/// `cmux workspace group <sub>` (and the `workspace-group` alias).
#[allow(clippy::too_many_lines)]
pub fn run_workspace_group_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    const HINT: &str = "list, create, ungroup, delete, rename, collapse, expand, pin, unpin, add, remove, set-anchor, new-workspace, set-color, set-icon, move, focus";
    if args.is_empty() {
        let _ = writeln!(io.stderr, "cmux workspace group: requires a subcommand ({HINT})");
        return 2;
    }
    let sub = args[0].as_str();
    let Some((_, flag_keys)) = WORKSPACE_GROUP_FLAG_KEYS.iter().find(|(name, _)| *name == sub)
    else {
        let _ =
            writeln!(io.stderr, "cmux workspace group: unknown subcommand {} ({HINT})", quote(sub));
        return 2;
    };
    let fail = |io: &mut CliIo<'_>, message: &str| -> i32 {
        let _ = writeln!(io.stderr, "cmux workspace group {sub}: {message}");
        2
    };
    let parsed = match parse_flags(&args[1..], flag_keys, None) {
        Ok(parsed) => parsed,
        Err(err) => return fail(io, &err),
    };
    let mut params = Params::new();
    if let Some(win) = parsed.flags.get("window") {
        params.insert("window_id".to_string(), Value::from(win.as_str()));
    }
    let mut positional: &[String] = &parsed.positional;
    let take_group_id = |params: &mut Params, positional: &mut &[String]| -> bool {
        if let Some(gid) = parsed.flags.get("group") {
            params.insert("group_id".to_string(), Value::from(gid.as_str()));
            return true;
        }
        if let Some((first, rest)) = positional.split_first() {
            params.insert("group_id".to_string(), Value::from(first.as_str()));
            *positional = rest;
            return true;
        }
        false
    };
    match sub {
        "list" => {}
        "create" => {
            let name = parsed
                .flags
                .get("name")
                .cloned()
                .or_else(|| positional.first().cloned())
                .unwrap_or_default();
            params.insert("name".to_string(), Value::from(name));
            if let Some(cwd) = parsed.flags.get("cwd") {
                params.insert("cwd".to_string(), Value::from(cwd.as_str()));
            }
            if let Some(from) = parsed.flags.get("from") {
                let ids: Vec<Value> = from
                    .split(',')
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(Value::from)
                    .collect();
                params.insert("child_workspace_ids".to_string(), Value::Array(ids));
            }
        }
        "ungroup" | "delete" | "collapse" | "expand" | "pin" | "unpin" | "focus" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
        }
        "rename" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
            let name = parsed.flags.get("name").cloned().or_else(|| positional.first().cloned());
            let Some(name) = name else {
                return fail(io, "requires --name <name>");
            };
            params.insert("name".to_string(), Value::from(name));
        }
        "add" | "set-anchor" => {
            let (Some(gid), Some(ws)) = (parsed.flags.get("group"), parsed.flags.get("workspace"))
            else {
                return fail(io, "requires --group <id> --workspace <id>");
            };
            params.insert("group_id".to_string(), Value::from(gid.as_str()));
            params.insert("workspace_id".to_string(), Value::from(ws.as_str()));
        }
        "remove" => {
            let ws = parsed.flags.get("workspace").cloned().or_else(|| positional.first().cloned());
            let Some(ws) = ws else {
                return fail(io, "requires --workspace <id>");
            };
            params.insert("workspace_id".to_string(), Value::from(ws));
        }
        "new-workspace" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
            if let Some(placement) = parsed.flags.get("placement") {
                params.insert("placement".to_string(), Value::from(placement.as_str()));
            }
        }
        "set-color" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
            // Omitting --hex clears the color, matching the macOS CLI.
            params.insert(
                "hex".to_string(),
                Value::from(parsed.flags.get("hex").cloned().unwrap_or_default()),
            );
        }
        "set-icon" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
            params.insert(
                "symbol".to_string(),
                Value::from(parsed.flags.get("symbol").cloned().unwrap_or_default()),
            );
        }
        "move" => {
            if !take_group_id(&mut params, &mut positional) {
                return fail(io, "requires a group id or --group <id>");
            }
            if let Some(v) = parsed.flags.get("to-index") {
                let Ok(n) = v.parse::<i64>() else {
                    return fail(io, "--to-index must be an integer");
                };
                params.insert("to_index".to_string(), Value::from(n));
            } else if let Some(v) = parsed.flags.get("before") {
                params.insert("before_group_id".to_string(), Value::from(v.as_str()));
            } else if let Some(v) = parsed.flags.get("after") {
                params.insert("after_group_id".to_string(), Value::from(v.as_str()));
            } else {
                return fail(io, "requires --to-index <n>, --before <group>, or --after <group>");
            }
        }
        _ => {}
    }
    // Forward the SSH caller's workspace/surface context so methods without
    // a group id resolve the caller's window instead of whichever local
    // window is focused.
    apply_workspace_env_fallback(&mut params);
    apply_surface_env_fallback(&mut params);
    let method = format!("workspace.group.{}", sub.replace('-', "_"));
    match socket_round_trip_v2(socket_path, &method, Some(params), refresh_addr) {
        Ok(resp) => {
            print_relay_output(io, &resp, json_output);
            0
        }
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            1
        }
    }
}

/// `cmux browser <subcommand>` mapped to browser.* v2 methods.
pub fn run_browser_relay(
    socket_path: &str,
    args: &[String],
    json_output: bool,
    refresh_addr: RefreshAddr,
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
    let Some(spec) = browser_command(sub) else {
        let _ = writeln!(io.stderr, "cmux browser: unknown subcommand {}", quote(sub));
        return 2;
    };
    let parsed = match parse_flags(&args[1..], spec.flag_keys, None) {
        Ok(parsed) => parsed,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux browser: {err}");
            return 2;
        }
    };
    let mut params = Params::new();
    for key in spec.flag_keys {
        if let Some(val) = parsed.flags.get(*key) {
            params.insert(flag_to_param_key(key), Value::from(val.as_str()));
        }
    }
    let joined = parsed.positional.join(" ");
    if spec.allow_positional_url && !params.contains_key("url") && !parsed.positional.is_empty() {
        params.insert("url".to_string(), Value::from(joined.clone()));
    }
    if spec.allow_positional_script
        && !params.contains_key("script")
        && !parsed.positional.is_empty()
    {
        params.insert("script".to_string(), Value::from(joined.clone()));
    }
    if spec.allow_positional_key && !params.contains_key("key") && !parsed.positional.is_empty() {
        params.insert("key".to_string(), Value::from(joined.clone()));
    }
    if spec.allow_positional_query
        && !params.contains_key("selector")
        && !parsed.positional.is_empty()
    {
        params.insert("selector".to_string(), Value::from(joined));
    }
    if spec.allow_positional_value {
        apply_browser_value_positionals(
            &mut params,
            &parsed.positional,
            browser_spec_supports_param(&spec, "value"),
            browser_spec_supports_param(&spec, "text"),
        );
    }
    if spec.use_workspace_env {
        apply_workspace_env_fallback(&mut params);
    }
    if spec.use_surface_env {
        apply_surface_env_fallback(&mut params);
    }
    match socket_round_trip_v2(socket_path, spec.method, Some(params), refresh_addr) {
        Ok(resp) => {
            print_relay_output(io, &resp, json_output);
            0
        }
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux: {err}");
            1
        }
    }
}

fn browser_spec_supports_param(spec: &BrowserCommandSpec, param_key: &str) -> bool {
    spec.flag_keys.iter().any(|key| flag_to_param_key(key) == param_key)
}

pub fn apply_browser_value_positionals(
    params: &mut Params,
    positionals: &[String],
    allow_value: bool,
    allow_text: bool,
) {
    if positionals.is_empty() {
        return;
    }
    let mut rest = positionals;
    if !params.contains_key("selector") {
        params.insert("selector".to_string(), Value::from(rest[0].as_str()));
        rest = &rest[1..];
    }
    let joined = rest.join(" ");
    if allow_value && !params.contains_key("value") {
        if !joined.is_empty() {
            params.insert("value".to_string(), Value::from(joined.clone()));
        } else if let Some(text) = params.get("text").cloned() {
            params.insert("value".to_string(), text);
        }
    }
    if allow_text && !params.contains_key("text") {
        if !joined.is_empty() {
            params.insert("text".to_string(), Value::from(joined));
        } else if let Some(value) = params.get("value").cloned() {
            params.insert("text".to_string(), value);
        }
    }
}

fn spec_uses_param(spec: &CommandSpec, param_key: &str) -> bool {
    spec.flag_keys.iter().any(|k| {
        let resolved = spec
            .param_key_overrides
            .get(k)
            .map_or_else(|| flag_to_param_key(k), |o| (*o).to_string());
        resolved == param_key
    })
}

pub fn apply_workspace_env_fallback(params: &mut Params) {
    if params.contains_key("workspace_id") {
        return;
    }
    if let Ok(ws) = std::env::var("CMUX_WORKSPACE_ID")
        && !ws.is_empty()
    {
        params.insert("workspace_id".to_string(), Value::from(ws));
    }
}

pub fn apply_surface_env_fallback(params: &mut Params) {
    if params.contains_key("surface_id") {
        return;
    }
    if let Ok(sf) = std::env::var("CMUX_SURFACE_ID")
        && !sf.is_empty()
    {
        params.insert("surface_id".to_string(), Value::from(sf));
    }
}

#[must_use]
pub fn apply_notify_caller_env(method: &str, params: &mut Params) -> String {
    if method != "notification.create" {
        return method.to_string();
    }
    let workspace_id =
        params.get("workspace_id").and_then(Value::as_str).unwrap_or("").trim().to_string();
    let surface_id =
        params.get("surface_id").and_then(Value::as_str).unwrap_or("").trim().to_string();
    if workspace_id.is_empty() || surface_id.is_empty() {
        return method.to_string();
    }
    params.insert("preferred_workspace_id".to_string(), Value::from(workspace_id));
    params.insert("preferred_surface_id".to_string(), Value::from(surface_id));
    params.remove("workspace_id");
    params.remove("surface_id");
    "notification.create_for_caller".to_string()
}

/// Human-friendly rendering of a relay result: "OK" for empty results,
/// bare strings as-is, everything else pretty-printed.
#[must_use]
pub fn default_relay_output(resp: &str) -> String {
    let Ok(result) = serde_json::from_str::<Value>(resp) else {
        let trimmed = resp.trim();
        if trimmed.is_empty() {
            return "OK".to_string();
        }
        return trimmed.to_string();
    };
    if relay_result_is_empty(&result) {
        return "OK".to_string();
    }
    match result {
        Value::String(s) => s,
        other => go_json_indent(&other),
    }
}

fn relay_result_is_empty(result: &Value) -> bool {
    match result {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

/// Map a CLI flag name to its JSON-RPC param key.
#[must_use]
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
        _ => key.replace('-', "_"),
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedFlags {
    pub flags: std::collections::HashMap<String, String>,
    pub repeated: std::collections::HashMap<String, Vec<String>>,
    pub positional: Vec<String>,
}

/// Extract `--key value` pairs for the allowed keys. Keys in `repeat_keys`
/// may appear more than once; non-flag arguments are positional.
pub fn parse_flags(
    args: &[String],
    keys: &[&str],
    repeat_keys: Option<&[&str]>,
) -> Result<ParsedFlags, String> {
    let repeat = repeat_keys.unwrap_or(&[]);
    let allowed = |key: &str| keys.contains(&key) || repeat.contains(&key);
    let mut result = ParsedFlags::default();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--" {
            result.positional.extend_from_slice(&args[i + 1..]);
            break;
        }
        let Some(key) = args[i].strip_prefix("--") else {
            result.positional.push(args[i].clone());
            i += 1;
            continue;
        };
        if !allowed(key) {
            return Err(format!("unknown flag --{key}"));
        }
        if i + 1 >= args.len() {
            return Err(format!("flag --{key} requires a value"));
        }
        let val = args[i + 1].clone();
        i += 2;
        if repeat.contains(&key) {
            result.repeated.entry(key.to_string()).or_default().push(val);
        } else {
            result.flags.insert(key.to_string(), val);
        }
    }
    Ok(result)
}

/// `~/.cmux/socket_addr`, written by the app once the relay is up.
#[must_use]
pub fn read_socket_addr_file() -> String {
    let Some(home) = user_home_dir() else { return String::new() };
    std::fs::read_to_string(Path::new(&home).join(".cmux").join("socket_addr"))
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct RelayAuthState {
    #[serde(default)]
    pub relay_id: String,
    #[serde(default)]
    pub relay_token: String,
}

fn read_relay_auth_file(socket_path: &str) -> Option<RelayAuthState> {
    if !socket_path.contains(':') || socket_path.starts_with('/') {
        return None;
    }
    let (_, port) = split_host_port(socket_path)?;
    if port.is_empty() {
        return None;
    }
    let home = user_home_dir()?;
    let data =
        std::fs::read(Path::new(&home).join(".cmux").join("relay").join(format!("{port}.auth")))
            .ok()?;
    let state: RelayAuthState = serde_json::from_slice(&data).ok()?;
    if state.relay_id.is_empty() || state.relay_token.is_empty() {
        return None;
    }
    Some(state)
}

fn split_host_port(addr: &str) -> Option<(String, String)> {
    let idx = addr.rfind(':')?;
    let host = &addr[..idx];
    let port = &addr[idx + 1..];
    if host.starts_with('[') {
        let end = host.find(']')?;
        return Some((host[1..end].to_string(), port.to_string()));
    }
    if host.contains(':') {
        return None;
    }
    Some((host.to_string(), port.to_string()))
}

fn current_relay_auth(socket_path: &str) -> Option<RelayAuthState> {
    let relay_id = std::env::var("CMUX_RELAY_ID").unwrap_or_default().trim().to_string();
    let relay_token = std::env::var("CMUX_RELAY_TOKEN").unwrap_or_default().trim().to_string();
    if !relay_id.is_empty() && !relay_token.is_empty() {
        return Some(RelayAuthState { relay_id, relay_token });
    }
    read_relay_auth_file(socket_path)
}

/// Either transport the relay socket can be reached over.
pub enum Conn {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Conn {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.set_read_timeout(timeout),
            Conn::Unix(s) => s.set_read_timeout(timeout),
        }
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.set_write_timeout(timeout),
            Conn::Unix(s) => s.set_write_timeout(timeout),
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conn::Tcp(s) => s.read(buf),
            Conn::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conn::Tcp(s) => s.write(buf),
            Conn::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Tcp(s) => s.flush(),
            Conn::Unix(s) => s.flush(),
        }
    }
}

/// Connect to the cmux socket: `host:port` is TCP, anything else a Unix
/// socket path. A refused TCP connection is retried once against a
/// refreshed `socket_addr`.
pub fn dial_socket(addr: &str, refresh_addr: RefreshAddr) -> Result<Conn, String> {
    if addr.contains(':') && !addr.starts_with('/') {
        let mut addr = addr.to_string();
        let mut result = dial_tcp(&addr);
        if let (Err(err), Some(refresh)) = (&result, refresh_addr)
            && is_connection_refused(err)
        {
            let refreshed = refresh().trim().to_string();
            if !refreshed.is_empty() && refreshed != addr {
                addr = refreshed;
                result = dial_tcp(&addr);
            }
        }
        let mut conn = result?;
        if let Some(auth) = current_relay_auth(&addr) {
            authenticate_relay_conn(&mut conn, &auth)?;
        }
        return Ok(Conn::Tcp(conn));
    }
    UnixStream::connect(addr)
        .map(Conn::Unix)
        .map_err(|e| format!("dial unix {addr}: connect: {}", io_error_text(&e)))
}

fn dial_tcp(addr: &str) -> Result<TcpStream, String> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<std::net::SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| format!("dial tcp: address {addr}: {}", io_error_text(&e)))?
        .collect();
    let mut last_err = format!("dial tcp: lookup {addr}: no such host");
    for socket_addr in addrs {
        match TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2)) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Err(e) => {
                last_err = if e.kind() == io::ErrorKind::TimedOut {
                    format!("dial tcp {socket_addr}: i/o timeout")
                } else {
                    format!("dial tcp {socket_addr}: connect: {}", io_error_text(&e))
                };
            }
        }
    }
    Err(last_err)
}

fn is_connection_refused(err: &str) -> bool {
    err.contains("connection refused")
}

fn authenticate_relay_conn(conn: &mut TcpStream, auth: &RelayAuthState) -> Result<(), String> {
    #[derive(Deserialize, Default)]
    struct Challenge {
        #[serde(default)]
        protocol: String,
        #[serde(default)]
        version: i64,
        #[serde(default)]
        relay_id: String,
        #[serde(default)]
        nonce: String,
    }
    #[derive(Deserialize, Default)]
    struct AuthResult {
        #[serde(default)]
        ok: bool,
    }
    let _ = conn.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = conn.set_write_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new(conn.try_clone().map_err(|e| io_error_text(&e))?);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|e| format!("failed to read relay auth challenge: {}", io_error_text(&e)))
        .and_then(|n| {
            if n == 0 {
                Err("failed to read relay auth challenge: EOF".to_string())
            } else {
                Ok(())
            }
        })?;
    let challenge: Challenge =
        serde_json::from_str(&line).map_err(|_| "invalid relay auth challenge".to_string())?;
    if challenge.protocol != "cmux-relay-auth"
        || challenge.version != 1
        || challenge.relay_id != auth.relay_id
        || challenge.nonce.is_empty()
    {
        return Err("relay auth challenge mismatch".to_string());
    }
    let token =
        hex::decode(&auth.relay_token).map_err(|_| "invalid relay auth token".to_string())?;
    let mac = compute_relay_mac(&token, &auth.relay_id, &challenge.nonce, challenge.version);
    let mut payload = Params::new();
    payload.insert("relay_id".to_string(), Value::from(auth.relay_id.as_str()));
    payload.insert("mac".to_string(), Value::from(hex::encode(mac)));
    let mut data = go_json(&Value::Object(payload)).into_bytes();
    data.push(b'\n');
    conn.write_all(&data)
        .map_err(|e| format!("failed to send relay auth response: {}", io_error_text(&e)))?;
    line.clear();
    reader
        .read_line(&mut line)
        .map_err(|e| format!("failed to read relay auth result: {}", io_error_text(&e)))
        .and_then(|n| {
            if n == 0 { Err("failed to read relay auth result: EOF".to_string()) } else { Ok(()) }
        })?;
    let result: AuthResult =
        serde_json::from_str(&line).map_err(|_| "invalid relay auth result".to_string())?;
    if !result.ok {
        return Err("relay auth rejected".to_string());
    }
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    Ok(())
}

#[must_use]
pub fn compute_relay_mac(token: &[u8], relay_id: &str, nonce: &str, version: i64) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(token).expect("HMAC accepts any key length");
    mac.update(format!("relay_id={relay_id}\nnonce={nonce}\nversion={version}").as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Send one JSON-RPC request and return the result JSON.
pub fn socket_round_trip_v2(
    socket_path: &str,
    method: &str,
    params: Option<Params>,
    refresh_addr: RefreshAddr,
) -> Result<String, String> {
    let mut conn = dial_socket(socket_path, refresh_addr)
        .map_err(|e| format!("failed to connect to {socket_path}: {e}"))?;
    let id = random_hex(8);
    let mut req = Params::new();
    req.insert("id".to_string(), Value::from(id));
    req.insert("method".to_string(), Value::from(method));
    req.insert("params".to_string(), Value::Object(params.unwrap_or_default()));
    let mut payload = go_json(&Value::Object(req)).into_bytes();
    payload.push(b'\n');
    conn.write_all(&payload)
        .map_err(|e| format!("failed to send request: {}", io_error_text(&e)))?;
    let _ = conn.set_read_timeout(Some(Duration::from_secs(15)));
    let _ = conn.set_write_timeout(None);
    let mut reader = BufReader::new(conn);
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => return Err("failed to read response: EOF".to_string()),
        Ok(_) if !line.ends_with('\n') => return Err("failed to read response: EOF".to_string()),
        Ok(_) => {}
        Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
            return Err("failed to read response: i/o timeout".to_string());
        }
        Err(e) => return Err(format!("failed to read response: {}", io_error_text(&e))),
    }
    let Ok(Value::Object(resp)) = serde_json::from_str::<Value>(&line) else {
        return Ok(line.trim_end_matches('\n').to_string());
    };
    if resp.get("ok").and_then(Value::as_bool) != Some(true) {
        if let Some(Value::Object(err)) = resp.get("error") {
            let code = err.get("code").and_then(Value::as_str).unwrap_or("");
            let msg = err.get("message").and_then(Value::as_str).unwrap_or("");
            return Err(format!("server error [{code}]: {msg}"));
        }
        return Err("server returned error response".to_string());
    }
    if let Some(result) = resp.get("result") {
        return Ok(go_json(result));
    }
    Ok("{}".to_string())
}

pub fn cli_usage(w: &mut dyn Write) {
    const LINES: &[&str] = &[
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
    for line in LINES {
        let _ = writeln!(w, "{line}");
    }
}

/// Connection context shared by the tmux shim and agent launchers.
pub struct RpcContext {
    pub socket_path: String,
    pub refresh_addr: RefreshAddr,
}

impl RpcContext {
    /// Make a JSON-RPC call and return the parsed result object (empty when
    /// the result is not an object, e.g. a bare string or null).
    pub fn call(&self, method: &str, params: Option<Params>) -> Result<Params, String> {
        let resp = socket_round_trip_v2(&self.socket_path, method, params, self.refresh_addr)?;
        match serde_json::from_str::<Value>(&resp) {
            Ok(Value::Object(map)) => Ok(map),
            _ => Ok(Params::new()),
        }
    }
}

//! `cmux __tmux-compat <args...>`: translates the tmux command subset that agent
//! shims rely on into cmux JSON-RPC calls over the relay socket. Mirrors
//! `tmux_compat.go`.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::cli::{CliIo, RefreshAddr, RpcContext};
use crate::util::{abs_path, clean_path, fnv1a64, home_dir, is_uuidish, json_number, path_join};

pub fn run_tmux_compat(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let (command, cmd_args) = match split_tmux_cmd(args) {
        Ok(split) => split,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux __tmux-compat: {err}");
            return 1;
        }
    };
    let rc = RpcContext {
        socket_path: socket_path.to_string(),
        refresh_addr,
    };
    if let Err(err) = dispatch_tmux_command(&rc, &command, &cmd_args, io.stdout) {
        let _ = writeln!(io.stderr, "cmux __tmux-compat: {err}");
        return 1;
    }
    0
}

// --- Tmux argument parsing ---

#[derive(Clone, Debug, Default)]
pub struct TmuxParsed {
    pub flags: HashMap<String, bool>,
    pub options: HashMap<String, Vec<String>>,
    pub positional: Vec<String>,
}

impl TmuxParsed {
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.get(flag).copied().unwrap_or(false)
    }

    pub fn value(&self, flag: &str) -> String {
        self.options
            .get(flag)
            .and_then(|vals| vals.last())
            .cloned()
            .unwrap_or_default()
    }
}

pub fn split_tmux_cmd(args: &[String]) -> Result<(String, Vec<String>), String> {
    let global_value_flags = ["-L", "-S", "-f"];
    let global_bool_flags = ["-V", "-v"];
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        if !arg.starts_with('-') || arg == "-" {
            return Ok((arg.to_lowercase(), args[i + 1..].to_vec()));
        }
        if arg == "--" {
            break;
        }
        if global_bool_flags.contains(&arg) {
            return Ok((arg.to_string(), Vec::new()));
        }
        if global_value_flags.contains(&arg) {
            i += 1;
        }
        i += 1;
    }
    Err("tmux shim requires a command".to_string())
}

pub fn parse_tmux_args(args: &[String], value_flags: &[&str], bool_flags: &[&str]) -> TmuxParsed {
    let mut parsed = TmuxParsed::default();
    let mut past_terminator = false;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        if past_terminator {
            parsed.positional.push(arg.to_string());
            i += 1;
            continue;
        }
        if arg == "--" {
            past_terminator = true;
            i += 1;
            continue;
        }
        if !arg.starts_with('-') || arg == "-" || arg.starts_with("--") {
            parsed.positional.push(arg.to_string());
            i += 1;
            continue;
        }
        // Cluster parsing: -dPh etc.
        let cluster: Vec<char> = arg[1..].chars().collect();
        let mut cursor = 0;
        let mut recognized = false;
        while cursor < cluster.len() {
            let flag = format!("-{}", cluster[cursor]);
            if bool_flags.contains(&flag.as_str()) {
                parsed.flags.insert(flag, true);
                cursor += 1;
                recognized = true;
                continue;
            }
            if value_flags.contains(&flag.as_str()) {
                let remainder: String = cluster[cursor + 1..].iter().collect();
                let value = if !remainder.is_empty() {
                    remainder
                } else if i + 1 < args.len() {
                    i += 1;
                    args[i].clone()
                } else {
                    String::new()
                };
                parsed.options.entry(flag).or_default().push(value);
                recognized = true;
                cursor = cluster.len();
                continue;
            }
            recognized = false;
            break;
        }
        if !recognized {
            parsed.positional.push(arg.to_string());
        }
        i += 1;
    }
    parsed
}

// --- Format string rendering ---

/// Remove any remaining `#{...}` variables (the Go code used the regexp
/// `#\{[^}]+\}`).
fn strip_unresolved_format_vars(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(rel) = input[i + 2..].find('}') {
                if rel > 0 {
                    i += 2 + rel + 1;
                    continue;
                }
            }
        }
        let ch = input[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

pub fn tmux_render_format(
    format: &str,
    context: &HashMap<String, String>,
    fallback: &str,
) -> String {
    if format.is_empty() {
        return fallback.to_string();
    }
    let mut rendered = format.to_string();
    for (key, value) in context {
        rendered = rendered.replace(&format!("#{{{key}}}"), value);
    }
    let rendered = strip_unresolved_format_vars(&rendered);
    let rendered = rendered.trim().to_string();
    if rendered.is_empty() {
        return fallback.to_string();
    }
    rendered
}

// --- Value helpers (Go's `any` conversions) ---

pub fn float_from_any(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

pub fn int_from_any_go(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Number(n)) => {
            if let Some(v) = n.as_i64() {
                v
            } else if let Some(v) = n.as_f64() {
                v as i64
            } else {
                -1
            }
        }
        _ => -1,
    }
}

pub fn bool_from_any_go(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) => match s.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        },
        Some(Value::Number(n)) => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            if f == 0.0 {
                Some(false)
            } else if f == 1.0 {
                Some(true)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn string_from_any_go(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.trim().to_string(),
        _ => String::new(),
    }
}

fn get_str<'a>(map: &'a Map<String, Value>, key: &str) -> &'a str {
    map.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

fn get_array(map: &Map<String, Value>, key: &str) -> Vec<Map<String, Value>> {
    match map.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_object().cloned())
            .collect(),
        _ => Vec::new(),
    }
}

fn params(pairs: &[(&str, Value)]) -> Option<Map<String, Value>> {
    let mut map = Map::new();
    for (key, value) in pairs {
        map.insert((*key).to_string(), value.clone());
    }
    Some(map)
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

// --- Format context building ---

pub fn tmux_format_context(
    rc: &RpcContext,
    workspace_id: &str,
    pane_id: &str,
    surface_id: &str,
) -> Result<HashMap<String, String>, String> {
    let canonical_ws_id = tmux_resolve_workspace_id(rc, workspace_id)?;

    let mut ctx: HashMap<String, String> = HashMap::from([
        ("session_name".to_string(), "cmux".to_string()),
        (
            "session_id".to_string(),
            format!("${}", tmux_stable_numeric_id(&canonical_ws_id)),
        ),
        ("session_attached".to_string(), "1".to_string()),
        (
            "window_id".to_string(),
            format!("@{}", tmux_stable_numeric_id(&canonical_ws_id)),
        ),
        ("window_uuid".to_string(), canonical_ws_id.clone()),
        ("window_active".to_string(), "0".to_string()),
        ("window_flags".to_string(), String::new()),
        ("window_width".to_string(), "80".to_string()),
        ("window_height".to_string(), "24".to_string()),
        ("pane_active".to_string(), "1".to_string()),
        ("pane_width".to_string(), "80".to_string()),
        ("pane_height".to_string(), "24".to_string()),
        (
            "pane_current_path".to_string(),
            tmux_fallback_current_path(),
        ),
    ]);
    let active_workspace_id = tmux_active_workspace_id(rc);
    let active_by_caller = active_workspace_id == canonical_ws_id;
    if active_by_caller {
        tmux_set_window_active(&mut ctx, true);
    }

    if let Ok(workspaces) = tmux_workspace_items(rc) {
        for ws in &workspaces {
            let ws_id = get_str(ws, "id");
            let ws_ref = get_str(ws, "ref");
            if ws_id == canonical_ws_id || ws_ref == workspace_id {
                if let Some(active) = bool_from_any_go(ws.get("active")) {
                    if !active_by_caller {
                        tmux_set_window_active(&mut ctx, active);
                    }
                } else if let Some(focused) = bool_from_any_go(ws.get("focused")) {
                    if !active_by_caller {
                        tmux_set_window_active(&mut ctx, focused);
                    }
                } else if let Some(selected) = bool_from_any_go(ws.get("selected")) {
                    if !active_by_caller {
                        tmux_set_window_active(&mut ctx, selected);
                    }
                }
                let idx = int_from_any_go(ws.get("index"));
                if idx >= 0 {
                    ctx.insert("window_index".to_string(), idx.to_string());
                }
                let title = get_str(ws, "title").trim();
                if !title.is_empty() {
                    ctx.insert("window_name".to_string(), title.to_string());
                }
                let path = tmux_path_from_object(ws);
                if !path.is_empty() {
                    ctx.insert("pane_current_path".to_string(), path);
                }
                let pane_count = int_from_any_go(ws.get("pane_count"));
                if pane_count >= 0 {
                    ctx.insert("window_panes".to_string(), pane_count.to_string());
                }
                break;
            }
        }
    }

    let current_payload = match rc.call(
        "surface.current",
        params(&[("workspace_id", s(&canonical_ws_id))]),
    ) {
        Ok(payload) => payload,
        Err(_) => return Ok(ctx),
    };

    let mut resolved_pane_id = String::new();
    if !pane_id.is_empty() {
        resolved_pane_id = tmux_canonical_pane_id(rc, pane_id, &canonical_ws_id)
            .unwrap_or_else(|_| pane_id.to_string());
    }
    if resolved_pane_id.is_empty() {
        if let Some(pid) = current_payload.get("pane_id").and_then(|v| v.as_str()) {
            resolved_pane_id = pid.to_string();
        } else if let Some(pref) = current_payload.get("pane_ref").and_then(|v| v.as_str()) {
            resolved_pane_id = tmux_canonical_pane_id(rc, pref, &canonical_ws_id)
                .unwrap_or_else(|_| pref.to_string());
        }
    }

    let mut resolved_surface_id = String::new();
    if !surface_id.is_empty() {
        resolved_surface_id = tmux_canonical_surface_id(rc, surface_id, &canonical_ws_id)
            .unwrap_or_else(|_| surface_id.to_string());
    }
    if resolved_surface_id.is_empty() && !resolved_pane_id.is_empty() {
        if let Ok(sid) = tmux_selected_surface_id(rc, &canonical_ws_id, &resolved_pane_id) {
            resolved_surface_id = sid;
        }
    }
    if resolved_surface_id.is_empty() {
        if let Some(sid) = current_payload.get("surface_id").and_then(|v| v.as_str()) {
            resolved_surface_id = sid.to_string();
        }
    }

    if !resolved_pane_id.is_empty() {
        ctx.insert(
            "pane_id".to_string(),
            format!("%{}", tmux_stable_numeric_id(&resolved_pane_id)),
        );
        ctx.insert("pane_uuid".to_string(), resolved_pane_id.clone());
        if let Ok(pane_payload) = rc.call(
            "pane.list",
            params(&[("workspace_id", s(&canonical_ws_id))]),
        ) {
            for pane in get_array(&pane_payload, "panes") {
                if get_str(&pane, "id") == resolved_pane_id {
                    let idx = int_from_any_go(pane.get("index"));
                    if idx >= 0 {
                        ctx.insert("pane_index".to_string(), idx.to_string());
                    }
                    if let Some(focused) = bool_from_any_go(pane.get("focused")) {
                        ctx.insert(
                            "pane_active".to_string(),
                            if focused { "1" } else { "0" }.to_string(),
                        );
                    }
                    break;
                }
            }
        }
    }

    if !resolved_surface_id.is_empty() {
        ctx.insert("surface_id".to_string(), resolved_surface_id.clone());
        if let Ok(surface_payload) = rc.call(
            "surface.list",
            params(&[("workspace_id", s(&canonical_ws_id))]),
        ) {
            for surface in get_array(&surface_payload, "surfaces") {
                if get_str(&surface, "id") == resolved_surface_id {
                    let title = get_str(&surface, "title").trim().to_string();
                    if !title.is_empty() {
                        ctx.insert("pane_title".to_string(), title.clone());
                        ctx.entry("window_name".to_string()).or_insert(title);
                    }
                    let path = tmux_path_from_object(&surface);
                    if !path.is_empty() {
                        ctx.insert("pane_current_path".to_string(), path);
                    }
                    break;
                }
            }
        }
    }

    Ok(ctx)
}

pub fn tmux_enrich_context_with_geometry(
    ctx: &mut HashMap<String, String>,
    pane: &Map<String, Value>,
    container_frame: Option<&Map<String, Value>>,
) {
    let is_focused = bool_from_any_go(pane.get("focused")).unwrap_or(false);
    ctx.insert(
        "pane_active".to_string(),
        if is_focused { "1" } else { "0" }.to_string(),
    );

    let columns = int_from_any_go(pane.get("columns"));
    let rows = int_from_any_go(pane.get("rows"));
    if columns < 0 || rows < 0 {
        return;
    }
    ctx.insert("pane_width".to_string(), columns.to_string());
    ctx.insert("pane_height".to_string(), rows.to_string());

    let cell_w = int_from_any_go(pane.get("cell_width_px"));
    let cell_h = int_from_any_go(pane.get("cell_height_px"));
    if cell_w <= 0 || cell_h <= 0 {
        return;
    }

    if let Some(Value::Object(frame)) = pane.get("pixel_frame") {
        let px = float_from_any(frame.get("x"));
        let py = float_from_any(frame.get("y"));
        ctx.insert("pane_left".to_string(), ((px as i64) / cell_w).to_string());
        ctx.insert("pane_top".to_string(), ((py as i64) / cell_h).to_string());
    }

    if let Some(frame) = container_frame {
        let cw = float_from_any(frame.get("width"));
        let ch = float_from_any(frame.get("height"));
        let ww = ((cw as i64) / cell_w).max(1);
        let wh = ((ch as i64) / cell_h).max(1);
        ctx.insert("window_width".to_string(), ww.to_string());
        ctx.insert("window_height".to_string(), wh.to_string());
    }
}

pub fn tmux_set_window_active(ctx: &mut HashMap<String, String>, active: bool) {
    if active {
        ctx.insert("window_active".to_string(), "1".to_string());
        ctx.insert("window_flags".to_string(), "*".to_string());
    } else {
        ctx.insert("window_active".to_string(), "0".to_string());
        ctx.insert("window_flags".to_string(), String::new());
    }
}

pub fn tmux_stable_numeric_id(raw: &str) -> String {
    let mut raw = raw.trim();
    if raw.is_empty() {
        raw = "cmux";
    }
    let mut value = fnv1a64(raw.as_bytes()) & 0x7fff_ffff_ffff_ffff;
    if value == 0 {
        value = 1;
    }
    value.to_string()
}

pub fn tmux_trim_id_sigil(raw: &str) -> String {
    let mut raw = raw.trim();
    while let Some(first) = raw.chars().next() {
        match first {
            '$' | '@' | '%' => raw = raw[1..].trim(),
            _ => return raw.to_string(),
        }
    }
    raw.to_string()
}

pub fn tmux_selector_token(raw: &str) -> (String, bool) {
    let trimmed = raw.trim();
    let token = tmux_trim_id_sigil(trimmed);
    let sigiled = token != trimmed;
    (token, sigiled)
}

pub fn tmux_numeric_id_matches(handle: &str, candidates: &[&str]) -> bool {
    let token = tmux_trim_id_sigil(handle);
    if token.is_empty() {
        return false;
    }
    candidates
        .iter()
        .any(|candidate| !candidate.trim().is_empty() && token == tmux_stable_numeric_id(candidate))
}

pub fn tmux_index_matches(handle: &str, index: i64) -> bool {
    if index < 0 {
        return false;
    }
    tmux_trim_id_sigil(handle) == index.to_string()
}

pub fn tmux_normalize_path(raw: &str) -> String {
    let mut raw = raw.trim().to_string();
    if raw.is_empty() {
        return String::new();
    }
    if raw.starts_with("~/") || raw == "~" {
        if let Some(home) = home_dir() {
            raw = if raw == "~" {
                home
            } else {
                path_join(&home, &raw[2..])
            };
        }
    }
    if !raw.starts_with('/') {
        if let Some(abs) = abs_path(&raw) {
            raw = abs;
        }
    }
    if raw.starts_with('/') {
        return clean_path(&raw);
    }
    String::new()
}

pub fn tmux_first_path(values: &[&str]) -> String {
    for value in values {
        let path = tmux_normalize_path(value);
        if !path.is_empty() {
            return path;
        }
    }
    String::new()
}

pub fn tmux_path_from_object(item: &Map<String, Value>) -> String {
    let candidates = [
        string_from_any_go(item.get("pane_current_path")),
        string_from_any_go(item.get("current_directory")),
        string_from_any_go(item.get("requested_working_directory")),
        string_from_any_go(item.get("working_directory")),
        string_from_any_go(item.get("cwd")),
    ];
    let refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
    let path = tmux_first_path(&refs);
    if !path.is_empty() {
        return path;
    }
    if let Some(Value::Object(binding)) = item.get("resume_binding") {
        return tmux_first_path(&[string_from_any_go(binding.get("cwd")).as_str()]);
    }
    String::new()
}

pub fn tmux_fallback_current_path() -> String {
    let pwd = crate::util::env_var_or_default("PWD");
    let path = tmux_normalize_path(&pwd);
    if !path.is_empty() {
        return path;
    }
    if let Ok(cwd) = std::env::current_dir() {
        let path = tmux_normalize_path(&cwd.to_string_lossy());
        if !path.is_empty() {
            return path;
        }
    }
    if let Some(home) = home_dir() {
        let path = tmux_normalize_path(&home);
        if !path.is_empty() {
            return path;
        }
    }
    "/".to_string()
}

// --- Target resolution ---

pub fn tmux_caller_workspace_handle() -> String {
    crate::util::env_var("CMUX_WORKSPACE_ID")
        .ok_or(std::env::VarError::NotPresent)
        .unwrap_or_default()
        .trim()
        .to_string()
}

pub fn tmux_caller_surface_handle() -> String {
    crate::util::env_var("CMUX_SURFACE_ID")
        .ok_or(std::env::VarError::NotPresent)
        .unwrap_or_default()
        .trim()
        .to_string()
}

pub fn tmux_resolved_caller_workspace_id(rc: &RpcContext) -> String {
    let caller = tmux_caller_workspace_handle();
    if caller.is_empty() {
        return String::new();
    }
    tmux_resolve_workspace_id(rc, &caller).unwrap_or_default()
}

pub fn tmux_active_workspace_id(rc: &RpcContext) -> String {
    let caller_ws = tmux_resolved_caller_workspace_id(rc);
    if !caller_ws.is_empty() {
        return caller_ws;
    }
    let payload = match rc.call("workspace.current", None) {
        Ok(payload) => payload,
        Err(_) => return String::new(),
    };
    let ws_id = get_str(&payload, "workspace_id");
    if !ws_id.is_empty() {
        return ws_id.to_string();
    }
    let ws_ref = get_str(&payload, "workspace_ref");
    if !ws_ref.is_empty() {
        if let Ok(ws_id) = tmux_resolve_workspace_id(rc, ws_ref) {
            return ws_id;
        }
    }
    String::new()
}

pub fn tmux_caller_pane_handle() -> String {
    for key in ["TMUX_PANE", "CMUX_PANE_ID"] {
        let v = crate::util::env_var_or_default(key).trim().to_string();
        if !v.is_empty() {
            return v.strip_prefix('%').unwrap_or(&v).to_string();
        }
    }
    String::new()
}

pub fn tmux_workspace_items(rc: &RpcContext) -> Result<Vec<Map<String, Value>>, String> {
    let payload = rc.call("workspace.list", None)?;
    Ok(get_array(&payload, "workspaces"))
}

pub fn tmux_resolve_workspace_id(rc: &RpcContext, raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "current" {
        let caller = tmux_caller_workspace_handle();
        if !caller.is_empty() {
            if is_uuidish(&caller) {
                return Ok(caller);
            }
            return tmux_resolve_workspace_id(rc, &caller);
        }
        let payload = rc
            .call("workspace.current", None)
            .map_err(|err| format!("no workspace selected: {err}"))?;
        if let Some(ws_id) = payload.get("workspace_id").and_then(|v| v.as_str()) {
            return Ok(ws_id.to_string());
        }
        return Err("no workspace selected".to_string());
    }

    if is_uuidish(raw) {
        return Ok(raw.to_string());
    }

    let (token, sigiled) = tmux_selector_token(raw);
    if is_uuidish(&token) {
        return Ok(token);
    }

    // Try to resolve as ref, tmux numeric id, or workspace index.
    let items = tmux_workspace_items(rc)?;
    for item in &items {
        let id = get_str(item, "id");
        let ws_ref = get_str(item, "ref");
        if !sigiled && ws_ref == raw && !id.is_empty() {
            return Ok(id.to_string());
        }
        if id == raw || id == token {
            return Ok(id.to_string());
        }
        if !id.is_empty()
            && (tmux_numeric_id_matches(&token, &[id])
                || tmux_numeric_id_matches(&token, &[string_from_any_go(item.get("ref")).as_str()]))
        {
            return Ok(id.to_string());
        }
        if !sigiled
            && tmux_index_matches(&token, int_from_any_go(item.get("index")))
            && !id.is_empty()
        {
            return Ok(id.to_string());
        }
    }

    if !sigiled {
        let needle = token.trim();
        for item in &items {
            let title = get_str(item, "title");
            if title.trim() == needle {
                let id = get_str(item, "id");
                if !id.is_empty() {
                    return Ok(id.to_string());
                }
            }
        }
    }

    Err(format!("workspace not found: {raw}"))
}

pub fn tmux_resolve_workspace_target(rc: &RpcContext, raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        let caller = tmux_caller_workspace_handle();
        if !caller.is_empty() {
            return tmux_resolve_workspace_id(rc, &caller);
        }
        return tmux_resolve_workspace_id(rc, "");
    }

    if raw == "!" || raw == "^" || raw == "-" {
        let payload = rc
            .call("workspace.last", None)
            .map_err(|err| format!("previous workspace not found: {err}"))?;
        if let Some(ws_id) = payload.get("workspace_id").and_then(|v| v.as_str()) {
            return Ok(ws_id.to_string());
        }
        return Err("previous workspace not found".to_string());
    }

    // Strip session:window.pane format
    let mut token = raw.to_string();
    if let Some(dot) = token.rfind('.') {
        token.truncate(dot);
    }
    if let Some(colon) = token.rfind(':') {
        let suffix = token[colon + 1..].to_string();
        if !suffix.is_empty() {
            token = suffix;
        } else {
            token.truncate(colon);
        }
    }
    tmux_resolve_workspace_id(rc, &token)
}

pub fn tmux_pane_selector(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    if raw.starts_with('%') || raw.starts_with("pane:") {
        return raw.to_string();
    }
    if let Some(dot) = raw.rfind('.') {
        return raw[dot + 1..].to_string();
    }
    String::new()
}

pub fn tmux_window_selector(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    if raw.starts_with('%') || raw.starts_with("pane:") {
        return String::new();
    }
    if let Some(dot) = raw.rfind('.') {
        return raw[..dot].to_string();
    }
    raw.to_string()
}

pub fn tmux_canonical_pane_id(
    rc: &RpcContext,
    handle: &str,
    workspace_id: &str,
) -> Result<String, String> {
    let (handle, sigiled) = tmux_selector_token(handle);
    if is_uuidish(&handle) {
        return Ok(handle);
    }
    let payload = rc.call("pane.list", params(&[("workspace_id", s(workspace_id))]))?;
    let panes = get_array(&payload, "panes");
    for pane in &panes {
        let id = get_str(pane, "id");
        let pane_ref = get_str(pane, "ref");
        if !sigiled && pane_ref == handle && !id.is_empty() {
            return Ok(id.to_string());
        }
        if id == handle {
            return Ok(id.to_string());
        }
        if (tmux_numeric_id_matches(&handle, &[id])
            || tmux_numeric_id_matches(&handle, &[pane_ref]))
            && !id.is_empty()
        {
            return Ok(id.to_string());
        }
    }
    if !sigiled {
        for pane in &panes {
            let id = get_str(pane, "id");
            if tmux_index_matches(&handle, int_from_any_go(pane.get("index"))) && !id.is_empty() {
                return Ok(id.to_string());
            }
        }
    }
    Err(format!("pane not found: {handle}"))
}

pub fn tmux_canonical_surface_id(
    rc: &RpcContext,
    handle: &str,
    workspace_id: &str,
) -> Result<String, String> {
    let (handle, sigiled) = tmux_selector_token(handle);
    let payload = rc.call("surface.list", params(&[("workspace_id", s(workspace_id))]))?;
    let surfaces = get_array(&payload, "surfaces");
    for surface in &surfaces {
        let id = get_str(surface, "id");
        let surface_ref = get_str(surface, "ref");
        if !sigiled && surface_ref == handle && !id.is_empty() {
            return Ok(id.to_string());
        }
        if id == handle {
            return Ok(id.to_string());
        }
        if (tmux_numeric_id_matches(&handle, &[id])
            || tmux_numeric_id_matches(&handle, &[surface_ref]))
            && !id.is_empty()
        {
            return Ok(id.to_string());
        }
    }
    if !sigiled {
        for surface in &surfaces {
            let id = get_str(surface, "id");
            if tmux_index_matches(&handle, int_from_any_go(surface.get("index"))) && !id.is_empty()
            {
                return Ok(id.to_string());
            }
        }
    }
    Err(format!("surface not found: {handle}"))
}

pub fn tmux_focused_pane_id(rc: &RpcContext, workspace_id: &str) -> Result<String, String> {
    let payload = rc.call(
        "surface.current",
        params(&[("workspace_id", s(workspace_id))]),
    )?;
    if let Some(pid) = payload.get("pane_id").and_then(|v| v.as_str()) {
        return Ok(pid.to_string());
    }
    if let Some(pref) = payload.get("pane_ref").and_then(|v| v.as_str()) {
        return tmux_canonical_pane_id(rc, pref, workspace_id);
    }
    Err("pane not found".to_string())
}

pub fn tmux_workspace_id_for_pane_handle(rc: &RpcContext, handle: &str) -> Result<String, String> {
    let (handle, sigiled) = tmux_selector_token(handle);
    let workspaces = tmux_workspace_items(rc)?;
    for ws in &workspaces {
        let ws_id = get_str(ws, "id");
        if ws_id.is_empty() {
            continue;
        }
        let payload = match rc.call("pane.list", params(&[("workspace_id", s(ws_id))])) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        for pane in get_array(&payload, "panes") {
            let pid = get_str(&pane, "id");
            let pref = get_str(&pane, "ref");
            if pid == handle {
                return Ok(ws_id.to_string());
            }
            if !sigiled && pref == handle {
                return Ok(ws_id.to_string());
            }
            if tmux_numeric_id_matches(&handle, &[pid]) || tmux_numeric_id_matches(&handle, &[pref])
            {
                return Ok(ws_id.to_string());
            }
            if !sigiled && tmux_index_matches(&handle, int_from_any_go(pane.get("index"))) {
                return Ok(ws_id.to_string());
            }
        }
    }
    Err("pane not found in any workspace".to_string())
}

pub fn tmux_resolve_pane_target(rc: &RpcContext, raw: &str) -> Result<(String, String), String> {
    let raw = raw.trim();
    let pane_selector = tmux_pane_selector(raw);
    let window_selector = tmux_window_selector(raw);

    let mut workspace_id = String::new();
    if !window_selector.is_empty() {
        workspace_id = tmux_resolve_workspace_target(rc, &window_selector)?;
    } else if !pane_selector.is_empty() {
        let caller_ws = tmux_resolved_caller_workspace_id(rc);
        if !caller_ws.is_empty() && tmux_canonical_pane_id(rc, &pane_selector, &caller_ws).is_ok() {
            workspace_id = caller_ws;
        }
        if workspace_id.is_empty() {
            match tmux_workspace_id_for_pane_handle(rc, &pane_selector) {
                Ok(ws) => workspace_id = ws,
                Err(_) => workspace_id = tmux_resolve_workspace_target(rc, "")?,
            }
        }
    } else {
        workspace_id = tmux_resolve_workspace_target(rc, "")?;
    }

    let mut pane_id = String::new();
    if !pane_selector.is_empty() {
        pane_id = tmux_canonical_pane_id(rc, &pane_selector, &workspace_id)?;
    } else if tmux_resolved_caller_workspace_id(rc) == workspace_id {
        let caller_pane = tmux_caller_pane_handle();
        if !caller_pane.is_empty() {
            if let Ok(pid) = tmux_canonical_pane_id(rc, &caller_pane, &workspace_id) {
                pane_id = pid;
            }
        }
    }

    if pane_id.is_empty() {
        pane_id = tmux_focused_pane_id(rc, &workspace_id)?;
    }
    Ok((workspace_id, pane_id))
}

pub fn tmux_selected_surface_id(
    rc: &RpcContext,
    workspace_id: &str,
    pane_id: &str,
) -> Result<String, String> {
    let payload = rc.call(
        "pane.surfaces",
        params(&[("workspace_id", s(workspace_id)), ("pane_id", s(pane_id))]),
    )?;
    let surfaces = get_array(&payload, "surfaces");
    for surface in &surfaces {
        if bool_from_any_go(surface.get("selected")).unwrap_or(false) {
            let id = get_str(surface, "id");
            if !id.is_empty() {
                return Ok(id.to_string());
            }
        }
    }
    if let Some(surface) = surfaces.first() {
        let id = get_str(surface, "id");
        if !id.is_empty() {
            return Ok(id.to_string());
        }
    }
    Err("pane has no surface".to_string())
}

pub fn tmux_resolve_surface_target(
    rc: &RpcContext,
    raw: &str,
) -> Result<(String, String, String), String> {
    let raw = raw.trim();

    if !tmux_pane_selector(raw).is_empty() {
        let (workspace_id, pane_id) = tmux_resolve_pane_target(rc, raw)?;
        // When target pane matches caller's pane, prefer caller's surface
        let caller_pane = tmux_caller_pane_handle();
        let caller_surface = tmux_caller_surface_handle();
        if !caller_pane.is_empty() && !caller_surface.is_empty() {
            let canonical_caller_pane =
                tmux_canonical_pane_id(rc, &caller_pane, &workspace_id).unwrap_or_default();
            if pane_id == caller_pane || pane_id == canonical_caller_pane {
                if let Ok(surface_id) =
                    tmux_canonical_surface_id(rc, &caller_surface, &workspace_id)
                {
                    return Ok((workspace_id, pane_id, surface_id));
                }
            }
        }
        let surface_id = tmux_selected_surface_id(rc, &workspace_id, &pane_id)?;
        return Ok((workspace_id, pane_id, surface_id));
    }

    let win_sel = tmux_window_selector(raw);
    let workspace_id = tmux_resolve_workspace_target(rc, &win_sel)?;

    // When no explicit target and caller workspace matches, use caller's surface
    if win_sel.is_empty() && tmux_resolved_caller_workspace_id(rc) == workspace_id {
        let caller_surface = tmux_caller_surface_handle();
        if !caller_surface.is_empty() {
            if let Ok(surface_id) = tmux_canonical_surface_id(rc, &caller_surface, &workspace_id) {
                return Ok((workspace_id, String::new(), surface_id));
            }
        }
    }

    // Fall back to focused surface
    if let Ok(payload) = rc.call(
        "surface.current",
        params(&[("workspace_id", s(&workspace_id))]),
    ) {
        if let Some(sid) = payload.get("surface_id").and_then(|v| v.as_str()) {
            return Ok((workspace_id, String::new(), sid.to_string()));
        }
    }

    // Last resort: first surface in the workspace
    if let Ok(surf_payload) = rc.call(
        "surface.list",
        params(&[("workspace_id", s(&workspace_id))]),
    ) {
        let surfs = get_array(&surf_payload, "surfaces");
        for surf in &surfs {
            if bool_from_any_go(surf.get("focused")).unwrap_or(false) {
                let id = get_str(surf, "id");
                if !id.is_empty() {
                    return Ok((workspace_id, String::new(), id.to_string()));
                }
            }
        }
        if let Some(surf) = surfs.first() {
            let id = get_str(surf, "id");
            if !id.is_empty() {
                return Ok((workspace_id, String::new(), id.to_string()));
            }
        }
    }

    Err("unable to resolve surface".to_string())
}

pub struct TmuxSplitAnchor {
    pub target_surface_id: String,
    pub caller_surface_id: String,
    pub direction: String,
}

pub fn tmux_anchored_split_target(rc: &RpcContext, workspace_id: &str) -> Option<TmuxSplitAnchor> {
    let mut store = load_tmux_compat_store();
    if let Some(mv_state) = store.main_vertical_layouts.get(workspace_id).cloned() {
        if !mv_state.last_column_surface_id.is_empty() {
            if let Ok(last_column_id) =
                tmux_canonical_surface_id(rc, &mv_state.last_column_surface_id, workspace_id)
            {
                return Some(TmuxSplitAnchor {
                    target_surface_id: last_column_id,
                    caller_surface_id: String::new(),
                    direction: "down".to_string(),
                });
            }
            // Right-column anchors can outlive the pane they pointed at. Drop
            // stale state and rebuild from the caller surface instead.
            let mut updated = mv_state;
            updated.last_column_surface_id = String::new();
            store
                .main_vertical_layouts
                .insert(workspace_id.to_string(), updated);
            store.last_split_surface.remove(workspace_id);
            let _ = save_tmux_compat_store(&store);
        }
    }

    let mut candidate_anchors = vec![tmux_caller_surface_handle()];
    if let Some(mv_state) = store.main_vertical_layouts.get(workspace_id) {
        if !mv_state.main_surface_id.is_empty() {
            candidate_anchors.push(mv_state.main_surface_id.clone());
        }
    }
    for candidate in candidate_anchors {
        if candidate.is_empty() {
            continue;
        }
        if let Ok(anchor_surface_id) = tmux_canonical_surface_id(rc, &candidate, workspace_id) {
            return Some(TmuxSplitAnchor {
                target_surface_id: anchor_surface_id.clone(),
                caller_surface_id: anchor_surface_id,
                direction: "right".to_string(),
            });
        }
    }

    if store.main_vertical_layouts.contains_key(workspace_id) {
        store.main_vertical_layouts.remove(workspace_id);
        store.last_split_surface.remove(workspace_id);
        let _ = save_tmux_compat_store(&store);
    }
    None
}

// --- TmuxCompatStore (local JSON state) ---

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MainVerticalState {
    #[serde(rename = "mainSurfaceId", default)]
    pub main_surface_id: String,
    #[serde(
        rename = "lastColumnSurfaceId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub last_column_surface_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TmuxCompatStore {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub buffers: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub hooks: std::collections::BTreeMap<String, String>,
    #[serde(
        rename = "mainVerticalLayouts",
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub main_vertical_layouts: std::collections::BTreeMap<String, MainVerticalState>,
    #[serde(
        rename = "lastSplitSurface",
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub last_split_surface: std::collections::BTreeMap<String, String>,
}

pub fn tmux_compat_store_url() -> String {
    let home = home_dir().unwrap_or_default();
    path_join(&home, ".cmuxterm/tmux-compat-store.json")
}

pub fn load_tmux_compat_store() -> TmuxCompatStore {
    match fs::read(tmux_compat_store_url()) {
        Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
        Err(_) => TmuxCompatStore::default(),
    }
}

pub fn save_tmux_compat_store(store: &TmuxCompatStore) -> Result<(), String> {
    let path = tmux_compat_store_url();
    fs::create_dir_all(crate::util::path_dir(&path)).map_err(|err| err.to_string())?;
    let data = serde_json::to_vec(store).map_err(|err| err.to_string())?;
    fs::write(&path, data).map_err(|err| err.to_string())
}

pub fn tmux_prune_compat_workspace_state(workspace_id: &str) -> Result<(), String> {
    let mut store = load_tmux_compat_store();
    let mut changed = false;
    if store.main_vertical_layouts.remove(workspace_id).is_some() {
        changed = true;
    }
    if store.last_split_surface.remove(workspace_id).is_some() {
        changed = true;
    }
    if changed {
        return save_tmux_compat_store(&store);
    }
    Ok(())
}

pub fn tmux_prune_compat_surface_state(workspace_id: &str, surface_id: &str) -> Result<(), String> {
    let mut store = load_tmux_compat_store();
    let mut changed = false;
    if store
        .last_split_surface
        .get(workspace_id)
        .map(|v| v == surface_id)
        .unwrap_or(false)
    {
        store.last_split_surface.remove(workspace_id);
        changed = true;
    }
    if let Some(layout) = store.main_vertical_layouts.get(workspace_id).cloned() {
        if layout.main_surface_id == surface_id {
            store.main_vertical_layouts.remove(workspace_id);
            store.last_split_surface.remove(workspace_id);
            changed = true;
        } else if layout.last_column_surface_id == surface_id {
            let mut updated = layout;
            updated.last_column_surface_id = String::new();
            store
                .main_vertical_layouts
                .insert(workspace_id.to_string(), updated);
            changed = true;
        }
    }
    if changed {
        return save_tmux_compat_store(&store);
    }
    Ok(())
}

// --- Special key translation ---

pub fn tmux_special_key_text(token: &str) -> &'static str {
    match token.to_lowercase().as_str() {
        "enter" | "c-m" | "kpenter" => "\r",
        "tab" | "c-i" => "\t",
        "space" => " ",
        "bspace" | "backspace" => "\x7f",
        "escape" | "esc" | "c-[" => "\x1b",
        "c-c" => "\x03",
        "c-d" => "\x04",
        "c-z" => "\x1a",
        "c-l" => "\x0c",
        _ => "",
    }
}

pub fn tmux_send_keys_text(tokens: &[String], literal: bool) -> String {
    if literal {
        return tokens.join(" ");
    }
    let mut result = String::new();
    let mut pending_space = false;
    for token in tokens {
        let special = tmux_special_key_text(token);
        if !special.is_empty() {
            result.push_str(special);
            pending_space = false;
            continue;
        }
        if pending_space {
            result.push(' ');
        }
        result.push_str(token);
        pending_space = true;
    }
    result
}

pub fn tmux_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub fn tmux_shell_command_text(positional: &[String], cwd: &str) -> String {
    let cwd = cwd.trim();
    let cmd = positional.join(" ").trim().to_string();
    if cwd.is_empty() && cmd.is_empty() {
        return String::new();
    }
    let mut pieces = Vec::new();
    if !cwd.is_empty() {
        pieces.push(format!("cd -- {}", tmux_shell_quote(cwd)));
    }
    if !cmd.is_empty() {
        pieces.push(cmd);
    }
    format!("{}\r", pieces.join(" && "))
}

// --- Wait-for (filesystem-based signaling) ---

pub fn tmux_wait_for_signal_path(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("/tmp/cmux-wait-for-{sanitized}.sig")
}

// --- Main dispatch ---

pub fn dispatch_tmux_command(
    rc: &RpcContext,
    command: &str,
    args: &[String],
    out: &mut dyn Write,
) -> Result<(), String> {
    match command {
        "-v" | "-V" => {
            let _ = writeln!(out, "tmux 3.4");
            Ok(())
        }
        "new-session" | "new" => tmux_new_session(rc, args, out),
        "new-window" | "neww" => tmux_new_window(rc, args, out),
        "split-window" | "splitw" => tmux_split_window(rc, args, out),
        "select-window" | "selectw" => tmux_select_window(rc, args),
        "select-pane" | "selectp" => tmux_select_pane(rc, args),
        "kill-window" | "killw" => tmux_kill_window(rc, args),
        "kill-pane" | "killp" => tmux_kill_pane(rc, args),
        "send-keys" | "send" => tmux_send_keys(rc, args),
        "capture-pane" | "capturep" => tmux_capture_pane(rc, args, out),
        "display-message" | "display" | "displayp" => tmux_display_message(rc, args, out),
        "list-windows" | "lsw" => tmux_list_windows(rc, args, out),
        "list-panes" | "lsp" => tmux_list_panes(rc, args, out),
        "rename-window" | "renamew" => tmux_rename_window(rc, args),
        "resize-pane" | "resizep" => tmux_resize_pane(rc, args),
        "wait-for" => tmux_wait_for(args, out),
        "last-pane" => tmux_last_pane(rc, args),
        "has-session" | "has" => tmux_has_session(rc, args),
        "select-layout" => tmux_select_layout(rc, args),
        "show-buffer" | "showb" => tmux_show_buffer(args, out),
        "save-buffer" | "saveb" => tmux_save_buffer(args, out),
        // No-ops
        "set-option" | "set" | "set-window-option" | "setw" | "source-file" | "refresh-client"
        | "attach-session" | "detach-client" | "last-window" | "next-window"
        | "previous-window" | "set-hook" | "set-buffer" | "list-buffers" => Ok(()),
        other => Err(format!("unsupported tmux command: {other}")),
    }
}

// --- Command implementations ---

fn print_created_workspace(rc: &RpcContext, ws_id: &str, format: &str, out: &mut dyn Write) {
    match tmux_format_context(rc, ws_id, "", "") {
        Ok(ctx) => {
            let _ = writeln!(
                out,
                "{}",
                tmux_render_format(format, &ctx, &format!("@{ws_id}"))
            );
        }
        Err(_) => {
            let _ = writeln!(out, "@{ws_id}");
        }
    }
}

fn tmux_new_session(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-c", "-F", "-n", "-s"], &["-A", "-d", "-P"]);
    if p.has_flag("-A") {
        return Err("new-session -A is not supported".to_string());
    }
    let mut create_params = Map::new();
    create_params.insert("focus".to_string(), Value::Bool(false));
    let cwd = p.value("-c");
    if !cwd.is_empty() {
        create_params.insert("cwd".to_string(), s(&cwd));
    }
    let created = rc.call("workspace.create", Some(create_params))?;
    let ws_id = get_str(&created, "workspace_id").to_string();
    if ws_id.is_empty() {
        return Err("workspace.create did not return workspace_id".to_string());
    }
    let title = crate::util::first_non_empty(&[&p.value("-n"), &p.value("-s")]).to_string();
    if !title.trim().is_empty() {
        let _ = rc.call(
            "workspace.rename",
            params(&[("workspace_id", s(&ws_id)), ("title", s(&title))]),
        );
    }
    let text = tmux_shell_command_text(&p.positional, &cwd);
    if !text.is_empty() {
        if let Ok(surface_id) = tmux_get_first_surface(rc, &ws_id) {
            let _ = rc.call(
                "surface.send_text",
                params(&[
                    ("workspace_id", s(&ws_id)),
                    ("surface_id", s(&surface_id)),
                    ("text", s(&text)),
                ]),
            );
        }
    }
    if p.has_flag("-P") {
        print_created_workspace(rc, &ws_id, &p.value("-F"), out);
    }
    Ok(())
}

fn tmux_new_window(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-c", "-F", "-n", "-t"], &["-d", "-P"]);
    let mut create_params = Map::new();
    create_params.insert("focus".to_string(), Value::Bool(false));
    let cwd = p.value("-c");
    if !cwd.is_empty() {
        create_params.insert("cwd".to_string(), s(&cwd));
    }
    let created = rc.call("workspace.create", Some(create_params))?;
    let ws_id = get_str(&created, "workspace_id").to_string();
    if ws_id.is_empty() {
        return Err("workspace.create did not return workspace_id".to_string());
    }
    let title = p.value("-n");
    if !title.trim().is_empty() {
        let _ = rc.call(
            "workspace.rename",
            params(&[("workspace_id", s(&ws_id)), ("title", s(&title))]),
        );
    }
    let text = tmux_shell_command_text(&p.positional, &cwd);
    if !text.is_empty() {
        if let Ok(surface_id) = tmux_get_first_surface(rc, &ws_id) {
            let _ = rc.call(
                "surface.send_text",
                params(&[
                    ("workspace_id", s(&ws_id)),
                    ("surface_id", s(&surface_id)),
                    ("text", s(&text)),
                ]),
            );
        }
    }
    if p.has_flag("-P") {
        print_created_workspace(rc, &ws_id, &p.value("-F"), out);
    }
    Ok(())
}

fn tmux_split_window(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(
        args,
        &["-c", "-F", "-l", "-t"],
        &["-P", "-b", "-d", "-h", "-v"],
    );

    let (mut target_ws, _, mut target_surface) = tmux_resolve_surface_target(rc, &p.value("-t"))?;

    let mut direction = "down".to_string();
    if p.has_flag("-h") {
        direction = "right".to_string();
        if p.has_flag("-b") {
            direction = "left".to_string();
        }
    } else if p.has_flag("-b") {
        direction = "up".to_string();
    }

    // Anchor splits to the leader surface for agent teams.
    let caller_workspace = tmux_caller_workspace_handle();
    let mut anchored_caller_surface = String::new();
    if !caller_workspace.is_empty() {
        if let Ok(ws_id) = tmux_resolve_workspace_id(rc, &caller_workspace) {
            if let Some(anchored) = tmux_anchored_split_target(rc, &ws_id) {
                target_ws = ws_id;
                target_surface = anchored.target_surface_id;
                direction = anchored.direction;
                anchored_caller_surface = anchored.caller_surface_id;
            }
        }
    }

    let focus_new_pane = !p.has_flag("-d");
    let created = rc.call(
        "surface.split",
        params(&[
            ("workspace_id", s(&target_ws)),
            ("surface_id", s(&target_surface)),
            ("direction", s(&direction)),
            ("focus", Value::Bool(focus_new_pane)),
        ]),
    )?;
    let surface_id = get_str(&created, "surface_id").to_string();
    if surface_id.is_empty() {
        return Err("surface.split did not return surface_id".to_string());
    }
    let new_pane_id = get_str(&created, "pane_id").to_string();

    // Track for main-vertical layout
    let mut store = load_tmux_compat_store();
    store
        .last_split_surface
        .insert(target_ws.clone(), surface_id.clone());
    if let Some(mvs) = store.main_vertical_layouts.get(&target_ws).cloned() {
        let mut updated = mvs;
        updated.last_column_surface_id = surface_id.clone();
        store
            .main_vertical_layouts
            .insert(target_ws.clone(), updated);
    } else if direction == "right" && !anchored_caller_surface.is_empty() {
        store.main_vertical_layouts.insert(
            target_ws.clone(),
            MainVerticalState {
                main_surface_id: anchored_caller_surface,
                last_column_surface_id: surface_id.clone(),
            },
        );
    }
    let _ = save_tmux_compat_store(&store);

    // Equalize vertical splits
    let _ = rc.call(
        "workspace.equalize_splits",
        params(&[
            ("workspace_id", s(&target_ws)),
            ("orientation", s("vertical")),
        ]),
    );

    let text = tmux_shell_command_text(&p.positional, &p.value("-c"));
    if !text.is_empty() {
        let _ = rc.call(
            "surface.send_text",
            params(&[
                ("workspace_id", s(&target_ws)),
                ("surface_id", s(&surface_id)),
                ("text", s(&text)),
            ]),
        );
    }

    if p.has_flag("-P") {
        match tmux_format_context(rc, &target_ws, &new_pane_id, &surface_id) {
            Ok(ctx) => {
                let fallback = ctx
                    .get("pane_id")
                    .cloned()
                    .unwrap_or_else(|| surface_id.clone());
                let _ = writeln!(
                    out,
                    "{}",
                    tmux_render_format(&p.value("-F"), &ctx, &fallback)
                );
            }
            Err(_) => {
                let _ = writeln!(out, "{surface_id}");
            }
        }
    }
    Ok(())
}

fn tmux_select_window(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("workspace.select", params(&[("workspace_id", s(&ws_id))]))
        .map(|_| ())
}

fn tmux_select_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-P", "-T", "-t"], &[]);
    // -P (style) and -T (title) are no-ops
    if !p.value("-P").is_empty() || !p.value("-T").is_empty() {
        return Ok(());
    }
    let (ws_id, pane_id) = tmux_resolve_pane_target(rc, &p.value("-t"))?;
    rc.call(
        "pane.focus",
        params(&[("workspace_id", s(&ws_id)), ("pane_id", s(&pane_id))]),
    )
    .map(|_| ())
}

fn tmux_kill_window(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("workspace.close", params(&[("workspace_id", s(&ws_id))]))?;
    let _ = tmux_prune_compat_workspace_state(&ws_id);
    Ok(())
}

fn tmux_kill_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    rc.call(
        "surface.close",
        params(&[("workspace_id", s(&ws_id)), ("surface_id", s(&surf_id))]),
    )?;
    let _ = tmux_prune_compat_surface_state(&ws_id, &surf_id);
    // Re-equalize after removal
    let _ = rc.call(
        "workspace.equalize_splits",
        params(&[("workspace_id", s(&ws_id)), ("orientation", s("vertical"))]),
    );
    Ok(())
}

fn tmux_send_keys(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &["-l"]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let text = tmux_send_keys_text(&p.positional, p.has_flag("-l"));
    if !text.is_empty() {
        rc.call(
            "surface.send_text",
            params(&[
                ("workspace_id", s(&ws_id)),
                ("surface_id", s(&surf_id)),
                ("text", s(&text)),
            ]),
        )?;
    }
    Ok(())
}

fn tmux_capture_pane(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-E", "-S", "-t"], &["-J", "-N", "-p"]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let mut read_params = Map::new();
    read_params.insert("workspace_id".to_string(), s(&ws_id));
    read_params.insert("surface_id".to_string(), s(&surf_id));
    read_params.insert("scrollback".to_string(), Value::Bool(true));
    let start = p.value("-S");
    if !start.is_empty() {
        let lines = parse_int(&start);
        if lines < 0 {
            read_params.insert("lines".to_string(), Value::from(lines.abs()));
        }
    }
    let payload = rc.call("surface.read_text", Some(read_params))?;
    let text = get_str(&payload, "text").to_string();
    if p.has_flag("-p") {
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    } else {
        let mut store = load_tmux_compat_store();
        store.buffers.insert("default".to_string(), text);
        let _ = save_tmux_compat_store(&store);
    }
    Ok(())
}

fn tmux_display_message(
    rc: &RpcContext,
    args: &[String],
    out: &mut dyn Write,
) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-F", "-t"], &["-p"]);
    let (ws_id, pane_id, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let mut ctx = tmux_format_context(rc, &ws_id, &pane_id, &surf_id).unwrap_or_default();

    // Enrich with geometry
    if let Ok(pane_payload) = rc.call("pane.list", params(&[("workspace_id", s(&ws_id))])) {
        let panes = get_array(&pane_payload, "panes");
        let container_frame = pane_payload
            .get("container_frame")
            .and_then(|v| v.as_object());
        let mut matching: Option<&Map<String, Value>> = None;
        if !pane_id.is_empty() {
            matching = panes.iter().find(|pane| get_str(pane, "id") == pane_id);
        }
        if matching.is_none() {
            matching = panes
                .iter()
                .find(|pane| bool_from_any_go(pane.get("focused")).unwrap_or(false));
        }
        if matching.is_none() {
            matching = panes.first();
        }
        if let Some(pane) = matching {
            tmux_enrich_context_with_geometry(&mut ctx, pane, container_frame);
        }
    }

    let mut format = p.value("-F");
    if !p.positional.is_empty() {
        format = p.positional.join(" ");
    }
    let rendered = tmux_render_format(&format, &ctx, "");
    if p.has_flag("-p") || !rendered.is_empty() {
        let _ = writeln!(out, "{rendered}");
    }
    Ok(())
}

fn tmux_list_windows(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-F", "-t"], &[]);
    let items = tmux_workspace_items(rc)?;
    for item in &items {
        let ws_id = get_str(item, "id");
        if ws_id.is_empty() {
            continue;
        }
        let ctx = match tmux_format_context(rc, ws_id, "", "") {
            Ok(ctx) => ctx,
            Err(_) => continue,
        };
        let mut fallback = ctx
            .get("window_index")
            .cloned()
            .unwrap_or_else(|| "?".to_string());
        match ctx.get("window_name") {
            Some(name) => fallback = format!("{fallback} {name}"),
            None => fallback = format!("{fallback} {ws_id}"),
        }
        let _ = writeln!(
            out,
            "{}",
            tmux_render_format(&p.value("-F"), &ctx, &fallback)
        );
    }
    Ok(())
}

fn tmux_list_panes(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-F", "-t"], &[]);
    let target = p.value("-t");
    let ws_id = if !target.is_empty() && !tmux_pane_selector(&target).is_empty() {
        tmux_resolve_pane_target(rc, &target)?.0
    } else {
        tmux_resolve_workspace_target(rc, &target)?
    };

    let payload = rc.call("pane.list", params(&[("workspace_id", s(&ws_id))]))?;
    let panes = get_array(&payload, "panes");
    let container_frame = payload.get("container_frame").and_then(|v| v.as_object());

    for pane in &panes {
        let pane_id = get_str(pane, "id");
        if pane_id.is_empty() {
            continue;
        }
        let mut ctx = match tmux_format_context(rc, &ws_id, pane_id, "") {
            Ok(ctx) => ctx,
            Err(_) => continue,
        };
        tmux_enrich_context_with_geometry(&mut ctx, pane, container_frame);
        let fallback = ctx
            .get("pane_id")
            .cloned()
            .unwrap_or_else(|| format!("%{pane_id}"));
        let _ = writeln!(
            out,
            "{}",
            tmux_render_format(&p.value("-F"), &ctx, &fallback)
        );
    }
    Ok(())
}

fn tmux_rename_window(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let title = p.positional.join(" ").trim().to_string();
    if title.is_empty() {
        return Err("rename-window requires a title".to_string());
    }
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call(
        "workspace.rename",
        params(&[("workspace_id", s(&ws_id)), ("title", s(&title))]),
    )
    .map(|_| ())
}

fn tmux_resize_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t", "-x", "-y"], &["-D", "-L", "-R", "-U"]);
    let (ws_id, pane_id) = tmux_resolve_pane_target(rc, &p.value("-t"))?;

    let has_directional =
        p.has_flag("-L") || p.has_flag("-R") || p.has_flag("-U") || p.has_flag("-D");

    if !has_directional {
        let target_size = p.value("-x").trim().to_string();
        // Deliberately preserve the daemon's historical height-only no-op:
        // recurring OMX HUD probes share this shape, and this shim has no
        // deterministic HUD identity signal. Applying -y here would overwrite
        // the user's layout.
        if target_size.is_empty() {
            return Ok(());
        }
        let is_percentage = target_size.ends_with('%');
        let target = parse_int(target_size.trim_end_matches('%'));
        if target <= 0 {
            return Err("resize-pane size must be greater than zero".to_string());
        }
        let pane_payload = rc.call("pane.list", params(&[("workspace_id", s(&ws_id))]))?;
        let mut target_points = 0.0_f64;
        if is_percentage {
            if let Some(Value::Object(frame)) = pane_payload.get("container_frame") {
                target_points = float_from_any(frame.get("width")) * target as f64 / 100.0;
            }
        }
        for pane in get_array(&pane_payload, "panes") {
            if get_str(&pane, "id") == pane_id {
                let cell_points = float_from_any(pane.get("cell_width_points"));
                if !is_percentage && target_points <= 0.0 && cell_points > 0.0 {
                    let columns = float_from_any(pane.get("columns"));
                    let pane_width = pane
                        .get("pixel_frame")
                        .and_then(|v| v.as_object())
                        .map(|frame| float_from_any(frame.get("width")))
                        .unwrap_or(0.0);
                    if columns > 0.0 && pane_width > 0.0 {
                        let residual = (pane_width - columns * cell_points).max(0.0);
                        target_points = target as f64 * cell_points + residual;
                    }
                }
                break;
            }
        }
        let mut resize_params = Map::new();
        resize_params.insert("workspace_id".to_string(), s(&ws_id));
        resize_params.insert("pane_id".to_string(), s(&pane_id));
        resize_params.insert("absolute_axis".to_string(), s("horizontal"));
        resize_params.insert("tmux_compat".to_string(), Value::Bool(true));
        if target_points > 0.0 {
            resize_params.insert("target_pixels".to_string(), json_number(target_points));
        }
        if is_percentage {
            resize_params.insert("target_percentage".to_string(), Value::from(target));
        } else {
            resize_params.insert("target_cells".to_string(), Value::from(target));
        }
        return rc.call("pane.resize", Some(resize_params)).map(|_| ());
    }

    let (dir, direction_flag) = if p.has_flag("-L") {
        ("left", "-L")
    } else if p.has_flag("-U") {
        ("up", "-U")
    } else if p.has_flag("-D") {
        ("down", "-D")
    } else {
        ("right", "-R")
    };
    let mut raw_amount = if let Some(first) = p.positional.first() {
        let mut raw = first.clone();
        if raw.starts_with(direction_flag) && raw.len() > direction_flag.len() {
            raw = raw[direction_flag.len()..].to_string();
        }
        raw
    } else {
        crate::util::first_non_empty(&[&p.value("-x"), &p.value("-y"), "1"]).to_string()
    };
    raw_amount = raw_amount.replace('%', "");
    let mut amount = parse_int(&raw_amount);
    if amount <= 0 {
        amount = 1;
    }
    let mut amount_points: i64 = 0;
    let pane_payload = rc.call("pane.list", params(&[("workspace_id", s(&ws_id))]))?;
    for pane in get_array(&pane_payload, "panes") {
        if get_str(&pane, "id") == pane_id {
            let points_key = if dir == "up" || dir == "down" {
                "cell_height_points"
            } else {
                "cell_width_points"
            };
            let cell_points = float_from_any(pane.get(points_key));
            if cell_points > 0.0 {
                amount_points = ((amount as f64 * cell_points).round() as i64).max(1);
            }
            break;
        }
    }
    let mut resize_params = Map::new();
    resize_params.insert("workspace_id".to_string(), s(&ws_id));
    resize_params.insert("pane_id".to_string(), s(&pane_id));
    resize_params.insert("direction".to_string(), s(dir));
    resize_params.insert("amount_cells".to_string(), Value::from(amount));
    resize_params.insert("tmux_compat".to_string(), Value::Bool(true));
    if amount_points > 0 {
        resize_params.insert("amount".to_string(), Value::from(amount_points));
    }
    rc.call("pane.resize", Some(resize_params)).map(|_| ())
}

fn tmux_wait_for(args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["--timeout"], &["-S"]);
    let name = p
        .positional
        .iter()
        .find(|pos| !pos.starts_with('-'))
        .cloned()
        .unwrap_or_default();
    if name.is_empty() {
        return Err("wait-for requires a name".to_string());
    }

    let signal_path = tmux_wait_for_signal_path(&name);

    if p.has_flag("-S") {
        // Signal mode: create the file
        let _ = fs::write(&signal_path, b"");
        let _ = writeln!(out, "OK");
        return Ok(());
    }

    // Wait mode: poll for the file
    let timeout_str = p.value("--timeout");
    let mut timeout = 30.0_f64;
    if !timeout_str.is_empty() {
        let t = parse_float(&timeout_str);
        if t > 0.0 {
            timeout = t;
        }
    }

    let deadline = Instant::now() + Duration::from_secs_f64(timeout);
    while Instant::now() < deadline {
        if fs::metadata(&signal_path).is_ok() {
            let _ = fs::remove_file(&signal_path);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!("wait-for timeout: {name}"))
}

fn tmux_last_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("pane.last", params(&[("workspace_id", s(&ws_id))]))
        .map(|_| ())
}

fn tmux_has_session(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    tmux_resolve_workspace_target(rc, &p.value("-t")).map(|_| ())
}

fn tmux_select_layout(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let layout_name = p.positional.first().cloned().unwrap_or_default();

    // Resolve workspace from target (may be a pane reference)
    let target = p.value("-t");
    let ws_id = if !target.is_empty() {
        if !tmux_pane_selector(&target).is_empty() {
            tmux_resolve_pane_target(rc, &target)?.0
        } else {
            tmux_resolve_workspace_target(rc, &target)?
        }
    } else {
        tmux_resolve_workspace_target(rc, "")?
    };

    if layout_name == "main-vertical" || layout_name == "main-horizontal" {
        let orientation = if layout_name == "main-horizontal" {
            "horizontal"
        } else {
            "vertical"
        };
        let _ = rc.call(
            "workspace.equalize_splits",
            params(&[("workspace_id", s(&ws_id)), ("orientation", s(orientation))]),
        );
    } else {
        let _ = rc.call(
            "workspace.equalize_splits",
            params(&[("workspace_id", s(&ws_id))]),
        );
    }

    if layout_name == "main-vertical" {
        let caller_surface = tmux_caller_surface_handle();
        if !caller_surface.is_empty() {
            let mut store = load_tmux_compat_store();
            let existing_column = store
                .main_vertical_layouts
                .get(&ws_id)
                .map(|existing| existing.last_column_surface_id.clone())
                .unwrap_or_default();
            let mut seed_column = existing_column;
            if seed_column.is_empty() {
                seed_column = store
                    .last_split_surface
                    .get(&ws_id)
                    .cloned()
                    .unwrap_or_default();
            }
            store.main_vertical_layouts.insert(
                ws_id.clone(),
                MainVerticalState {
                    main_surface_id: caller_surface,
                    last_column_surface_id: seed_column,
                },
            );
            let _ = save_tmux_compat_store(&store);
        }
    } else if !layout_name.is_empty() {
        let _ = tmux_prune_compat_workspace_state(&ws_id);
    }

    Ok(())
}

pub fn tmux_show_buffer(args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-b"], &[]);
    let mut name = p.value("-b");
    if name.is_empty() {
        name = "default".to_string();
    }
    let store = load_tmux_compat_store();
    if let Some(buf) = store.buffers.get(&name) {
        let _ = out.write_all(buf.as_bytes());
        let _ = out.flush();
    }
    Ok(())
}

fn tmux_save_buffer(args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-b"], &[]);
    let mut name = p.value("-b");
    if name.is_empty() {
        name = "default".to_string();
    }
    let store = load_tmux_compat_store();
    let buf = match store.buffers.get(&name) {
        Some(buf) => buf,
        None => return Err(format!("buffer not found: {name}")),
    };
    if let Some(last) = p.positional.last() {
        let output_path = last.trim();
        if !output_path.is_empty() {
            return fs::write(output_path, buf.as_bytes()).map_err(|err| err.to_string());
        }
    }
    let _ = out.write_all(buf.as_bytes());
    let _ = out.flush();
    Ok(())
}

// --- Helpers ---

pub fn tmux_get_first_surface(rc: &RpcContext, workspace_id: &str) -> Result<String, String> {
    let payload = rc.call("surface.list", params(&[("workspace_id", s(workspace_id))]))?;
    let surfaces = get_array(&payload, "surfaces");
    if surfaces.is_empty() {
        return Err("workspace has no surfaces".to_string());
    }
    // Prefer focused surface
    for surf in &surfaces {
        if bool_from_any_go(surf.get("focused")).unwrap_or(false) {
            let id = get_str(surf, "id");
            if !id.is_empty() {
                return Ok(id.to_string());
            }
        }
    }
    let id = get_str(&surfaces[0], "id");
    if !id.is_empty() {
        return Ok(id.to_string());
    }
    Err("workspace has no surfaces".to_string())
}

/// Go's `fmt.Sscanf(s, "%d", &n)`: a leading optionally-signed decimal
/// integer; anything else yields 0.
pub fn parse_int(input: &str) -> i64 {
    let trimmed = input.trim();
    let bytes = trimmed.as_bytes();
    let mut i = 0;
    let mut negative = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negative = bytes[i] == b'-';
        i += 1;
    }
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if start == i {
        return 0;
    }
    let value: i64 = trimmed[start..i].parse().unwrap_or(0);
    if negative {
        -value
    } else {
        value
    }
}

/// Go's `fmt.Sscanf(s, "%f", &f)`: the longest leading float prefix.
pub fn parse_float(input: &str) -> f64 {
    let trimmed = input.trim();
    let mut end = trimmed.len();
    while end > 0 {
        if let Ok(value) = trimmed[..end].parse::<f64>() {
            return value;
        }
        end -= 1;
    }
    0.0
}

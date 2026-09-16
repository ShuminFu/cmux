//! `cmux __tmux-compat`: translates the tmux commands agent shims issue
//! into cmux JSON-RPC calls over the relay socket.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{CliIo, Params, RefreshAddr, RpcContext};
use crate::util::{clean_path, fnv1a_64, parse_float_lenient, parse_int_lenient, user_home_dir};

pub fn run_tmux_compat(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let (command, cmd_args) = match split_tmux_cmd(args) {
        Ok(v) => v,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux __tmux-compat: {err}");
            return 1;
        }
    };
    let rc = RpcContext { socket_path: socket_path.to_string(), refresh_addr };
    if let Err(err) = dispatch_tmux_command(&rc, &command, &cmd_args, io.stdout) {
        let _ = writeln!(io.stderr, "cmux __tmux-compat: {err}");
        return 1;
    }
    0
}

// --- Tmux argument parsing ---

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TmuxParsed {
    pub flags: HashMap<String, bool>,
    pub options: HashMap<String, Vec<String>>,
    pub positional: Vec<String>,
}

impl TmuxParsed {
    #[must_use]
    pub fn has_flag(&self, f: &str) -> bool {
        self.flags.get(f).copied().unwrap_or(false)
    }

    #[must_use]
    pub fn value(&self, f: &str) -> String {
        self.options.get(f).and_then(|v| v.last()).cloned().unwrap_or_default()
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

#[must_use]
pub fn parse_tmux_args(args: &[String], value_flags: &[&str], bool_flags: &[&str]) -> TmuxParsed {
    let mut p = TmuxParsed::default();
    let mut past_terminator = false;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        if past_terminator {
            p.positional.push(arg.to_string());
            i += 1;
            continue;
        }
        if arg == "--" {
            past_terminator = true;
            i += 1;
            continue;
        }
        if !arg.starts_with('-') || arg == "-" || arg.starts_with("--") {
            p.positional.push(arg.to_string());
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
                p.flags.insert(flag, true);
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
                p.options.entry(flag).or_default().push(value);
                recognized = true;
                cursor = cluster.len();
                continue;
            }
            recognized = false;
            break;
        }
        if !recognized {
            p.positional.push(arg.to_string());
        }
        i += 1;
    }
    p
}

// --- Format string rendering ---

pub type FormatContext = HashMap<String, String>;

#[must_use]
pub fn tmux_render_format(format: &str, context: &FormatContext, fallback: &str) -> String {
    if format.is_empty() {
        return fallback.to_string();
    }
    let mut rendered = format.to_string();
    let mut keys: Vec<&String> = context.keys().collect();
    keys.sort();
    for key in keys {
        rendered = rendered.replace(&format!("#{{{key}}}"), &context[key]);
    }
    rendered = strip_unresolved_format_vars(&rendered);
    let rendered = rendered.trim();
    if rendered.is_empty() {
        return fallback.to_string();
    }
    rendered.to_string()
}

/// Remove every remaining `#{...}` variable (`#\{[^}]+\}`).
fn strip_unresolved_format_vars(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#'
            && i + 1 < bytes.len()
            && bytes[i + 1] == b'{'
            && let Some(rel) = input[i + 2..].find('}')
            && rel > 0
        {
            i += 2 + rel + 1;
            continue;
        }
        let ch = input[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

// --- JSON accessors with Go's `.(type)` semantics ---

fn get_str(obj: &Params, key: &str) -> Option<String> {
    obj.get(key).and_then(Value::as_str).map(str::to_string)
}

fn string_from_any_go(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).map(|s| s.trim().to_string()).unwrap_or_default()
}

fn get_obj<'a>(obj: &'a Params, key: &str) -> Option<&'a Params> {
    obj.get(key).and_then(Value::as_object)
}

fn get_list<'a>(obj: &'a Params, key: &str) -> Vec<&'a Params> {
    obj.get(key)
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_object).collect())
        .unwrap_or_default()
}

#[must_use]
pub fn float_from_any(v: Option<&Value>) -> f64 {
    v.and_then(Value::as_f64).unwrap_or(0.0)
}

#[must_use]
pub fn int_from_any_go(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                return i;
            }
            #[allow(clippy::cast_possible_truncation)]
            n.as_f64().map_or(-1, |f| f as i64)
        }
        _ => -1,
    }
}

#[must_use]
pub fn bool_from_any_go(v: Option<&Value>) -> Option<bool> {
    match v {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) => match s.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        },
        Some(Value::Number(n)) => {
            let f = n.as_f64()?;
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

fn tmux_set_window_active(ctx: &mut FormatContext, active: bool) {
    if active {
        ctx.insert("window_active".into(), "1".into());
        ctx.insert("window_flags".into(), "*".into());
    } else {
        ctx.insert("window_active".into(), "0".into());
        ctx.insert("window_flags".into(), String::new());
    }
}

#[must_use]
pub fn tmux_stable_numeric_id(raw: &str) -> String {
    let raw = raw.trim();
    let raw = if raw.is_empty() { "cmux" } else { raw };
    let mut value = fnv1a_64(raw.as_bytes()) & 0x7fff_ffff_ffff_ffff;
    if value == 0 {
        value = 1;
    }
    value.to_string()
}

#[must_use]
pub fn tmux_trim_id_sigil(raw: &str) -> String {
    let mut raw = raw.trim();
    while let Some(rest) = raw.strip_prefix(['$', '@', '%']) {
        raw = rest.trim();
    }
    raw.to_string()
}

fn tmux_selector_token(raw: &str) -> (String, bool) {
    let trimmed = raw.trim();
    let token = tmux_trim_id_sigil(trimmed);
    let sigiled = token != trimmed;
    (token, sigiled)
}

fn tmux_numeric_id_matches(handle: &str, candidates: &[&str]) -> bool {
    let token = tmux_trim_id_sigil(handle);
    if token.is_empty() {
        return false;
    }
    candidates.iter().any(|c| !c.trim().is_empty() && token == tmux_stable_numeric_id(c))
}

fn tmux_index_matches(handle: &str, index: i64) -> bool {
    index >= 0 && tmux_trim_id_sigil(handle) == index.to_string()
}

#[must_use]
pub fn tmux_normalize_path(raw: &str) -> String {
    let mut raw = raw.trim().to_string();
    if raw.is_empty() {
        return String::new();
    }
    if (raw.starts_with("~/") || raw == "~")
        && let Some(home) = user_home_dir().filter(|h| !h.is_empty())
    {
        raw = if raw == "~" { home } else { clean_path(&format!("{home}/{}", &raw[2..])) };
    }
    if !raw.starts_with('/')
        && let Ok(cwd) = std::env::current_dir()
    {
        raw = clean_path(&format!("{}/{raw}", cwd.to_string_lossy()));
    }
    if raw.starts_with('/') {
        return clean_path(&raw);
    }
    String::new()
}

fn tmux_first_path(values: &[String]) -> String {
    values.iter().map(|v| tmux_normalize_path(v)).find(|p| !p.is_empty()).unwrap_or_default()
}

fn tmux_path_from_object(item: &Params) -> String {
    let path = tmux_first_path(&[
        string_from_any_go(item.get("pane_current_path")),
        string_from_any_go(item.get("current_directory")),
        string_from_any_go(item.get("requested_working_directory")),
        string_from_any_go(item.get("working_directory")),
        string_from_any_go(item.get("cwd")),
    ]);
    if !path.is_empty() {
        return path;
    }
    if let Some(binding) = get_obj(item, "resume_binding") {
        return tmux_first_path(&[string_from_any_go(binding.get("cwd"))]);
    }
    String::new()
}

fn tmux_fallback_current_path() -> String {
    let path = tmux_normalize_path(&std::env::var("PWD").unwrap_or_default());
    if !path.is_empty() {
        return path;
    }
    if let Ok(cwd) = std::env::current_dir() {
        let path = tmux_normalize_path(&cwd.to_string_lossy());
        if !path.is_empty() {
            return path;
        }
    }
    if let Some(home) = user_home_dir() {
        let path = tmux_normalize_path(&home);
        if !path.is_empty() {
            return path;
        }
    }
    "/".to_string()
}

// --- Target resolution ---

fn env_trimmed(key: &str) -> String {
    std::env::var(key).unwrap_or_default().trim().to_string()
}

fn tmux_caller_workspace_handle() -> String {
    env_trimmed("CMUX_WORKSPACE_ID")
}

fn tmux_caller_surface_handle() -> String {
    env_trimmed("CMUX_SURFACE_ID")
}

fn tmux_resolved_caller_workspace_id(rc: &RpcContext) -> String {
    let caller = tmux_caller_workspace_handle();
    if caller.is_empty() {
        return String::new();
    }
    tmux_resolve_workspace_id(rc, &caller).unwrap_or_default()
}

fn tmux_active_workspace_id(rc: &RpcContext) -> String {
    let caller = tmux_resolved_caller_workspace_id(rc);
    if !caller.is_empty() {
        return caller;
    }
    let Ok(payload) = rc.call("workspace.current", None) else { return String::new() };
    if let Some(ws) = get_str(&payload, "workspace_id").filter(|s| !s.is_empty()) {
        return ws;
    }
    if let Some(ws_ref) = get_str(&payload, "workspace_ref").filter(|s| !s.is_empty())
        && let Ok(ws) = tmux_resolve_workspace_id(rc, &ws_ref)
    {
        return ws;
    }
    String::new()
}

fn tmux_caller_pane_handle() -> String {
    for key in ["TMUX_PANE", "CMUX_PANE_ID"] {
        let v = env_trimmed(key);
        if !v.is_empty() {
            return v.strip_prefix('%').unwrap_or(&v).to_string();
        }
    }
    String::new()
}

fn tmux_workspace_items(rc: &RpcContext) -> Result<Vec<Params>, String> {
    let payload = rc.call("workspace.list", None)?;
    Ok(get_list(&payload, "workspaces").into_iter().cloned().collect())
}

#[must_use]
pub fn is_uuidish(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    s.chars()
        .enumerate()
        .all(|(i, c)| if matches!(i, 8 | 13 | 18 | 23) { c == '-' } else { c.is_ascii_hexdigit() })
}

fn params1(key: &str, value: &str) -> Option<Params> {
    let mut p = Params::new();
    p.insert(key.to_string(), Value::from(value));
    Some(p)
}

fn params2(k1: &str, v1: &str, k2: &str, v2: &str) -> Option<Params> {
    let mut p = Params::new();
    p.insert(k1.to_string(), Value::from(v1));
    p.insert(k2.to_string(), Value::from(v2));
    Some(p)
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
            .map_err(|e| format!("no workspace selected: {e}"))?;
        if let Some(ws) = get_str(&payload, "workspace_id") {
            return Ok(ws);
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
    let items = tmux_workspace_items(rc)?;
    for item in &items {
        let id = get_str(item, "id").unwrap_or_default();
        let item_ref = get_str(item, "ref").unwrap_or_default();
        if !sigiled && item_ref == raw && !id.is_empty() {
            return Ok(id);
        }
        if id == raw || id == token {
            return Ok(id);
        }
        if (tmux_numeric_id_matches(&token, &[&id])
            || tmux_numeric_id_matches(&token, &[&string_from_any_go(item.get("ref"))]))
            && !id.is_empty()
        {
            return Ok(id);
        }
        if !sigiled
            && tmux_index_matches(&token, int_from_any_go(item.get("index")))
            && !id.is_empty()
        {
            return Ok(id);
        }
    }
    if !sigiled {
        let needle = token.trim();
        for item in &items {
            let title = get_str(item, "title").unwrap_or_default();
            if title.trim() == needle
                && let Some(id) = get_str(item, "id").filter(|s| !s.is_empty())
            {
                return Ok(id);
            }
        }
    }
    Err(format!("workspace not found: {raw}"))
}

fn tmux_resolve_workspace_target(rc: &RpcContext, raw: &str) -> Result<String, String> {
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
            .map_err(|e| format!("previous workspace not found: {e}"))?;
        if let Some(ws) = get_str(&payload, "workspace_id") {
            return Ok(ws);
        }
        return Err("previous workspace not found".to_string());
    }
    let mut token = raw.to_string();
    if let Some(dot) = token.rfind('.') {
        token.truncate(dot);
    }
    if let Some(colon) = token.rfind(':') {
        let suffix = token[colon + 1..].to_string();
        token = if suffix.is_empty() { token[..colon].to_string() } else { suffix };
    }
    tmux_resolve_workspace_id(rc, &token)
}

#[must_use]
pub fn tmux_pane_selector(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    if raw.starts_with('%') || raw.starts_with("pane:") {
        return raw.to_string();
    }
    match raw.rfind('.') {
        Some(dot) => raw[dot + 1..].to_string(),
        None => String::new(),
    }
}

#[must_use]
pub fn tmux_window_selector(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with('%') || raw.starts_with("pane:") {
        return String::new();
    }
    match raw.rfind('.') {
        Some(dot) => raw[..dot].to_string(),
        None => raw.to_string(),
    }
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
    let payload = rc.call("pane.list", params1("workspace_id", workspace_id))?;
    let panes = get_list(&payload, "panes");
    for pane in &panes {
        let id = get_str(pane, "id").unwrap_or_default();
        let pane_ref = get_str(pane, "ref").unwrap_or_default();
        if !sigiled && pane_ref == handle && !id.is_empty() {
            return Ok(id);
        }
        if id == handle {
            return Ok(id);
        }
        if (tmux_numeric_id_matches(&handle, &[&id])
            || tmux_numeric_id_matches(&handle, &[&pane_ref]))
            && !id.is_empty()
        {
            return Ok(id);
        }
    }
    if !sigiled {
        for pane in &panes {
            let id = get_str(pane, "id").unwrap_or_default();
            if tmux_index_matches(&handle, int_from_any_go(pane.get("index"))) && !id.is_empty() {
                return Ok(id);
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
    let payload = rc.call("surface.list", params1("workspace_id", workspace_id))?;
    let surfaces = get_list(&payload, "surfaces");
    for surface in &surfaces {
        let id = get_str(surface, "id").unwrap_or_default();
        let surface_ref = get_str(surface, "ref").unwrap_or_default();
        if !sigiled && surface_ref == handle && !id.is_empty() {
            return Ok(id);
        }
        if id == handle {
            return Ok(id);
        }
        if (tmux_numeric_id_matches(&handle, &[&id])
            || tmux_numeric_id_matches(&handle, &[&surface_ref]))
            && !id.is_empty()
        {
            return Ok(id);
        }
    }
    if !sigiled {
        for surface in &surfaces {
            let id = get_str(surface, "id").unwrap_or_default();
            if tmux_index_matches(&handle, int_from_any_go(surface.get("index"))) && !id.is_empty()
            {
                return Ok(id);
            }
        }
    }
    Err(format!("surface not found: {handle}"))
}

fn tmux_focused_pane_id(rc: &RpcContext, workspace_id: &str) -> Result<String, String> {
    let payload = rc.call("surface.current", params1("workspace_id", workspace_id))?;
    if let Some(pid) = get_str(&payload, "pane_id") {
        return Ok(pid);
    }
    if let Some(pref) = get_str(&payload, "pane_ref") {
        return tmux_canonical_pane_id(rc, &pref, workspace_id);
    }
    Err("pane not found".to_string())
}

fn tmux_workspace_id_for_pane_handle(rc: &RpcContext, handle: &str) -> Result<String, String> {
    let (handle, sigiled) = tmux_selector_token(handle);
    let workspaces = tmux_workspace_items(rc)?;
    for ws in &workspaces {
        let ws_id = get_str(ws, "id").unwrap_or_default();
        if ws_id.is_empty() {
            continue;
        }
        let Ok(payload) = rc.call("pane.list", params1("workspace_id", &ws_id)) else { continue };
        for pane in get_list(&payload, "panes") {
            let pid = get_str(pane, "id").unwrap_or_default();
            let pref = get_str(pane, "ref").unwrap_or_default();
            if pid == handle || (!sigiled && pref == handle) {
                return Ok(ws_id);
            }
            if tmux_numeric_id_matches(&handle, &[&pid])
                || tmux_numeric_id_matches(&handle, &[&pref])
            {
                return Ok(ws_id);
            }
            if !sigiled && tmux_index_matches(&handle, int_from_any_go(pane.get("index"))) {
                return Ok(ws_id);
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
        if !caller_pane.is_empty()
            && let Ok(pid) = tmux_canonical_pane_id(rc, &caller_pane, &workspace_id)
        {
            pane_id = pid;
        }
    }
    if pane_id.is_empty() {
        pane_id = tmux_focused_pane_id(rc, &workspace_id)?;
    }
    Ok((workspace_id, pane_id))
}

fn tmux_selected_surface_id(
    rc: &RpcContext,
    workspace_id: &str,
    pane_id: &str,
) -> Result<String, String> {
    let payload =
        rc.call("pane.surfaces", params2("workspace_id", workspace_id, "pane_id", pane_id))?;
    let surfaces = get_list(&payload, "surfaces");
    for surface in &surfaces {
        if bool_from_any_go(surface.get("selected")).unwrap_or(false)
            && let Some(id) = get_str(surface, "id").filter(|s| !s.is_empty())
        {
            return Ok(id);
        }
    }
    if let Some(first) = surfaces.first()
        && let Some(id) = get_str(first, "id").filter(|s| !s.is_empty())
    {
        return Ok(id);
    }
    Err("pane has no surface".to_string())
}

/// Resolve `raw` to (workspace, pane, surface). The pane is empty when the
/// surface was found without going through a pane.
pub fn tmux_resolve_surface_target(
    rc: &RpcContext,
    raw: &str,
) -> Result<(String, String, String), String> {
    let raw = raw.trim();
    if !tmux_pane_selector(raw).is_empty() {
        let (workspace_id, pane_id) = tmux_resolve_pane_target(rc, raw)?;
        let caller_pane = tmux_caller_pane_handle();
        let caller_surface = tmux_caller_surface_handle();
        if !caller_pane.is_empty() && !caller_surface.is_empty() {
            let canonical_caller_pane =
                tmux_canonical_pane_id(rc, &caller_pane, &workspace_id).unwrap_or_default();
            if (pane_id == caller_pane || pane_id == canonical_caller_pane)
                && let Ok(surface_id) =
                    tmux_canonical_surface_id(rc, &caller_surface, &workspace_id)
            {
                return Ok((workspace_id, pane_id, surface_id));
            }
        }
        let surface_id = tmux_selected_surface_id(rc, &workspace_id, &pane_id)?;
        return Ok((workspace_id, pane_id, surface_id));
    }
    let win_sel = tmux_window_selector(raw);
    let workspace_id = tmux_resolve_workspace_target(rc, &win_sel)?;
    if win_sel.is_empty() && tmux_resolved_caller_workspace_id(rc) == workspace_id {
        let caller_surface = tmux_caller_surface_handle();
        if !caller_surface.is_empty()
            && let Ok(surface_id) = tmux_canonical_surface_id(rc, &caller_surface, &workspace_id)
        {
            return Ok((workspace_id, String::new(), surface_id));
        }
    }
    if let Ok(payload) = rc.call("surface.current", params1("workspace_id", &workspace_id))
        && let Some(sid) = get_str(&payload, "surface_id")
    {
        return Ok((workspace_id, String::new(), sid));
    }
    if let Ok(payload) = rc.call("surface.list", params1("workspace_id", &workspace_id)) {
        let surfs = get_list(&payload, "surfaces");
        for surf in &surfs {
            if bool_from_any_go(surf.get("focused")).unwrap_or(false)
                && let Some(id) = get_str(surf, "id").filter(|s| !s.is_empty())
            {
                return Ok((workspace_id, String::new(), id));
            }
        }
        if let Some(first) = surfs.first()
            && let Some(id) = get_str(first, "id").filter(|s| !s.is_empty())
        {
            return Ok((workspace_id, String::new(), id));
        }
    }
    Err("unable to resolve surface".to_string())
}

struct SplitAnchor {
    target_surface_id: String,
    caller_surface_id: String,
    direction: &'static str,
}

fn tmux_anchored_split_target(rc: &RpcContext, workspace_id: &str) -> Option<SplitAnchor> {
    let mut store = load_tmux_compat_store();
    if let Some(mv) = store.main_vertical_layouts.get(workspace_id).cloned()
        && !mv.last_column_surface_id.is_empty()
    {
        if let Ok(last_column_id) =
            tmux_canonical_surface_id(rc, &mv.last_column_surface_id, workspace_id)
        {
            return Some(SplitAnchor {
                target_surface_id: last_column_id,
                caller_surface_id: String::new(),
                direction: "down",
            });
        }
        // Right-column anchors can outlive the pane they pointed at.
        // Drop stale state and rebuild from the caller surface instead.
        let mut refreshed = mv;
        refreshed.last_column_surface_id = String::new();
        store.main_vertical_layouts.insert(workspace_id.to_string(), refreshed);
        store.last_split_surface.remove(workspace_id);
        let _ = save_tmux_compat_store(&store);
    }
    let mut candidates = vec![tmux_caller_surface_handle()];
    if let Some(mv) = store.main_vertical_layouts.get(workspace_id)
        && !mv.main_surface_id.is_empty()
    {
        candidates.push(mv.main_surface_id.clone());
    }
    for candidate in candidates {
        if candidate.is_empty() {
            continue;
        }
        if let Ok(anchor) = tmux_canonical_surface_id(rc, &candidate, workspace_id) {
            return Some(SplitAnchor {
                target_surface_id: anchor.clone(),
                caller_surface_id: anchor,
                direction: "right",
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MainVerticalState {
    #[serde(rename = "mainSurfaceId", default)]
    pub main_surface_id: String,
    #[serde(rename = "lastColumnSurfaceId", default, skip_serializing_if = "String::is_empty")]
    pub last_column_surface_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TmuxCompatStore {
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub buffers: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub hooks: Map<String, Value>,
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

impl TmuxCompatStore {
    fn buffer(&self, name: &str) -> Option<String> {
        self.buffers.get(name).and_then(Value::as_str).map(str::to_string)
    }
}

#[must_use]
pub fn tmux_compat_store_path() -> std::path::PathBuf {
    let home = user_home_dir().unwrap_or_default();
    Path::new(&home).join(".cmuxterm").join("tmux-compat-store.json")
}

#[must_use]
pub fn load_tmux_compat_store() -> TmuxCompatStore {
    let Ok(data) = std::fs::read(tmux_compat_store_path()) else {
        return TmuxCompatStore::default();
    };
    serde_json::from_slice(&data).unwrap_or_default()
}

pub fn save_tmux_compat_store(store: &TmuxCompatStore) -> Result<(), String> {
    let path = tmux_compat_store_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| crate::util::path_error("mkdir", &dir.to_string_lossy(), &e))?;
    }
    // Serialize the struct directly so field order matches Go's encoder.
    let data = crate::util::go_escape(&serde_json::to_string(store).map_err(|e| e.to_string())?);
    std::fs::write(&path, data)
        .map_err(|e| crate::util::path_error("open", &path.to_string_lossy(), &e))
}

fn tmux_prune_compat_workspace_state(workspace_id: &str) -> Result<(), String> {
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

fn tmux_prune_compat_surface_state(workspace_id: &str, surface_id: &str) -> Result<(), String> {
    let mut store = load_tmux_compat_store();
    let mut changed = false;
    if store.last_split_surface.get(workspace_id).is_some_and(|s| s == surface_id) {
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
            store.main_vertical_layouts.insert(workspace_id.to_string(), updated);
            changed = true;
        }
    }
    if changed {
        return save_tmux_compat_store(&store);
    }
    Ok(())
}

// --- Special key translation ---

#[must_use]
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

#[must_use]
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

#[must_use]
pub fn tmux_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[must_use]
pub fn tmux_shell_command_text(positional: &[String], cwd: &str) -> String {
    let cwd = cwd.trim();
    let cmd = positional.join(" ");
    let cmd = cmd.trim();
    if cwd.is_empty() && cmd.is_empty() {
        return String::new();
    }
    let mut pieces = Vec::new();
    if !cwd.is_empty() {
        pieces.push(format!("cd -- {}", tmux_shell_quote(cwd)));
    }
    if !cmd.is_empty() {
        pieces.push(cmd.to_string());
    }
    format!("{}\r", pieces.join(" && "))
}

#[must_use]
pub fn tmux_wait_for_signal_path(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' })
        .collect();
    format!("/tmp/cmux-wait-for-{sanitized}.sig")
}

// --- Format context building ---

#[allow(clippy::too_many_lines)]
pub fn tmux_format_context(
    rc: &RpcContext,
    workspace_id: &str,
    pane_id: &str,
    surface_id: &str,
) -> Result<FormatContext, String> {
    let canonical_ws = tmux_resolve_workspace_id(rc, workspace_id)?;
    let mut ctx: FormatContext = [
        ("session_name", "cmux".to_string()),
        ("session_id", format!("${}", tmux_stable_numeric_id(&canonical_ws))),
        ("session_attached", "1".to_string()),
        ("window_id", format!("@{}", tmux_stable_numeric_id(&canonical_ws))),
        ("window_uuid", canonical_ws.clone()),
        ("window_active", "0".to_string()),
        ("window_flags", String::new()),
        ("window_width", "80".to_string()),
        ("window_height", "24".to_string()),
        ("pane_active", "1".to_string()),
        ("pane_width", "80".to_string()),
        ("pane_height", "24".to_string()),
        ("pane_current_path", tmux_fallback_current_path()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let active_ws = tmux_active_workspace_id(rc);
    let active_by_caller = active_ws == canonical_ws;
    if active_by_caller {
        tmux_set_window_active(&mut ctx, true);
    }
    if let Ok(workspaces) = tmux_workspace_items(rc) {
        for ws in &workspaces {
            let ws_id = get_str(ws, "id").unwrap_or_default();
            let ws_ref = get_str(ws, "ref").unwrap_or_default();
            if ws_id == canonical_ws || ws_ref == workspace_id {
                if let (Some(active), false) =
                    (bool_from_any_go(ws.get("active")), active_by_caller)
                {
                    tmux_set_window_active(&mut ctx, active);
                } else if let (Some(focused), false) =
                    (bool_from_any_go(ws.get("focused")), active_by_caller)
                {
                    tmux_set_window_active(&mut ctx, focused);
                } else if let (Some(selected), false) =
                    (bool_from_any_go(ws.get("selected")), active_by_caller)
                {
                    tmux_set_window_active(&mut ctx, selected);
                }
                let idx = int_from_any_go(ws.get("index"));
                if idx >= 0 {
                    ctx.insert("window_index".into(), idx.to_string());
                }
                if let Some(title) = get_str(ws, "title").filter(|t| !t.trim().is_empty()) {
                    ctx.insert("window_name".into(), title.trim().to_string());
                }
                let path = tmux_path_from_object(ws);
                if !path.is_empty() {
                    ctx.insert("pane_current_path".into(), path);
                }
                let pane_count = int_from_any_go(ws.get("pane_count"));
                if pane_count >= 0 {
                    ctx.insert("window_panes".into(), pane_count.to_string());
                }
                break;
            }
        }
    }
    let Ok(current) = rc.call("surface.current", params1("workspace_id", &canonical_ws)) else {
        return Ok(ctx);
    };
    let mut resolved_pane = String::new();
    if !pane_id.is_empty() {
        resolved_pane = tmux_canonical_pane_id(rc, pane_id, &canonical_ws)
            .unwrap_or_else(|_| pane_id.to_string());
    }
    if resolved_pane.is_empty() {
        if let Some(pid) = get_str(&current, "pane_id") {
            resolved_pane = pid;
        } else if let Some(pref) = get_str(&current, "pane_ref") {
            resolved_pane = tmux_canonical_pane_id(rc, &pref, &canonical_ws).unwrap_or(pref);
        }
    }
    let mut resolved_surface = String::new();
    if !surface_id.is_empty() {
        resolved_surface = tmux_canonical_surface_id(rc, surface_id, &canonical_ws)
            .unwrap_or_else(|_| surface_id.to_string());
    }
    if resolved_surface.is_empty()
        && !resolved_pane.is_empty()
        && let Ok(sid) = tmux_selected_surface_id(rc, &canonical_ws, &resolved_pane)
    {
        resolved_surface = sid;
    }
    if resolved_surface.is_empty()
        && let Some(sid) = get_str(&current, "surface_id")
    {
        resolved_surface = sid;
    }
    if !resolved_pane.is_empty() {
        ctx.insert("pane_id".into(), format!("%{}", tmux_stable_numeric_id(&resolved_pane)));
        ctx.insert("pane_uuid".into(), resolved_pane.clone());
        if let Ok(pane_payload) = rc.call("pane.list", params1("workspace_id", &canonical_ws)) {
            for pane in get_list(&pane_payload, "panes") {
                if get_str(pane, "id").unwrap_or_default() == resolved_pane {
                    let idx = int_from_any_go(pane.get("index"));
                    if idx >= 0 {
                        ctx.insert("pane_index".into(), idx.to_string());
                    }
                    if let Some(focused) = bool_from_any_go(pane.get("focused")) {
                        ctx.insert(
                            "pane_active".into(),
                            if focused { "1" } else { "0" }.to_string(),
                        );
                    }
                    break;
                }
            }
        }
    }
    if !resolved_surface.is_empty() {
        ctx.insert("surface_id".into(), resolved_surface.clone());
        if let Ok(surface_payload) = rc.call("surface.list", params1("workspace_id", &canonical_ws))
        {
            for surface in get_list(&surface_payload, "surfaces") {
                if get_str(surface, "id").unwrap_or_default() == resolved_surface {
                    if let Some(title) = get_str(surface, "title").filter(|t| !t.trim().is_empty())
                    {
                        ctx.insert("pane_title".into(), title.trim().to_string());
                        ctx.entry("window_name".into()).or_insert_with(|| title.trim().to_string());
                    }
                    let path = tmux_path_from_object(surface);
                    if !path.is_empty() {
                        ctx.insert("pane_current_path".into(), path);
                    }
                    break;
                }
            }
        }
    }
    Ok(ctx)
}

#[allow(clippy::cast_possible_truncation)]
pub fn tmux_enrich_context_with_geometry(
    ctx: &mut FormatContext,
    pane: &Params,
    container_frame: Option<&Params>,
) {
    let focused = bool_from_any_go(pane.get("focused")).unwrap_or(false);
    ctx.insert("pane_active".into(), if focused { "1" } else { "0" }.to_string());
    let columns = int_from_any_go(pane.get("columns"));
    let rows = int_from_any_go(pane.get("rows"));
    if columns < 0 || rows < 0 {
        return;
    }
    ctx.insert("pane_width".into(), columns.to_string());
    ctx.insert("pane_height".into(), rows.to_string());
    let cell_w = int_from_any_go(pane.get("cell_width_px"));
    let cell_h = int_from_any_go(pane.get("cell_height_px"));
    if cell_w <= 0 || cell_h <= 0 {
        return;
    }
    if let Some(frame) = get_obj(pane, "pixel_frame") {
        let px = float_from_any(frame.get("x")) as i64;
        let py = float_from_any(frame.get("y")) as i64;
        ctx.insert("pane_left".into(), (px / cell_w).to_string());
        ctx.insert("pane_top".into(), (py / cell_h).to_string());
    }
    if let Some(container) = container_frame {
        let cw = float_from_any(container.get("width")) as i64;
        let ch = float_from_any(container.get("height")) as i64;
        let ww = (cw / cell_w).max(1);
        let wh = (ch / cell_h).max(1);
        ctx.insert("window_width".into(), ww.to_string());
        ctx.insert("window_height".into(), wh.to_string());
    }
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
        "set-option" | "set" | "set-window-option" | "setw" | "source-file" | "refresh-client"
        | "attach-session" | "detach-client" | "last-window" | "next-window"
        | "previous-window" | "set-hook" | "set-buffer" | "list-buffers" => Ok(()),
        _ => Err(format!("unsupported tmux command: {command}")),
    }
}

fn create_workspace(rc: &RpcContext, cwd: &str) -> Result<String, String> {
    let mut params = Params::new();
    params.insert("focus".into(), Value::Bool(false));
    if !cwd.is_empty() {
        params.insert("cwd".into(), Value::from(cwd));
    }
    let created = rc.call("workspace.create", Some(params))?;
    let ws_id = get_str(&created, "workspace_id").unwrap_or_default();
    if ws_id.is_empty() {
        return Err("workspace.create did not return workspace_id".to_string());
    }
    Ok(ws_id)
}

fn send_startup_text(rc: &RpcContext, ws_id: &str, positional: &[String], cwd: &str) {
    let text = tmux_shell_command_text(positional, cwd);
    if text.is_empty() {
        return;
    }
    if let Ok(surface_id) = tmux_get_first_surface(rc, ws_id) {
        let mut params = Params::new();
        params.insert("workspace_id".into(), Value::from(ws_id));
        params.insert("surface_id".into(), Value::from(surface_id));
        params.insert("text".into(), Value::from(text));
        let _ = rc.call("surface.send_text", Some(params));
    }
}

fn print_window_handle(rc: &RpcContext, ws_id: &str, format: &str, out: &mut dyn Write) {
    match tmux_format_context(rc, ws_id, "", "") {
        Ok(ctx) => {
            let _ = writeln!(out, "{}", tmux_render_format(format, &ctx, &format!("@{ws_id}")));
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
    let ws_id = create_workspace(rc, &p.value("-c"))?;
    let title = crate::util::first_non_empty(&[&p.value("-n"), &p.value("-s")]).to_string();
    if !title.trim().is_empty() {
        let _ = rc.call("workspace.rename", params2("workspace_id", &ws_id, "title", &title));
    }
    send_startup_text(rc, &ws_id, &p.positional, &p.value("-c"));
    if p.has_flag("-P") {
        print_window_handle(rc, &ws_id, &p.value("-F"), out);
    }
    Ok(())
}

fn tmux_new_window(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-c", "-F", "-n", "-t"], &["-d", "-P"]);
    let ws_id = create_workspace(rc, &p.value("-c"))?;
    let title = p.value("-n");
    if !title.trim().is_empty() {
        let _ = rc.call("workspace.rename", params2("workspace_id", &ws_id, "title", &title));
    }
    send_startup_text(rc, &ws_id, &p.positional, &p.value("-c"));
    if p.has_flag("-P") {
        print_window_handle(rc, &ws_id, &p.value("-F"), out);
    }
    Ok(())
}

fn equalize(rc: &RpcContext, ws_id: &str, orientation: Option<&str>) {
    let mut params = Params::new();
    params.insert("workspace_id".into(), Value::from(ws_id));
    if let Some(orientation) = orientation {
        params.insert("orientation".into(), Value::from(orientation));
    }
    let _ = rc.call("workspace.equalize_splits", Some(params));
}

fn tmux_split_window(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-c", "-F", "-l", "-t"], &["-P", "-b", "-d", "-h", "-v"]);
    let (mut target_ws, _, mut target_surface) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let mut direction = "down";
    if p.has_flag("-h") {
        direction = if p.has_flag("-b") { "left" } else { "right" };
    } else if p.has_flag("-b") {
        direction = "up";
    }
    // Anchor splits to the leader surface for agent teams.
    let caller_workspace = tmux_caller_workspace_handle();
    let mut anchored_caller_surface = String::new();
    if !caller_workspace.is_empty()
        && let Ok(ws_id) = tmux_resolve_workspace_id(rc, &caller_workspace)
        && let Some(anchored) = tmux_anchored_split_target(rc, &ws_id)
    {
        target_ws = ws_id;
        target_surface = anchored.target_surface_id;
        direction = anchored.direction;
        anchored_caller_surface = anchored.caller_surface_id;
    }
    let focus_new_pane = !p.has_flag("-d");
    let mut params = Params::new();
    params.insert("workspace_id".into(), Value::from(target_ws.as_str()));
    params.insert("surface_id".into(), Value::from(target_surface.as_str()));
    params.insert("direction".into(), Value::from(direction));
    params.insert("focus".into(), Value::Bool(focus_new_pane));
    let created = rc.call("surface.split", Some(params))?;
    let surface_id = get_str(&created, "surface_id").unwrap_or_default();
    if surface_id.is_empty() {
        return Err("surface.split did not return surface_id".to_string());
    }
    let new_pane_id = get_str(&created, "pane_id").unwrap_or_default();

    let mut store = load_tmux_compat_store();
    store.last_split_surface.insert(target_ws.clone(), surface_id.clone());
    if let Some(mvs) = store.main_vertical_layouts.get_mut(&target_ws) {
        mvs.last_column_surface_id = surface_id.clone();
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
    equalize(rc, &target_ws, Some("vertical"));

    let text = tmux_shell_command_text(&p.positional, &p.value("-c"));
    if !text.is_empty() {
        let mut params = Params::new();
        params.insert("workspace_id".into(), Value::from(target_ws.as_str()));
        params.insert("surface_id".into(), Value::from(surface_id.as_str()));
        params.insert("text".into(), Value::from(text));
        let _ = rc.call("surface.send_text", Some(params));
    }
    if p.has_flag("-P") {
        match tmux_format_context(rc, &target_ws, &new_pane_id, &surface_id) {
            Ok(ctx) => {
                let fallback = ctx.get("pane_id").cloned().unwrap_or_else(|| surface_id.clone());
                let _ = writeln!(out, "{}", tmux_render_format(&p.value("-F"), &ctx, &fallback));
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
    rc.call("workspace.select", params1("workspace_id", &ws_id)).map(|_| ())
}

fn tmux_select_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-P", "-T", "-t"], &[]);
    if !p.value("-P").is_empty() || !p.value("-T").is_empty() {
        return Ok(());
    }
    let (ws_id, pane_id) = tmux_resolve_pane_target(rc, &p.value("-t"))?;
    rc.call("pane.focus", params2("workspace_id", &ws_id, "pane_id", &pane_id)).map(|_| ())
}

fn tmux_kill_window(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("workspace.close", params1("workspace_id", &ws_id))?;
    let _ = tmux_prune_compat_workspace_state(&ws_id);
    Ok(())
}

fn tmux_kill_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    rc.call("surface.close", params2("workspace_id", &ws_id, "surface_id", &surf_id))?;
    let _ = tmux_prune_compat_surface_state(&ws_id, &surf_id);
    equalize(rc, &ws_id, Some("vertical"));
    Ok(())
}

fn tmux_send_keys(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &["-l"]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let text = tmux_send_keys_text(&p.positional, p.has_flag("-l"));
    if !text.is_empty() {
        let mut params = Params::new();
        params.insert("workspace_id".into(), Value::from(ws_id));
        params.insert("surface_id".into(), Value::from(surf_id));
        params.insert("text".into(), Value::from(text));
        rc.call("surface.send_text", Some(params))?;
    }
    Ok(())
}

fn tmux_capture_pane(rc: &RpcContext, args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-E", "-S", "-t"], &["-J", "-N", "-p"]);
    let (ws_id, _, surf_id) = tmux_resolve_surface_target(rc, &p.value("-t"))?;
    let mut params = Params::new();
    params.insert("workspace_id".into(), Value::from(ws_id));
    params.insert("surface_id".into(), Value::from(surf_id));
    params.insert("scrollback".into(), Value::Bool(true));
    let start = p.value("-S");
    if !start.is_empty() {
        let lines = parse_int_lenient(&start);
        if lines < 0 {
            params.insert("lines".into(), Value::from(lines.abs()));
        }
    }
    let payload = rc.call("surface.read_text", Some(params))?;
    let text = get_str(&payload, "text").unwrap_or_default();
    if p.has_flag("-p") {
        let _ = write!(out, "{text}");
    } else {
        let mut store = load_tmux_compat_store();
        store.buffers.insert("default".into(), Value::from(text));
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
    if let Ok(pane_payload) = rc.call("pane.list", params1("workspace_id", &ws_id)) {
        let panes = get_list(&pane_payload, "panes");
        let container = get_obj(&pane_payload, "container_frame");
        let mut matching: Option<&Params> = None;
        if !pane_id.is_empty() {
            matching =
                panes.iter().copied().find(|pn| get_str(pn, "id").unwrap_or_default() == pane_id);
        }
        if matching.is_none() {
            matching = panes
                .iter()
                .copied()
                .find(|pn| bool_from_any_go(pn.get("focused")).unwrap_or(false));
        }
        if matching.is_none() {
            matching = panes.first().copied();
        }
        if let Some(pane) = matching {
            tmux_enrich_context_with_geometry(&mut ctx, pane, container);
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
        let ws_id = get_str(item, "id").unwrap_or_default();
        if ws_id.is_empty() {
            continue;
        }
        let Ok(ctx) = tmux_format_context(rc, &ws_id, "", "") else { continue };
        let mut fallback = ctx.get("window_index").cloned().unwrap_or_else(|| "?".to_string());
        match ctx.get("window_name") {
            Some(name) => fallback.push_str(&format!(" {name}")),
            None => fallback.push_str(&format!(" {ws_id}")),
        }
        let _ = writeln!(out, "{}", tmux_render_format(&p.value("-F"), &ctx, &fallback));
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
    let payload = rc.call("pane.list", params1("workspace_id", &ws_id))?;
    let container = get_obj(&payload, "container_frame");
    for pane in get_list(&payload, "panes") {
        let pane_id = get_str(pane, "id").unwrap_or_default();
        if pane_id.is_empty() {
            continue;
        }
        let Ok(mut ctx) = tmux_format_context(rc, &ws_id, &pane_id, "") else { continue };
        tmux_enrich_context_with_geometry(&mut ctx, pane, container);
        let fallback = ctx.get("pane_id").cloned().unwrap_or_else(|| format!("%{pane_id}"));
        let _ = writeln!(out, "{}", tmux_render_format(&p.value("-F"), &ctx, &fallback));
    }
    Ok(())
}

fn tmux_rename_window(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let title = p.positional.join(" ");
    let title = title.trim();
    if title.is_empty() {
        return Err("rename-window requires a title".to_string());
    }
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("workspace.rename", params2("workspace_id", &ws_id, "title", title)).map(|_| ())
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn tmux_resize_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t", "-x", "-y"], &["-D", "-L", "-R", "-U"]);
    let (ws_id, pane_id) = tmux_resolve_pane_target(rc, &p.value("-t"))?;
    let has_directional =
        p.has_flag("-L") || p.has_flag("-R") || p.has_flag("-U") || p.has_flag("-D");
    if !has_directional {
        let target_size = p.value("-x");
        let target_size = target_size.trim();
        // Deliberately preserve the daemon's historical height-only no-op.
        if target_size.is_empty() {
            return Ok(());
        }
        let is_percentage = target_size.ends_with('%');
        let target = parse_int_lenient(target_size.trim_end_matches('%'));
        if target <= 0 {
            return Err("resize-pane size must be greater than zero".to_string());
        }
        let pane_payload = rc.call("pane.list", params1("workspace_id", &ws_id))?;
        let mut target_points = 0.0f64;
        if is_percentage && let Some(frame) = get_obj(&pane_payload, "container_frame") {
            target_points = float_from_any(frame.get("width")) * target as f64 / 100.0;
        }
        for pane in get_list(&pane_payload, "panes") {
            if get_str(pane, "id").unwrap_or_default() == pane_id {
                let cell_points = float_from_any(pane.get("cell_width_points"));
                if !is_percentage && target_points <= 0.0 && cell_points > 0.0 {
                    let columns = float_from_any(pane.get("columns"));
                    let pane_width = get_obj(pane, "pixel_frame")
                        .map_or(0.0, |f| float_from_any(f.get("width")));
                    if columns > 0.0 && pane_width > 0.0 {
                        let residual = (pane_width - columns * cell_points).max(0.0);
                        target_points = target as f64 * cell_points + residual;
                    }
                }
                break;
            }
        }
        let mut params = Params::new();
        params.insert("workspace_id".into(), Value::from(ws_id));
        params.insert("pane_id".into(), Value::from(pane_id));
        params.insert("absolute_axis".into(), Value::from("horizontal"));
        params.insert("tmux_compat".into(), Value::Bool(true));
        if target_points > 0.0 {
            params.insert("target_pixels".into(), float_value(target_points));
        }
        if is_percentage {
            params.insert("target_percentage".into(), Value::from(target));
        } else {
            params.insert("target_cells".into(), Value::from(target));
        }
        return rc.call("pane.resize", Some(params)).map(|_| ());
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
    let mut amount = parse_int_lenient(&raw_amount);
    if amount <= 0 {
        amount = 1;
    }
    let mut amount_points: i64 = 0;
    let pane_payload = rc.call("pane.list", params1("workspace_id", &ws_id))?;
    for pane in get_list(&pane_payload, "panes") {
        if get_str(pane, "id").unwrap_or_default() == pane_id {
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
    let mut params = Params::new();
    params.insert("workspace_id".into(), Value::from(ws_id));
    params.insert("pane_id".into(), Value::from(pane_id));
    params.insert("direction".into(), Value::from(dir));
    params.insert("amount_cells".into(), Value::from(amount));
    params.insert("tmux_compat".into(), Value::Bool(true));
    if amount_points > 0 {
        params.insert("amount".into(), Value::from(amount_points));
    }
    rc.call("pane.resize", Some(params)).map(|_| ())
}

fn float_value(f: f64) -> Value {
    serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)
}

fn tmux_wait_for(args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["--timeout"], &["-S"]);
    let name = p.positional.iter().find(|pos| !pos.starts_with('-')).cloned().unwrap_or_default();
    if name.is_empty() {
        return Err("wait-for requires a name".to_string());
    }
    let signal_path = tmux_wait_for_signal_path(&name);
    if p.has_flag("-S") {
        let _ = std::fs::write(&signal_path, b"");
        let _ = writeln!(out, "OK");
        return Ok(());
    }
    let mut timeout = 30.0f64;
    let timeout_str = p.value("--timeout");
    if !timeout_str.is_empty() {
        let t = parse_float_lenient(&timeout_str);
        if t > 0.0 {
            timeout = t;
        }
    }
    let deadline = Instant::now() + Duration::from_secs_f64(timeout);
    while Instant::now() < deadline {
        if std::fs::metadata(&signal_path).is_ok() {
            let _ = std::fs::remove_file(&signal_path);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!("wait-for timeout: {name}"))
}

fn tmux_last_pane(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let ws_id = tmux_resolve_workspace_target(rc, &p.value("-t"))?;
    rc.call("pane.last", params1("workspace_id", &ws_id)).map(|_| ())
}

fn tmux_has_session(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    tmux_resolve_workspace_target(rc, &p.value("-t")).map(|_| ())
}

fn tmux_select_layout(rc: &RpcContext, args: &[String]) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-t"], &[]);
    let layout_name = p.positional.first().cloned().unwrap_or_default();
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
        let orientation = if layout_name == "main-horizontal" { "horizontal" } else { "vertical" };
        equalize(rc, &ws_id, Some(orientation));
    } else {
        equalize(rc, &ws_id, None);
    }
    if layout_name == "main-vertical" {
        let caller_surface = tmux_caller_surface_handle();
        if !caller_surface.is_empty() {
            let mut store = load_tmux_compat_store();
            let existing_column = store
                .main_vertical_layouts
                .get(&ws_id)
                .map(|e| e.last_column_surface_id.clone())
                .unwrap_or_default();
            let seed_column = if existing_column.is_empty() {
                store.last_split_surface.get(&ws_id).cloned().unwrap_or_default()
            } else {
                existing_column
            };
            store.main_vertical_layouts.insert(
                ws_id,
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

fn tmux_show_buffer(args: &[String], out: &mut dyn Write) -> Result<(), String> {
    let p = parse_tmux_args(args, &["-b"], &[]);
    let mut name = p.value("-b");
    if name.is_empty() {
        name = "default".to_string();
    }
    let store = load_tmux_compat_store();
    if let Some(buf) = store.buffer(&name) {
        let _ = write!(out, "{buf}");
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
    let Some(buf) = store.buffer(&name) else {
        return Err(format!("buffer not found: {name}"));
    };
    if let Some(last) = p.positional.last() {
        let output_path = last.trim();
        if !output_path.is_empty() {
            return std::fs::write(output_path, buf.as_bytes())
                .map_err(|e| crate::util::path_error("open", output_path, &e));
        }
    }
    let _ = write!(out, "{buf}");
    Ok(())
}

fn tmux_get_first_surface(rc: &RpcContext, workspace_id: &str) -> Result<String, String> {
    let payload = rc.call("surface.list", params1("workspace_id", workspace_id))?;
    let surfaces = get_list(&payload, "surfaces");
    if surfaces.is_empty() {
        return Err("workspace has no surfaces".to_string());
    }
    for surf in &surfaces {
        if bool_from_any_go(surf.get("focused")).unwrap_or(false)
            && let Some(id) = get_str(surf, "id").filter(|s| !s.is_empty())
        {
            return Ok(id);
        }
    }
    if let Some(id) = get_str(surfaces[0], "id").filter(|s| !s.is_empty()) {
        return Ok(id);
    }
    Err("workspace has no surfaces".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn split_command_skips_global_flags() {
        assert_eq!(
            split_tmux_cmd(&args(&["-L", "sock", "New-Window", "-d"])).unwrap(),
            ("new-window".to_string(), args(&["-d"]))
        );
        assert_eq!(split_tmux_cmd(&args(&["-V"])).unwrap(), ("-V".to_string(), Vec::new()));
        assert!(split_tmux_cmd(&args(&["-S", "x"])).is_err());
        assert!(split_tmux_cmd(&args(&["--"])).is_err());
        assert_eq!(split_tmux_cmd(&args(&["-", "x"])).unwrap().0, "-");
    }

    #[test]
    fn cluster_flags_and_values() {
        let p = parse_tmux_args(
            &args(&["-dPh", "-t", "win.1", "-l30", "cmd", "--", "-x"]),
            &["-t", "-l"],
            &["-d", "-P", "-h"],
        );
        assert!(p.has_flag("-d") && p.has_flag("-P") && p.has_flag("-h"));
        assert_eq!(p.value("-t"), "win.1");
        assert_eq!(p.value("-l"), "30");
        assert_eq!(p.positional, args(&["cmd", "-x"]));
        let p = parse_tmux_args(&args(&["-z", "--long", "-t"]), &["-t"], &[]);
        assert_eq!(p.positional, args(&["-z", "--long"]));
        assert_eq!(p.value("-t"), "");
        assert_eq!(p.options["-t"], vec![String::new()]);
    }

    #[test]
    fn render_format_substitutes_and_strips() {
        let mut ctx = FormatContext::new();
        ctx.insert("pane_id".into(), "%7".into());
        ctx.insert("window_name".into(), "w".into());
        assert_eq!(tmux_render_format("#{pane_id}:#{window_name}:#{missing}", &ctx, "fb"), "%7:w:");
        assert_eq!(tmux_render_format("#{missing}", &ctx, "fb"), "fb");
        assert_eq!(tmux_render_format("", &ctx, "fb"), "fb");
        assert_eq!(tmux_render_format("#{}", &ctx, "fb"), "#{}");
    }

    #[test]
    fn send_keys_translation() {
        assert_eq!(tmux_send_keys_text(&args(&["echo", "hi", "Enter"]), false), "echo hi\r");
        assert_eq!(tmux_send_keys_text(&args(&["C-c", "ls", "Tab", "x"]), false), "\x03ls\tx");
        assert_eq!(tmux_send_keys_text(&args(&["a", "Enter"]), true), "a Enter");
        assert_eq!(
            tmux_shell_command_text(&args(&["make"]), "/tmp/x y"),
            "cd -- '/tmp/x y' && make\r"
        );
        assert_eq!(tmux_shell_command_text(&[], ""), "");
        assert_eq!(tmux_shell_quote("it's"), "'it'\"'\"'s'");
    }

    #[test]
    fn selectors_and_ids() {
        assert_eq!(tmux_pane_selector("win.3"), "3");
        assert_eq!(tmux_pane_selector("%5"), "%5");
        assert_eq!(tmux_pane_selector("win"), "");
        assert_eq!(tmux_window_selector("win.3"), "win");
        assert_eq!(tmux_window_selector("%5"), "");
        assert_eq!(tmux_trim_id_sigil(" %@$x "), "x");
        assert!(is_uuidish("123e4567-e89b-12d3-a456-426614174000"));
        assert!(!is_uuidish("123e4567-e89b-12d3-a456-42661417400g"));
        assert_eq!(tmux_stable_numeric_id(""), tmux_stable_numeric_id("cmux"));
        assert_eq!(tmux_wait_for_signal_path("a/b c"), "/tmp/cmux-wait-for-a_b_c.sig");
    }

    #[test]
    fn go_value_coercions() {
        assert_eq!(bool_from_any_go(Some(&Value::from("Yes"))), Some(true));
        assert_eq!(bool_from_any_go(Some(&Value::from(1))), Some(true));
        assert_eq!(bool_from_any_go(Some(&Value::from(2))), None);
        assert_eq!(int_from_any_go(Some(&Value::from(3.9))), 3);
        assert_eq!(int_from_any_go(Some(&Value::from("3"))), -1);
        assert_eq!(int_from_any_go(None), -1);
    }
}

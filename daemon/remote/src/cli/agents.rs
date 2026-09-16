//! `cmux claude-teams|omo|omx|omc`: launch agent CLIs with tmux shims and a
//! cmux-aware environment, replacing the current process.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use super::tmux::{tmux_canonical_pane_id, tmux_resolve_workspace_id, tmux_stable_numeric_id};
use super::{CliIo, RefreshAddr, RpcContext};
use crate::util::{io_error_text, path_error, user_home_dir};

const CLAUDE_NODE_OPTIONS_RESTORE_MODULE_SCRIPT: &str = r#"const hadOriginalNodeOptions = process.env.CMUX_ORIGINAL_NODE_OPTIONS_PRESENT === "1";
if (hadOriginalNodeOptions) {
  process.env.NODE_OPTIONS = process.env.CMUX_ORIGINAL_NODE_OPTIONS ?? "";
} else {
  delete process.env.NODE_OPTIONS;
}
delete process.env.CMUX_ORIGINAL_NODE_OPTIONS;
delete process.env.CMUX_ORIGINAL_NODE_OPTIONS_PRESENT;
"#;

pub const CLAUDE_TEAMS_SHIM_SCRIPT: &str = "#!/usr/bin/env bash
set -euo pipefail
exec \"${CMUX_CLAUDE_TEAMS_CMUX_BIN:-cmux}\" __tmux-compat \"$@\"
";

pub const OMO_TMUX_SHIM_SCRIPT: &str = "#!/usr/bin/env bash
set -euo pipefail
# Only match -V/-v as the first arg (top-level tmux flag).
# -v inside subcommands (e.g. split-window -v) is a vertical split flag.
case \"${1:-}\" in
  -V|-v) echo \"tmux 3.4\"; exit 0 ;;
esac
exec \"${CMUX_OMO_CMUX_BIN:-cmux}\" __tmux-compat \"$@\"
";

pub const OMX_SHIM_SCRIPT: &str = "#!/usr/bin/env bash
set -euo pipefail
case \"${1:-}\" in
  -V|-v) echo \"tmux 3.4\"; exit 0 ;;
esac
exec \"${CMUX_OMX_CMUX_BIN:-cmux}\" __tmux-compat \"$@\"
";

pub const OMC_SHIM_SCRIPT: &str = "#!/usr/bin/env bash
set -euo pipefail
case \"${1:-}\" in
  -V|-v) echo \"tmux 3.4\"; exit 0 ;;
esac
exec \"${CMUX_OMC_CMUX_BIN:-cmux}\" __tmux-compat \"$@\"
";

pub const OMO_NOTIFIER_SHIM_SCRIPT: &str = "#!/usr/bin/env bash
# Intercept terminal-notifier calls and route through cmux notify.
TITLE=\"\" BODY=\"\"
while [[ $# -gt 0 ]]; do
  case \"$1\" in
    -title)   TITLE=\"$2\"; shift 2 ;;
    -message) BODY=\"$2\"; shift 2 ;;
    *)        shift ;;
  esac
done
exec \"${CMUX_OMO_CMUX_BIN:-cmux}\" notify --title \"${TITLE:-OpenCode}\" --body \"${BODY:-}\"
";

/// A copy of the process environment that launchers mutate before exec.
pub type Env = HashMap<String, String>;

fn current_env() -> Env {
    std::env::vars().collect()
}

fn exec_with_env(path: &str, argv: &[String], env: &Env) -> std::io::Error {
    let mut cmd = Command::new(path);
    if let Some(arg0) = argv.first() {
        cmd.arg0(arg0);
    }
    cmd.args(&argv[1..]);
    cmd.env_clear();
    cmd.envs(env);
    cmd.exec()
}

pub fn run_claude_teams_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext { socket_path: socket_path.to_string(), refresh_addr };
    let shim_dir = match create_tmux_shim_dir("claude-teams-bin", CLAUDE_TEAMS_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ =
                writeln!(io.stderr, "cmux claude-teams: failed to create shim directory: {err}");
            return 1;
        }
    };
    let mut env = current_env();
    let original_path = env.get("PATH").cloned().unwrap_or_default();
    let claude_path = find_executable_in_path("claude", &original_path, &shim_dir);
    let focused = get_focused_context(&rc);
    configure_agent_environment(
        &mut env,
        &AgentConfig {
            shim_dir,
            socket_path: socket_path.to_string(),
            focused: focused.as_ref(),
            tmux_path_prefix: "cmux-claude-teams",
            cmux_bin_env_var: "CMUX_CLAUDE_TEAMS_CMUX_BIN",
            term_env_var: "CMUX_CLAUDE_TEAMS_TERM",
            extra_env: &[("CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS", "1")],
        },
    );
    if let Ok(restore) = ensure_claude_node_options_restore_module() {
        configure_claude_node_options(&mut env, &restore);
    }
    let launch_args = claude_teams_launch_args(args);
    if claude_path.is_empty() {
        let _ = writeln!(io.stderr, "cmux claude-teams: claude not found in PATH");
        return 1;
    }
    let mut argv = vec![claude_path.clone()];
    argv.extend(launch_args);
    let err = exec_with_env(&claude_path, &argv, &env);
    let _ = writeln!(io.stderr, "cmux claude-teams: exec failed: {}", io_error_text(&err));
    1
}

pub fn run_omo_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext { socket_path: socket_path.to_string(), refresh_addr };
    let shim_dir = match create_omo_shim_dir() {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux omo: failed to create shim directory: {err}");
            return 1;
        }
    };
    let mut env = current_env();
    let original_path = env.get("PATH").cloned().unwrap_or_default();
    let opencode_path = find_executable_in_path("opencode", &original_path, &shim_dir);
    if opencode_path.is_empty() {
        let _ = write!(
            io.stderr,
            "cmux omo: opencode not found in PATH\nInstall it first:\n  npm install -g opencode-ai\n  # or\n  bun install -g opencode-ai\n"
        );
        return 1;
    }
    match omo_ensure_plugin(&original_path, io.stderr) {
        Ok(config_dir) => {
            env.insert("OPENCODE_CONFIG_DIR".into(), config_dir);
        }
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux omo: plugin setup: {err}");
            return 1;
        }
    }
    let focused = get_focused_context(&rc);
    configure_agent_environment(
        &mut env,
        &AgentConfig {
            shim_dir: shim_dir.clone(),
            socket_path: socket_path.to_string(),
            focused: focused.as_ref(),
            tmux_path_prefix: "cmux-omo",
            cmux_bin_env_var: "CMUX_OMO_CMUX_BIN",
            term_env_var: "CMUX_OMO_TERM",
            extra_env: &[],
        },
    );
    if env.get("OPENCODE_PORT").is_none_or(String::is_empty) {
        env.insert("OPENCODE_PORT".into(), "4096".into());
    }
    let mut launch_args = args.to_vec();
    let has_port = launch_args.iter().any(|a| a == "--port" || a.starts_with("--port="));
    if !has_port {
        let mut port = env.get("OPENCODE_PORT").cloned().unwrap_or_default();
        if port.is_empty() {
            port = "4096".into();
        }
        launch_args.splice(0..0, ["--port".to_string(), port]);
    }
    let (launch_path, launch_argv) =
        resolve_node_script_exec(&opencode_path, &launch_args, &original_path, &shim_dir);
    let err = exec_with_env(&launch_path, &launch_argv, &env);
    let _ = writeln!(io.stderr, "cmux omo: exec failed: {}", io_error_text(&err));
    1
}

pub fn run_omx_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext { socket_path: socket_path.to_string(), refresh_addr };
    let shim_dir = match create_tmux_shim_dir("omx-bin", OMX_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux omx: failed to create shim directory: {err}");
            return 1;
        }
    };
    let mut env = current_env();
    let original_path = env.get("PATH").cloned().unwrap_or_default();
    let omx_path = find_executable_in_path("omx", &original_path, &shim_dir);
    if omx_path.is_empty() {
        let _ = write!(
            io.stderr,
            "cmux omx: omx not found in PATH\nInstall it first:\n  npm install -g oh-my-codex\n"
        );
        return 1;
    }
    let focused = get_focused_context(&rc);
    configure_agent_environment(
        &mut env,
        &AgentConfig {
            shim_dir: shim_dir.clone(),
            socket_path: socket_path.to_string(),
            focused: focused.as_ref(),
            tmux_path_prefix: "cmux-omx",
            cmux_bin_env_var: "CMUX_OMX_CMUX_BIN",
            term_env_var: "CMUX_OMX_TERM",
            extra_env: &[],
        },
    );
    let (launch_path, launch_argv) =
        resolve_node_script_exec(&omx_path, args, &original_path, &shim_dir);
    let err = exec_with_env(&launch_path, &launch_argv, &env);
    let _ = writeln!(io.stderr, "cmux omx: exec failed: {}", io_error_text(&err));
    1
}

pub fn run_omc_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext { socket_path: socket_path.to_string(), refresh_addr };
    let shim_dir = match create_tmux_shim_dir("omc-bin", OMC_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(io.stderr, "cmux omc: failed to create shim directory: {err}");
            return 1;
        }
    };
    let mut env = current_env();
    let original_path = env.get("PATH").cloned().unwrap_or_default();
    let omc_path = find_executable_in_path("omc", &original_path, &shim_dir);
    if omc_path.is_empty() {
        let _ = write!(
            io.stderr,
            "cmux omc: omc not found in PATH\nInstall it first:\n  npm install -g oh-my-claude-sisyphus\n"
        );
        return 1;
    }
    let focused = get_focused_context(&rc);
    configure_agent_environment(
        &mut env,
        &AgentConfig {
            shim_dir: shim_dir.clone(),
            socket_path: socket_path.to_string(),
            focused: focused.as_ref(),
            tmux_path_prefix: "cmux-omc",
            cmux_bin_env_var: "CMUX_OMC_CMUX_BIN",
            term_env_var: "CMUX_OMC_TERM",
            extra_env: &[],
        },
    );
    match ensure_claude_node_options_restore_module() {
        Ok(restore) => configure_claude_node_options(&mut env, &restore),
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux omc: warning: failed to create NODE_OPTIONS restore module: {err}"
            );
        }
    }
    let (launch_path, launch_argv) =
        resolve_node_script_exec(&omc_path, args, &original_path, &shim_dir);
    let err = exec_with_env(&launch_path, &launch_argv, &env);
    let _ = writeln!(io.stderr, "cmux omc: exec failed: {}", io_error_text(&err));
    1
}

// --- Shim creation ---

pub fn create_tmux_shim_dir(dir_name: &str, tmux_script: &str) -> Result<String, String> {
    let home = user_home_dir().ok_or_else(|| "$HOME is not defined".to_string())?;
    let dir = Path::new(&home).join(".cmuxterm").join(dir_name);
    std::fs::create_dir_all(&dir).map_err(|e| path_error("mkdir", &dir.to_string_lossy(), &e))?;
    write_shim_if_changed(&dir.join("tmux"), tmux_script)?;
    Ok(dir.to_string_lossy().into_owned())
}

pub fn create_omo_shim_dir() -> Result<String, String> {
    let dir = create_tmux_shim_dir("omo-bin", OMO_TMUX_SHIM_SCRIPT)?;
    write_shim_if_changed(&Path::new(&dir).join("terminal-notifier"), OMO_NOTIFIER_SHIM_SCRIPT)?;
    Ok(dir)
}

pub fn write_shim_if_changed(path: &Path, content: &str) -> Result<(), String> {
    if std::fs::read(path).is_ok_and(|existing| existing == content.as_bytes()) {
        return Ok(());
    }
    let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    let base = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let (mut file, tmp_path) = crate::util::create_temp_file(&dir, &format!(".{base}.tmp-"), "")
        .map_err(|e| e.to_string())?;
    let result = (|| {
        use std::os::unix::fs::PermissionsExt;
        file.write_all(content.as_bytes())
            .map_err(|e| path_error("write", &tmp_path.to_string_lossy(), &e))?;
        drop(file);
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| path_error("chmod", &tmp_path.to_string_lossy(), &e))?;
        std::fs::rename(&tmp_path, path).map_err(|e| {
            format!("rename {} {}: {}", tmp_path.display(), path.display(), io_error_text(&e))
        })
    })();
    let _ = std::fs::remove_file(&tmp_path);
    result
}

pub fn ensure_claude_node_options_restore_module() -> Result<String, String> {
    let dir = Path::new(&crate::util::temp_dir()).join("cmux-claude-node-options");
    std::fs::create_dir_all(&dir).map_err(|e| path_error("mkdir", &dir.to_string_lossy(), &e))?;
    let restore = dir.join("restore-node-options.cjs");
    write_shim_if_changed(&restore, CLAUDE_NODE_OPTIONS_RESTORE_MODULE_SCRIPT)?;
    Ok(restore.to_string_lossy().into_owned())
}

// --- Focused context ---

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FocusedContext {
    pub workspace_id: String,
    pub window_id: String,
    pub pane_handle: String,
    pub pane_id: String,
    pub surface_id: String,
}

pub fn get_focused_context(rc: &RpcContext) -> Option<FocusedContext> {
    get_focused_context_with_timeout(rc, Duration::from_secs(5))
}

pub fn get_focused_context_with_timeout(
    rc: &RpcContext,
    timeout: Duration,
) -> Option<FocusedContext> {
    let started = Instant::now();
    let (tx, rx) = crossbeam_channel::bounded(1);
    {
        let rc = RpcContext { socket_path: rc.socket_path.clone(), refresh_addr: rc.refresh_addr };
        std::thread::spawn(move || {
            let _ = tx.send(rc.call("system.identify", None).ok());
        });
    }
    let payload = rx.recv_timeout(timeout).ok()??;
    let focused = payload.get("focused").and_then(Value::as_object)?.clone();
    let ctx = focused_context_from_identify(&focused)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Some(ctx);
    }
    Some(canonicalize_focused_context_with_timeout(rc, &focused, &ctx, remaining))
}

fn string_from_any(values: &[Option<&Value>]) -> String {
    values
        .iter()
        .filter_map(|v| v.and_then(Value::as_str))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_default()
}

#[must_use]
pub fn focused_context_from_identify(focused: &Map<String, Value>) -> Option<FocusedContext> {
    let ws_id = string_from_any(&[focused.get("workspace_id"), focused.get("workspace_ref")]);
    let pane_handle = string_from_any(&[focused.get("pane_id"), focused.get("pane_ref")]);
    if ws_id.is_empty() || pane_handle.is_empty() {
        return None;
    }
    Some(FocusedContext {
        workspace_id: ws_id,
        window_id: string_from_any(&[focused.get("window_id"), focused.get("window_ref")]),
        pane_handle: pane_handle.trim().to_string(),
        pane_id: string_from_any(&[focused.get("pane_uuid"), focused.get("pane_id")])
            .trim()
            .to_string(),
        surface_id: string_from_any(&[focused.get("surface_id"), focused.get("surface_ref")]),
    })
}

fn canonicalize_focused_context_with_timeout(
    rc: &RpcContext,
    focused: &Map<String, Value>,
    base: &FocusedContext,
    timeout: Duration,
) -> FocusedContext {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let rc = RpcContext { socket_path: rc.socket_path.clone(), refresh_addr: rc.refresh_addr };
    let focused = focused.clone();
    let mut enriched = base.clone();
    std::thread::spawn(move || {
        canonicalize_focused_context(&rc, &focused, &mut enriched);
        let _ = tx.send(enriched);
    });
    rx.recv_timeout(timeout).unwrap_or_else(|_| base.clone())
}

fn canonicalize_focused_context(
    rc: &RpcContext,
    focused: &Map<String, Value>,
    ctx: &mut FocusedContext,
) {
    let mut canonical_pane = string_from_any(&[focused.get("pane_uuid")]);
    if let Ok(canonical_ws) = tmux_resolve_workspace_id(rc, &ctx.workspace_id) {
        if canonical_pane.is_empty() {
            let pid = string_from_any(&[focused.get("pane_id")]);
            if !pid.is_empty()
                && let Ok(resolved) = tmux_canonical_pane_id(rc, &pid, &canonical_ws)
            {
                canonical_pane = resolved;
            }
        }
        if canonical_pane.is_empty()
            && let Ok(pid) = tmux_canonical_pane_id(rc, &ctx.pane_handle, &canonical_ws)
        {
            canonical_pane = pid;
        }
    }
    if canonical_pane.is_empty() {
        canonical_pane = string_from_any(&[focused.get("pane_id")]);
    }
    if !canonical_pane.is_empty() {
        ctx.pane_id = canonical_pane.trim().to_string();
    }
}

pub fn configure_claude_node_options(env: &mut Env, restore_module_path: &str) {
    let existing = env.get("NODE_OPTIONS").cloned();
    match &existing {
        Some(value) => {
            env.insert("CMUX_ORIGINAL_NODE_OPTIONS_PRESENT".into(), "1".into());
            env.insert("CMUX_ORIGINAL_NODE_OPTIONS".into(), value.clone());
        }
        None => {
            env.insert("CMUX_ORIGINAL_NODE_OPTIONS_PRESENT".into(), "0".into());
            env.remove("CMUX_ORIGINAL_NODE_OPTIONS");
        }
    }
    env.insert(
        "NODE_OPTIONS".into(),
        merge_node_options(existing.as_deref().unwrap_or(""), restore_module_path),
    );
}

#[must_use]
pub fn merge_node_options(existing: &str, restore_module_path: &str) -> String {
    let require_flag = format!("--require={restore_module_path}");
    const MEMORY_FLAG: &str = "--max-old-space-size=4096";
    let cleaned = cleaned_node_options(existing);
    if cleaned.is_empty() {
        return format!("{require_flag} {MEMORY_FLAG}");
    }
    format!("{require_flag} {MEMORY_FLAG} {cleaned}")
}

#[must_use]
pub fn cleaned_node_options(existing: &str) -> String {
    let tokens: Vec<&str> = existing.split_whitespace().collect();
    let mut filtered = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i];
        if token == "--max-old-space-size" {
            if i + 1 < tokens.len() {
                i += 1;
            }
            i += 1;
            continue;
        }
        if token.starts_with("--max-old-space-size=") {
            i += 1;
            continue;
        }
        filtered.push(token);
        i += 1;
    }
    filtered.join(" ")
}

// --- Environment configuration ---

pub struct AgentConfig<'a> {
    pub shim_dir: String,
    pub socket_path: String,
    pub focused: Option<&'a FocusedContext>,
    pub tmux_path_prefix: &'static str,
    pub cmux_bin_env_var: &'static str,
    pub term_env_var: &'static str,
    pub extra_env: &'a [(&'static str, &'static str)],
}

pub fn configure_agent_environment(env: &mut Env, cfg: &AgentConfig<'_>) {
    let mut self_path =
        std::env::current_exe().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
    if self_path.is_empty() {
        self_path = "cmux".into();
    }
    env.insert(cfg.cmux_bin_env_var.into(), self_path);
    let current_path = env.get("PATH").cloned().unwrap_or_default();
    env.insert("PATH".into(), format!("{}:{current_path}", cfg.shim_dir));
    let (fake_tmux, fake_tmux_pane) = fake_tmux_env(cfg.tmux_path_prefix, cfg.focused);
    env.insert("TMUX".into(), fake_tmux);
    env.insert("TMUX_PANE".into(), fake_tmux_pane);
    let mut fake_term = env.get(cfg.term_env_var).cloned().unwrap_or_default();
    if fake_term.is_empty() {
        fake_term = "screen-256color".into();
    }
    env.insert("TERM".into(), fake_term);
    env.insert("CMUX_SOCKET_PATH".into(), cfg.socket_path.clone());
    env.remove("CMUX_SOCKET");
    // Unset TERM_PROGRAM so apps don't detect the host terminal and override
    // tmux-compatible behavior.
    env.remove("TERM_PROGRAM");
    if env.get("COLORTERM").is_none_or(String::is_empty) {
        env.insert("COLORTERM".into(), "truecolor".into());
    }
    if let Some(focused) = cfg.focused {
        env.insert("CMUX_WORKSPACE_ID".into(), focused.workspace_id.clone());
        if !focused.surface_id.is_empty() {
            env.insert("CMUX_SURFACE_ID".into(), focused.surface_id.clone());
        }
    }
    for (k, v) in cfg.extra_env {
        env.insert((*k).to_string(), (*v).to_string());
    }
}

/// The fake `TMUX` / `TMUX_PANE` values agents see.
#[must_use]
pub fn fake_tmux_env(prefix: &str, focused: Option<&FocusedContext>) -> (String, String) {
    let Some(focused) = focused else {
        return (format!("/tmp/{prefix}/default,0,0"), "%1".to_string());
    };
    let window_token =
        if focused.window_id.is_empty() { &focused.workspace_id } else { &focused.window_id };
    let pane_for_token =
        if focused.pane_id.is_empty() { &focused.pane_handle } else { &focused.pane_id };
    let pane_token = tmux_stable_numeric_id(pane_for_token);
    (
        format!("/tmp/{prefix}/{},{window_token},{pane_token}", focused.workspace_id),
        format!("%{pane_token}"),
    )
}

// --- oh-my-opencode plugin setup ---

const OMO_PLUGIN_NAME: &str = "oh-my-opencode";

fn omo_user_config_dir() -> PathBuf {
    Path::new(&user_home_dir().unwrap_or_default()).join(".config").join("opencode")
}

fn omo_shadow_config_dir() -> PathBuf {
    Path::new(&user_home_dir().unwrap_or_default()).join(".cmuxterm").join("omo-config")
}

fn file_exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

fn dir_exists(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|m| m.is_dir())
}

fn read_json_object(path: &Path) -> Option<Map<String, Value>> {
    let data = std::fs::read(path).ok()?;
    match serde_json::from_slice::<Value>(&data) {
        Ok(Value::Object(map)) => Some(map),
        _ => None,
    }
}

/// Create a shadow config directory that layers the oh-my-opencode plugin on
/// top of the user's opencode config, installing the plugin if needed.
/// Returns the directory to export as `OPENCODE_CONFIG_DIR`.
#[allow(clippy::too_many_lines)]
pub fn omo_ensure_plugin(search_path: &str, stderr: &mut dyn Write) -> Result<String, String> {
    let user_dir = omo_user_config_dir();
    let shadow_dir = omo_shadow_config_dir();
    std::fs::create_dir_all(&shadow_dir).map_err(|e| {
        format!(
            "create shadow config dir: {}",
            path_error("mkdir", &shadow_dir.to_string_lossy(), &e)
        )
    })?;
    let user_json = user_dir.join("opencode.json");
    let shadow_json = shadow_dir.join("opencode.json");
    let mut config: Map<String, Value> = match std::fs::read(&user_json) {
        Ok(data) => match serde_json::from_slice::<Value>(&data) {
            Ok(Value::Object(map)) => map,
            _ => return Err("invalid opencode.json: fix the JSON syntax and retry".to_string()),
        },
        Err(_) => Map::new(),
    };
    let mut plugins: Vec<String> = config
        .get("plugin")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default();
    let already = plugins
        .iter()
        .any(|p| p == OMO_PLUGIN_NAME || p.starts_with(&format!("{OMO_PLUGIN_NAME}@")));
    if !already {
        plugins.push(OMO_PLUGIN_NAME.to_string());
    }
    config.insert("plugin".into(), Value::Array(plugins.into_iter().map(Value::from).collect()));
    let output = crate::util::go_json_indent(&Value::Object(config));
    std::fs::write(&shadow_json, output)
        .map_err(|e| path_error("open", &shadow_json.to_string_lossy(), &e))?;

    let shadow_node_modules = shadow_dir.join("node_modules");
    let user_node_modules = user_dir.join("node_modules");
    if dir_exists(&user_node_modules) {
        let target = std::fs::read_link(&shadow_node_modules).unwrap_or_default();
        if target != user_node_modules {
            let _ = std::fs::remove_file(&shadow_node_modules);
            let _ = std::os::unix::fs::symlink(&user_node_modules, &shadow_node_modules);
        }
    }
    for filename in ["package.json", "bun.lock", "oh-my-opencode.json", "oh-my-opencode.jsonc"] {
        let user_file = user_dir.join(filename);
        let shadow_file = shadow_dir.join(filename);
        if file_exists(&user_file) && !file_exists(&shadow_file) {
            let _ = std::os::unix::fs::symlink(&user_file, &shadow_file);
        }
    }

    let plugin_package_dir = shadow_node_modules.join(OMO_PLUGIN_NAME);
    if !dir_exists(&plugin_package_dir) {
        let mut install_dir = user_dir.clone();
        if !dir_exists(&user_node_modules) {
            install_dir = shadow_dir.clone();
            let _ = std::fs::remove_file(&shadow_node_modules);
        }
        let _ = std::fs::create_dir_all(&install_dir);
        let bun_path = find_executable_in_path("bun", search_path, "");
        let npm_path = find_executable_in_path("npm", search_path, "");
        if bun_path.is_empty() && npm_path.is_empty() {
            return Err("neither bun nor npm found in PATH. Install oh-my-opencode manually: bunx oh-my-opencode install".to_string());
        }
        let _ = writeln!(stderr, "Installing oh-my-opencode plugin...");
        let mut cmd = if bun_path.is_empty() {
            let mut c = Command::new(&npm_path);
            c.args(["install", OMO_PLUGIN_NAME]);
            c
        } else {
            let mut c = Command::new(&bun_path);
            c.args(["add", OMO_PLUGIN_NAME]);
            c
        };
        cmd.current_dir(&install_dir);
        cmd.stdout(std::process::Stdio::inherit()).stderr(std::process::Stdio::inherit());
        let status = cmd.status().map_err(|e| {
            format!(
                "failed to install oh-my-opencode: {}\nTry manually: npm install -g oh-my-opencode",
                io_error_text(&e)
            )
        })?;
        if !status.success() {
            return Err(format!(
                "failed to install oh-my-opencode: exit status {}\nTry manually: npm install -g oh-my-opencode",
                status.code().unwrap_or(-1)
            ));
        }
        let _ = writeln!(stderr, "oh-my-opencode plugin installed");
        if install_dir == user_dir && !file_exists(&shadow_node_modules) {
            let _ = std::os::unix::fs::symlink(&user_node_modules, &shadow_node_modules);
        }
    }

    let omo_config_path = shadow_dir.join("oh-my-opencode.json");
    let mut omo_config = read_json_object(&omo_config_path);
    if omo_config.is_none() {
        let user_omo = user_dir.join("oh-my-opencode.json");
        if let Some(map) = read_json_object(&user_omo) {
            omo_config = Some(map);
            let _ = std::fs::remove_file(&omo_config_path);
        }
    }
    let mut omo_config = omo_config.unwrap_or_default();
    let mut tmux_config =
        omo_config.get("tmux").and_then(Value::as_object).cloned().unwrap_or_default();
    let mut needs_write = false;
    if !tmux_config.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
        tmux_config.insert("enabled".into(), Value::Bool(true));
        needs_write = true;
    }
    for (key, value) in
        [("main_pane_min_width", 60), ("agent_pane_min_width", 30), ("main_pane_size", 50)]
    {
        if tmux_config.get(key).is_none_or(Value::is_null) {
            tmux_config.insert(key.into(), Value::from(value));
            needs_write = true;
        }
    }
    if needs_write {
        omo_config.insert("tmux".into(), Value::Object(tmux_config));
        if std::fs::read_link(&omo_config_path).is_ok() {
            let _ = std::fs::remove_file(&omo_config_path);
        }
        let _ = std::fs::write(
            &omo_config_path,
            crate::util::go_json_indent(&Value::Object(omo_config)),
        );
    }
    Ok(shadow_dir.to_string_lossy().into_owned())
}

// --- Node script resolution ---

#[must_use]
pub fn resolve_node_script_exec(
    bin_path: &str,
    args: &[String],
    search_path: &str,
    skip_dir: &str,
) -> (String, Vec<String>) {
    let direct = || {
        let mut argv = vec![bin_path.to_string()];
        argv.extend(args.iter().cloned());
        (bin_path.to_string(), argv)
    };
    if !is_node_script(bin_path) {
        return direct();
    }
    if !find_executable_in_path("node", search_path, skip_dir).is_empty() {
        return direct();
    }
    let bun_path = find_executable_in_path("bun", search_path, skip_dir);
    if !bun_path.is_empty() {
        let mut argv = vec![bun_path.clone(), bin_path.to_string()];
        argv.extend(args.iter().cloned());
        return (bun_path, argv);
    }
    direct()
}

#[must_use]
pub fn is_node_script(path: &str) -> bool {
    let Ok(mut f) = std::fs::File::open(path) else { return false };
    let mut buf = [0u8; 64];
    let n = f.read(&mut buf).unwrap_or(0);
    let line = String::from_utf8_lossy(&buf[..n]);
    line.contains("/env node") || line.contains("/bin/node")
}

/// Search `path_env` for an executable, skipping `skip_dir` (the shim dir).
#[must_use]
pub fn find_executable_in_path(name: &str, path_env: &str, skip_dir: &str) -> String {
    use std::os::unix::fs::PermissionsExt;
    for dir in path_env.split(':') {
        if dir.is_empty() || dir == skip_dir {
            continue;
        }
        let candidate = Path::new(dir).join(name);
        if let Ok(info) = std::fs::metadata(&candidate)
            && !info.is_dir()
            && info.permissions().mode() & 0o111 != 0
        {
            return candidate.to_string_lossy().into_owned();
        }
    }
    String::new()
}

#[must_use]
pub fn claude_teams_launch_args(args: &[String]) -> Vec<String> {
    if args.iter().any(|a| a == "--teammate-mode" || a.starts_with("--teammate-mode=")) {
        return args.to_vec();
    }
    let mut out = vec!["--teammate-mode".to_string(), "auto".to_string()];
    out.extend(args.iter().cloned());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_options_merge_and_clean() {
        assert_eq!(merge_node_options("", "/r.cjs"), "--require=/r.cjs --max-old-space-size=4096");
        assert_eq!(
            merge_node_options(
                "--max-old-space-size=8192 --inspect --max-old-space-size 100 --x",
                "/r.cjs"
            ),
            "--require=/r.cjs --max-old-space-size=4096 --inspect --x"
        );
        assert_eq!(cleaned_node_options("  --a   --b "), "--a --b");
        let mut env = Env::new();
        env.insert("NODE_OPTIONS".into(), "--inspect".into());
        configure_claude_node_options(&mut env, "/r.cjs");
        assert_eq!(env["CMUX_ORIGINAL_NODE_OPTIONS_PRESENT"], "1");
        assert_eq!(env["CMUX_ORIGINAL_NODE_OPTIONS"], "--inspect");
        let mut env = Env::new();
        configure_claude_node_options(&mut env, "/r.cjs");
        assert_eq!(env["CMUX_ORIGINAL_NODE_OPTIONS_PRESENT"], "0");
        assert!(!env.contains_key("CMUX_ORIGINAL_NODE_OPTIONS"));
    }

    #[test]
    fn teammate_mode_default() {
        let args = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        assert_eq!(
            claude_teams_launch_args(&args(&["-p", "x"])),
            args(&["--teammate-mode", "auto", "-p", "x"])
        );
        assert_eq!(
            claude_teams_launch_args(&args(&["--teammate-mode=off"])),
            args(&["--teammate-mode=off"])
        );
    }

    #[test]
    fn focused_context_parsing_and_tmux_env() {
        let focused: Map<String, Value> = serde_json::from_str(
            r#"{"workspace_ref":"workspace:1","pane_ref":" pane:2 ","surface_id":"s1","window_id":"w1","pane_uuid":"11111111-1111-1111-1111-111111111111"}"#,
        )
        .unwrap();
        let ctx = focused_context_from_identify(&focused).unwrap();
        assert_eq!(ctx.workspace_id, "workspace:1");
        assert_eq!(ctx.pane_handle, "pane:2");
        assert_eq!(ctx.pane_id, "11111111-1111-1111-1111-111111111111");
        let (tmux, pane) = fake_tmux_env("cmux-omx", Some(&ctx));
        assert_eq!(
            tmux,
            format!("/tmp/cmux-omx/workspace:1,w1,{}", tmux_stable_numeric_id(&ctx.pane_id))
        );
        assert_eq!(pane, format!("%{}", tmux_stable_numeric_id(&ctx.pane_id)));
        assert_eq!(
            fake_tmux_env("cmux-omo", None),
            ("/tmp/cmux-omo/default,0,0".to_string(), "%1".to_string())
        );
        assert!(focused_context_from_identify(&Map::new()).is_none());
        let mut env = Env::new();
        env.insert("PATH".into(), "/usr/bin".into());
        env.insert("TERM_PROGRAM".into(), "ghostty".into());
        env.insert("CMUX_SOCKET".into(), "x".into());
        configure_agent_environment(
            &mut env,
            &AgentConfig {
                shim_dir: "/shim".into(),
                socket_path: "127.0.0.1:1".into(),
                focused: Some(&ctx),
                tmux_path_prefix: "cmux-omx",
                cmux_bin_env_var: "CMUX_OMX_CMUX_BIN",
                term_env_var: "CMUX_OMX_TERM",
                extra_env: &[("EXTRA", "1")],
            },
        );
        assert_eq!(env["PATH"], "/shim:/usr/bin");
        assert_eq!(env["TERM"], "screen-256color");
        assert_eq!(env["COLORTERM"], "truecolor");
        assert_eq!(env["CMUX_WORKSPACE_ID"], "workspace:1");
        assert_eq!(env["CMUX_SURFACE_ID"], "s1");
        assert_eq!(env["EXTRA"], "1");
        assert!(!env.contains_key("TERM_PROGRAM") && !env.contains_key("CMUX_SOCKET"));
    }

    #[test]
    fn shim_write_is_idempotent_and_executable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tmux");
        write_shim_if_changed(&path, "#!/bin/sh\necho hi\n").unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o755);
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        write_shim_if_changed(&path, "#!/bin/sh\necho hi\n").unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), before);
        assert!(!is_node_script(&path.to_string_lossy()));
        std::fs::write(&path, "#!/usr/bin/env node\n").unwrap();
        assert!(is_node_script(&path.to_string_lossy()));
        assert!(
            std::fs::read_dir(dir.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with('.'))
        );
    }
}

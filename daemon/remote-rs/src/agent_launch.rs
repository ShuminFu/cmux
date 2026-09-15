//! Agent launch relays (`cmux claude-teams`, `cmux omo`, `cmux omx`, `cmux omc`)
//! that install tmux shims, derive the focused cmux context, configure the
//! environment and `exec` into the agent. Mirrors `agent_launch.go`.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::cli::{CliIo, RefreshAddr, RpcContext};
use crate::tmux_compat::{
    tmux_canonical_pane_id, tmux_resolve_workspace_id, tmux_stable_numeric_id,
};
use crate::util::{go_json_pretty, home_dir, path_base, path_dir, path_join, random_hex, temp_dir};

pub const CLAUDE_NODE_OPTIONS_RESTORE_MODULE_SCRIPT: &str =
    "const hadOriginalNodeOptions = process.env.CMUX_ORIGINAL_NODE_OPTIONS_PRESENT === \"1\";
if (hadOriginalNodeOptions) {
  process.env.NODE_OPTIONS = process.env.CMUX_ORIGINAL_NODE_OPTIONS ?? \"\";
} else {
  delete process.env.NODE_OPTIONS;
}
delete process.env.CMUX_ORIGINAL_NODE_OPTIONS;
delete process.env.CMUX_ORIGINAL_NODE_OPTIONS_PRESENT;
";

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

fn exec_argv(path: &str, argv: &[String]) -> std::io::Error {
    let mut cmd = Command::new(path);
    if let Some(first) = argv.first() {
        cmd.arg0(first);
    }
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.exec()
}

/// `cmux claude-teams` on the remote side: create tmux shim scripts, set up
/// environment variables, get the focused context via system.identify, and
/// exec into `claude`.
pub fn run_claude_teams_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext {
        socket_path: socket_path.to_string(),
        refresh_addr,
    };

    let shim_dir = match create_tmux_shim_dir("claude-teams-bin", CLAUDE_TEAMS_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux claude-teams: failed to create shim directory: {err}"
            );
            return 1;
        }
    };

    // Resolve the agent executable BEFORE modifying PATH (so the shim
    // directory doesn't shadow anything).
    let original_path = std::env::var("PATH").unwrap_or_default();
    let claude_path = find_executable_in_path("claude", &original_path, &shim_dir);

    let focused = get_focused_context(&rc);

    configure_agent_environment(&AgentConfig {
        shim_dir: shim_dir.clone(),
        socket_path: socket_path.to_string(),
        focused,
        tmux_path_prefix: "cmux-claude-teams".to_string(),
        cmux_bin_env_var: "CMUX_CLAUDE_TEAMS_CMUX_BIN".to_string(),
        term_env_var: "CMUX_CLAUDE_TEAMS_TERM".to_string(),
        extra_env: HashMap::from([(
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS".to_string(),
            "1".to_string(),
        )]),
    });
    if let Ok(restore_module_path) = ensure_claude_node_options_restore_module() {
        configure_claude_node_options(&restore_module_path);
    }

    let launch_args = claude_teams_launch_args(args);

    if claude_path.is_empty() {
        let _ = writeln!(io.stderr, "cmux claude-teams: claude not found in PATH");
        return 1;
    }
    let mut argv = vec![claude_path.clone()];
    argv.extend(launch_args);
    let exec_err = exec_argv(&claude_path, &argv);
    let _ = writeln!(io.stderr, "cmux claude-teams: exec failed: {exec_err}");
    1
}

/// `cmux omo` on the remote side.
pub fn run_omo_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext {
        socket_path: socket_path.to_string(),
        refresh_addr,
    };

    let shim_dir = match create_omo_shim_dir() {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux omo: failed to create shim directory: {err}"
            );
            return 1;
        }
    };

    let original_path = std::env::var("PATH").unwrap_or_default();
    let opencode_path = find_executable_in_path("opencode", &original_path, &shim_dir);
    if opencode_path.is_empty() {
        let _ = write!(
            io.stderr,
            "cmux omo: opencode not found in PATH\nInstall it first:\n  npm install -g opencode-ai\n  # or\n  bun install -g opencode-ai\n"
        );
        return 1;
    }

    if let Err(err) = omo_ensure_plugin(&original_path, io.stderr) {
        let _ = writeln!(io.stderr, "cmux omo: plugin setup: {err}");
        return 1;
    }

    let focused = get_focused_context(&rc);

    configure_agent_environment(&AgentConfig {
        shim_dir: shim_dir.clone(),
        socket_path: socket_path.to_string(),
        focused,
        tmux_path_prefix: "cmux-omo".to_string(),
        cmux_bin_env_var: "CMUX_OMO_CMUX_BIN".to_string(),
        term_env_var: "CMUX_OMO_TERM".to_string(),
        extra_env: HashMap::new(),
    });

    if std::env::var("OPENCODE_PORT")
        .unwrap_or_default()
        .is_empty()
    {
        std::env::set_var("OPENCODE_PORT", "4096");
    }

    let mut launch_args: Vec<String> = args.to_vec();
    let has_port = launch_args
        .iter()
        .any(|arg| arg == "--port" || arg.starts_with("--port="));
    if !has_port {
        let mut port = std::env::var("OPENCODE_PORT").unwrap_or_default();
        if port.is_empty() {
            port = "4096".to_string();
        }
        let mut with_port = vec!["--port".to_string(), port];
        with_port.extend(launch_args);
        launch_args = with_port;
    }

    let (launch_path, launch_argv) =
        resolve_node_script_exec(&opencode_path, &launch_args, &original_path, &shim_dir);
    let exec_err = exec_argv(&launch_path, &launch_argv);
    let _ = writeln!(io.stderr, "cmux omo: exec failed: {exec_err}");
    1
}

/// `cmux omx` on the remote side.
pub fn run_omx_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext {
        socket_path: socket_path.to_string(),
        refresh_addr,
    };

    let shim_dir = match create_tmux_shim_dir("omx-bin", OMX_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux omx: failed to create shim directory: {err}"
            );
            return 1;
        }
    };

    let original_path = std::env::var("PATH").unwrap_or_default();
    let omx_path = find_executable_in_path("omx", &original_path, &shim_dir);
    if omx_path.is_empty() {
        let _ = write!(
            io.stderr,
            "cmux omx: omx not found in PATH\nInstall it first:\n  npm install -g oh-my-codex\n"
        );
        return 1;
    }

    let focused = get_focused_context(&rc);

    configure_agent_environment(&AgentConfig {
        shim_dir: shim_dir.clone(),
        socket_path: socket_path.to_string(),
        focused,
        tmux_path_prefix: "cmux-omx".to_string(),
        cmux_bin_env_var: "CMUX_OMX_CMUX_BIN".to_string(),
        term_env_var: "CMUX_OMX_TERM".to_string(),
        extra_env: HashMap::new(),
    });

    let (launch_path, launch_argv) =
        resolve_node_script_exec(&omx_path, args, &original_path, &shim_dir);
    let exec_err = exec_argv(&launch_path, &launch_argv);
    let _ = writeln!(io.stderr, "cmux omx: exec failed: {exec_err}");
    1
}

/// `cmux omc` on the remote side.
pub fn run_omc_relay(
    socket_path: &str,
    args: &[String],
    refresh_addr: RefreshAddr,
    io: &mut CliIo<'_>,
) -> i32 {
    let rc = RpcContext {
        socket_path: socket_path.to_string(),
        refresh_addr,
    };

    let shim_dir = match create_tmux_shim_dir("omc-bin", OMC_SHIM_SCRIPT) {
        Ok(dir) => dir,
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux omc: failed to create shim directory: {err}"
            );
            return 1;
        }
    };

    let original_path = std::env::var("PATH").unwrap_or_default();
    let omc_path = find_executable_in_path("omc", &original_path, &shim_dir);
    if omc_path.is_empty() {
        let _ = write!(io.stderr, "cmux omc: omc not found in PATH\nInstall it first:\n  npm install -g oh-my-claude-sisyphus\n");
        return 1;
    }

    let focused = get_focused_context(&rc);

    configure_agent_environment(&AgentConfig {
        shim_dir: shim_dir.clone(),
        socket_path: socket_path.to_string(),
        focused,
        tmux_path_prefix: "cmux-omc".to_string(),
        cmux_bin_env_var: "CMUX_OMC_CMUX_BIN".to_string(),
        term_env_var: "CMUX_OMC_TERM".to_string(),
        extra_env: HashMap::new(),
    });

    // omc wraps Claude Code, so configure NODE_OPTIONS restore module
    match ensure_claude_node_options_restore_module() {
        Ok(restore_module_path) => configure_claude_node_options(&restore_module_path),
        Err(err) => {
            let _ = writeln!(
                io.stderr,
                "cmux omc: warning: failed to create NODE_OPTIONS restore module: {err}"
            );
        }
    }

    let (launch_path, launch_argv) =
        resolve_node_script_exec(&omc_path, args, &original_path, &shim_dir);
    let exec_err = exec_argv(&launch_path, &launch_argv);
    let _ = writeln!(io.stderr, "cmux omc: exec failed: {exec_err}");
    1
}

// --- Shim creation ---

pub fn create_tmux_shim_dir(dir_name: &str, tmux_script: &str) -> std::io::Result<String> {
    let home = home_dir().ok_or_else(|| std::io::Error::other("$HOME is not defined"))?;
    let dir = path_join(&home, &format!(".cmuxterm/{dir_name}"));
    fs::create_dir_all(&dir)?;
    write_shim_if_changed(&path_join(&dir, "tmux"), tmux_script)?;
    Ok(dir)
}

pub fn create_omo_shim_dir() -> std::io::Result<String> {
    let dir = create_tmux_shim_dir("omo-bin", OMO_TMUX_SHIM_SCRIPT)?;
    write_shim_if_changed(
        &path_join(&dir, "terminal-notifier"),
        OMO_NOTIFIER_SHIM_SCRIPT,
    )?;
    Ok(dir)
}

pub fn write_shim_if_changed(path: &str, content: &str) -> std::io::Result<()> {
    if let Ok(existing) = fs::read(path) {
        if existing == content.as_bytes() {
            return Ok(());
        }
    }
    let dir = path_dir(path);
    let temp_path = path_join(&dir, &format!(".{}.tmp-{}", path_base(path), random_hex(6)));
    let result = (|| {
        fs::write(&temp_path, content.as_bytes())?;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o755))?;
        fs::rename(&temp_path, path)
    })();
    let _ = fs::remove_file(&temp_path);
    result
}

pub fn ensure_claude_node_options_restore_module() -> std::io::Result<String> {
    let dir = temp_dir().join("cmux-claude-node-options");
    fs::create_dir_all(&dir)?;
    let restore_module_path = dir
        .join("restore-node-options.cjs")
        .to_string_lossy()
        .into_owned();
    write_shim_if_changed(
        &restore_module_path,
        CLAUDE_NODE_OPTIONS_RESTORE_MODULE_SCRIPT,
    )?;
    Ok(restore_module_path)
}

// --- Focused context ---

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    // Use a worker thread with a timeout so a slow/stale relay doesn't block
    // agent launch.
    let (tx, rx) = flume::bounded::<Option<Map<String, Value>>>(1);
    let started = Instant::now();
    {
        let rc = rc.clone();
        std::thread::Builder::new()
            .name("cmuxd-identify".to_string())
            .spawn(move || {
                let _ = tx.send(rc.call("system.identify", None).ok());
            })
            .expect("spawn identify thread");
    }
    let payload = match rx.recv_timeout(timeout) {
        Ok(payload) => payload?,
        Err(_) => return None,
    };
    let focused = payload.get("focused").and_then(|v| v.as_object())?.clone();
    let ctx = focused_context_from_identify(&focused)?;

    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Some(ctx);
    }
    Some(canonicalize_focused_context_with_timeout(
        rc, &focused, &ctx, remaining,
    ))
}

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

pub fn canonicalize_focused_context_with_timeout(
    rc: &RpcContext,
    focused: &Map<String, Value>,
    base: &FocusedContext,
    timeout: Duration,
) -> FocusedContext {
    let (tx, rx) = flume::bounded::<FocusedContext>(1);
    {
        let rc = rc.clone();
        let focused = focused.clone();
        let mut enriched = base.clone();
        std::thread::Builder::new()
            .name("cmuxd-canonicalize".to_string())
            .spawn(move || {
                canonicalize_focused_context(&rc, &focused, &mut enriched);
                let _ = tx.send(enriched);
            })
            .expect("spawn canonicalize thread");
    }
    match rx.recv_timeout(timeout) {
        Ok(enriched) => enriched,
        Err(_) => base.clone(),
    }
}

pub fn canonicalize_focused_context(
    rc: &RpcContext,
    focused: &Map<String, Value>,
    ctx: &mut FocusedContext,
) {
    let mut canonical_pane_id = string_from_any(&[focused.get("pane_uuid")])
        .trim()
        .to_string();
    if let Ok(canonical_ws_id) = tmux_resolve_workspace_id(rc, &ctx.workspace_id) {
        if canonical_pane_id.is_empty() {
            let pid = string_from_any(&[focused.get("pane_id")])
                .trim()
                .to_string();
            if !pid.is_empty() {
                if let Ok(resolved) = tmux_canonical_pane_id(rc, &pid, &canonical_ws_id) {
                    canonical_pane_id = resolved;
                }
            }
        }
        if canonical_pane_id.is_empty() {
            if let Ok(pid) = tmux_canonical_pane_id(rc, &ctx.pane_handle, &canonical_ws_id) {
                canonical_pane_id = pid;
            }
        }
    }
    if canonical_pane_id.is_empty() {
        canonical_pane_id = string_from_any(&[focused.get("pane_id")])
            .trim()
            .to_string();
    }
    if !canonical_pane_id.is_empty() {
        ctx.pane_id = canonical_pane_id.trim().to_string();
    }
}

pub fn configure_claude_node_options(restore_module_path: &str) {
    let existing = std::env::var("NODE_OPTIONS").ok();
    match &existing {
        Some(value) => {
            std::env::set_var("CMUX_ORIGINAL_NODE_OPTIONS_PRESENT", "1");
            std::env::set_var("CMUX_ORIGINAL_NODE_OPTIONS", value);
        }
        None => {
            std::env::set_var("CMUX_ORIGINAL_NODE_OPTIONS_PRESENT", "0");
            std::env::remove_var("CMUX_ORIGINAL_NODE_OPTIONS");
        }
    }
    std::env::set_var(
        "NODE_OPTIONS",
        merge_node_options(existing.as_deref().unwrap_or(""), restore_module_path),
    );
}

pub fn merge_node_options(existing: &str, restore_module_path: &str) -> String {
    let require_flag = format!("--require={restore_module_path}");
    const MEMORY_FLAG: &str = "--max-old-space-size=4096";
    let cleaned = cleaned_node_options(existing);
    if cleaned.is_empty() {
        return format!("{require_flag} {MEMORY_FLAG}");
    }
    format!("{require_flag} {MEMORY_FLAG} {cleaned}")
}

pub fn cleaned_node_options(existing: &str) -> String {
    let tokens: Vec<&str> = existing.split_whitespace().collect();
    if tokens.is_empty() {
        return String::new();
    }
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

pub fn string_from_any(values: &[Option<&Value>]) -> String {
    for value in values {
        if let Some(Value::String(s)) = value {
            if !s.trim().is_empty() {
                return s.trim().to_string();
            }
        }
    }
    String::new()
}

// --- Environment configuration ---

#[derive(Clone, Debug, Default)]
pub struct AgentConfig {
    pub shim_dir: String,
    pub socket_path: String,
    pub focused: Option<FocusedContext>,
    pub tmux_path_prefix: String,
    pub cmux_bin_env_var: String,
    pub term_env_var: String,
    pub extra_env: HashMap<String, String>,
}

pub fn configure_agent_environment(cfg: &AgentConfig) {
    // Find our own executable path for the shim to call back
    let mut self_path = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    if self_path.is_empty() {
        self_path = "cmux".to_string();
    }
    std::env::set_var(&cfg.cmux_bin_env_var, self_path);

    // Prepend shim directory to PATH
    let current_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", cfg.shim_dir, current_path));

    // Set fake TMUX/TMUX_PANE
    let mut fake_tmux = format!("/tmp/{}/default,0,0", cfg.tmux_path_prefix);
    let mut fake_tmux_pane = "%1".to_string();
    if let Some(focused) = &cfg.focused {
        let mut window_token = focused.window_id.clone();
        if window_token.is_empty() {
            window_token = focused.workspace_id.clone();
        }
        let mut pane_id_for_token = focused.pane_id.clone();
        if pane_id_for_token.is_empty() {
            pane_id_for_token = focused.pane_handle.clone();
        }
        let pane_token = tmux_stable_numeric_id(&pane_id_for_token);
        fake_tmux = format!(
            "/tmp/{}/{},{},{}",
            cfg.tmux_path_prefix, focused.workspace_id, window_token, pane_token
        );
        fake_tmux_pane = format!("%{pane_token}");
    }
    std::env::set_var("TMUX", fake_tmux);
    std::env::set_var("TMUX_PANE", fake_tmux_pane);

    // Terminal settings
    let mut fake_term = std::env::var(&cfg.term_env_var).unwrap_or_default();
    if fake_term.is_empty() {
        fake_term = "screen-256color".to_string();
    }
    std::env::set_var("TERM", fake_term);

    // Socket path
    std::env::set_var("CMUX_SOCKET_PATH", &cfg.socket_path);
    std::env::remove_var("CMUX_SOCKET");

    // Unset TERM_PROGRAM so apps don't detect the host terminal and override
    // tmux-compatible behavior.
    std::env::remove_var("TERM_PROGRAM");

    // Preserve COLORTERM for truecolor support in subagent panes.
    if std::env::var("COLORTERM").unwrap_or_default().is_empty() {
        std::env::set_var("COLORTERM", "truecolor");
    }

    // Set workspace/surface IDs from focused context
    if let Some(focused) = &cfg.focused {
        std::env::set_var("CMUX_WORKSPACE_ID", &focused.workspace_id);
        if !focused.surface_id.is_empty() {
            std::env::set_var("CMUX_SURFACE_ID", &focused.surface_id);
        }
    }

    for (k, v) in &cfg.extra_env {
        std::env::set_var(k, v);
    }
}

// --- oh-my-opencode plugin setup ---

const OMO_PLUGIN_NAME: &str = "oh-my-opencode";

pub fn omo_user_config_dir() -> String {
    path_join(&home_dir().unwrap_or_default(), ".config/opencode")
}

pub fn omo_shadow_config_dir() -> String {
    path_join(&home_dir().unwrap_or_default(), ".cmuxterm/omo-config")
}

/// Create a shadow config directory that layers the oh-my-opencode plugin on
/// top of the user's opencode config, install the plugin if needed, and set
/// `OPENCODE_CONFIG_DIR`.
pub fn omo_ensure_plugin(search_path: &str, stderr: &mut dyn Write) -> Result<(), String> {
    let user_dir = omo_user_config_dir();
    let shadow_dir = omo_shadow_config_dir();

    fs::create_dir_all(&shadow_dir).map_err(|err| format!("create shadow config dir: {err}"))?;

    // Read user's opencode.json, add the plugin, write to shadow dir
    let user_json_path = path_join(&user_dir, "opencode.json");
    let shadow_json_path = path_join(&shadow_dir, "opencode.json");

    let mut config: Map<String, Value> = match fs::read(&user_json_path) {
        Ok(data) => match serde_json::from_slice::<Value>(&data) {
            Ok(Value::Object(map)) => map,
            Ok(Value::Null) => Map::new(),
            _ => return Err("invalid opencode.json: fix the JSON syntax and retry".to_string()),
        },
        Err(_) => Map::new(),
    };

    // Add oh-my-opencode to the plugins list
    let mut plugins: Vec<String> = match config.get("plugin") {
        Some(Value::Array(raw)) => raw
            .iter()
            .filter_map(|p| p.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    };
    let already_present = plugins
        .iter()
        .any(|p| p == OMO_PLUGIN_NAME || p.starts_with(&format!("{OMO_PLUGIN_NAME}@")));
    if !already_present {
        plugins.push(OMO_PLUGIN_NAME.to_string());
    }
    config.insert(
        "plugin".to_string(),
        Value::Array(plugins.into_iter().map(Value::String).collect()),
    );

    let output = go_json_pretty(&Value::Object(config));
    fs::write(&shadow_json_path, output.as_bytes()).map_err(|err| err.to_string())?;

    // Symlink node_modules from user config dir
    let shadow_node_modules = path_join(&shadow_dir, "node_modules");
    let user_node_modules = path_join(&user_dir, "node_modules");
    if dir_exists(&user_node_modules) {
        let target = fs::read_link(&shadow_node_modules)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if target != user_node_modules {
            let _ = fs::remove_file(&shadow_node_modules);
            let _ = std::os::unix::fs::symlink(&user_node_modules, &shadow_node_modules);
        }
    }

    // Symlink package.json and bun.lock
    for filename in ["package.json", "bun.lock"] {
        let user_file = path_join(&user_dir, filename);
        let shadow_file = path_join(&shadow_dir, filename);
        if file_exists(&user_file) && !file_exists(&shadow_file) {
            let _ = std::os::unix::fs::symlink(&user_file, &shadow_file);
        }
    }

    // Symlink oh-my-opencode config files
    for filename in ["oh-my-opencode.json", "oh-my-opencode.jsonc"] {
        let user_file = path_join(&user_dir, filename);
        let shadow_file = path_join(&shadow_dir, filename);
        if file_exists(&user_file) && !file_exists(&shadow_file) {
            let _ = std::os::unix::fs::symlink(&user_file, &shadow_file);
        }
    }

    // Install the plugin if not available
    let plugin_package_dir = path_join(&shadow_node_modules, OMO_PLUGIN_NAME);
    if !dir_exists(&plugin_package_dir) {
        let mut install_dir = user_dir.clone();
        if !dir_exists(&user_node_modules) {
            install_dir = shadow_dir.clone();
            let _ = fs::remove_file(&shadow_node_modules); // Remove symlink so we can install directly
        }
        let _ = fs::create_dir_all(&install_dir);

        let bun_path = find_executable_in_path("bun", search_path, "");
        let npm_path = find_executable_in_path("npm", search_path, "");
        if bun_path.is_empty() && npm_path.is_empty() {
            return Err("neither bun nor npm found in PATH. Install oh-my-opencode manually: bunx oh-my-opencode install".to_string());
        }

        let _ = writeln!(stderr, "Installing oh-my-opencode plugin...");
        let mut cmd = if !bun_path.is_empty() {
            let mut cmd = Command::new(&bun_path);
            cmd.arg("add").arg(OMO_PLUGIN_NAME);
            cmd
        } else {
            let mut cmd = Command::new(&npm_path);
            cmd.arg("install").arg(OMO_PLUGIN_NAME);
            cmd
        };
        cmd.current_dir(&install_dir);
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        match cmd.status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                return Err(format!(
                    "failed to install oh-my-opencode: exit status {}\nTry manually: npm install -g oh-my-opencode",
                    status.code().unwrap_or(-1)
                ))
            }
            Err(err) => return Err(format!("failed to install oh-my-opencode: {err}\nTry manually: npm install -g oh-my-opencode")),
        }
        let _ = writeln!(stderr, "oh-my-opencode plugin installed");

        // Re-create symlink if we installed into user dir
        if install_dir == user_dir && !file_exists(&shadow_node_modules) {
            let _ = std::os::unix::fs::symlink(&user_node_modules, &shadow_node_modules);
        }
    }

    // Configure oh-my-opencode.json with tmux settings
    let omo_config_path = path_join(&shadow_dir, "oh-my-opencode.json");
    let mut omo_config: Option<Map<String, Value>> = fs::read(&omo_config_path)
        .ok()
        .and_then(|data| serde_json::from_slice::<Value>(&data).ok())
        .and_then(|v| v.as_object().cloned());
    if omo_config.is_none() {
        // Check if user had one we symlinked
        let user_omo_config = path_join(&user_dir, "oh-my-opencode.json");
        if let Ok(data) = fs::read(&user_omo_config) {
            omo_config = serde_json::from_slice::<Value>(&data)
                .ok()
                .and_then(|v| v.as_object().cloned());
            let _ = fs::remove_file(&omo_config_path); // Remove symlink so we can write our own copy
        }
    }
    let mut omo_config = omo_config.unwrap_or_default();

    let mut tmux_config = omo_config
        .get("tmux")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let mut needs_write = false;
    if !tmux_config
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        tmux_config.insert("enabled".to_string(), Value::Bool(true));
        needs_write = true;
    }
    for (key, value) in [
        ("main_pane_min_width", 60),
        ("agent_pane_min_width", 30),
        ("main_pane_size", 50),
    ] {
        if tmux_config.get(key).map(|v| v.is_null()).unwrap_or(true) {
            tmux_config.insert(key.to_string(), Value::from(value));
            needs_write = true;
        }
    }
    if needs_write {
        omo_config.insert("tmux".to_string(), Value::Object(tmux_config));
        // Remove symlink if it exists
        if fs::read_link(&omo_config_path)
            .map(|p| !p.as_os_str().is_empty())
            .unwrap_or(false)
        {
            let _ = fs::remove_file(&omo_config_path);
        }
        let data = go_json_pretty(&Value::Object(omo_config));
        let _ = fs::write(&omo_config_path, data.as_bytes());
    }

    std::env::set_var("OPENCODE_CONFIG_DIR", &shadow_dir);
    Ok(())
}

pub fn file_exists(path: &str) -> bool {
    fs::symlink_metadata(path).is_ok()
}

pub fn dir_exists(path: &str) -> bool {
    fs::metadata(path)
        .map(|info| info.is_dir())
        .unwrap_or(false)
}

// --- Node script resolution ---

/// If the target binary is a `#!/usr/bin/env node` script and node is not in
/// PATH but bun is, rewrite the exec to use bun as the runtime.
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

pub fn is_node_script(path: &str) -> bool {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut buf = [0u8; 64];
    let n = file.read(&mut buf).unwrap_or(0);
    let line = String::from_utf8_lossy(&buf[..n]);
    line.contains("/env node") || line.contains("/bin/node")
}

// --- Executable resolution ---

/// Search the given PATH string for an executable, skipping `skip_dir` (the
/// shim directory).
pub fn find_executable_in_path(name: &str, path_env: &str, skip_dir: &str) -> String {
    for dir in path_env.split(':') {
        if dir.is_empty() || dir == skip_dir {
            continue;
        }
        let candidate = path_join(dir, name);
        if let Ok(info) = fs::metadata(&candidate) {
            if !info.is_dir() && info.permissions().mode() & 0o111 != 0 {
                return candidate;
            }
        }
    }
    String::new()
}

// --- Claude Teams launch args ---

pub fn claude_teams_launch_args(args: &[String]) -> Vec<String> {
    if args
        .iter()
        .any(|arg| arg == "--teammate-mode" || arg.starts_with("--teammate-mode="))
    {
        return args.to_vec();
    }
    let mut out = vec!["--teammate-mode".to_string(), "auto".to_string()];
    out.extend(args.iter().cloned());
    out
}

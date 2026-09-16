//! The relay's command table: CLI command name → v2 JSON-RPC method and the
//! flags it accepts, with the relay-specific overrides already applied.

use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub name: &'static str,
    pub v2_method: &'static str,
    pub flag_keys: &'static [&'static str],
    pub bool_flags: &'static [&'static str],
    pub no_params: bool,
    pub param_key_overrides: HashMap<&'static str, &'static str>,
    pub default_params: serde_json::Map<String, Value>,
    pub positional_key: &'static str,
    pub repeat_keys: &'static [&'static str],
}

#[derive(Debug, Clone, Default)]
pub struct CommandOverride {
    pub param_key_overrides: &'static [(&'static str, &'static str)],
    pub disable_positional: bool,
    pub default_params: &'static [(&'static str, &'static str)],
    pub special_dispatch: bool,
    pub client_only_flags: &'static [&'static str],
}

struct Base {
    name: &'static str,
    v2_method: &'static str,
    flag_keys: &'static [&'static str],
    bool_flags: &'static [&'static str],
    no_params: bool,
    positional_key: &'static str,
    repeat_keys: &'static [&'static str],
}

const fn base(
    name: &'static str,
    v2_method: &'static str,
    flag_keys: &'static [&'static str],
    bool_flags: &'static [&'static str],
    positional_key: &'static str,
) -> Base {
    Base {
        name,
        v2_method,
        flag_keys,
        bool_flags,
        no_params: false,
        positional_key,
        repeat_keys: &[],
    }
}

const fn no_params(name: &'static str, v2_method: &'static str) -> Base {
    Base {
        name,
        v2_method,
        flag_keys: &[],
        bool_flags: &[],
        no_params: true,
        positional_key: "",
        repeat_keys: &[],
    }
}

const COMMANDS: &[Base] = &[
    base(
        "break-pane",
        "pane.break",
        &["pane", "surface", "workspace", "window", "focus", "no-focus"],
        &["focus", "no-focus"],
        "",
    ),
    no_params("capabilities", "system.capabilities"),
    base("clear-history", "surface.clear_history", &["surface", "workspace", "window"], &[], ""),
    base("close-surface", "surface.close", &["surface", "panel", "workspace", "window"], &[], ""),
    base("close-window", "window.close", &["window"], &[], ""),
    base("close-workspace", "workspace.close", &["workspace", "window"], &[], ""),
    no_params("current-window", "window.current"),
    base("current-workspace", "workspace.current", &["window"], &[], ""),
    base("dismiss-notification", "notification.dismiss", &["id", "all-read"], &["all-read"], ""),
    base("equalize-splits", "workspace.equalize_splits", &["workspace", "window"], &[], ""),
    base("focus-panel", "surface.focus", &["panel", "workspace", "window"], &[], ""),
    base("focus-window", "window.focus", &["window"], &[], ""),
    base(
        "join-pane",
        "pane.join",
        &["target-pane", "pane", "surface", "workspace", "window", "focus", "no-focus"],
        &["focus", "no-focus"],
        "",
    ),
    no_params("jump-to-unread", "notification.jump_to_unread"),
    base("last-pane", "pane.last", &["workspace", "window"], &[], ""),
    base("last-workspace", "workspace.last", &["window"], &[], ""),
    base("list-pane-surfaces", "pane.surfaces", &["pane", "workspace", "window"], &[], ""),
    base("list-panes", "pane.list", &["workspace", "window"], &[], ""),
    base("list-panels", "surface.list", &["workspace", "window"], &[], ""),
    no_params("list-windows", "window.list"),
    base("list-workspaces", "workspace.list", &["window"], &[], ""),
    base(
        "mark-notification-read",
        "notification.mark_read",
        &["id", "workspace", "surface", "window", "all"],
        &["all"],
        "",
    ),
    base("move-workspace-to-window", "workspace.move_to_window", &["workspace", "window"], &[], ""),
    base(
        "new-pane",
        "pane.create",
        &["type", "direction", "placement", "workspace", "window", "url", "focus"],
        &["focus"],
        "",
    ),
    base(
        "new-split",
        "surface.split",
        &["surface", "panel", "workspace", "window", "focus"],
        &["focus"],
        "direction",
    ),
    base(
        "new-surface",
        "surface.create",
        &[
            "type",
            "pane",
            "placement",
            "workspace",
            "window",
            "url",
            "provider",
            "renderer",
            "working-directory",
            "focus",
        ],
        &["focus"],
        "",
    ),
    no_params("new-window", "window.create"),
    Base {
        name: "new-workspace",
        v2_method: "workspace.create",
        flag_keys: &[
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
        ],
        bool_flags: &["focus"],
        no_params: false,
        positional_key: "",
        repeat_keys: &["env"],
    },
    base("next-workspace", "workspace.next", &["window"], &[], ""),
    base(
        "notify",
        "notification.create",
        &["title", "subtitle", "body", "workspace", "surface", "window"],
        &[],
        "",
    ),
    base("open-notification", "notification.open", &["id"], &[], ""),
    no_params("ping", "system.ping"),
    base("previous-workspace", "workspace.previous", &["window"], &[], ""),
    base(
        "read-screen",
        "surface.read_text",
        &["surface", "workspace", "window", "scrollback", "lines"],
        &["scrollback"],
        "",
    ),
    no_params("refresh-surfaces", "surface.refresh"),
    base("rename-workspace", "workspace.rename", &["workspace", "window", "title"], &[], "title"),
    base(
        "resize-pane",
        "pane.resize",
        &["pane", "workspace", "window", "direction", "amount"],
        &[],
        "",
    ),
    base("select-workspace", "workspace.select", &["workspace", "window"], &[], ""),
    base("send", "surface.send_text", &["surface", "workspace", "window"], &[], "text"),
    base("send-key", "surface.send_key", &["surface", "workspace", "window"], &[], "key"),
    base(
        "swap-pane",
        "pane.swap",
        &["pane", "target-pane", "workspace", "window", "focus"],
        &["focus"],
        "",
    ),
];

/// Relay-specific deviations from the generated command table.
#[must_use]
pub fn command_override(name: &str) -> CommandOverride {
    match name {
        // --name is the CLI flag; the server param is "title". --command,
        // --env-file, and --layout are handled client-side by the
        // new-workspace relay (post-create send, file read, JSON parse).
        "new-workspace" => CommandOverride {
            param_key_overrides: &[("name", "title")],
            client_only_flags: &["command", "env-file", "layout"],
            special_dispatch: true,
            ..CommandOverride::default()
        },
        // Mac CLI shows "title" as a positional arg; the relay accepts
        // --title as a flag instead, so positional args are rejected.
        "rename-workspace" => {
            CommandOverride { disable_positional: true, ..CommandOverride::default() }
        }
        // new-pane defaults direction to "right" when the flag is omitted.
        "new-pane" => CommandOverride {
            default_params: &[("direction", "right")],
            ..CommandOverride::default()
        },
        // --panel is an alias for --surface that maps to surface_id.
        "focus-panel" | "close-surface" | "new-split" => CommandOverride {
            param_key_overrides: &[("panel", "surface_id")],
            ..CommandOverride::default()
        },
        // --target-pane maps to the server param target_pane_id.
        "join-pane" => CommandOverride {
            param_key_overrides: &[("target-pane", "target_pane_id")],
            ..CommandOverride::default()
        },
        _ => CommandOverride::default(),
    }
}

/// Look up a command with its overrides applied.
#[must_use]
pub fn command_spec(name: &str) -> Option<CommandSpec> {
    let base = COMMANDS.iter().find(|c| c.name == name)?;
    let ov = command_override(name);
    Some(CommandSpec {
        name: base.name,
        v2_method: base.v2_method,
        flag_keys: base.flag_keys,
        bool_flags: base.bool_flags,
        no_params: base.no_params,
        param_key_overrides: ov.param_key_overrides.iter().copied().collect(),
        default_params: ov
            .default_params
            .iter()
            .map(|(k, v)| ((*k).to_string(), Value::from(*v)))
            .collect(),
        positional_key: if ov.disable_positional { "" } else { base.positional_key },
        repeat_keys: base.repeat_keys,
    })
}

/// Every command name in table order.
#[must_use]
pub fn command_names() -> Vec<&'static str> {
    COMMANDS.iter().map(|c| c.name).collect()
}

#[derive(Debug, Clone, Copy)]
pub struct BrowserCommandSpec {
    pub method: &'static str,
    pub flag_keys: &'static [&'static str],
    pub allow_positional_url: bool,
    pub allow_positional_script: bool,
    pub allow_positional_key: bool,
    pub allow_positional_query: bool,
    pub allow_positional_value: bool,
    pub use_workspace_env: bool,
    pub use_surface_env: bool,
}

const fn browser(method: &'static str, flag_keys: &'static [&'static str]) -> BrowserCommandSpec {
    BrowserCommandSpec {
        method,
        flag_keys,
        allow_positional_url: false,
        allow_positional_script: false,
        allow_positional_key: false,
        allow_positional_query: false,
        allow_positional_value: false,
        use_workspace_env: false,
        use_surface_env: true,
    }
}

const fn browser_url(
    method: &'static str,
    flag_keys: &'static [&'static str],
    workspace: bool,
) -> BrowserCommandSpec {
    BrowserCommandSpec {
        allow_positional_url: true,
        use_workspace_env: workspace,
        use_surface_env: !workspace,
        ..browser(method, flag_keys)
    }
}

const fn browser_query(method: &'static str) -> BrowserCommandSpec {
    BrowserCommandSpec { allow_positional_query: true, ..browser(method, &["surface", "selector"]) }
}

const fn browser_key(method: &'static str) -> BrowserCommandSpec {
    BrowserCommandSpec { allow_positional_key: true, ..browser(method, &["surface", "key"]) }
}

pub const BROWSER_COMMANDS: &[(&str, BrowserCommandSpec)] = &[
    ("open", browser_url("browser.open_split", &["url", "workspace", "surface"], true)),
    ("open-split", browser_url("browser.open_split", &["url", "workspace", "surface"], true)),
    ("new", browser_url("browser.open_split", &["url", "workspace", "surface"], true)),
    ("navigate", browser_url("browser.navigate", &["url", "surface"], false)),
    ("goto", browser_url("browser.navigate", &["url", "surface"], false)),
    ("back", browser("browser.back", &["surface"])),
    ("forward", browser("browser.forward", &["surface"])),
    ("reload", browser("browser.reload", &["surface"])),
    ("get-url", browser("browser.url.get", &["surface"])),
    ("url", browser("browser.url.get", &["surface"])),
    ("snapshot", browser("browser.snapshot", &["surface", "selector", "max-depth"])),
    (
        "eval",
        BrowserCommandSpec {
            allow_positional_script: true,
            ..browser("browser.eval", &["surface", "script"])
        },
    ),
    (
        "wait",
        browser(
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
        ),
    ),
    ("click", browser_query("browser.click")),
    ("dblclick", browser_query("browser.dblclick")),
    ("hover", browser_query("browser.hover")),
    ("focus", browser_query("browser.focus")),
    ("check", browser_query("browser.check")),
    ("uncheck", browser_query("browser.uncheck")),
    (
        "type",
        BrowserCommandSpec {
            allow_positional_value: true,
            ..browser("browser.type", &["surface", "selector", "text"])
        },
    ),
    (
        "fill",
        BrowserCommandSpec {
            allow_positional_value: true,
            ..browser("browser.fill", &["surface", "selector", "text"])
        },
    ),
    ("press", browser_key("browser.press")),
    ("key", browser_key("browser.press")),
    ("keydown", browser_key("browser.keydown")),
    ("keyup", browser_key("browser.keyup")),
    (
        "select",
        BrowserCommandSpec {
            allow_positional_value: true,
            ..browser("browser.select", &["surface", "selector", "value"])
        },
    ),
    ("screenshot", browser("browser.screenshot", &["surface"])),
];

#[must_use]
pub fn browser_command(name: &str) -> Option<BrowserCommandSpec> {
    BROWSER_COMMANDS.iter().find(|(n, _)| *n == name).map(|(_, spec)| *spec)
}

#[must_use]
pub fn browser_subcommand_hint() -> String {
    let mut names: Vec<&str> = BROWSER_COMMANDS.iter().map(|(n, _)| *n).collect();
    names.sort_unstable();
    names.join(", ")
}

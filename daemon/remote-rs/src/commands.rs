//! The relay's command table: each entry maps a CLI command name to a v2
//! JSON-RPC method and declares which flags it accepts. Relay-specific
//! behaviour (param key overrides, special dispatch, default params) lives in
//! [`command_overrides`]; [`crate::cli`] applies those overrides on top.

use std::collections::HashMap;

use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct CommandSpec {
    pub name: &'static str,
    pub v2_method: &'static str,
    pub flag_keys: Vec<&'static str>,
    pub bool_flags: Vec<&'static str>,
    pub no_params: bool,
    pub param_key_overrides: HashMap<&'static str, &'static str>,
    pub default_params: HashMap<&'static str, Value>,
    pub positional_key: &'static str,
    pub repeat_keys: Vec<&'static str>,
}

fn spec(name: &'static str, v2_method: &'static str) -> CommandSpec {
    CommandSpec {
        name,
        v2_method,
        ..Default::default()
    }
}

fn flags(mut spec: CommandSpec, keys: &[&'static str]) -> CommandSpec {
    spec.flag_keys = keys.to_vec();
    spec
}

fn bools(mut spec: CommandSpec, keys: &[&'static str]) -> CommandSpec {
    spec.bool_flags = keys.to_vec();
    spec
}

fn no_params(mut spec: CommandSpec) -> CommandSpec {
    spec.no_params = true;
    spec
}

fn positional(mut spec: CommandSpec, key: &'static str) -> CommandSpec {
    spec.positional_key = key;
    spec
}

fn repeat(mut spec: CommandSpec, keys: &[&'static str]) -> CommandSpec {
    spec.repeat_keys = keys.to_vec();
    spec
}

pub fn base_commands() -> Vec<CommandSpec> {
    vec![
        bools(
            flags(
                spec("break-pane", "pane.break"),
                &[
                    "pane",
                    "surface",
                    "workspace",
                    "window",
                    "focus",
                    "no-focus",
                ],
            ),
            &["focus", "no-focus"],
        ),
        no_params(spec("capabilities", "system.capabilities")),
        flags(
            spec("clear-history", "surface.clear_history"),
            &["surface", "workspace", "window"],
        ),
        flags(
            spec("close-surface", "surface.close"),
            &["surface", "panel", "workspace", "window"],
        ),
        flags(spec("close-window", "window.close"), &["window"]),
        flags(
            spec("close-workspace", "workspace.close"),
            &["workspace", "window"],
        ),
        no_params(spec("current-window", "window.current")),
        flags(spec("current-workspace", "workspace.current"), &["window"]),
        bools(
            flags(
                spec("dismiss-notification", "notification.dismiss"),
                &["id", "all-read"],
            ),
            &["all-read"],
        ),
        flags(
            spec("equalize-splits", "workspace.equalize_splits"),
            &["workspace", "window"],
        ),
        flags(
            spec("focus-panel", "surface.focus"),
            &["panel", "workspace", "window"],
        ),
        flags(spec("focus-window", "window.focus"), &["window"]),
        bools(
            flags(
                spec("join-pane", "pane.join"),
                &[
                    "target-pane",
                    "pane",
                    "surface",
                    "workspace",
                    "window",
                    "focus",
                    "no-focus",
                ],
            ),
            &["focus", "no-focus"],
        ),
        no_params(spec("jump-to-unread", "notification.jump_to_unread")),
        flags(spec("last-pane", "pane.last"), &["workspace", "window"]),
        flags(spec("last-workspace", "workspace.last"), &["window"]),
        flags(
            spec("list-pane-surfaces", "pane.surfaces"),
            &["pane", "workspace", "window"],
        ),
        flags(spec("list-panes", "pane.list"), &["workspace", "window"]),
        flags(
            spec("list-panels", "surface.list"),
            &["workspace", "window"],
        ),
        no_params(spec("list-windows", "window.list")),
        flags(spec("list-workspaces", "workspace.list"), &["window"]),
        bools(
            flags(
                spec("mark-notification-read", "notification.mark_read"),
                &["id", "workspace", "surface", "window", "all"],
            ),
            &["all"],
        ),
        flags(
            spec("move-workspace-to-window", "workspace.move_to_window"),
            &["workspace", "window"],
        ),
        bools(
            flags(
                spec("new-pane", "pane.create"),
                &[
                    "type",
                    "direction",
                    "placement",
                    "workspace",
                    "window",
                    "url",
                    "focus",
                ],
            ),
            &["focus"],
        ),
        positional(
            bools(
                flags(
                    spec("new-split", "surface.split"),
                    &["surface", "panel", "workspace", "window", "focus"],
                ),
                &["focus"],
            ),
            "direction",
        ),
        bools(
            flags(
                spec("new-surface", "surface.create"),
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
            ),
            &["focus"],
        ),
        no_params(spec("new-window", "window.create")),
        repeat(
            bools(
                flags(
                    spec("new-workspace", "workspace.create"),
                    &[
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
                ),
                &["focus"],
            ),
            &["env"],
        ),
        flags(spec("next-workspace", "workspace.next"), &["window"]),
        flags(
            spec("notify", "notification.create"),
            &[
                "title",
                "subtitle",
                "body",
                "workspace",
                "surface",
                "window",
            ],
        ),
        flags(spec("open-notification", "notification.open"), &["id"]),
        no_params(spec("ping", "system.ping")),
        flags(
            spec("previous-workspace", "workspace.previous"),
            &["window"],
        ),
        bools(
            flags(
                spec("read-screen", "surface.read_text"),
                &["surface", "workspace", "window", "scrollback", "lines"],
            ),
            &["scrollback"],
        ),
        no_params(spec("refresh-surfaces", "surface.refresh")),
        positional(
            flags(
                spec("rename-workspace", "workspace.rename"),
                &["workspace", "window", "title"],
            ),
            "title",
        ),
        flags(
            spec("resize-pane", "pane.resize"),
            &["pane", "workspace", "window", "direction", "amount"],
        ),
        flags(
            spec("select-workspace", "workspace.select"),
            &["workspace", "window"],
        ),
        positional(
            flags(
                spec("send", "surface.send_text"),
                &["surface", "workspace", "window"],
            ),
            "text",
        ),
        positional(
            flags(
                spec("send-key", "surface.send_key"),
                &["surface", "workspace", "window"],
            ),
            "key",
        ),
        bools(
            flags(
                spec("swap-pane", "pane.swap"),
                &["pane", "target-pane", "workspace", "window", "focus"],
            ),
            &["focus"],
        ),
    ]
}

/// Relay-specific behaviour that cannot be expressed in the generated command
/// spec.
#[derive(Clone, Debug, Default)]
pub struct CommandOverride {
    /// Maps a CLI flag name to the JSON param key sent to the server when they
    /// differ (e.g. `--name` must be sent as `title`).
    pub param_key_overrides: HashMap<&'static str, &'static str>,
    /// Clears any positional key inherited from the base table, making the
    /// command accept the positional value only via an explicit flag.
    pub disable_positional: bool,
    /// Params always included in the RPC call even when the flag is absent.
    pub default_params: HashMap<&'static str, Value>,
    /// Marks the command as having a dedicated relay function in `cli.rs`.
    pub special_dispatch: bool,
    /// Flags handled client-side that must never be forwarded as RPC params.
    pub client_only_flags: Vec<&'static str>,
}

pub fn command_overrides() -> HashMap<&'static str, CommandOverride> {
    let mut overrides = HashMap::new();
    overrides.insert(
        "new-workspace",
        CommandOverride {
            param_key_overrides: HashMap::from([("name", "title")]),
            client_only_flags: vec!["command", "env-file", "layout"],
            special_dispatch: true,
            ..Default::default()
        },
    );
    overrides.insert(
        "rename-workspace",
        CommandOverride {
            disable_positional: true,
            ..Default::default()
        },
    );
    overrides.insert(
        "new-pane",
        CommandOverride {
            default_params: HashMap::from([("direction", Value::from("right"))]),
            ..Default::default()
        },
    );
    overrides.insert(
        "focus-panel",
        CommandOverride {
            param_key_overrides: HashMap::from([("panel", "surface_id")]),
            ..Default::default()
        },
    );
    overrides.insert(
        "close-surface",
        CommandOverride {
            param_key_overrides: HashMap::from([("panel", "surface_id")]),
            ..Default::default()
        },
    );
    overrides.insert(
        "new-split",
        CommandOverride {
            param_key_overrides: HashMap::from([("panel", "surface_id")]),
            ..Default::default()
        },
    );
    overrides.insert(
        "join-pane",
        CommandOverride {
            param_key_overrides: HashMap::from([("target-pane", "target_pane_id")]),
            ..Default::default()
        },
    );
    overrides
}

/// Command table with overrides applied, indexed by name.
pub fn command_index() -> HashMap<&'static str, CommandSpec> {
    let overrides = command_overrides();
    let mut index = HashMap::new();
    for mut command in base_commands() {
        if let Some(ov) = overrides.get(command.name) {
            if !ov.param_key_overrides.is_empty() {
                command.param_key_overrides = ov.param_key_overrides.clone();
            }
            if ov.disable_positional {
                command.positional_key = "";
            }
            if !ov.default_params.is_empty() {
                command.default_params = ov.default_params.clone();
            }
        }
        index.insert(command.name, command);
    }
    index
}

//! `cmuxd-remote`: the daemon cmux bootstraps over SSH (and bakes into cloud
//! VM images). It serves newline-delimited JSON-RPC over stdio, keeps
//! persistent per-slot PTY sessions alive across local reconnects, exposes a
//! lease-gated WebSocket PTY transport for cloud VMs, and doubles as the
//! remote `cmux` CLI relay when invoked through the `cmux` wrapper.
//!
//! This crate is a port of the original Go implementation. Wire formats,
//! file layouts under `~/.cmux`, exit codes, and message text are preserved.

pub mod cli;
pub mod cli_bridge;
pub mod flags;
pub mod logger;
pub mod persistent;
pub mod pty;
pub mod rpc;
pub mod serve;
pub mod signal;
pub mod util;
pub mod ws;

/// Build-time daemon version (`CMUXD_REMOTE_VERSION`), defaulting to `dev`
/// exactly like the Go `-X main.version` ldflag default.
pub const VERSION: &str = match option_env!("CMUXD_REMOTE_VERSION") {
    Some(v) => v,
    None => "dev",
};

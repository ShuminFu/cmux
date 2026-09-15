//! Rust port of `cmuxd-remote`, the remote daemon behind `cmux ssh`.
//!
//! The crate is organized like the original Go package:
//! - [`rpc`]: JSON-RPC framing, proxy streams, session coordinator, PTY RPC.
//! - [`pty_hub`]: PTY session hub shared by every transport.
//! - [`persistent`]: per-slot persistent daemon and stdio proxy.
//! - [`ws`]: cloud WebSocket PTY/RPC transport.
//! - [`cloud_cli_bridge`]: cloud VM CLI request forwarding.
//! - [`cli`], [`commands`], [`tmux_compat`], [`agent_launch`]: the `cmux`
//!   CLI relay that runs on the remote host.

pub mod agent_launch;
pub mod cli;
pub mod cloud_cli_bridge;
pub mod commands;
pub mod daemon;
pub mod persistent;
pub mod pty_hub;
pub mod rpc;
pub mod tmux_compat;
pub mod util;
pub mod ws;

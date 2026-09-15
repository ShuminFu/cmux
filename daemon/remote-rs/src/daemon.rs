//! Process entry points: argv dispatch, the `serve` flag surface, and usage.
//! Mirrors `main()`/`run()` in `main.go`.

use std::io::{Read, Write};

use crate::cli::{run_cli, CliIo};
use crate::persistent::{
    run_persistent_daemon_server, run_persistent_stdio_proxy, stop_persistent_daemon,
};
use crate::rpc::run_stdio_server;
use crate::util::{first_non_empty, path_base, version, LogSink};
use crate::ws::{run_websocket_pty_server_blocking, WsServerConfig};

pub fn should_run_cli_for_invocation(argv0: &str, args: &[String]) -> bool {
    let base = path_base(argv0);
    if base == "cmux" {
        return true;
    }
    if !base.starts_with("cmuxd-remote") || args.is_empty() {
        return false;
    }
    !is_daemon_entry_command(&args[0])
}

pub fn is_daemon_entry_command(arg: &str) -> bool {
    matches!(arg, "version" | "serve" | "cli")
}

pub fn usage(w: &mut dyn Write) {
    let _ = writeln!(w, "Usage:");
    let _ = writeln!(w, "  cmuxd-remote version");
    let _ = writeln!(w, "  cmuxd-remote serve --stdio");
    let _ = writeln!(w, "  cmuxd-remote serve --stdio --persistent --slot <slot>");
    let _ = writeln!(
        w,
        "  cmuxd-remote serve --ws --auth-lease-file <path> [--rpc-auth-lease-file <path>] [--listen 127.0.0.1:7777]"
    );
    let _ = writeln!(w, "  cmuxd-remote cli <command> [args...]");
}

#[derive(Debug, Default)]
struct ServeFlags {
    stdio: bool,
    ws: bool,
    persistent: bool,
    persistent_server: bool,
    persistent_stop: bool,
    slot: String,
    persistent_lease_port: i64,
    listen: String,
    auth_lease_file: String,
    rpc_auth_lease_file: String,
    admin_token_sha256: String,
    admin_ed25519_public_key: String,
    shell: String,
}

/// A minimal port of Go's `flag` package semantics for the `serve` flag set:
/// `-name`/`--name`, `-name=value`, `-name value` for non-bool flags,
/// `-bool` / `-bool=false` for booleans, parsing stops at the first non-flag
/// argument, `--`, or `-`.
fn parse_serve_flags(args: &[String], stderr: &mut dyn Write) -> Result<ServeFlags, ()> {
    let mut flags = ServeFlags {
        listen: "127.0.0.1:7777".to_string(),
        ..Default::default()
    };
    let bool_names = [
        "stdio",
        "ws",
        "persistent",
        "persistent-server",
        "persistent-stop",
    ];
    let string_names = [
        "slot",
        "listen",
        "auth-lease-file",
        "rpc-auth-lease-file",
        "admin-token-sha256",
        "admin-ed25519-public-key",
        "shell",
    ];
    let int_names = ["persistent-lease-port"];
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if !arg.starts_with('-') || arg == "-" {
            break;
        }
        if arg == "--" {
            break;
        }
        let stripped = arg.trim_start_matches('-');
        if stripped.is_empty() {
            let _ = writeln!(stderr, "bad flag syntax: {arg}");
            return Err(());
        }
        let (name, inline_value) = match stripped.split_once('=') {
            Some((name, value)) => (name.to_string(), Some(value.to_string())),
            None => (stripped.to_string(), None),
        };
        if bool_names.contains(&name.as_str()) {
            let value = match inline_value {
                Some(value) => match value.to_lowercase().as_str() {
                    "1" | "t" | "true" => true,
                    "0" | "f" | "false" => false,
                    _ => {
                        let _ = writeln!(stderr, "invalid boolean value {value:?} for -{name}");
                        return Err(());
                    }
                },
                None => true,
            };
            match name.as_str() {
                "stdio" => flags.stdio = value,
                "ws" => flags.ws = value,
                "persistent" => flags.persistent = value,
                "persistent-server" => flags.persistent_server = value,
                "persistent-stop" => flags.persistent_stop = value,
                _ => unreachable!(),
            }
            i += 1;
            continue;
        }
        if string_names.contains(&name.as_str()) || int_names.contains(&name.as_str()) {
            let value = match inline_value {
                Some(value) => value,
                None => {
                    if i + 1 >= args.len() {
                        let _ = writeln!(stderr, "flag needs an argument: -{name}");
                        return Err(());
                    }
                    i += 1;
                    args[i].clone()
                }
            };
            if int_names.contains(&name.as_str()) {
                let parsed: i64 = match value.trim().parse() {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        let _ = writeln!(
                            stderr,
                            "invalid value {value:?} for flag -{name}: parse error"
                        );
                        return Err(());
                    }
                };
                flags.persistent_lease_port = parsed;
            } else {
                match name.as_str() {
                    "slot" => flags.slot = value,
                    "listen" => flags.listen = value,
                    "auth-lease-file" => flags.auth_lease_file = value,
                    "rpc-auth-lease-file" => flags.rpc_auth_lease_file = value,
                    "admin-token-sha256" => flags.admin_token_sha256 = value,
                    "admin-ed25519-public-key" => flags.admin_ed25519_public_key = value,
                    "shell" => flags.shell = value,
                    _ => unreachable!(),
                }
            }
            i += 1;
            continue;
        }
        let _ = writeln!(stderr, "flag provided but not defined: -{name}");
        return Err(());
    }
    Ok(flags)
}

/// Run the daemon with explicit streams and return the process exit code.
pub fn run(
    args: &[String],
    stdin: Box<dyn Read + Send>,
    stdout: Box<dyn Write + Send>,
    stderr: Box<dyn Write + Send>,
) -> i32 {
    let mut stderr = LogSink::new(stderr);
    let mut stdout = stdout;
    if args.is_empty() {
        usage(&mut stderr);
        return 2;
    }
    match args[0].as_str() {
        "version" => {
            let _ = writeln!(stdout, "{}", version());
            let _ = stdout.flush();
            0
        }
        "serve" => run_serve(&args[1..], stdin, stdout, stderr),
        "cli" => {
            let mut err_sink = stderr.clone();
            let mut io = CliIo {
                stdout: &mut *stdout,
                stderr: &mut err_sink,
            };
            let code = run_cli(&args[1..], &mut io);
            let _ = stdout.flush();
            code
        }
        _ => {
            usage(&mut stderr);
            2
        }
    }
}

fn run_serve(
    args: &[String],
    stdin: Box<dyn Read + Send>,
    stdout: Box<dyn Write + Send>,
    stderr: LogSink,
) -> i32 {
    let mut err_sink = stderr.clone();
    let flags = match parse_serve_flags(args, &mut err_sink) {
        Ok(flags) => flags,
        Err(()) => return 2,
    };
    let slot = flags.slot.trim().to_string();
    if flags.persistent_lease_port < 0 || flags.persistent_lease_port > 65535 {
        let _ = writeln!(
            err_sink,
            "serve --persistent-lease-port must be 0 or between 1 and 65535"
        );
        return 2;
    }
    if flags.persistent_server {
        if flags.stdio || flags.ws || flags.persistent || flags.persistent_stop {
            let _ = writeln!(
                err_sink,
                "serve --persistent-server cannot be combined with --stdio, --ws, --persistent, or --persistent-stop"
            );
            return 2;
        }
        if slot.is_empty() {
            let _ = writeln!(err_sink, "serve --persistent-server requires --slot");
            return 2;
        }
        if let Err(err) =
            run_persistent_daemon_server(&slot, flags.persistent_lease_port, stderr.clone())
        {
            let _ = writeln!(err_sink, "serve --persistent-server failed: {err}");
            return 1;
        }
        return 0;
    }
    if flags.persistent_stop {
        if flags.stdio || flags.ws || flags.persistent || flags.persistent_lease_port != 0 {
            let _ = writeln!(
                err_sink,
                "serve --persistent-stop cannot be combined with --stdio, --ws, --persistent, or --persistent-lease-port"
            );
            return 2;
        }
        if slot.is_empty() {
            let _ = writeln!(err_sink, "serve --persistent-stop requires --slot");
            return 2;
        }
        if let Err(err) = stop_persistent_daemon(&slot) {
            let _ = writeln!(err_sink, "serve --persistent-stop failed: {err}");
            return 1;
        }
        return 0;
    }
    if flags.stdio == flags.ws {
        let _ = writeln!(err_sink, "serve requires exactly one of --stdio or --ws");
        return 2;
    }
    if (flags.persistent || !slot.is_empty()) && !flags.stdio {
        let _ = writeln!(err_sink, "serve --persistent requires --stdio");
        return 2;
    }
    if !slot.is_empty() && !flags.persistent {
        let _ = writeln!(err_sink, "serve --slot requires --persistent");
        return 2;
    }
    if flags.persistent_lease_port != 0 && !flags.persistent {
        let _ = writeln!(
            err_sink,
            "serve --persistent-lease-port requires --persistent"
        );
        return 2;
    }
    if flags.ws {
        if flags.auth_lease_file.trim().is_empty() {
            let _ = writeln!(err_sink, "serve --ws requires --auth-lease-file");
            return 2;
        }
        let env_admin_token = std::env::var("CMUXD_WS_ADMIN_TOKEN_SHA256").unwrap_or_default();
        let env_admin_key = std::env::var("CMUXD_WS_ADMIN_ED25519_PUBLIC_KEY").unwrap_or_default();
        let cfg = WsServerConfig {
            listen_addr: flags.listen.trim().to_string(),
            pty_auth_lease_file: flags.auth_lease_file.trim().to_string(),
            rpc_auth_lease_file: flags.rpc_auth_lease_file.trim().to_string(),
            admin_token_sha256: first_non_empty(&[
                flags.admin_token_sha256.trim(),
                env_admin_token.trim(),
            ])
            .to_string(),
            admin_ed25519_pub_key: first_non_empty(&[
                flags.admin_ed25519_public_key.trim(),
                env_admin_key.trim(),
            ])
            .to_string(),
            shell: flags.shell.trim().to_string(),
            ..Default::default()
        };
        if let Err(err) = run_websocket_pty_server_blocking(cfg, stderr.clone()) {
            let _ = writeln!(err_sink, "serve --ws failed: {err}");
            return 1;
        }
        return 0;
    }
    if flags.persistent {
        if slot.is_empty() {
            let _ = writeln!(err_sink, "serve --persistent requires --slot");
            return 2;
        }
        if let Err(err) =
            run_persistent_stdio_proxy(stdin, stdout, &stderr, &slot, flags.persistent_lease_port)
        {
            let _ = writeln!(err_sink, "serve --stdio --persistent failed: {err}");
            return 1;
        }
        return 0;
    }
    if let Err(err) = run_stdio_server(stdin, stdout) {
        let _ = writeln!(err_sink, "serve failed: {err}");
        return 1;
    }
    0
}

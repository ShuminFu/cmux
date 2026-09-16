//! `cmuxd-remote` entrypoint: busybox-style dispatch between the daemon
//! (`version`, `serve`) and the embedded CLI relay.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::Arc;

use crate::VERSION;
use crate::flags::FlagSet;
use crate::logger::DiscardLogger;
use crate::pty::{PtyHub, PtyHubConfig};
use crate::rpc::frame::trim_frame;
use crate::rpc::{
    MAX_RPC_FRAME_BYTES, RpcFrame, RpcRequest, RpcResponse, RpcServer, StdioFrameWriter,
    read_rpc_frame,
};
use crate::util::first_non_empty;

/// The `main` of the binary: returns the process exit code.
pub fn main_entry(argv0: &str, args: &[String]) -> i32 {
    if should_run_cli_for_invocation(argv0, args) {
        return crate::cli::run_cli(args);
    }
    let mut stderr = io::stderr();
    run(args, io::stdin(), io::stdout(), &mut stderr)
}

#[must_use]
pub fn should_run_cli_for_invocation(argv0: &str, args: &[String]) -> bool {
    let base = std::path::Path::new(argv0)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| argv0.to_string());
    if base == "cmux" {
        return true;
    }
    if !base.starts_with("cmuxd-remote") || args.is_empty() {
        return false;
    }
    !is_daemon_entry_command(&args[0])
}

fn is_daemon_entry_command(arg: &str) -> bool {
    matches!(arg, "version" | "serve" | "cli")
}

pub fn run<R, W>(args: &[String], stdin: R, mut stdout: W, stderr: &mut dyn Write) -> i32
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    if args.is_empty() {
        usage(stderr);
        return 2;
    }
    match args[0].as_str() {
        "version" => {
            let _ = writeln!(stdout, "{VERSION}");
            0
        }
        "serve" => run_serve(&args[1..], stdin, stdout, stderr),
        "cli" => crate::cli::run_cli(&args[1..]),
        _ => {
            usage(stderr);
            2
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_serve<R, W>(args: &[String], stdin: R, stdout: W, stderr: &mut dyn Write) -> i32
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let mut fs = FlagSet::new("serve");
    fs.bool("stdio", false, "serve over stdin/stdout");
    fs.bool("ws", false, "serve terminal PTY transport over WebSocket");
    fs.bool("persistent", false, "proxy stdio to a persistent per-slot daemon");
    fs.bool("persistent-server", false, "run the persistent per-slot daemon");
    fs.bool("persistent-stop", false, "stop the persistent per-slot daemon");
    fs.string("slot", "", "persistent daemon slot");
    fs.int("persistent-lease-port", 0, "relay port whose slot file leases the persistent daemon");
    fs.string("listen", "127.0.0.1:7777", "address for --ws");
    fs.string("auth-lease-file", "", "required lease JSON path for --ws");
    fs.string("rpc-auth-lease-file", "", "optional daemon RPC lease JSON path for --ws /rpc");
    fs.string(
        "admin-token-sha256",
        "",
        "optional bearer token SHA-256 for HTTPS lease installation",
    );
    fs.string(
        "admin-ed25519-public-key",
        "",
        "optional base64 Ed25519 public key for signed HTTPS lease installation",
    );
    fs.string("shell", "", "shell path for --ws PTY sessions");
    if let Err(err) = fs.parse(args) {
        let _ = stderr.write_all(err.render().as_bytes());
        return 2;
    }
    let stdio = fs.get_bool("stdio");
    let ws = fs.get_bool("ws");
    let persistent = fs.get_bool("persistent");
    let persistent_server = fs.get_bool("persistent-server");
    let persistent_stop = fs.get_bool("persistent-stop");
    let slot = fs.get_string("slot");
    let slot = slot.trim();
    let lease_port = fs.get_int("persistent-lease-port");
    if !(0..=65535).contains(&lease_port) {
        let _ = writeln!(stderr, "serve --persistent-lease-port must be 0 or between 1 and 65535");
        return 2;
    }
    if persistent_server {
        if stdio || ws || persistent || persistent_stop {
            let _ = writeln!(
                stderr,
                "serve --persistent-server cannot be combined with --stdio, --ws, --persistent, or --persistent-stop"
            );
            return 2;
        }
        if slot.is_empty() {
            let _ = writeln!(stderr, "serve --persistent-server requires --slot");
            return 2;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let lease_port = lease_port as u16;
        if let Err(err) = crate::persistent::run_persistent_daemon_server(
            slot,
            lease_port,
            Arc::new(crate::logger::StderrLogger),
        ) {
            let _ = writeln!(stderr, "serve --persistent-server failed: {err}");
            return 1;
        }
        return 0;
    }
    if persistent_stop {
        if stdio || ws || persistent || lease_port != 0 {
            let _ = writeln!(
                stderr,
                "serve --persistent-stop cannot be combined with --stdio, --ws, --persistent, or --persistent-lease-port"
            );
            return 2;
        }
        if slot.is_empty() {
            let _ = writeln!(stderr, "serve --persistent-stop requires --slot");
            return 2;
        }
        if let Err(err) = crate::persistent::stop_persistent_daemon(slot) {
            let _ = writeln!(stderr, "serve --persistent-stop failed: {err}");
            return 1;
        }
        return 0;
    }
    if stdio == ws {
        let _ = writeln!(stderr, "serve requires exactly one of --stdio or --ws");
        return 2;
    }
    if (persistent || !slot.is_empty()) && !stdio {
        let _ = writeln!(stderr, "serve --persistent requires --stdio");
        return 2;
    }
    if !slot.is_empty() && !persistent {
        let _ = writeln!(stderr, "serve --slot requires --persistent");
        return 2;
    }
    if lease_port != 0 && !persistent {
        let _ = writeln!(stderr, "serve --persistent-lease-port requires --persistent");
        return 2;
    }
    if ws {
        let auth_lease_file = fs.get_string("auth-lease-file");
        if auth_lease_file.trim().is_empty() {
            let _ = writeln!(stderr, "serve --ws requires --auth-lease-file");
            return 2;
        }
        let env_token = std::env::var("CMUXD_WS_ADMIN_TOKEN_SHA256").unwrap_or_default();
        let env_key = std::env::var("CMUXD_WS_ADMIN_ED25519_PUBLIC_KEY").unwrap_or_default();
        let admin_token_sha256 = fs.get_string("admin-token-sha256");
        let admin_ed25519_public_key = fs.get_string("admin-ed25519-public-key");
        let config = crate::ws::WsPtyServerConfig {
            listen_addr: fs.get_string("listen").trim().to_string(),
            pty_auth_lease_file: auth_lease_file.trim().to_string(),
            rpc_auth_lease_file: fs.get_string("rpc-auth-lease-file").trim().to_string(),
            admin_token_sha256: first_non_empty(&[admin_token_sha256.trim(), env_token.trim()])
                .to_string(),
            admin_ed25519_pub_key: first_non_empty(&[
                admin_ed25519_public_key.trim(),
                env_key.trim(),
            ])
            .to_string(),
            shell: fs.get_string("shell").trim().to_string(),
            ..crate::ws::WsPtyServerConfig::default()
        };
        if let Err(err) =
            crate::ws::run_websocket_pty_server(config, Arc::new(crate::logger::StderrLogger))
        {
            let _ = writeln!(stderr, "serve --ws failed: {err}");
            return 1;
        }
        return 0;
    }
    if persistent {
        if slot.is_empty() {
            let _ = writeln!(stderr, "serve --persistent requires --slot");
            return 2;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let lease_port = lease_port as u16;
        if let Err(err) =
            crate::persistent::run_persistent_stdio_proxy(stdin, stdout, stderr, slot, lease_port)
        {
            let _ = writeln!(stderr, "serve --stdio --persistent failed: {err}");
            return 1;
        }
        return 0;
    }
    if let Err(err) = run_stdio_server(stdin, stdout) {
        let _ = writeln!(stderr, "serve failed: {err}");
        return 1;
    }
    0
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

pub fn run_stdio_server<R, W>(stdin: R, stdout: W) -> io::Result<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let hub = PtyHub::new(PtyHubConfig::default(), Arc::new(DiscardLogger));
    run_rpc_server(stdin, stdout, Some(hub), true)
}

pub fn run_rpc_server<R, W>(
    stdin: R,
    stdout: W,
    pty_hub: Option<Arc<PtyHub>>,
    owns_pty_hub: bool,
) -> io::Result<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let writer = Arc::new(StdioFrameWriter::new(stdout));
    let mut reader = BufReader::with_capacity(64 * 1024, stdin);
    run_rpc_server_with_reader(&mut reader, &writer, pty_hub, owns_pty_hub, None, None)
}

/// Serve one connection until EOF. `shutdown_method`, when given, names a
/// method (the persistent daemon's `daemon.shutdown`) that acknowledges,
/// invokes `request_shutdown`, and ends the loop.
pub fn run_rpc_server_with_reader<R: BufRead>(
    reader: &mut R,
    writer: &Arc<StdioFrameWriter>,
    pty_hub: Option<Arc<PtyHub>>,
    owns_pty_hub: bool,
    cli_bridge: Option<Arc<dyn crate::rpc::server::CliBridge>>,
    shutdown: Option<(&str, &dyn Fn())>,
) -> io::Result<()> {
    let server = RpcServer::new(writer.clone(), pty_hub, owns_pty_hub, cli_bridge);
    let result = serve_loop(reader, writer, &server, shutdown);
    let _ = writer.flush();
    writer.close();
    server.close_all();
    result
}

fn serve_loop<R: BufRead>(
    reader: &mut R,
    writer: &Arc<StdioFrameWriter>,
    server: &Arc<RpcServer>,
    shutdown: Option<(&str, &dyn Fn())>,
) -> io::Result<()> {
    use crate::rpc::FrameWriter as _;
    loop {
        let line = match read_rpc_frame(reader, MAX_RPC_FRAME_BYTES)? {
            RpcFrame::Eof => return Ok(()),
            RpcFrame::Oversized => {
                writer.write_response(&RpcResponse::failure(
                    None,
                    "invalid_request",
                    "request frame exceeds maximum size",
                ))?;
                continue;
            }
            RpcFrame::Line(line) => trim_frame(line),
        };
        if line.is_empty() {
            continue;
        }
        let Ok(req) = RpcRequest::parse(&line) else {
            writer.write_response(&RpcResponse::failure(
                None,
                "invalid_request",
                "invalid JSON request",
            ))?;
            continue;
        };
        if let Some((method, request_shutdown)) = shutdown
            && req.method == method
        {
            writer.write_response(&RpcResponse::success(
                req.id,
                serde_json::json!({"shutting_down": true}),
            ))?;
            request_shutdown();
            return Ok(());
        }
        server.handle_request_and_write_response(&req)?;
    }
}

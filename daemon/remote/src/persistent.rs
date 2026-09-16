//! Persistent per-slot daemon: a detached `cmuxd-remote serve
//! --persistent-server` process that keeps PTY sessions alive across SSH
//! reconnects, plus the stdio proxy that dials it and the stop command.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustix::event::{PollFd, PollFlags};
use rustix::fs::FlockOperation;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::VERSION;
use crate::logger::Logger;
use crate::pty::{PtyHub, PtyHubConfig, open::spawn_detached};
use crate::rpc::frame::trim_frame;
use crate::rpc::{
    FrameWriter as _, MAX_RPC_FRAME_BYTES, RpcFrame, RpcRequest, RpcResponse, StdioFrameWriter,
    get_string_param, read_rpc_frame,
};
use crate::serve::run_rpc_server_with_reader;
use crate::util::{
    create_temp_file, go_duration, io_error_text, lstat, owned_by_current_user, path_error, quote,
    random_bytes, temp_dir, user_home_dir,
};

pub const PERSISTENT_DAEMON_AUTH_METHOD: &str = "daemon.auth";
pub const PERSISTENT_DAEMON_SHUTDOWN_METHOD: &str = "daemon.shutdown";
pub const PERSISTENT_DAEMON_READY_FD_ENV: &str = "CMUX_REMOTE_DAEMON_READY_FD";
pub const PERSISTENT_DAEMON_AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const PERSISTENT_DAEMON_SOCKET_DIR_FILE: &str = "socket-dir";
pub const PERSISTENT_DAEMON_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
pub const PERSISTENT_DAEMON_EMPTY_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const PERSISTENT_DAEMON_EMPTY_IDLE_POLL_STEP: Duration = Duration::from_secs(1);
pub const PERSISTENT_DAEMON_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
pub const PERSISTENT_DAEMON_STOP_RETRY_STEP: Duration = Duration::from_millis(25);
pub const AUTH_FAILED_PREFIX: &str = "persistent daemon authentication failed";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentDaemonPaths {
    pub slot: String,
    pub root: PathBuf,
    pub socket: PathBuf,
    pub token_file: PathBuf,
    pub log_file: PathBuf,
    pub lock_file: PathBuf,
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn with_path(op: &str, path: &Path, err: io::Error) -> io::Error {
    io::Error::new(err.kind(), path_error(op, &path.to_string_lossy(), &err))
}

pub fn persistent_daemon_paths_for_slot(raw_slot: &str) -> io::Result<PersistentDaemonPaths> {
    let slot = validate_persistent_daemon_slot(raw_slot)?;
    let mut root_base =
        std::env::var("CMUX_REMOTE_DAEMON_ROOT").unwrap_or_default().trim().to_string();
    if root_base.is_empty() {
        let home = user_home_dir().unwrap_or_default();
        if home.trim().is_empty() {
            return Err(other("cannot resolve remote home directory"));
        }
        root_base = Path::new(&home).join(".cmux").join("daemon").to_string_lossy().into_owned();
    }
    let root = Path::new(&root_base).join(persistent_daemon_version_component()).join(&slot);
    let socket = persistent_daemon_socket_path(&root, &slot);
    Ok(PersistentDaemonPaths {
        token_file: root.join("auth.token"),
        log_file: root.join("daemon.log"),
        lock_file: root.join("daemon.lock"),
        slot,
        root,
        socket,
    })
}

#[must_use]
pub fn persistent_daemon_version_component() -> String {
    version_component_for(VERSION)
}

fn version_component_for(version: &str) -> String {
    let mut trimmed = version.trim();
    if trimmed.is_empty() {
        trimmed = "dev";
    }
    let component: String = trimmed
        .chars()
        .map(|r| if r.is_ascii_alphanumeric() || matches!(r, '-' | '_' | '.') { r } else { '_' })
        .collect();
    if component.is_empty() || component == "." || component == ".." {
        return "dev".to_string();
    }
    if component.len() <= 64 {
        return component;
    }
    let digest = Sha256::digest(trimmed.as_bytes());
    let prefix: String = component.chars().take(48).collect();
    format!("{prefix}-{}", hex::encode(&digest[..4]))
}

#[must_use]
pub fn persistent_daemon_socket_path(root: &Path, slot: &str) -> PathBuf {
    let socket_base = persistent_daemon_socket_base().unwrap_or_else(|| {
        Path::new("/tmp").join(format!("cmuxd-remote-{}", crate::util::getuid()))
    });
    let mut hasher = Sha256::new();
    hasher.update(root.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hasher.update(slot.as_bytes());
    let digest = hasher.finalize();
    socket_base.join(format!("cmuxd-{}.sock", hex::encode(&digest[..8])))
}

fn persistent_daemon_socket_base() -> Option<PathBuf> {
    let base = std::env::var("CMUX_REMOTE_DAEMON_SOCKET_DIR").unwrap_or_default();
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    Some(Path::new(base).join(format!("cmuxd-remote-{}", crate::util::getuid())))
}

pub fn validate_persistent_daemon_slot(raw_slot: &str) -> io::Result<String> {
    let slot = raw_slot.trim();
    if slot.is_empty() {
        return Err(other("persistent daemon slot is required"));
    }
    if slot == "." || slot == ".." || slot.len() > 128 {
        return Err(other(format!("invalid persistent daemon slot {}", quote(raw_slot))));
    }
    if !slot.chars().all(|r| r.is_ascii_alphanumeric() || matches!(r, '-' | '_' | '.')) {
        return Err(other(format!("invalid persistent daemon slot {}", quote(raw_slot))));
    }
    Ok(slot.to_string())
}

pub fn ensure_persistent_daemon_directory(
    mut paths: PersistentDaemonPaths,
) -> io::Result<PersistentDaemonPaths> {
    mkdir_all(&paths.root, 0o700)?;
    verify_private_daemon_directory(&paths.root)?;
    let socket_dir = paths.socket.parent().map(Path::to_path_buf).unwrap_or_default();
    let secure_socket_dir = ensure_persistent_daemon_socket_directory(&paths.root, &socket_dir)?;
    let base = paths.socket.file_name().map(std::ffi::OsStr::to_os_string).unwrap_or_default();
    paths.socket = secure_socket_dir.join(base);
    Ok(paths)
}

fn mkdir_all(path: &Path, mode: u32) -> io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(mode)
        .create(path)
        .map_err(|e| with_path("mkdir", path, e))
}

fn ensure_persistent_daemon_socket_directory(
    root: &Path,
    default_socket_dir: &Path,
) -> io::Result<PathBuf> {
    match read_persistent_daemon_socket_dir(root) {
        Ok(stored) => {
            if ensure_private_daemon_leaf_directory(&stored).is_ok() {
                return Ok(stored);
            }
            remove_persistent_daemon_socket_dir_metadata(root)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    if ensure_private_daemon_leaf_directory(default_socket_dir).is_ok() {
        return Ok(default_socket_dir.to_path_buf());
    }
    create_persistent_daemon_fallback_socket_dir(root)
}

fn ensure_private_daemon_leaf_directory(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        mkdir_all(parent, 0o755)?;
    }
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(with_path("mkdir", path, e)),
    }
    verify_private_daemon_directory(path)
}

pub fn verify_private_daemon_directory(path: &Path) -> io::Result<()> {
    let info = lstat(path)?;
    let text = path.to_string_lossy();
    if info.file_type().is_symlink() {
        return Err(other(format!("persistent daemon directory {} is a symlink", quote(&text))));
    }
    if !info.is_dir() {
        return Err(other(format!(
            "persistent daemon directory {} is not a directory",
            quote(&text)
        )));
    }
    if !owned_by_current_user(&info) {
        return Err(other(format!(
            "persistent daemon directory {} is not owned by uid {}",
            quote(&text),
            crate::util::getuid()
        )));
    }
    if info.permissions().mode() & 0o777 != 0o700 {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| with_path("chmod", path, e))?;
        let info = lstat(path)?;
        if info.file_type().is_symlink()
            || !info.is_dir()
            || !owned_by_current_user(&info)
            || info.permissions().mode() & 0o777 != 0o700
        {
            return Err(other(format!(
                "persistent daemon directory {} is not private",
                quote(&text)
            )));
        }
    }
    Ok(())
}

fn read_persistent_daemon_socket_dir(root: &Path) -> io::Result<PathBuf> {
    let path = root.join(PERSISTENT_DAEMON_SOCKET_DIR_FILE);
    let data = std::fs::read_to_string(&path).map_err(|e| with_path("open", &path, e))?;
    let socket_dir = data.trim();
    if socket_dir.is_empty() {
        return Err(other("persistent daemon socket directory file is empty"));
    }
    Ok(PathBuf::from(socket_dir))
}

fn create_persistent_daemon_fallback_socket_dir(root: &Path) -> io::Result<PathBuf> {
    for _ in 0..8 {
        let raw = random_bytes(8)?;
        let socket_dir = Path::new(&temp_dir()).join(format!(
            "cmuxd-remote-{}-{}",
            crate::util::getuid(),
            hex::encode(raw)
        ));
        match std::fs::DirBuilder::new().mode(0o700).create(&socket_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(with_path("mkdir", &socket_dir, e)),
        }
        match write_persistent_daemon_socket_dir(root, &socket_dir) {
            Ok(()) => return Ok(socket_dir),
            Err(e) => {
                let _ = std::fs::remove_dir(&socket_dir);
                if e.kind() == io::ErrorKind::AlreadyExists {
                    if let Ok(stored) = read_persistent_daemon_socket_dir(root)
                        && ensure_private_daemon_leaf_directory(&stored).is_ok()
                    {
                        return Ok(stored);
                    }
                    remove_persistent_daemon_socket_dir_metadata(root)?;
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(other("failed to create private persistent daemon socket directory"))
}

fn write_persistent_daemon_socket_dir(root: &Path, socket_dir: &Path) -> io::Result<()> {
    let (mut file, tmp_path) = create_temp_file(root, ".socket-dir.", ".tmp")?;
    let result = (|| {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| with_path("chmod", &tmp_path, e))?;
        file.write_all(format!("{}\n", socket_dir.to_string_lossy()).as_bytes())
            .map_err(|e| with_path("write", &tmp_path, e))?;
        drop(file);
        let target = root.join(PERSISTENT_DAEMON_SOCKET_DIR_FILE);
        std::fs::hard_link(&tmp_path, &target).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("link {} {}: {}", tmp_path.display(), target.display(), io_error_text(&e)),
            )
        })
    })();
    let _ = std::fs::remove_file(&tmp_path);
    result
}

fn remove_persistent_daemon_socket_dir_metadata(root: &Path) -> io::Result<()> {
    let path = root.join(PERSISTENT_DAEMON_SOCKET_DIR_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(with_path("remove", &path, e)),
    }
}

pub fn persistent_daemon_token(paths: &PersistentDaemonPaths) -> io::Result<String> {
    match read_persistent_daemon_token_file(&paths.token_file) {
        Ok(token) => return Ok(token),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let token = hex::encode(random_bytes(32)?);
    let dir = paths.token_file.parent().map(Path::to_path_buf).unwrap_or_default();
    let (mut file, tmp_path) = create_temp_file(&dir, ".auth.token.", ".tmp")?;
    let result = (|| {
        file.write_all(format!("{token}\n").as_bytes())
            .map_err(|e| with_path("write", &tmp_path, e))?;
        drop(file);
        match std::fs::hard_link(&tmp_path, &paths.token_file) {
            Ok(()) => Ok(token.clone()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                read_persistent_daemon_token_file(&paths.token_file)
            }
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!(
                    "link {} {}: {}",
                    tmp_path.display(),
                    paths.token_file.display(),
                    io_error_text(&e)
                ),
            )),
        }
    })();
    let _ = std::fs::remove_file(&tmp_path);
    result
}

pub fn read_persistent_daemon_token_file(token_file: &Path) -> io::Result<String> {
    let data = std::fs::read_to_string(token_file).map_err(|e| with_path("open", token_file, e))?;
    let token = data.trim();
    if token.is_empty() {
        return Err(other("persistent daemon token file is empty"));
    }
    Ok(token.to_string())
}

// ---------------------------------------------------------------------------
// Client side: stdio proxy.

pub fn run_persistent_stdio_proxy<R, W>(
    stdin: R,
    stdout: W,
    stderr: &mut dyn Write,
    slot: &str,
    lease_port: u16,
) -> io::Result<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let paths = persistent_daemon_paths_for_slot(slot)?;
    let paths = ensure_persistent_daemon_directory(paths)?;
    let token = persistent_daemon_token(&paths)?;
    ensure_persistent_daemon_running(&paths, &token, lease_port, Some(stderr))?;
    let conn = dial_persistent_daemon(&paths.socket, &token)?;
    proxy_persistent_daemon_conn(stdin, stdout, conn)
}

pub fn proxy_persistent_daemon_conn<R, W>(
    mut stdin: R,
    mut stdout: W,
    conn: UnixStream,
) -> io::Result<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded::<(&'static str, io::Result<()>)>(2);
    let writer_conn = conn.try_clone()?;
    let tx_in = tx.clone();
    std::thread::spawn(move || {
        let mut writer_conn = writer_conn;
        let result = copy_stream(&mut stdin, &mut writer_conn);
        let _ = writer_conn.shutdown(std::net::Shutdown::Write);
        let _ = tx_in.send(("stdin", result));
    });
    let reader_conn = conn.try_clone()?;
    std::thread::spawn(move || {
        let mut reader_conn = reader_conn;
        let result = copy_stream(&mut reader_conn, &mut stdout);
        let _ = tx.send(("stdout", result));
    });
    let first = rx.recv().map_err(|_| other("proxy copy aborted"))?;
    if first.0 == "stdout" {
        let _ = conn.shutdown(std::net::Shutdown::Both);
        return persistent_proxy_copy_error(first.1);
    }
    let second = rx.recv().map_err(|_| other("proxy copy aborted"))?;
    let _ = conn.shutdown(std::net::Shutdown::Both);
    persistent_proxy_copy_error(first.1)?;
    persistent_proxy_copy_error(second.1)
}

/// Forward bytes as they arrive (a plain loop rather than `io::copy`, whose
/// kernel fast paths can delay socket-to-pipe delivery).
fn copy_stream<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> io::Result<()> {
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let n = match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        writer.write_all(&buffer[..n])?;
        writer.flush()?;
    }
}

fn persistent_proxy_copy_error(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if matches!(e.kind(), io::ErrorKind::BrokenPipe | io::ErrorKind::NotConnected) => {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn ensure_persistent_daemon_running(
    paths: &PersistentDaemonPaths,
    token: &str,
    lease_port: u16,
    stderr: Option<&mut dyn Write>,
) -> io::Result<()> {
    match dial_persistent_daemon(&paths.socket, token) {
        Ok(conn) => {
            drop(conn);
            return Ok(());
        }
        Err(e) if should_remove_persistent_socket_after_dial_error(&e) => {
            let _ = std::fs::remove_file(&paths.socket);
        }
        Err(e) => return Err(e),
    }

    let executable = std::env::current_exe()?;
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&paths.log_file)
        .map_err(|e| with_path("open", &paths.log_file, e))?;
    let (ready_reader, ready_writer) = crate::util::cloexec_pipe()?;

    let mut cmd = Command::new(&executable);
    cmd.args(persistent_daemon_server_arguments(&paths.slot, lease_port));
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::from(log_file.try_clone()?));
    cmd.stderr(Stdio::from(log_file));
    cmd.env(PERSISTENT_DAEMON_READY_FD_ENV, "3");
    let child = spawn_detached(cmd, Some(ready_writer))?;
    drop(child);

    if let Err(err) = wait_persistent_daemon_ready(&ready_reader, &paths.log_file) {
        if let Ok(conn) = dial_persistent_daemon(&paths.socket, token) {
            drop(conn);
            return Ok(());
        }
        if let Some(stderr) = stderr {
            let _ = writeln!(stderr, "persistent daemon log: {}", paths.log_file.display());
        }
        return Err(err);
    }
    match dial_persistent_daemon(&paths.socket, token) {
        Ok(conn) => {
            drop(conn);
            Ok(())
        }
        Err(err) => {
            if let Some(stderr) = stderr {
                let _ = writeln!(stderr, "persistent daemon log: {}", paths.log_file.display());
            }
            Err(err)
        }
    }
}

#[must_use]
pub fn persistent_daemon_server_arguments(slot: &str, lease_port: u16) -> Vec<String> {
    let mut args = vec![
        "serve".to_string(),
        "--persistent-server".to_string(),
        "--slot".to_string(),
        slot.to_string(),
    ];
    if lease_port > 0 {
        args.push("--persistent-lease-port".to_string());
        args.push(lease_port.to_string());
    }
    args
}

fn should_remove_persistent_socket_after_dial_error(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused)
}

fn wait_persistent_daemon_ready(reader: &OwnedFd, log_file: &Path) -> io::Result<()> {
    let deadline = Instant::now() + PERSISTENT_DAEMON_STARTUP_TIMEOUT;
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(other(format!(
                "persistent daemon did not become ready; log: {}",
                log_file.display()
            )));
        }
        let mut fds = [PollFd::new(reader, PollFlags::IN)];
        let timespec = poll_timeout(remaining);
        match rustix::event::poll(&mut fds, Some(&timespec)) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(e) => return Err(e.into()),
        }
        match rustix::io::read(reader, &mut byte) {
            Ok(0) => {
                return Err(other(format!(
                    "persistent daemon exited before readiness signal; log: {}: EOF",
                    log_file.display()
                )));
            }
            Ok(_) => {
                if byte[0] == b'\n' {
                    let text = String::from_utf8_lossy(&line);
                    if text.trim() != "ready" {
                        return Err(other(format!(
                            "persistent daemon sent unexpected readiness signal {}; log: {}",
                            quote(text.trim()),
                            log_file.display()
                        )));
                    }
                    return Ok(());
                }
                line.push(byte[0]);
            }
            Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => {}
            Err(e) => {
                return Err(other(format!(
                    "persistent daemon exited before readiness signal; log: {}: {}",
                    log_file.display(),
                    io_error_text(&e.into())
                )));
            }
        }
    }
}

pub fn dial_persistent_daemon(socket_path: &Path, token: &str) -> io::Result<UnixStream> {
    let conn = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("dial unix {}: connect: {}", socket_path.display(), io_error_text(&e)),
        )
    })?;
    if let Err(e) = authenticate_persistent_daemon_client(&conn, token) {
        let _ = conn.shutdown(std::net::Shutdown::Both);
        return Err(e);
    }
    Ok(conn)
}

pub fn authenticate_persistent_daemon_client(conn: &UnixStream, token: &str) -> io::Result<()> {
    authenticate_persistent_daemon_client_with_timeout(conn, token, PERSISTENT_DAEMON_AUTH_TIMEOUT)
}

pub fn authenticate_persistent_daemon_client_with_timeout(
    conn: &UnixStream,
    token: &str,
    timeout: Duration,
) -> io::Result<()> {
    let timed = !timeout.is_zero();
    if timed {
        conn.set_read_timeout(Some(timeout))?;
        conn.set_write_timeout(Some(timeout))?;
    }
    let result = (|| {
        let mut params = Map::new();
        params.insert("token".to_string(), Value::from(token));
        let request = RpcRequest::new("auth", PERSISTENT_DAEMON_AUTH_METHOD, params);
        let mut data = serde_json::to_vec(&request).map_err(io::Error::other)?;
        data.push(b'\n');
        let mut writer = conn;
        writer.write_all(&data)?;
        writer.flush()?;
        let mut reader = BufReader::with_capacity(64 * 1024, conn);
        let line = match read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES)? {
            RpcFrame::Eof => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
            RpcFrame::Oversized => {
                return Err(other("persistent daemon auth response exceeded maximum size"));
            }
            RpcFrame::Line(line) => line,
        };
        let resp: RpcResponse = serde_json::from_slice(trim_frame(line).as_slice())
            .map_err(|e| other(e.to_string()))?;
        if !resp.ok {
            let mut message = "persistent daemon authentication failed".to_string();
            if !resp.error_message().trim().is_empty() {
                message = resp.error_message().trim().to_string();
            }
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{AUTH_FAILED_PREFIX}: {message}"),
            ));
        }
        Ok(())
    })();
    if timed {
        let _ = conn.set_read_timeout(None);
        let _ = conn.set_write_timeout(None);
    }
    result
}

// ---------------------------------------------------------------------------
// Server side.

pub struct PersistentDaemonServerConfig {
    pub empty_idle_timeout: Duration,
    pub accept_poll_step: Duration,
    pub slot_lease_present: Option<Box<dyn Fn() -> io::Result<bool> + Send + Sync>>,
    pub slot_lease_removed: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Default for PersistentDaemonServerConfig {
    fn default() -> Self {
        Self {
            empty_idle_timeout: Duration::ZERO,
            accept_poll_step: Duration::ZERO,
            slot_lease_present: None,
            slot_lease_removed: None,
        }
    }
}

pub fn run_persistent_daemon_server(
    slot: &str,
    lease_port: u16,
    logger: Arc<dyn Logger>,
) -> io::Result<()> {
    let paths = persistent_daemon_paths_for_slot(slot)?;
    let paths = ensure_persistent_daemon_directory(paths)?;
    let token = persistent_daemon_token(&paths)?;
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&paths.lock_file)
        .map_err(|e| with_path("open", &paths.lock_file, e))?;
    if rustix::fs::flock(&lock_file, FlockOperation::NonBlockingLockExclusive).is_err() {
        return Err(other(format!(
            "persistent daemon slot {} is already running",
            quote(&paths.slot)
        )));
    }

    let _ = std::fs::remove_file(&paths.socket);
    let listener = UnixListener::bind(&paths.socket).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("listen unix {}: bind: {}", paths.socket.display(), io_error_text(&e)),
        )
    })?;
    let _ = std::fs::set_permissions(&paths.socket, std::fs::Permissions::from_mode(0o600));
    let socket_path = paths.socket.clone();

    signal_persistent_daemon_ready();
    let mut config = PersistentDaemonServerConfig {
        empty_idle_timeout: PERSISTENT_DAEMON_EMPTY_IDLE_TIMEOUT,
        ..PersistentDaemonServerConfig::default()
    };
    let lease_removed = Arc::new(AtomicBool::new(false));
    if lease_port > 0 {
        let slot = paths.slot.clone();
        config.slot_lease_present =
            Some(Box::new(move || persistent_daemon_slot_lease_present(&slot, lease_port)));
        let flag = Arc::clone(&lease_removed);
        config.slot_lease_removed = Some(Box::new(move || flag.store(true, Ordering::SeqCst)));
    }
    let verifier = persistent_daemon_file_token_verifier(token, paths.token_file);
    let result = serve_persistent_daemon_with_verifier_config(listener, verifier, logger, config);
    let _ = std::fs::remove_file(&socket_path);
    let _ = rustix::fs::flock(&lock_file, FlockOperation::Unlock);
    drop(lock_file);
    if result.is_ok() && lease_removed.load(Ordering::SeqCst) {
        remove_persistent_daemon_relay_shell_directory_if_unleased(lease_port)?;
    }
    result
}

/// Write `ready\n` to the descriptor named by `CMUX_REMOTE_DAEMON_READY_FD`
/// (inherited from the spawning proxy) and close it.
#[allow(unsafe_code)]
pub fn signal_persistent_daemon_ready() {
    use std::os::fd::FromRawFd;
    let raw = std::env::var(PERSISTENT_DAEMON_READY_FD_ENV).unwrap_or_default();
    let raw = raw.trim();
    if raw.is_empty() {
        return;
    }
    let Ok(fd) = raw.parse::<i32>() else { return };
    if fd < 3 {
        return;
    }
    // SAFETY: the parent handed us this descriptor for exactly this purpose;
    // nothing else in the process refers to it, so taking ownership once and
    // closing it here is sound.
    let file = unsafe { File::from_raw_fd(fd) };
    let mut file = file;
    let _ = file.write_all(b"ready\n");
    drop(file);
}

pub type TokenVerifier = Arc<dyn Fn(&str) -> bool + Send + Sync>;

#[must_use]
pub fn persistent_daemon_fixed_token_verifier(token: String) -> TokenVerifier {
    Arc::new(move |provided| persistent_daemon_tokens_equal(provided, &token))
}

#[must_use]
pub fn persistent_daemon_file_token_verifier(
    initial_token: String,
    token_file: PathBuf,
) -> TokenVerifier {
    Arc::new(move |provided| {
        let token = read_persistent_daemon_token_file(&token_file)
            .unwrap_or_else(|_| initial_token.clone());
        persistent_daemon_tokens_equal(provided, &token)
    })
}

#[must_use]
pub fn persistent_daemon_tokens_equal(provided: &str, token: &str) -> bool {
    let provided = provided.trim();
    let token = token.trim();
    !provided.is_empty()
        && !token.is_empty()
        && bool::from(provided.as_bytes().ct_eq(token.as_bytes()))
}

pub fn serve_persistent_daemon_with_verifier(
    listener: UnixListener,
    verifier: TokenVerifier,
    logger: Arc<dyn Logger>,
) -> io::Result<()> {
    serve_persistent_daemon_with_verifier_config(
        listener,
        verifier,
        logger,
        PersistentDaemonServerConfig::default(),
    )
}

/// Shared shutdown request: closing the listener in Go; here a wake pipe the
/// accept loop polls alongside the listener.
struct ShutdownSignal {
    wake_tx: Mutex<Option<OwnedFd>>,
}

impl ShutdownSignal {
    fn request(&self) {
        // Dropping the write end makes the read end readable (POLLHUP).
        self.wake_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
    }
}

fn poll_timeout(d: Duration) -> rustix::time::Timespec {
    rustix::time::Timespec {
        tv_sec: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        tv_nsec: i64::from(d.subsec_nanos()),
    }
}

fn earliest_nonzero(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (None, b) => b,
        (Some(a), Some(b)) if b < a => Some(b),
        (a, _) => a,
    }
}

pub fn serve_persistent_daemon_with_verifier_config(
    listener: UnixListener,
    verifier: TokenVerifier,
    logger: Arc<dyn Logger>,
    config: PersistentDaemonServerConfig,
) -> io::Result<()> {
    let hub = PtyHub::new(PtyHubConfig::default(), logger);
    let result = accept_loop(&listener, &verifier, &hub, &config);
    hub.close_all();
    result
}

fn accept_loop(
    listener: &UnixListener,
    verifier: &TokenVerifier,
    hub: &Arc<PtyHub>,
    config: &PersistentDaemonServerConfig,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let (wake_rx, wake_tx) = crate::util::cloexec_pipe()?;
    let shutdown = Arc::new(ShutdownSignal { wake_tx: Mutex::new(Some(wake_tx)) });
    let active_connections = Arc::new(AtomicI64::new(0));
    let mut idle_since: Option<Instant> = None;
    let mut slot_lease_observed = false;
    let poll_step = if config.accept_poll_step.is_zero() {
        PERSISTENT_DAEMON_EMPTY_IDLE_POLL_STEP
    } else {
        config.accept_poll_step
    };
    loop {
        let now = Instant::now();
        let mut accept_deadline: Option<Instant> = None;
        if let Some(lease_present) = &config.slot_lease_present {
            if let Ok(present) = lease_present() {
                if present {
                    slot_lease_observed = true;
                } else if slot_lease_observed && active_connections.load(Ordering::SeqCst) == 0 {
                    if let Some(removed) = &config.slot_lease_removed {
                        removed();
                    }
                    return Ok(());
                }
            }
            accept_deadline = Some(now + poll_step);
        }
        if !config.empty_idle_timeout.is_zero() {
            let is_empty =
                active_connections.load(Ordering::SeqCst) == 0 && hub.active_session_count() == 0;
            if is_empty {
                let since = *idle_since.get_or_insert(now);
                let elapsed = now.saturating_duration_since(since);
                if elapsed >= config.empty_idle_timeout {
                    return Ok(());
                }
                let remaining = config.empty_idle_timeout - elapsed;
                let idle_deadline = now + remaining.min(poll_step);
                accept_deadline = earliest_nonzero(accept_deadline, Some(idle_deadline));
            } else {
                idle_since = None;
                accept_deadline = earliest_nonzero(accept_deadline, Some(now + poll_step));
            }
        }
        let timeout = accept_deadline.map(|d| d.saturating_duration_since(Instant::now()));
        let mut fds = [PollFd::new(listener, PollFlags::IN), PollFd::new(&wake_rx, PollFlags::IN)];
        let timespec = timeout.map(poll_timeout);
        match rustix::event::poll(&mut fds, timespec.as_ref()) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(e) => return Err(e.into()),
        }
        if !fds[1].revents().is_empty() {
            return Ok(());
        }
        let conn = match listener.accept() {
            Ok((conn, _)) => conn,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        active_connections.fetch_add(1, Ordering::SeqCst);
        let active = Arc::clone(&active_connections);
        let verifier = Arc::clone(verifier);
        let hub = Arc::clone(hub);
        let shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            handle_persistent_daemon_conn(conn, &verifier, &hub, &move || shutdown.request());
            active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

pub fn handle_persistent_daemon_conn(
    conn: UnixStream,
    verifier: &TokenVerifier,
    hub: &Arc<PtyHub>,
    request_shutdown: &dyn Fn(),
) {
    handle_persistent_daemon_conn_with_auth_timeout(
        conn,
        verifier,
        hub,
        PERSISTENT_DAEMON_AUTH_TIMEOUT,
        request_shutdown,
    );
}

pub fn handle_persistent_daemon_conn_with_auth_timeout(
    conn: UnixStream,
    verifier: &TokenVerifier,
    hub: &Arc<PtyHub>,
    timeout: Duration,
    request_shutdown: &dyn Fn(),
) {
    let _ = conn.set_nonblocking(false);
    if !timeout.is_zero()
        && (conn.set_read_timeout(Some(timeout)).is_err()
            || conn.set_write_timeout(Some(timeout)).is_err())
    {
        return;
    }
    let Ok(write_half) = conn.try_clone() else { return };
    let mut reader = BufReader::with_capacity(64 * 1024, conn);
    let writer = Arc::new(StdioFrameWriter::new(write_half));
    if !authenticate_persistent_daemon_conn(&mut reader, &writer, verifier) {
        let _ = writer.flush();
        return;
    }
    if !timeout.is_zero() {
        let conn = reader.get_ref();
        if conn.set_read_timeout(None).is_err() || conn.set_write_timeout(None).is_err() {
            return;
        }
    }
    let _ = run_rpc_server_with_reader(
        &mut reader,
        &writer,
        Some(Arc::clone(hub)),
        false,
        None,
        Some((PERSISTENT_DAEMON_SHUTDOWN_METHOD, request_shutdown)),
    );
    let _ = reader.get_ref().shutdown(std::net::Shutdown::Both);
}

pub fn authenticate_persistent_daemon_conn<R: BufRead>(
    reader: &mut R,
    writer: &StdioFrameWriter,
    verifier: &TokenVerifier,
) -> bool {
    let line = match read_rpc_frame(reader, MAX_RPC_FRAME_BYTES) {
        Ok(RpcFrame::Line(line)) => trim_frame(line),
        _ => {
            let _ = writer.write_response(&RpcResponse::failure(
                None,
                "unauthorized",
                "persistent daemon authentication required",
            ));
            return false;
        }
    };
    let Ok(req) = RpcRequest::parse(&line) else {
        let _ = writer.write_response(&RpcResponse::failure(
            None,
            "invalid_request",
            "invalid JSON request",
        ));
        return false;
    };
    if req.method != PERSISTENT_DAEMON_AUTH_METHOD {
        let _ = writer.write_response(&RpcResponse::failure(
            req.id,
            "unauthorized",
            "persistent daemon authentication required",
        ));
        return false;
    }
    let provided = get_string_param(&req.params, "token").unwrap_or_default();
    if !verifier(&provided) {
        let _ = writer.write_response(&RpcResponse::failure(
            req.id,
            "unauthorized",
            "invalid persistent daemon token",
        ));
        return false;
    }
    let _ = writer.write_response(&RpcResponse::success(req.id, json!({"authenticated": true})));
    true
}

// ---------------------------------------------------------------------------
// Stop.

pub fn existing_persistent_daemon_paths_for_slot(
    slot: &str,
) -> io::Result<(PersistentDaemonPaths, bool)> {
    let mut paths = persistent_daemon_paths_for_slot(slot)?;
    match verify_private_daemon_directory(&paths.root) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((paths, false)),
        Err(e) => return Err(e),
    }
    match read_persistent_daemon_socket_dir(&paths.root) {
        Ok(stored) => {
            let base =
                paths.socket.file_name().map(std::ffi::OsStr::to_os_string).unwrap_or_default();
            paths.socket = stored.join(base);
            match verify_private_daemon_directory(&stored) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((paths, true)),
                Err(e) => return Err(e),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    Ok((paths, true))
}

pub fn stop_persistent_daemon(slot: &str) -> io::Result<()> {
    let (paths, exists) = existing_persistent_daemon_paths_for_slot(slot)?;
    if !exists {
        return Ok(());
    }
    let token = match read_persistent_daemon_token_file(&paths.token_file) {
        Ok(token) => token,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            wait_for_persistent_daemon_stop(&paths.lock_file)?;
            let _ = std::fs::remove_file(&paths.socket);
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let conn = match dial_persistent_daemon(&paths.socket, &token) {
        Ok(conn) => conn,
        Err(e) => {
            if should_remove_persistent_socket_after_dial_error(&e) {
                let _ = std::fs::remove_file(&paths.socket);
                return wait_for_persistent_daemon_stop(&paths.lock_file);
            }
            return Err(e);
        }
    };
    let result = request_persistent_daemon_shutdown(&conn);
    let _ = conn.shutdown(std::net::Shutdown::Both);
    result?;
    wait_for_persistent_daemon_stop(&paths.lock_file)
}

pub fn request_persistent_daemon_shutdown(conn: &UnixStream) -> io::Result<()> {
    conn.set_read_timeout(Some(PERSISTENT_DAEMON_AUTH_TIMEOUT))?;
    conn.set_write_timeout(Some(PERSISTENT_DAEMON_AUTH_TIMEOUT))?;
    let result = (|| {
        let request = RpcRequest::new("shutdown", PERSISTENT_DAEMON_SHUTDOWN_METHOD, Map::new());
        let mut data = serde_json::to_vec(&request).map_err(io::Error::other)?;
        data.push(b'\n');
        let mut writer = conn;
        writer.write_all(&data)?;
        writer.flush()?;
        let mut reader = BufReader::with_capacity(64 * 1024, conn);
        let line = match read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES)? {
            RpcFrame::Eof => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
            RpcFrame::Oversized => {
                return Err(other("persistent daemon shutdown response exceeds maximum size"));
            }
            RpcFrame::Line(line) => line,
        };
        let resp: RpcResponse = serde_json::from_slice(trim_frame(line).as_slice())
            .map_err(|e| other(e.to_string()))?;
        if !resp.ok {
            return Err(other("persistent daemon shutdown rejected"));
        }
        Ok(())
    })();
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    result
}

pub fn wait_for_persistent_daemon_stop(lock_path: &Path) -> io::Result<()> {
    wait_for_persistent_daemon_stop_with_timeout(
        lock_path,
        PERSISTENT_DAEMON_STOP_WAIT_TIMEOUT,
        PERSISTENT_DAEMON_STOP_RETRY_STEP,
    )
}

pub fn wait_for_persistent_daemon_stop_with_timeout(
    lock_path: &Path,
    timeout: Duration,
    retry_step: Duration,
) -> io::Result<()> {
    if timeout.is_zero() || retry_step.is_zero() {
        return Err(other("persistent daemon stop wait requires positive timeout and retry step"));
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(lock_path)
        .map_err(|e| with_path("open", lock_path, e))?;
    let deadline = Instant::now() + timeout;
    loop {
        match rustix::fs::flock(&lock_file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {
                return rustix::fs::flock(&lock_file, FlockOperation::Unlock)
                    .map_err(io::Error::from);
            }
            Err(rustix::io::Errno::WOULDBLOCK) => {}
            Err(e) => return Err(e.into()),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(other(format!(
                "timed out waiting for persistent daemon ownership release after {}",
                go_duration(timeout)
            )));
        }
        std::thread::sleep(retry_step.min(remaining));
    }
}

// ---------------------------------------------------------------------------
// Relay slot leases.

pub fn persistent_daemon_relay_path(lease_port: u16, suffix: &str) -> io::Result<PathBuf> {
    if lease_port == 0 {
        return Err(other(format!("invalid persistent daemon lease port {lease_port}")));
    }
    let home = user_home_dir().unwrap_or_default();
    if home.trim().is_empty() {
        return Err(other("cannot resolve remote home directory"));
    }
    Ok(Path::new(&home).join(".cmux").join("relay").join(format!("{lease_port}{suffix}")))
}

pub fn persistent_daemon_slot_lease_present(slot: &str, lease_port: u16) -> io::Result<bool> {
    let lease_path = persistent_daemon_relay_path(lease_port, ".slot")?;
    let info = match lstat(&lease_path) {
        Ok(info) => info,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    if !info.file_type().is_file() || !owned_by_current_user(&info) {
        return Err(other(format!(
            "persistent daemon lease {} is not a private regular file",
            quote(&lease_path.to_string_lossy())
        )));
    }
    let data =
        std::fs::read_to_string(&lease_path).map_err(|e| with_path("open", &lease_path, e))?;
    Ok(data.trim() == slot)
}

pub fn remove_persistent_daemon_relay_shell_directory_if_unleased(
    lease_port: u16,
) -> io::Result<()> {
    let lease_path = persistent_daemon_relay_path(lease_port, ".slot")?;
    match lstat(&lease_path) {
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let shell_path = persistent_daemon_relay_path(lease_port, ".shell")?;
    match std::fs::remove_dir_all(&shell_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(with_path("unlinkat", &shell_path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_component_sanitizes_and_bounds() {
        assert_eq!(version_component_for(""), "dev");
        assert_eq!(version_component_for(" v1.2.3 "), "v1.2.3");
        assert_eq!(version_component_for("a/b c"), "a_b_c");
        assert_eq!(version_component_for(".."), "dev");
        let long = "x".repeat(100);
        let component = version_component_for(&long);
        assert_eq!(component.len(), 48 + 1 + 8);
        assert!(component.starts_with(&"x".repeat(48)));
    }

    #[test]
    fn slot_validation_matches_go() {
        assert_eq!(validate_persistent_daemon_slot(" abc-1_2.3 ").unwrap(), "abc-1_2.3");
        assert_eq!(
            validate_persistent_daemon_slot("").unwrap_err().to_string(),
            "persistent daemon slot is required"
        );
        assert_eq!(
            validate_persistent_daemon_slot("a/b").unwrap_err().to_string(),
            "invalid persistent daemon slot \"a/b\""
        );
        assert!(validate_persistent_daemon_slot("..").is_err());
        assert!(validate_persistent_daemon_slot(&"a".repeat(129)).is_err());
    }

    #[test]
    fn tokens_compare_trimmed_and_nonempty() {
        assert!(persistent_daemon_tokens_equal(" abc ", "abc"));
        assert!(!persistent_daemon_tokens_equal("", ""));
        assert!(!persistent_daemon_tokens_equal("abc", "abd"));
        let verifier = persistent_daemon_fixed_token_verifier("tok".to_string());
        assert!(verifier("tok"));
        assert!(!verifier("nope"));
    }

    #[test]
    fn go_duration_formatting() {
        assert_eq!(go_duration(Duration::from_secs(5)), "5s");
        assert_eq!(go_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(go_duration(Duration::from_millis(25)), "25ms");
        assert_eq!(go_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(go_duration(Duration::from_secs(3600)), "1h0m0s");
        assert_eq!(go_duration(Duration::from_micros(1500)), "1.5ms");
    }
}

//! Persistent per-slot daemon: slot paths and credentials, the authenticated
//! Unix socket server, the stdio proxy that `cmux ssh` talks to, and the slot
//! teardown control plane. Mirrors the persistent parts of `main.go` and
//! `persistent_lifecycle.go`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use sha2::{Digest, Sha256};

use crate::pty_hub::{PtyHub, PtyHubConfig};
use crate::rpc::{
    get_string_param, read_rpc_frame, run_rpc_server_with_reader, trim_frame, FrameWriter,
    RpcRequest, RpcResponse, StdioFrameWriter, MAX_RPC_FRAME_BYTES,
    PERSISTENT_DAEMON_SHUTDOWN_METHOD,
};
use crate::util::{
    accept_unix_with_stop, constant_time_eq, getuid, home_dir, path_base, path_dir, path_join,
    random_hex, temp_dir, version, LogSink, StopSignal,
};

pub const PERSISTENT_DAEMON_AUTH_METHOD: &str = "daemon.auth";
pub const PERSISTENT_DAEMON_READY_FD_ENV: &str = "CMUX_REMOTE_DAEMON_READY_FD";
pub const PERSISTENT_DAEMON_AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const PERSISTENT_DAEMON_SOCKET_DIR_FILE: &str = "socket-dir";
pub const PERSISTENT_DAEMON_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
pub const PERSISTENT_DAEMON_EMPTY_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const PERSISTENT_DAEMON_EMPTY_IDLE_POLL_STEP: Duration = Duration::from_secs(1);
pub const PERSISTENT_DAEMON_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
pub const PERSISTENT_DAEMON_STOP_RETRY_STEP: Duration = Duration::from_millis(25);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistentDaemonPaths {
    pub slot: String,
    pub root: String,
    pub socket: String,
    pub token_file: String,
    pub log_file: String,
    pub lock_file: String,
}

pub type SlotLeasePresentFn = Box<dyn Fn() -> io::Result<bool> + Send + Sync>;
pub type SlotLeaseRemovedFn = Box<dyn Fn() + Send + Sync>;

#[derive(Default)]
pub struct PersistentServerConfig {
    pub empty_idle_timeout: Duration,
    pub accept_poll_step: Duration,
    pub slot_lease_present: Option<SlotLeasePresentFn>,
    pub slot_lease_removed: Option<SlotLeaseRemovedFn>,
}

/// Error marker for authentication failures against the persistent daemon.
#[derive(Debug)]
pub struct PersistentDaemonAuthFailed {
    pub message: String,
}

impl std::fmt::Display for PersistentDaemonAuthFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "persistent daemon authentication failed: {}",
            self.message
        )
    }
}

impl std::error::Error for PersistentDaemonAuthFailed {}

pub fn is_persistent_daemon_auth_failed(err: &io::Error) -> bool {
    err.get_ref()
        .map(|inner| inner.is::<PersistentDaemonAuthFailed>())
        .unwrap_or(false)
}

fn other_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

pub fn persistent_daemon_paths_for_slot(raw_slot: &str) -> io::Result<PersistentDaemonPaths> {
    let slot = validate_persistent_daemon_slot(raw_slot)?;
    let mut root_base = std::env::var("CMUX_REMOTE_DAEMON_ROOT")
        .unwrap_or_default()
        .trim()
        .to_string();
    if root_base.is_empty() {
        let home = home_dir().ok_or_else(|| other_error("cannot resolve remote home directory"))?;
        root_base = path_join(&home, ".cmux/daemon");
    }
    let root = path_join(
        &path_join(&root_base, &persistent_daemon_version_component()),
        &slot,
    );
    let socket = persistent_daemon_socket_path(&root, &slot);
    Ok(PersistentDaemonPaths {
        slot,
        token_file: path_join(&root, "auth.token"),
        log_file: path_join(&root, "daemon.log"),
        lock_file: path_join(&root, "daemon.lock"),
        root,
        socket,
    })
}

pub fn persistent_daemon_version_component() -> String {
    let mut trimmed = version().trim().to_string();
    if trimmed.is_empty() {
        trimmed = "dev".to_string();
    }
    let component: String = trimmed
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if component.is_empty() || component == "." || component == ".." {
        return "dev".to_string();
    }
    if component.len() <= 64 {
        return component;
    }
    let digest = Sha256::digest(trimmed.as_bytes());
    format!("{}-{}", &component[..48], hex::encode(&digest[..4]))
}

pub fn persistent_daemon_socket_path(root: &str, slot: &str) -> String {
    let socket_base = persistent_daemon_socket_base()
        .unwrap_or_else(|| format!("/tmp/cmuxd-remote-{}", getuid()));
    let mut hasher = Sha256::new();
    hasher.update(root.as_bytes());
    hasher.update(b"\0");
    hasher.update(slot.as_bytes());
    let digest = hasher.finalize();
    path_join(
        &socket_base,
        &format!("cmuxd-{}.sock", hex::encode(&digest[..8])),
    )
}

fn persistent_daemon_socket_base() -> Option<String> {
    let base = std::env::var("CMUX_REMOTE_DAEMON_SOCKET_DIR")
        .unwrap_or_default()
        .trim()
        .to_string();
    if base.is_empty() {
        return None;
    }
    Some(path_join(&base, &format!("cmuxd-remote-{}", getuid())))
}

pub fn validate_persistent_daemon_slot(raw_slot: &str) -> io::Result<String> {
    let slot = raw_slot.trim();
    if slot.is_empty() {
        return Err(other_error("persistent daemon slot is required"));
    }
    if slot == "." || slot == ".." || slot.len() > 128 {
        return Err(other_error(format!(
            "invalid persistent daemon slot {raw_slot:?}"
        )));
    }
    if !slot
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(other_error(format!(
            "invalid persistent daemon slot {raw_slot:?}"
        )));
    }
    Ok(slot.to_string())
}

pub fn ensure_persistent_daemon_directory(
    mut paths: PersistentDaemonPaths,
) -> io::Result<PersistentDaemonPaths> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&paths.root)?;
    verify_private_daemon_directory(&paths.root)?;
    let socket_dir = path_dir(&paths.socket);
    let secure_socket_dir = ensure_persistent_daemon_socket_directory(&paths.root, &socket_dir)?;
    paths.socket = path_join(&secure_socket_dir, &path_base(&paths.socket));
    Ok(paths)
}

fn ensure_persistent_daemon_socket_directory(
    root: &str,
    default_socket_dir: &str,
) -> io::Result<String> {
    match read_persistent_daemon_socket_dir(root) {
        Ok(stored) => {
            if ensure_private_daemon_leaf_directory(&stored).is_ok() {
                return Ok(stored);
            }
            remove_persistent_daemon_socket_dir_metadata(root)?;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    if ensure_private_daemon_leaf_directory(default_socket_dir).is_ok() {
        return Ok(default_socket_dir.to_string());
    }
    create_persistent_daemon_fallback_socket_dir(root)
}

fn ensure_private_daemon_leaf_directory(path: &str) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(path_dir(path))?;
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err),
    }
    verify_private_daemon_directory(path)
}

pub fn verify_private_daemon_directory(path: &str) -> io::Result<()> {
    let info = fs::symlink_metadata(path)?;
    if info.file_type().is_symlink() {
        return Err(other_error(format!(
            "persistent daemon directory {path:?} is a symlink"
        )));
    }
    if !info.is_dir() {
        return Err(other_error(format!(
            "persistent daemon directory {path:?} is not a directory"
        )));
    }
    if !daemon_directory_owned_by_current_user(&info) {
        return Err(other_error(format!(
            "persistent daemon directory {path:?} is not owned by uid {}",
            getuid()
        )));
    }
    if info.permissions().mode() & 0o777 != 0o700 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        let info = fs::symlink_metadata(path)?;
        if info.file_type().is_symlink()
            || !info.is_dir()
            || !daemon_directory_owned_by_current_user(&info)
            || info.permissions().mode() & 0o777 != 0o700
        {
            return Err(other_error(format!(
                "persistent daemon directory {path:?} is not private"
            )));
        }
    }
    Ok(())
}

fn daemon_directory_owned_by_current_user(info: &fs::Metadata) -> bool {
    info.uid() == getuid()
}

pub fn read_persistent_daemon_socket_dir(root: &str) -> io::Result<String> {
    let data = fs::read_to_string(path_join(root, PERSISTENT_DAEMON_SOCKET_DIR_FILE))?;
    let socket_dir = data.trim().to_string();
    if socket_dir.is_empty() {
        return Err(other_error(
            "persistent daemon socket directory file is empty",
        ));
    }
    Ok(socket_dir)
}

fn create_persistent_daemon_fallback_socket_dir(root: &str) -> io::Result<String> {
    for _ in 0..8 {
        let socket_dir = temp_dir().join(format!("cmuxd-remote-{}-{}", getuid(), random_hex(8)));
        let socket_dir = socket_dir.to_string_lossy().into_owned();
        match fs::DirBuilder::new().mode(0o700).create(&socket_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
        match write_persistent_daemon_socket_dir(root, &socket_dir) {
            Ok(()) => return Ok(socket_dir),
            Err(err) => {
                let _ = fs::remove_dir(&socket_dir);
                if err.kind() == io::ErrorKind::AlreadyExists {
                    if let Ok(stored) = read_persistent_daemon_socket_dir(root) {
                        if ensure_private_daemon_leaf_directory(&stored).is_ok() {
                            return Ok(stored);
                        }
                    }
                    remove_persistent_daemon_socket_dir_metadata(root)?;
                    continue;
                }
                return Err(err);
            }
        }
    }
    Err(other_error(
        "failed to create private persistent daemon socket directory",
    ))
}

pub fn write_persistent_daemon_socket_dir(root: &str, socket_dir: &str) -> io::Result<()> {
    let tmp_path = path_join(root, &format!(".socket-dir.{}.tmp", random_hex(6)));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(format!("{socket_dir}\n").as_bytes())?;
        drop(file);
        fs::hard_link(
            &tmp_path,
            path_join(root, PERSISTENT_DAEMON_SOCKET_DIR_FILE),
        )
    })();
    let _ = fs::remove_file(&tmp_path);
    result
}

fn remove_persistent_daemon_socket_dir_metadata(root: &str) -> io::Result<()> {
    match fs::remove_file(path_join(root, PERSISTENT_DAEMON_SOCKET_DIR_FILE)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub fn persistent_daemon_token(paths: &PersistentDaemonPaths) -> io::Result<String> {
    match read_persistent_daemon_token_file(&paths.token_file) {
        Ok(token) => return Ok(token),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let token = random_hex(32);
    let dir = path_dir(&paths.token_file);
    let tmp_path = path_join(&dir, &format!(".auth.token.{}.tmp", random_hex(6)));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)?;
        file.write_all(format!("{token}\n").as_bytes())?;
        drop(file);
        fs::hard_link(&tmp_path, &paths.token_file)
    })();
    let _ = fs::remove_file(&tmp_path);
    match result {
        Ok(()) => Ok(token),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            read_persistent_daemon_token_file(&paths.token_file)
        }
        Err(err) => Err(err),
    }
}

pub fn read_persistent_daemon_token_file(token_file: &str) -> io::Result<String> {
    let data = fs::read_to_string(token_file)?;
    let token = data.trim().to_string();
    if token.is_empty() {
        return Err(other_error("persistent daemon token file is empty"));
    }
    Ok(token)
}

pub fn run_persistent_stdio_proxy(
    stdin: Box<dyn Read + Send>,
    stdout: Box<dyn Write + Send>,
    stderr: &LogSink,
    slot: &str,
    lease_port: i64,
) -> io::Result<()> {
    let paths = persistent_daemon_paths_for_slot(slot)?;
    let paths = ensure_persistent_daemon_directory(paths)?;
    let token = persistent_daemon_token(&paths)?;
    ensure_persistent_daemon_running(&paths, &token, lease_port, stderr)?;
    let conn = dial_persistent_daemon(&paths.socket, &token)?;
    proxy_persistent_daemon_conn(stdin, stdout, conn)
}

/// Pump stdin into the daemon socket and daemon frames back to stdout. Returns
/// as soon as the daemon side closes; a stdin EOF half-closes the socket and
/// then waits for the daemon to finish.
pub fn proxy_persistent_daemon_conn(
    mut stdin: Box<dyn Read + Send>,
    mut stdout: Box<dyn Write + Send>,
    conn: UnixStream,
) -> io::Result<()> {
    let (tx, rx) = flume::bounded::<(&'static str, io::Result<()>)>(2);
    let write_conn = conn.try_clone()?;
    {
        let tx = tx.clone();
        std::thread::Builder::new()
            .name("cmuxd-proxy-stdin".to_string())
            .spawn(move || {
                let mut write_conn = write_conn;
                let result = io::copy(&mut stdin, &mut write_conn).map(|_| ());
                let _ = write_conn.shutdown(std::net::Shutdown::Write);
                let _ = tx.send(("stdin", result));
            })
            .expect("spawn stdin proxy thread");
    }
    {
        let mut read_conn = conn.try_clone()?;
        std::thread::Builder::new()
            .name("cmuxd-proxy-stdout".to_string())
            .spawn(move || {
                let result = io::copy(&mut read_conn, &mut stdout).map(|_| ());
                let _ = stdout.flush();
                let _ = tx.send(("stdout", result));
            })
            .expect("spawn stdout proxy thread");
    }
    let (first_stream, first_result) = rx
        .recv()
        .map_err(|_| other_error("proxy threads exited unexpectedly"))?;
    if first_stream == "stdout" {
        let _ = conn.shutdown(std::net::Shutdown::Both);
        return persistent_proxy_copy_error(first_result);
    }
    let (_, second_result) = rx
        .recv()
        .map_err(|_| other_error("proxy threads exited unexpectedly"))?;
    let _ = conn.shutdown(std::net::Shutdown::Both);
    persistent_proxy_copy_error(first_result)?;
    persistent_proxy_copy_error(second_result)
}

fn persistent_proxy_copy_error(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) => match err.kind() {
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::ConnectionReset => Ok(()),
            _ if err.raw_os_error() == Some(libc::EBADF) => Ok(()),
            _ => Err(err),
        },
    }
}

fn ensure_persistent_daemon_running(
    paths: &PersistentDaemonPaths,
    token: &str,
    lease_port: i64,
    stderr: &LogSink,
) -> io::Result<()> {
    match dial_persistent_daemon(&paths.socket, token) {
        Ok(conn) => {
            drop(conn);
            return Ok(());
        }
        Err(err) => {
            if should_remove_persistent_socket_after_dial_error(&err) {
                let _ = fs::remove_file(&paths.socket);
            } else {
                return Err(err);
            }
        }
    }

    let executable = std::env::current_exe()?;
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&paths.log_file)?;
    let (ready_read, ready_write) = nix::unistd::pipe().map_err(io::Error::from)?;

    let mut cmd = Command::new(executable);
    cmd.args(persistent_daemon_server_arguments(&paths.slot, lease_port));
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::from(log_file.try_clone()?));
    cmd.stderr(Stdio::from(log_file));
    cmd.env(PERSISTENT_DAEMON_READY_FD_ENV, "3");
    let ready_write_raw = ready_write.as_raw_fd();
    // SAFETY: the pre_exec closure only calls async-signal-safe functions.
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(ready_write_raw, 3) < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    drop(ready_write);
    // The daemon outlives this proxy; do not wait for it.
    std::mem::forget(child);

    let ready_file = File::from(ready_read);
    if let Err(err) = wait_persistent_daemon_ready(ready_file, &paths.log_file) {
        if let Ok(conn) = dial_persistent_daemon(&paths.socket, token) {
            drop(conn);
            return Ok(());
        }
        stderr.write_str(&format!("persistent daemon log: {}\n", paths.log_file));
        return Err(err);
    }

    match dial_persistent_daemon(&paths.socket, token) {
        Ok(conn) => {
            drop(conn);
            Ok(())
        }
        Err(err) => {
            stderr.write_str(&format!("persistent daemon log: {}\n", paths.log_file));
            Err(err)
        }
    }
}

pub fn persistent_daemon_server_arguments(slot: &str, lease_port: i64) -> Vec<String> {
    let mut arguments = vec![
        "serve".to_string(),
        "--persistent-server".to_string(),
        "--slot".to_string(),
        slot.to_string(),
    ];
    if lease_port > 0 {
        arguments.push("--persistent-lease-port".to_string());
        arguments.push(lease_port.to_string());
    }
    arguments
}

fn should_remove_persistent_socket_after_dial_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    ) || matches!(err.raw_os_error(), Some(code) if code == libc::ENOENT || code == libc::ECONNREFUSED)
}

fn wait_persistent_daemon_ready(reader: File, log_file: &str) -> io::Result<()> {
    let (tx, rx) = flume::bounded::<io::Result<()>>(1);
    let log_file_owned = log_file.to_string();
    std::thread::Builder::new()
        .name("cmuxd-ready-wait".to_string())
        .spawn(move || {
            let mut line = String::new();
            let result = match BufReader::new(reader).read_line(&mut line) {
                Ok(0) => Err(other_error(format!(
                    "persistent daemon exited before readiness signal; log: {log_file_owned}: EOF"
                ))),
                Ok(_) => {
                    if line.trim() != "ready" {
                        Err(other_error(format!(
                            "persistent daemon sent unexpected readiness signal {:?}; log: {log_file_owned}",
                            line.trim()
                        )))
                    } else {
                        Ok(())
                    }
                }
                Err(err) => Err(other_error(format!(
                    "persistent daemon exited before readiness signal; log: {log_file_owned}: {err}"
                ))),
            };
            let _ = tx.send(result);
        })
        .expect("spawn ready wait thread");
    match rx.recv_timeout(PERSISTENT_DAEMON_STARTUP_TIMEOUT) {
        Ok(result) => result,
        Err(_) => Err(other_error(format!(
            "persistent daemon did not become ready; log: {log_file}"
        ))),
    }
}

pub fn run_persistent_daemon_server(
    slot: &str,
    lease_port: i64,
    stderr: LogSink,
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
        .open(&paths.lock_file)?;
    // SAFETY: flock on an open descriptor.
    if unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(other_error(format!(
            "persistent daemon slot {:?} is already running",
            paths.slot
        )));
    }
    let result = run_persistent_daemon_server_locked(&paths, &token, lease_port, stderr);
    // SAFETY: releasing the lock we hold.
    unsafe {
        libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN);
    }
    result
}

fn run_persistent_daemon_server_locked(
    paths: &PersistentDaemonPaths,
    token: &str,
    lease_port: i64,
    stderr: LogSink,
) -> io::Result<()> {
    let _ = fs::remove_file(&paths.socket);
    let listener = UnixListener::bind(&paths.socket)?;
    let _ = fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600));

    signal_persistent_daemon_ready();
    let lease_removed = Arc::new(AtomicBool::new(false));
    let mut config = PersistentServerConfig {
        empty_idle_timeout: PERSISTENT_DAEMON_EMPTY_IDLE_TIMEOUT,
        ..Default::default()
    };
    if lease_port > 0 {
        let slot = paths.slot.clone();
        config.slot_lease_present = Some(Box::new(move || {
            persistent_daemon_slot_lease_present(&slot, lease_port)
        }));
        let flag = Arc::clone(&lease_removed);
        config.slot_lease_removed = Some(Box::new(move || flag.store(true, Ordering::SeqCst)));
    }
    let stop = StopSignal::new()?;
    let result = serve_persistent_daemon_with_verifier_config(
        listener,
        persistent_daemon_file_token_verifier(token, &paths.token_file),
        stderr,
        config,
        stop,
    );
    let _ = fs::remove_file(&paths.socket);
    if result.is_ok() && lease_removed.load(Ordering::SeqCst) {
        remove_persistent_daemon_relay_shell_directory_if_unleased(lease_port)?;
    }
    result
}

pub fn signal_persistent_daemon_ready() {
    let raw_fd = std::env::var(PERSISTENT_DAEMON_READY_FD_ENV)
        .unwrap_or_default()
        .trim()
        .to_string();
    if raw_fd.is_empty() {
        return;
    }
    let fd: i32 = match raw_fd.parse() {
        Ok(fd) if fd >= 3 => fd,
        _ => return,
    };
    // SAFETY: the parent handed us this descriptor for exactly this purpose.
    let mut file = unsafe { File::from_raw_fd(fd) };
    let _ = file.write_all(b"ready\n");
    drop(file);
}

pub type TokenVerifier = Arc<dyn Fn(&str) -> bool + Send + Sync>;

pub fn persistent_daemon_fixed_token_verifier(token: &str) -> TokenVerifier {
    let token = token.to_string();
    Arc::new(move |provided: &str| persistent_daemon_tokens_equal(provided, &token))
}

pub fn persistent_daemon_file_token_verifier(
    initial_token: &str,
    token_file: &str,
) -> TokenVerifier {
    let initial = initial_token.to_string();
    let token_file = token_file.to_string();
    Arc::new(move |provided: &str| {
        let token =
            read_persistent_daemon_token_file(&token_file).unwrap_or_else(|_| initial.clone());
        persistent_daemon_tokens_equal(provided, &token)
    })
}

pub fn persistent_daemon_tokens_equal(provided: &str, token: &str) -> bool {
    let provided = provided.trim();
    let token = token.trim();
    !provided.is_empty()
        && !token.is_empty()
        && constant_time_eq(provided.as_bytes(), token.as_bytes())
}

pub fn serve_persistent_daemon_with_verifier(
    listener: UnixListener,
    verifier: TokenVerifier,
    stderr: LogSink,
    stop: Arc<StopSignal>,
) -> io::Result<()> {
    serve_persistent_daemon_with_verifier_config(
        listener,
        verifier,
        stderr,
        PersistentServerConfig::default(),
        stop,
    )
}

pub fn serve_persistent_daemon_with_verifier_config(
    listener: UnixListener,
    verifier: TokenVerifier,
    stderr: LogSink,
    config: PersistentServerConfig,
    stop: Arc<StopSignal>,
) -> io::Result<()> {
    let hub = PtyHub::new(PtyHubConfig::default(), Some(stderr.clone()));
    let result = serve_persistent_daemon_loop(&listener, verifier, stderr, config, &stop, &hub);
    hub.close_all();
    result
}

fn serve_persistent_daemon_loop(
    listener: &UnixListener,
    verifier: TokenVerifier,
    stderr: LogSink,
    config: PersistentServerConfig,
    stop: &Arc<StopSignal>,
    hub: &Arc<PtyHub>,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let active_connections = Arc::new(AtomicI64::new(0));
    let mut idle_since: Option<Instant> = None;
    let mut slot_lease_observed = false;
    let request_shutdown: Arc<dyn Fn() + Send + Sync> = {
        let stop = Arc::clone(stop);
        Arc::new(move || stop.stop())
    };
    loop {
        let now = Instant::now();
        let mut accept_deadline: Option<Instant> = None;
        if let Some(present_fn) = &config.slot_lease_present {
            if let Ok(present) = present_fn() {
                if present {
                    slot_lease_observed = true;
                } else if slot_lease_observed && active_connections.load(Ordering::SeqCst) == 0 {
                    if let Some(removed) = &config.slot_lease_removed {
                        removed();
                    }
                    return Ok(());
                }
            }
            accept_deadline = Some(now + persistent_daemon_accept_poll_step(&config));
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
                let idle_deadline =
                    now + remaining.min(persistent_daemon_accept_poll_step(&config));
                accept_deadline = Some(earliest(accept_deadline, idle_deadline));
            } else {
                idle_since = None;
                accept_deadline = Some(earliest(
                    accept_deadline,
                    now + persistent_daemon_accept_poll_step(&config),
                ));
            }
        }
        let timeout =
            accept_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
        match accept_unix_with_stop(listener, stop, timeout)? {
            Some(conn) => {
                active_connections.fetch_add(1, Ordering::SeqCst);
                let verifier = Arc::clone(&verifier);
                let hub = Arc::clone(hub);
                let active = Arc::clone(&active_connections);
                let request_shutdown = Arc::clone(&request_shutdown);
                let stderr = stderr.clone();
                std::thread::Builder::new()
                    .name("cmuxd-persistent-conn".to_string())
                    .spawn(move || {
                        handle_persistent_daemon_conn_with_auth_timeout(
                            conn,
                            verifier,
                            hub,
                            PERSISTENT_DAEMON_AUTH_TIMEOUT,
                            Some(request_shutdown),
                            stderr,
                        );
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .expect("spawn persistent conn thread");
            }
            None => {
                if stop.is_stopped() {
                    return Ok(());
                }
            }
        }
    }
}

fn earliest(a: Option<Instant>, b: Instant) -> Instant {
    match a {
        Some(a) if a < b => a,
        _ => b,
    }
}

fn persistent_daemon_accept_poll_step(config: &PersistentServerConfig) -> Duration {
    if !config.accept_poll_step.is_zero() {
        return config.accept_poll_step;
    }
    PERSISTENT_DAEMON_EMPTY_IDLE_POLL_STEP
}

pub fn handle_persistent_daemon_conn_with_auth_timeout(
    conn: UnixStream,
    verifier: TokenVerifier,
    hub: Arc<PtyHub>,
    timeout: Duration,
    request_shutdown: Option<Arc<dyn Fn() + Send + Sync>>,
    stderr: LogSink,
) {
    let deadline = if timeout.is_zero() {
        None
    } else {
        Some(timeout)
    };
    if conn.set_read_timeout(deadline).is_err() || conn.set_write_timeout(deadline).is_err() {
        return;
    }
    let read_half = match conn.try_clone() {
        Ok(half) => half,
        Err(_) => return,
    };
    let write_half = match conn.try_clone() {
        Ok(half) => half,
        Err(_) => return,
    };
    let mut reader = BufReader::with_capacity(64 * 1024, read_half);
    let writer = StdioFrameWriter::new(Box::new(write_half));
    if !authenticate_persistent_daemon_conn(&mut reader, &writer, &verifier) {
        return;
    }
    if !timeout.is_zero()
        && (conn.set_read_timeout(None).is_err() || conn.set_write_timeout(None).is_err())
    {
        return;
    }
    let shutdown_ref = request_shutdown.as_ref().map(|f| f.as_ref());
    let _ = run_rpc_server_with_reader(&mut reader, writer, hub, false, shutdown_ref, stderr);
    let _ = conn.shutdown(std::net::Shutdown::Both);
}

fn authenticate_persistent_daemon_conn<R: BufRead>(
    reader: &mut R,
    writer: &Arc<StdioFrameWriter>,
    verifier: &TokenVerifier,
) -> bool {
    let (line, oversized) = match read_rpc_frame(reader, MAX_RPC_FRAME_BYTES) {
        Ok(frame) => frame,
        Err(_) => {
            let _ = writer.write_response(&RpcResponse::err(
                None,
                "unauthorized",
                "persistent daemon authentication required",
            ));
            return false;
        }
    };
    if oversized {
        let _ = writer.write_response(&RpcResponse::err(
            None,
            "unauthorized",
            "persistent daemon authentication required",
        ));
        return false;
    }
    let req = match RpcRequest::parse(trim_frame(&line)) {
        Ok(req) => req,
        Err(_) => {
            let _ = writer.write_response(&RpcResponse::err(
                None,
                "invalid_request",
                "invalid JSON request",
            ));
            return false;
        }
    };
    if req.method != PERSISTENT_DAEMON_AUTH_METHOD {
        let _ = writer.write_response(&RpcResponse::err(
            req.id.clone(),
            "unauthorized",
            "persistent daemon authentication required",
        ));
        return false;
    }
    let provided = get_string_param(req.params(), "token").unwrap_or_default();
    if !verifier(&provided) {
        let _ = writer.write_response(&RpcResponse::err(
            req.id.clone(),
            "unauthorized",
            "invalid persistent daemon token",
        ));
        return false;
    }
    let _ = writer.write_response(&RpcResponse::ok(
        req.id.clone(),
        json!({"authenticated": true}),
    ));
    true
}

pub fn dial_persistent_daemon(socket_path: &str, token: &str) -> io::Result<UnixStream> {
    let conn = connect_unix_timeout(socket_path, Duration::from_secs(2))?;
    if let Err(err) = authenticate_persistent_daemon_client(&conn, token) {
        let _ = conn.shutdown(std::net::Shutdown::Both);
        return Err(err);
    }
    Ok(conn)
}

fn connect_unix_timeout(socket_path: &str, _timeout: Duration) -> io::Result<UnixStream> {
    // Unix socket connects complete immediately (or fail with ENOENT /
    // ECONNREFUSED), so the timeout is only nominal here.
    UnixStream::connect(socket_path)
}

pub fn authenticate_persistent_daemon_client(conn: &UnixStream, token: &str) -> io::Result<()> {
    authenticate_persistent_daemon_client_with_timeout(conn, token, PERSISTENT_DAEMON_AUTH_TIMEOUT)
}

pub fn authenticate_persistent_daemon_client_with_timeout(
    conn: &UnixStream,
    token: &str,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = if timeout.is_zero() {
        None
    } else {
        Some(timeout)
    };
    conn.set_read_timeout(deadline)?;
    conn.set_write_timeout(deadline)?;
    let result = authenticate_client_inner(conn, token);
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    result
}

fn authenticate_client_inner(conn: &UnixStream, token: &str) -> io::Result<()> {
    let mut params = serde_json::Map::new();
    params.insert(
        "token".to_string(),
        serde_json::Value::String(token.to_string()),
    );
    let request = RpcRequest::new("auth", PERSISTENT_DAEMON_AUTH_METHOD, Some(params));
    let mut writer = conn;
    writer.write_all(request.to_json().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let mut reader = BufReader::with_capacity(64 * 1024, conn);
    let (line, oversized) = read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES)?;
    if oversized {
        return Err(other_error(
            "persistent daemon auth response exceeded maximum size",
        ));
    }
    let resp: RpcResponse =
        serde_json::from_slice(trim_frame(&line)).map_err(|err| other_error(err.to_string()))?;
    if !resp.ok {
        let mut message = "persistent daemon authentication failed".to_string();
        if !resp.error_message().trim().is_empty() {
            message = resp.error_message().trim().to_string();
        }
        return Err(io::Error::other(PersistentDaemonAuthFailed { message }));
    }
    Ok(())
}

pub fn existing_persistent_daemon_paths_for_slot(
    slot: &str,
) -> io::Result<(PersistentDaemonPaths, bool)> {
    let mut paths = persistent_daemon_paths_for_slot(slot)?;
    match verify_private_daemon_directory(&paths.root) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok((paths, false)),
        Err(err) => return Err(err),
    }
    match read_persistent_daemon_socket_dir(&paths.root) {
        Ok(stored) => {
            paths.socket = path_join(&stored, &path_base(&paths.socket));
            match verify_private_daemon_directory(&stored) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok((paths, true)),
                Err(err) => return Err(err),
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
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
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            wait_for_persistent_daemon_stop(&paths.lock_file)?;
            let _ = fs::remove_file(&paths.socket);
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let conn = match dial_persistent_daemon(&paths.socket, &token) {
        Ok(conn) => conn,
        Err(err) => {
            if should_remove_persistent_socket_after_dial_error(&err) {
                let _ = fs::remove_file(&paths.socket);
                return wait_for_persistent_daemon_stop(&paths.lock_file);
            }
            return Err(err);
        }
    };
    if let Err(err) = request_persistent_daemon_shutdown(&conn) {
        let _ = conn.shutdown(std::net::Shutdown::Both);
        return Err(err);
    }
    let _ = conn.shutdown(std::net::Shutdown::Both);
    wait_for_persistent_daemon_stop(&paths.lock_file)
}

pub fn request_persistent_daemon_shutdown(conn: &UnixStream) -> io::Result<()> {
    conn.set_read_timeout(Some(PERSISTENT_DAEMON_AUTH_TIMEOUT))?;
    conn.set_write_timeout(Some(PERSISTENT_DAEMON_AUTH_TIMEOUT))?;
    let result = request_shutdown_inner(conn);
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    result
}

fn request_shutdown_inner(conn: &UnixStream) -> io::Result<()> {
    let request = RpcRequest::new(
        "shutdown",
        PERSISTENT_DAEMON_SHUTDOWN_METHOD,
        Some(serde_json::Map::new()),
    );
    let mut writer = conn;
    writer.write_all(format!("{}\n", request.to_json()).as_bytes())?;
    writer.flush()?;
    let mut reader = BufReader::with_capacity(64 * 1024, conn);
    let (line, oversized) = read_rpc_frame(&mut reader, MAX_RPC_FRAME_BYTES)?;
    if oversized {
        return Err(other_error(
            "persistent daemon shutdown response exceeds maximum size",
        ));
    }
    let response: RpcResponse =
        serde_json::from_slice(trim_frame(&line)).map_err(|err| other_error(err.to_string()))?;
    if !response.ok {
        return Err(other_error("persistent daemon shutdown rejected"));
    }
    Ok(())
}

pub fn wait_for_persistent_daemon_stop(lock_path: &str) -> io::Result<()> {
    wait_for_persistent_daemon_stop_with_timeout(
        lock_path,
        PERSISTENT_DAEMON_STOP_WAIT_TIMEOUT,
        PERSISTENT_DAEMON_STOP_RETRY_STEP,
    )
}

pub fn wait_for_persistent_daemon_stop_with_timeout(
    lock_path: &str,
    timeout: Duration,
    retry_step: Duration,
) -> io::Result<()> {
    if timeout.is_zero() || retry_step.is_zero() {
        return Err(other_error(
            "persistent daemon stop wait requires positive timeout and retry step",
        ));
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(lock_path)?;
    let deadline = Instant::now() + timeout;
    loop {
        // SAFETY: flock on an open descriptor.
        let rc = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // SAFETY: releasing the lock we just acquired.
            let rc = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if !matches!(err.raw_os_error(), Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN)
        {
            return Err(err);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(other_error(format!(
                "timed out waiting for persistent daemon ownership release after {}",
                go_duration(timeout)
            )));
        }
        std::thread::sleep(retry_step.min(remaining));
    }
}

/// Format a duration the way Go's `time.Duration.String` does for the common
/// second/millisecond cases used in daemon diagnostics.
pub fn go_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_string();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format!("{}µs", trim_float(nanos as f64 / 1_000.0));
    }
    if nanos < 1_000_000_000 {
        return format!("{}ms", trim_float(nanos as f64 / 1_000_000.0));
    }
    let total_secs = nanos as f64 / 1_000_000_000.0;
    if total_secs < 60.0 {
        return format!("{}s", trim_float(total_secs));
    }
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let rem_secs = (nanos as f64 - (hours * 3600 + minutes * 60) as f64 * 1e9) / 1e9;
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    out.push_str(&format!("{minutes}m"));
    out.push_str(&format!("{}s", trim_float(rem_secs)));
    out
}

fn trim_float(value: f64) -> String {
    let text = format!("{value:.9}");
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn persistent_daemon_relay_path(lease_port: i64, suffix: &str) -> io::Result<String> {
    if lease_port <= 0 || lease_port > 65535 {
        return Err(other_error(format!(
            "invalid persistent daemon lease port {lease_port}"
        )));
    }
    let home = home_dir().ok_or_else(|| other_error("cannot resolve remote home directory"))?;
    Ok(path_join(
        &home,
        &format!(".cmux/relay/{lease_port}{suffix}"),
    ))
}

pub fn persistent_daemon_slot_lease_present(slot: &str, lease_port: i64) -> io::Result<bool> {
    let lease_path = persistent_daemon_relay_path(lease_port, ".slot")?;
    let info = match fs::symlink_metadata(&lease_path) {
        Ok(info) => info,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    if !info.file_type().is_file() || !daemon_directory_owned_by_current_user(&info) {
        return Err(other_error(format!(
            "persistent daemon lease {lease_path:?} is not a private regular file"
        )));
    }
    let data = fs::read_to_string(&lease_path)?;
    Ok(data.trim() == slot)
}

pub fn remove_persistent_daemon_relay_shell_directory_if_unleased(
    lease_port: i64,
) -> io::Result<()> {
    let lease_path = persistent_daemon_relay_path(lease_port, ".slot")?;
    match fs::symlink_metadata(&lease_path) {
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let shell_path = persistent_daemon_relay_path(lease_port, ".shell")?;
    match fs::remove_dir_all(&shell_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Guard used by tests to hold the slot lock the way a running daemon does.
#[doc(hidden)]
pub struct SlotLockGuard {
    file: File,
}

#[doc(hidden)]
impl SlotLockGuard {
    pub fn acquire(lock_path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(lock_path)?;
        // SAFETY: flock on an open descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { file })
    }

    pub fn release(self) {
        // SAFETY: releasing the lock we hold.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[doc(hidden)]
pub static PERSISTENT_TEST_GUARD: Mutex<()> = Mutex::new(());

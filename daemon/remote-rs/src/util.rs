//! Shared helpers that mirror small pieces of the Go standard library the
//! original daemon relied on (path cleaning, JSON marshalling quirks, timers,
//! close-once signals) so the Rust port keeps byte-compatible behavior.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

const COMPILE_TIME_VERSION: Option<&str> = option_env!("CMUXD_REMOTE_VERSION");

fn version_cell() -> &'static Mutex<String> {
    static CELL: OnceLock<Mutex<String>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(COMPILE_TIME_VERSION.unwrap_or("dev").to_string()))
}

/// Daemon version string. Set at build time through `CMUXD_REMOTE_VERSION`
/// (the Go build used `-X main.version=`), defaulting to `dev`.
pub fn version() -> String {
    version_cell().lock().unwrap().clone()
}

/// Override the reported version. Intended for tests that pin the version
/// component of persistent daemon paths.
pub fn set_version(value: &str) {
    *version_cell().lock().unwrap() = value.to_string();
}

/// Go's `os.UserHomeDir` on Unix: `$HOME`, error when unset or empty.
pub fn home_dir() -> Option<String> {
    let home = crate::util::env_var("HOME")?;
    if home.trim().is_empty() {
        return None;
    }
    Some(home)
}

/// Go's `os.TempDir` on Unix: `$TMPDIR` else `/tmp`.
pub fn temp_dir() -> PathBuf {
    match crate::util::env_var("TMPDIR").ok_or(std::env::VarError::NotPresent) {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => PathBuf::from("/tmp"),
    }
}

pub fn getuid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

/// Go's `filepath.Clean` for Unix paths.
pub fn clean_path(raw: &str) -> String {
    if raw.is_empty() {
        return ".".to_string();
    }
    let rooted = raw.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in raw.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

/// Go's `filepath.Abs`: absolute paths are cleaned, relative ones are joined
/// with the current working directory.
pub fn abs_path(raw: &str) -> Option<String> {
    if raw.starts_with('/') {
        return Some(clean_path(raw));
    }
    let cwd = std::env::current_dir().ok()?;
    let cwd = cwd.to_string_lossy();
    Some(clean_path(&format!("{cwd}/{raw}")))
}

pub fn path_join(base: &str, rest: &str) -> String {
    if rest.is_empty() {
        return clean_path(base);
    }
    if base.is_empty() {
        return clean_path(rest);
    }
    clean_path(&format!("{base}/{rest}"))
}

pub fn path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        Some(idx) => trimmed[idx + 1..].to_string(),
        None => trimmed.to_string(),
    }
}

pub fn path_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(idx) => {
            let dir = &path[..=idx];
            clean_path(dir)
        }
        None => ".".to_string(),
    }
}

pub fn first_non_empty<'a>(values: &[&'a str]) -> &'a str {
    values.iter().copied().find(|v| !v.is_empty()).unwrap_or("")
}

/// Go's `encoding/json` escapes `<`, `>`, `&`, U+2028 and U+2029 inside
/// strings. Those characters can only appear inside string literals in valid
/// JSON, so a blanket substitution on serde's output is safe.
pub fn go_json_escape(encoded: &str) -> String {
    if !encoded
        .chars()
        .any(|c| matches!(c, '<' | '>' | '&' | '\u{2028}' | '\u{2029}'))
    {
        return encoded.to_string();
    }
    let mut out = String::with_capacity(encoded.len() + 16);
    for c in encoded.chars() {
        match c {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            other => out.push(other),
        }
    }
    out
}

/// Serialize like Go's `json.Marshal` (compact, sorted map keys, HTML-safe).
pub fn go_json(value: &Value) -> String {
    go_json_escape(&serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()))
}

/// Serialize like Go's `json.MarshalIndent(v, "", "  ")`.
pub fn go_json_pretty(value: &Value) -> String {
    go_json_escape(&serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_string()))
}

/// Go encodes `float64` values without a fractional part as integers. Use this
/// when building params from computed floats.
pub fn json_number(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 9.0e15 {
        Value::from(value as i64)
    } else {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut buf = vec![0u8; n];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

pub fn random_hex(n: usize) -> String {
    hex::encode(random_bytes(n))
}

/// FNV-1a 64-bit hash, matching Go's `hash/fnv` `New64a`.
pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub fn now_rfc3339_nano() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_rfc3339_nano(now.as_secs() as i64, now.subsec_nanos())
}

/// Format a UTC timestamp like Go's `time.RFC3339Nano` (trailing zeros in the
/// fractional part are trimmed).
pub fn format_rfc3339_nano(unix_secs: i64, nanos: u32) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(unix_secs);
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos > 0 {
        let frac = format!("{nanos:09}");
        let trimmed = frac.trim_end_matches('0');
        out.push('.');
        out.push_str(trimmed);
    }
    out.push('Z');
    out
}

fn civil_from_unix(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hour = (rem / 3600) as u32;
    let minute = ((rem % 3600) / 60) as u32;
    let second = (rem % 60) as u32;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, hour, minute, second)
}

/// A shared, thread-safe diagnostic sink (the Go code passed `io.Writer`s for
/// stderr around; tests capture them in buffers).
#[derive(Clone)]
pub struct LogSink {
    inner: Arc<Mutex<Box<dyn Write + Send>>>,
}

impl LogSink {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(writer)),
        }
    }

    pub fn discard() -> Self {
        Self::new(Box::new(io::sink()))
    }

    pub fn stderr() -> Self {
        Self::new(Box::new(io::stderr()))
    }

    pub fn write_str(&self, text: &str) {
        if let Ok(mut guard) = self.inner.lock() {
            let _ = guard.write_all(text.as_bytes());
            let _ = guard.flush();
        }
    }
}

impl Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.inner.lock().unwrap();
        guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self.inner.lock().unwrap();
        guard.flush()
    }
}

/// In-memory buffer that can be shared between the daemon (as a writer) and a
/// test (as a reader).
#[derive(Clone, Default)]
pub struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contents(&self) -> Vec<u8> {
        self.inner.lock().unwrap().clone()
    }

    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.contents()).into_owned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A close-once broadcast signal. Mirrors Go's `close(chan struct{})`: every
/// receiver observes the disconnect, and waiting on it before close blocks.
#[derive(Clone)]
pub struct DoneSignal {
    sender: Arc<Mutex<Option<flume::Sender<()>>>>,
    receiver: flume::Receiver<()>,
}

impl Default for DoneSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl DoneSignal {
    pub fn new() -> Self {
        let (sender, receiver) = flume::bounded::<()>(0);
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver,
        }
    }

    pub fn close(&self) {
        if let Ok(mut guard) = self.sender.lock() {
            guard.take();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.receiver.is_disconnected()
    }

    pub fn receiver(&self) -> &flume::Receiver<()> {
        &self.receiver
    }

    pub fn wait(&self) {
        let _ = self.receiver.recv();
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        matches!(
            self.receiver.recv_timeout(timeout),
            Err(flume::RecvTimeoutError::Disconnected)
        )
    }

    pub async fn wait_async(&self) {
        let _ = self.receiver.recv_async().await;
    }
}

/// A single-thread timer queue standing in for Go's `time.AfterFunc`.
pub struct TimerQueue {
    state: Mutex<TimerState>,
    cv: Condvar,
}

#[derive(Default)]
struct TimerState {
    next_id: u64,
    entries: BTreeMap<(Instant, u64), Box<dyn FnOnce() + Send>>,
    started: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerHandle {
    deadline: Instant,
    id: u64,
}

impl Default for TimerQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TimerQueue {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TimerState::default()),
            cv: Condvar::new(),
        }
    }

    pub fn shared() -> Arc<TimerQueue> {
        static GLOBAL: OnceLock<Arc<TimerQueue>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(TimerQueue::new())).clone()
    }

    pub fn schedule(
        self: &Arc<Self>,
        delay: Duration,
        callback: Box<dyn FnOnce() + Send>,
    ) -> TimerHandle {
        let deadline = Instant::now() + delay;
        let mut state = self.state.lock().unwrap();
        state.next_id += 1;
        let handle = TimerHandle {
            deadline,
            id: state.next_id,
        };
        state.entries.insert((deadline, handle.id), callback);
        if !state.started {
            state.started = true;
            let queue = Arc::clone(self);
            std::thread::Builder::new()
                .name("cmuxd-timer".to_string())
                .spawn(move || queue.run())
                .expect("spawn timer thread");
        }
        drop(state);
        self.cv.notify_all();
        handle
    }

    pub fn cancel(&self, handle: TimerHandle) -> bool {
        let mut state = self.state.lock().unwrap();
        state
            .entries
            .remove(&(handle.deadline, handle.id))
            .is_some()
    }

    fn run(&self) {
        loop {
            let callback = {
                let mut state = self.state.lock().unwrap();
                loop {
                    let now = Instant::now();
                    match state.entries.keys().next().copied() {
                        None => {
                            state = self.cv.wait(state).unwrap();
                        }
                        Some(key) if key.0 <= now => {
                            break state.entries.remove(&key).unwrap();
                        }
                        Some(key) => {
                            let wait = key.0.saturating_duration_since(now);
                            state = self.cv.wait_timeout(state, wait).unwrap().0;
                        }
                    }
                }
            };
            callback();
        }
    }
}

/// Poll a file descriptor for readability, optionally waking early through a
/// second descriptor. Returns `Ok(true)` when `fd` is readable, `Ok(false)` on
/// timeout or wake, and errors otherwise.
pub fn poll_readable(
    fd: BorrowedFd<'_>,
    wake: Option<BorrowedFd<'_>>,
    timeout: Option<Duration>,
) -> io::Result<PollOutcome> {
    let mut fds = [
        libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.map(|w| w.as_raw_fd()).unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let count = if wake.is_some() { 2 } else { 1 };
    let timeout_ms: libc::c_int = match timeout {
        None => -1,
        Some(d) => d.as_millis().min(i32::MAX as u128) as libc::c_int,
    };
    loop {
        // SAFETY: fds points to a valid array of `count` pollfd entries.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), count as libc::nfds_t, timeout_ms) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if rc == 0 {
            return Ok(PollOutcome::Timeout);
        }
        if wake.is_some() && fds[1].revents != 0 {
            return Ok(PollOutcome::Woken);
        }
        if fds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
            return Ok(PollOutcome::Ready);
        }
        return Ok(PollOutcome::Timeout);
    }
}

pub fn poll_writable(
    fd: BorrowedFd<'_>,
    wake: Option<BorrowedFd<'_>>,
    timeout: Option<Duration>,
) -> io::Result<PollOutcome> {
    let mut fds = [
        libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.map(|w| w.as_raw_fd()).unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let count = if wake.is_some() { 2 } else { 1 };
    let timeout_ms: libc::c_int = match timeout {
        None => -1,
        Some(d) => d.as_millis().min(i32::MAX as u128) as libc::c_int,
    };
    loop {
        // SAFETY: fds points to a valid array of `count` pollfd entries.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), count as libc::nfds_t, timeout_ms) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if rc == 0 {
            return Ok(PollOutcome::Timeout);
        }
        if wake.is_some() && fds[1].revents != 0 {
            return Ok(PollOutcome::Woken);
        }
        return Ok(PollOutcome::Ready);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollOutcome {
    Ready,
    Timeout,
    Woken,
}

/// A self-pipe used to wake blocking poll loops.
pub struct WakePipe {
    read: OwnedFd,
    write: OwnedFd,
}

impl WakePipe {
    pub fn new() -> io::Result<Self> {
        let (read, write) = nix::unistd::pipe().map_err(io::Error::from)?;
        set_nonblocking(&read)?;
        set_nonblocking(&write)?;
        set_cloexec(&read)?;
        set_cloexec(&write)?;
        Ok(Self { read, write })
    }

    pub fn wake(&self) {
        let byte = [1u8];
        // SAFETY: valid fd and buffer; EAGAIN when full is fine (already woken).
        let _ = unsafe { libc::write(self.write.as_raw_fd(), byte.as_ptr().cast(), 1) };
    }

    pub fn read_fd(&self) -> BorrowedFd<'_> {
        use std::os::fd::AsFd;
        self.read.as_fd()
    }

    pub fn drain(&self) {
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: valid fd and buffer.
            let n =
                unsafe { libc::read(self.read.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                break;
            }
        }
    }
}

pub fn set_nonblocking(fd: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: fcntl on a valid descriptor.
    unsafe {
        let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn set_cloexec(fd: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: fcntl on a valid descriptor.
    unsafe {
        let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFD);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Constant-time byte comparison.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

pub fn is_uuidish(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    for (i, c) in s.chars().enumerate() {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            if c != '-' {
                return false;
            }
        } else if !c.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

/// Cooperative stop flag with a wake pipe so blocking accept/poll loops can be
/// interrupted from another thread (Go closed the listener instead).
pub struct StopSignal {
    flag: std::sync::atomic::AtomicBool,
    wake: WakePipe,
}

impl StopSignal {
    pub fn new() -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            flag: std::sync::atomic::AtomicBool::new(false),
            wake: WakePipe::new()?,
        }))
    }

    pub fn stop(&self) {
        self.flag.store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake.wake();
    }

    pub fn is_stopped(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn read_fd(&self) -> BorrowedFd<'_> {
        self.wake.read_fd()
    }
}

/// Accept a Unix connection, returning `Ok(None)` when the timeout elapses or
/// the stop signal fires. The listener must be non-blocking.
pub fn accept_unix_with_stop(
    listener: &std::os::unix::net::UnixListener,
    stop: &StopSignal,
    timeout: Option<Duration>,
) -> io::Result<Option<std::os::unix::net::UnixStream>> {
    use std::os::fd::AsFd;
    loop {
        if stop.is_stopped() {
            return Ok(None);
        }
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                return Ok(Some(stream));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
        match poll_readable(listener.as_fd(), Some(stop.read_fd()), timeout)? {
            PollOutcome::Ready => continue,
            PollOutcome::Timeout | PollOutcome::Woken => return Ok(None),
        }
    }
}

// --- process environment, serialized like Go's os.Getenv/os.Setenv ---
//
// glibc's setenv may reallocate `environ` while another thread's getenv walks
// it. Worker threads abandoned after a relay timeout (see agent_launch.rs) can
// still be reading the environment while agent launch rewrites it, so every
// env access in the crate goes through this lock.

fn env_lock() -> &'static std::sync::RwLock<()> {
    static LOCK: OnceLock<std::sync::RwLock<()>> = OnceLock::new();
    LOCK.get_or_init(|| std::sync::RwLock::new(()))
}

pub fn env_var(key: &str) -> Option<String> {
    let _guard = env_lock()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::env::var(key).ok()
}

pub fn env_var_or_default(key: &str) -> String {
    env_var(key).unwrap_or_default()
}

pub fn env_vars_os() -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let _guard = env_lock()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::env::vars_os().collect()
}

pub fn env_set(key: &str, value: impl AsRef<str>) {
    let _guard = env_lock()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::env::set_var(key, value.as_ref());
}

pub fn env_remove(key: &str) {
    let _guard = env_lock()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::env::remove_var(key);
}

/// Render an I/O error the way Go's `syscall.Errno` / `os.PathError` would
/// ("no such file or directory", "connection refused"), so relay and daemon
/// error strings match the Go implementation byte for byte.
pub fn go_io_error(err: &io::Error) -> String {
    if let Some(code) = err.raw_os_error() {
        // SAFETY: strerror returns a pointer to a static, NUL-terminated string.
        let text = unsafe { std::ffi::CStr::from_ptr(libc::strerror(code)) }
            .to_string_lossy()
            .into_owned();
        let mut chars = text.chars();
        return match chars.next() {
            Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
            None => text,
        };
    }
    let text = err.to_string();
    match text.find(" (os error ") {
        Some(idx) => text[..idx].to_string(),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_path_matches_go_semantics() {
        assert_eq!(clean_path("/a/b/../c/./d/"), "/a/c/d");
        assert_eq!(clean_path("a//b"), "a/b");
        assert_eq!(clean_path("/.."), "/");
        assert_eq!(clean_path("../a"), "../a");
        assert_eq!(clean_path(""), ".");
        assert_eq!(clean_path("/"), "/");
    }

    #[test]
    fn go_json_escapes_html_characters() {
        let value = serde_json::json!({"text": "<a&b>"});
        let expected = format!("{{\"text\":\"{}a{}b{}\"}}", "\\u003c", "\\u0026", "\\u003e");
        assert_eq!(go_json(&value), expected);
    }

    #[test]
    fn rfc3339_nano_trims_zero_fraction() {
        assert_eq!(format_rfc3339_nano(0, 0), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_rfc3339_nano(1_700_000_000, 120_000_000),
            "2023-11-14T22:13:20.12Z"
        );
    }

    #[test]
    fn fnv1a64_matches_reference_vector() {
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
    }

    #[test]
    fn done_signal_reports_close() {
        let done = DoneSignal::new();
        assert!(!done.is_closed());
        assert!(!done.wait_timeout(Duration::from_millis(10)));
        done.close();
        assert!(done.is_closed());
        assert!(done.wait_timeout(Duration::from_millis(10)));
    }

    #[test]
    fn timer_queue_runs_and_cancels() {
        let queue = Arc::new(TimerQueue::new());
        let (tx, rx) = flume::unbounded();
        let tx2 = tx.clone();
        let cancelled = queue.schedule(
            Duration::from_millis(200),
            Box::new(move || tx2.send(2).unwrap()),
        );
        queue.schedule(
            Duration::from_millis(20),
            Box::new(move || tx.send(1).unwrap()),
        );
        assert!(queue.cancel(cancelled));
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());
    }
}

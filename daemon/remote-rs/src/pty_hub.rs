//! PTY session hub shared by the stdio RPC server, the persistent per-slot
//! daemon, and the cloud WebSocket transport. Mirrors `ws_pty.go`.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::util::{
    constant_time_eq, poll_readable, poll_writable, set_cloexec, set_nonblocking, DoneSignal,
    LogSink, PollOutcome, TimerHandle, TimerQueue, WakePipe,
};

pub const DEFAULT_PTY_COLS: i64 = 80;
pub const DEFAULT_PTY_ROWS: i64 = 24;
pub const MAX_PTY_DIMENSION: i64 = 65535;
pub const DEFAULT_WEBSOCKET_SCROLLBACK_CAP: usize = 1 << 20;
pub const DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES: usize = 48 * 1024;
pub const DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP: usize = 256;
pub const DEFAULT_PTY_INPUT_QUEUE_CAP: usize = 256;
pub const DEFAULT_PTY_INPUT_CHUNK_BYTES: usize = 16 * 1024;
pub const DEFAULT_WEBSOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WEBSOCKET_SESSION_IDLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Startup scripts above this size are written to a temp file so `execve`
/// does not fail with E2BIG (Linux MAX_ARG_STRLEN is ~128KB).
const MAX_INLINE_STARTUP_SCRIPT_BYTES: usize = 120 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SessionKind {
    Persistent = 0,
    Anonymous = 1,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub kind: SessionKind,
    pub session_id: String,
    pub anonymous_id: u64,
}

pub fn persistent_pty_session_key(session_id: &str) -> SessionKey {
    SessionKey {
        kind: SessionKind::Persistent,
        session_id: session_id.to_string(),
        anonymous_id: 0,
    }
}

pub fn anonymous_pty_session_key(session_id: &str, anonymous_id: u64) -> SessionKey {
    SessionKey {
        kind: SessionKind::Anonymous,
        session_id: session_id.to_string(),
        anonymous_id,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    Binary,
    Text,
}

#[derive(Clone, Debug)]
pub struct OutgoingFrame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
    pub input_ack: bool,
}

impl OutgoingFrame {
    pub fn binary(payload: Vec<u8>) -> Self {
        Self {
            kind: FrameKind::Binary,
            payload,
            input_ack: false,
        }
    }

    pub fn text(payload: Vec<u8>) -> Self {
        Self {
            kind: FrameKind::Text,
            payload,
            input_ack: false,
        }
    }

    pub fn input_ack() -> Self {
        Self {
            kind: FrameKind::Binary,
            payload: Vec::new(),
            input_ack: true,
        }
    }
}

#[derive(Clone)]
pub struct InputChunk {
    pub attachment_id: String,
    pub attachment: Option<Arc<PtyAttachment>>,
    pub payload: Vec<u8>,
    pub seq: u64,
    pub final_seq_chunk: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputWriteStatus {
    Ok,
    NotFound,
    QueueFull,
    SeqGap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputWriteResult {
    pub status: InputWriteStatus,
    pub got: u64,
    pub want: u64,
}

impl InputWriteResult {
    fn status(status: InputWriteStatus) -> Self {
        Self {
            status,
            got: 0,
            want: 0,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WsPtyEventFrame {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WsPtyControlFrame {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cols: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub rows: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Default)]
struct AckState {
    queued: bool,
    seq: u64,
}

/// A drop-to-cancel token mirroring Go's `context.CancelFunc`.
#[derive(Clone)]
pub struct CancelToken {
    signal: DoneSignal,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            signal: DoneSignal::new(),
        }
    }

    pub fn cancel(&self) {
        self.signal.close();
    }

    pub fn is_cancelled(&self) -> bool {
        self.signal.is_closed()
    }

    pub fn receiver(&self) -> &flume::Receiver<()> {
        self.signal.receiver()
    }

    pub async fn cancelled(&self) {
        self.signal.wait_async().await;
    }
}

pub struct PtyAttachment {
    pub session_key: SessionKey,
    pub id: String,
    pub client_token: String,
    cols: AtomicI64,
    rows: AtomicI64,
    send_tx: flume::Sender<OutgoingFrame>,
    send_rx: flume::Receiver<OutgoingFrame>,
    cancel: CancelToken,
    pub persistent: bool,
    input_seq_ack: AtomicBool,
    last_accepted_seq: AtomicU64,
    ack: Mutex<AckState>,
}

impl PtyAttachment {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_test(
        session_key: SessionKey,
        id: &str,
        client_token: &str,
        cols: i64,
        rows: i64,
        queue_cap: usize,
        persistent: bool,
        input_seq_ack: bool,
    ) -> Arc<Self> {
        let (send_tx, send_rx) = flume::bounded(queue_cap);
        Arc::new(Self {
            session_key,
            id: id.to_string(),
            client_token: client_token.to_string(),
            cols: AtomicI64::new(cols),
            rows: AtomicI64::new(rows),
            send_tx,
            send_rx,
            cancel: CancelToken::new(),
            persistent,
            input_seq_ack: AtomicBool::new(input_seq_ack),
            last_accepted_seq: AtomicU64::new(0),
            ack: Mutex::new(AckState::default()),
        })
    }

    pub fn cols(&self) -> i64 {
        self.cols.load(Ordering::SeqCst)
    }

    pub fn rows(&self) -> i64 {
        self.rows.load(Ordering::SeqCst)
    }

    pub fn input_seq_ack(&self) -> bool {
        self.input_seq_ack.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn set_input_seq_ack(&self, value: bool) {
        self.input_seq_ack.store(value, Ordering::SeqCst);
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn cancel_token(&self) -> &CancelToken {
        &self.cancel
    }

    pub fn frames(&self) -> &flume::Receiver<OutgoingFrame> {
        &self.send_rx
    }

    pub fn queue_capacity(&self) -> usize {
        self.send_tx.capacity().unwrap_or(usize::MAX)
    }

    /// Queue a raw frame. Mirrors the Go channel send with `default:` fallback:
    /// a full queue cancels the attachment and returns `false`.
    #[doc(hidden)]
    pub fn enqueue_frame(&self, frame: OutgoingFrame) -> bool {
        match self.send_tx.try_send(frame) {
            Ok(()) => true,
            Err(_) => {
                self.cancel();
                false
            }
        }
    }

    pub fn enqueue_binary(&self, payload: &[u8]) -> bool {
        self.enqueue_frame(OutgoingFrame::binary(payload.to_vec()))
    }

    pub fn enqueue_json<T: Serialize>(&self, payload: &T) -> bool {
        match serde_json::to_vec(payload) {
            Ok(data) => self.enqueue_frame(OutgoingFrame::text(data)),
            Err(_) => {
                self.cancel();
                false
            }
        }
    }

    pub fn enqueue_ready(&self, session_id: &str) -> bool {
        self.enqueue_json(&WsPtyEventFrame {
            kind: "ready".to_string(),
            session_id: session_id.to_string(),
            attachment_id: self.id.clone(),
            message: String::new(),
        })
    }

    pub fn enqueue_input_ack(&self, seq: u64) -> bool {
        {
            let mut ack = self.ack.lock().unwrap();
            if seq > ack.seq {
                ack.seq = seq;
            }
            if ack.queued {
                return true;
            }
            ack.queued = true;
        }
        match self.send_tx.try_send(OutgoingFrame::input_ack()) {
            Ok(()) => true,
            Err(_) => {
                self.ack.lock().unwrap().queued = false;
                self.cancel();
                false
            }
        }
    }

    pub fn consume_input_ack(&self) -> u64 {
        let mut ack = self.ack.lock().unwrap();
        ack.queued = false;
        ack.seq
    }

    /// Mirrors `closeNow`: WebSocket attachments are torn down through their
    /// cancel token, which the transport tasks observe.
    pub fn close_now(&self) {
        self.cancel();
    }
}

pub fn enqueue_pty_replay(attachment: &PtyAttachment, replay: &[u8]) -> bool {
    let mut start = 0;
    while start < replay.len() {
        let end = (start + DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES).min(replay.len());
        if !attachment.enqueue_binary(&replay[start..end]) {
            return false;
        }
        start += DEFAULT_WEBSOCKET_REPLAY_CHUNK_BYTES;
    }
    true
}

/// The master side of a PTY (or any pollable descriptor) with cooperative
/// close semantics: `close` wakes blocked readers and writers, and the
/// descriptor is released once every holder is dropped.
pub struct PtyMaster {
    fd: OwnedFd,
    wake: WakePipe,
    closed: AtomicBool,
}

impl PtyMaster {
    pub fn new(fd: OwnedFd) -> io::Result<Self> {
        set_nonblocking(&fd)?;
        set_cloexec(&fd)?;
        Ok(Self {
            fd,
            wake: WakePipe::new()?,
            closed: AtomicBool::new(false),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.wake.wake();
    }

    fn closed_error() -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, "use of closed pty")
    }

    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.is_closed() {
                return Err(Self::closed_error());
            }
            match poll_readable(self.fd.as_fd(), Some(self.wake.read_fd()), None)? {
                PollOutcome::Woken => {
                    self.wake.drain();
                    return Err(Self::closed_error());
                }
                PollOutcome::Timeout => continue,
                PollOutcome::Ready => {}
            }
            // SAFETY: valid fd and buffer.
            let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n < 0 {
                let err = io::Error::last_os_error();
                match err.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => continue,
                    _ => return Err(err),
                }
            }
            return Ok(n as usize);
        }
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            if self.is_closed() {
                return Err(Self::closed_error());
            }
            // SAFETY: valid fd and buffer.
            let n = unsafe { libc::write(self.fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
            if n < 0 {
                let err = io::Error::last_os_error();
                match err.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => {
                        match poll_writable(self.fd.as_fd(), Some(self.wake.read_fd()), None)? {
                            PollOutcome::Woken => {
                                self.wake.drain();
                                return Err(Self::closed_error());
                            }
                            _ => continue,
                        }
                    }
                    _ => return Err(err),
                }
            }
            return Ok(n as usize);
        }
    }

    pub fn set_size(&self, cols: i64, rows: i64) -> io::Result<()> {
        set_winsize(&self.fd, cols, rows)
    }

    pub fn get_size(&self) -> io::Result<(i64, i64)> {
        get_winsize(&self.fd)
    }
}

pub fn set_winsize(fd: &impl AsRawFd, cols: i64, rows: i64) -> io::Result<()> {
    let ws = libc::winsize {
        ws_row: rows.clamp(0, 65535) as u16,
        ws_col: cols.clamp(0, 65535) as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ takes a pointer to a winsize struct.
    let rc = unsafe { libc::ioctl(fd.as_raw_fd(), libc::TIOCSWINSZ, &ws) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub fn get_winsize(fd: &impl AsRawFd) -> io::Result<(i64, i64)> {
    let mut ws = libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCGWINSZ fills a winsize struct.
    let rc = unsafe { libc::ioctl(fd.as_raw_fd(), libc::TIOCGWINSZ, &mut ws) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((i64::from(ws.ws_col), i64::from(ws.ws_row)))
}

/// Allocates a PTY master/slave pair. The production implementation opens
/// `/dev/ptmx`; tests substitute failures.
pub type PtyOpener = Box<dyn Fn() -> io::Result<(OwnedFd, OwnedFd)> + Send + Sync>;

pub fn default_pty_opener() -> PtyOpener {
    Box::new(|| {
        let pair = nix::pty::openpty(None, None).map_err(|err| {
            io::Error::new(
                io::Error::from(err).kind(),
                format!("open /dev/ptmx: {err}"),
            )
        })?;
        // Go opens /dev/ptmx and the tty through os.OpenFile, which is always
        // O_CLOEXEC; nix::openpty is not. Without this every shell spawned on
        // the PTY inherits the master and slave descriptors.
        set_cloexec(&pair.master)?;
        set_cloexec(&pair.slave)?;
        Ok((pair.master, pair.slave))
    })
}

pub struct SessionMut {
    pub attachments: HashMap<String, Arc<PtyAttachment>>,
    pub effective_cols: i64,
    pub effective_rows: i64,
    pub last_known_cols: i64,
    pub last_known_rows: i64,
    pub resize_confirms: i32,
    pub scrollback: Vec<u8>,
    idle_timer: Option<TimerHandle>,
    pub closed: bool,
}

pub struct PtySession {
    pub id: String,
    pub key: SessionKey,
    pid: Option<i32>,
    exited: AtomicBool,
    tmp_script: Option<PathBuf>,
    pty: Option<Arc<PtyMaster>>,
    tty: Mutex<Option<OwnedFd>>,
    input_tx: Option<flume::Sender<InputChunk>>,
    input_rx: Option<flume::Receiver<InputChunk>>,
    input_enqueue_mu: Mutex<()>,
    pub done: DoneSignal,
    pty_write_mu: Mutex<()>,
    st: Mutex<SessionMut>,
}

impl PtySession {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_test(
        id: &str,
        key: SessionKey,
        pty: Option<Arc<PtyMaster>>,
        attachments: Vec<Arc<PtyAttachment>>,
        cols: i64,
        rows: i64,
        with_input_queue: bool,
    ) -> Arc<Self> {
        let (input_tx, input_rx) = if with_input_queue {
            let (tx, rx) = flume::bounded(DEFAULT_PTY_INPUT_QUEUE_CAP);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let mut map = HashMap::new();
        for attachment in attachments {
            map.insert(attachment.id.clone(), attachment);
        }
        Arc::new(Self {
            id: id.to_string(),
            key,
            pid: None,
            exited: AtomicBool::new(true),
            tmp_script: None,
            pty,
            tty: Mutex::new(None),
            input_tx,
            input_rx,
            input_enqueue_mu: Mutex::new(()),
            done: DoneSignal::new(),
            pty_write_mu: Mutex::new(()),
            st: Mutex::new(SessionMut {
                attachments: map,
                effective_cols: cols,
                effective_rows: rows,
                last_known_cols: cols,
                last_known_rows: rows,
                resize_confirms: 0,
                scrollback: Vec::new(),
                idle_timer: None,
                closed: false,
            }),
        })
    }

    /// Access mutable session state. Callers must hold the hub lock; the
    /// session mutex only exists to give `Arc<PtySession>` interior mutability.
    pub fn state(&self, _hub: &HubGuard<'_>) -> MutexGuard<'_, SessionMut> {
        self.st.lock().unwrap()
    }

    pub fn pty(&self) -> Option<&Arc<PtyMaster>> {
        self.pty.as_ref()
    }

    pub fn input_len(&self) -> usize {
        self.input_tx.as_ref().map(|tx| tx.len()).unwrap_or(0)
    }

    #[doc(hidden)]
    pub fn push_input_for_test(&self, chunk: InputChunk) {
        if let Some(tx) = &self.input_tx {
            tx.send(chunk).unwrap();
        }
    }

    #[doc(hidden)]
    pub fn drain_input_for_test(&self) -> Vec<InputChunk> {
        let mut out = Vec::new();
        if let Some(rx) = &self.input_rx {
            while let Ok(chunk) = rx.try_recv() {
                out.push(chunk);
            }
        }
        out
    }

    /// Hold the PTY write lock (tests use this to stall the writer).
    #[doc(hidden)]
    pub fn lock_pty_writer(&self) -> MutexGuard<'_, ()> {
        self.pty_write_mu.lock().unwrap()
    }

    fn kill(&self) {
        if let Some(pid) = self.pid {
            if !self.exited.load(Ordering::SeqCst) {
                // SAFETY: sending SIGKILL to a pid this process spawned.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
    }

    fn close_tty_file(&self) {
        if let Ok(mut tty) = self.tty.lock() {
            tty.take();
        }
    }

    fn close_pty_file(&self) {
        if let Some(pty) = &self.pty {
            pty.close();
        }
    }

    pub fn close_pty_files(&self) {
        self.close_tty_file();
        self.close_pty_file();
    }
}

pub struct HubState {
    sessions: HashMap<SessionKey, Arc<PtySession>>,
    next_attachment_id: u64,
    next_anonymous_id: u64,
}

pub type HubGuard<'a> = MutexGuard<'a, HubState>;

#[derive(Default, Clone)]
pub struct PtyHubConfig {
    pub shell: String,
    pub scrollback_limit: usize,
    pub session_idle_ttl: Option<Duration>,
}

pub struct PtyHub {
    state: Mutex<HubState>,
    shell: String,
    stderr: Option<LogSink>,
    scrollback_limit: usize,
    session_idle_ttl: Duration,
    open_pty: Mutex<Option<PtyOpener>>,
    timers: Arc<TimerQueue>,
}

impl PtyHub {
    pub fn new(cfg: PtyHubConfig, stderr: Option<LogSink>) -> Arc<Self> {
        let limit = if cfg.scrollback_limit == 0 {
            DEFAULT_WEBSOCKET_SCROLLBACK_CAP
        } else {
            cfg.scrollback_limit
        };
        let idle_ttl = match cfg.session_idle_ttl {
            Some(ttl) if !ttl.is_zero() => ttl,
            _ => DEFAULT_WEBSOCKET_SESSION_IDLE_TTL,
        };
        Arc::new(Self {
            state: Mutex::new(HubState {
                sessions: HashMap::new(),
                next_attachment_id: 0,
                next_anonymous_id: 0,
            }),
            shell: cfg.shell.trim().to_string(),
            stderr,
            scrollback_limit: limit,
            session_idle_ttl: idle_ttl,
            open_pty: Mutex::new(None),
            timers: TimerQueue::shared(),
        })
    }

    pub fn set_pty_opener(&self, opener: PtyOpener) {
        *self.open_pty.lock().unwrap() = Some(opener);
    }

    pub fn lock(&self) -> HubGuard<'_> {
        self.state.lock().unwrap()
    }

    fn log(&self, text: &str) {
        if let Some(sink) = &self.stderr {
            sink.write_str(text);
        }
    }

    /// Register a test-constructed session under its key.
    #[doc(hidden)]
    pub fn insert_session_for_test(&self, session: Arc<PtySession>) {
        self.lock().sessions.insert(session.key.clone(), session);
    }

    #[doc(hidden)]
    pub fn remove_session_for_test(&self, key: &SessionKey) {
        self.lock().sessions.remove(key);
    }

    pub fn session(&self, key: &SessionKey) -> Option<Arc<PtySession>> {
        self.lock().sessions.get(key).cloned()
    }

    pub fn shell(&self) -> &str {
        &self.shell
    }

    pub fn attach_ws(
        self: &Arc<Self>,
        session_id: &str,
        session_id_explicit: bool,
        attachment_id: &str,
        cols: i64,
        rows: i64,
    ) -> Result<(Arc<PtyAttachment>, DoneSignal), String> {
        let mut session_id = session_id.trim().to_string();
        if session_id.is_empty() {
            session_id = "default".to_string();
        }
        let (cols, rows) = normalize_pty_size(cols, rows);
        let attachment_id = attachment_id.trim().to_string();
        let persistent = !attachment_id.is_empty() && session_id_explicit;
        self.prepare_attachment(
            &session_id,
            &attachment_id,
            cols,
            rows,
            persistent,
            "",
            "",
            false,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn attach_rpc(
        self: &Arc<Self>,
        session_id: &str,
        attachment_id: &str,
        cols: i64,
        rows: i64,
        command: &str,
        client_token: &str,
        require_existing: bool,
        input_seq_ack: bool,
    ) -> Result<(Arc<PtyAttachment>, DoneSignal), String> {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return Err("session_id is required".to_string());
        }
        let attachment_id = attachment_id.trim();
        let (cols, rows) = normalize_pty_size(cols, rows);
        self.prepare_attachment(
            session_id,
            attachment_id,
            cols,
            rows,
            true,
            command,
            client_token,
            require_existing,
            input_seq_ack,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_attachment(
        self: &Arc<Self>,
        session_id: &str,
        attachment_id: &str,
        cols: i64,
        rows: i64,
        persistent: bool,
        command: &str,
        client_token: &str,
        require_existing: bool,
        input_seq_ack: bool,
    ) -> Result<(Arc<PtyAttachment>, DoneSignal), String> {
        let mut hub = self.lock();

        let session_key = if persistent {
            persistent_pty_session_key(session_id)
        } else {
            let key = anonymous_pty_session_key(session_id, hub.next_anonymous_id);
            hub.next_anonymous_id += 1;
            key
        };
        let existing = hub.sessions.get(&session_key).cloned();
        let session = match existing {
            Some(session) if !session.state(&hub).closed => session,
            _ => {
                if require_existing {
                    return Err(format!(
                        "persistent PTY session \"{session_id}\" is not running"
                    ));
                }
                let session =
                    self.start_session_locked(&session_key, session_id, cols, rows, command)?;
                hub.sessions
                    .insert(session_key.clone(), Arc::clone(&session));
                session
            }
        };

        let mut attachment_id = attachment_id.to_string();
        if attachment_id.is_empty() {
            attachment_id = format!("att-{}", hub.next_attachment_id);
            hub.next_attachment_id += 1;
        }

        // Supersede any existing attachment with this id. Input the old
        // attachment already accepted stays queued and reaches the PTY ahead
        // of anything the replacement enqueues: the session input queue is
        // FIFO with the write loop as its only consumer, whole writes enqueue
        // atomically under the enqueue mutex, and the chunk writer does not
        // require the chunk's attachment to still be registered. Never wait
        // on that drain here.
        let mut st = session.state(&hub);
        let superseded = st.attachments.remove(&attachment_id);
        if let Some(old) = &superseded {
            old.cancel();
        }

        let (send_tx, send_rx) = flume::bounded(DEFAULT_WEBSOCKET_WRITE_QUEUE_CAP);
        let attachment = Arc::new(PtyAttachment {
            session_key: session_key.clone(),
            id: attachment_id.clone(),
            client_token: client_token.trim().to_string(),
            cols: AtomicI64::new(cols),
            rows: AtomicI64::new(rows),
            send_tx,
            send_rx,
            cancel: CancelToken::new(),
            persistent,
            input_seq_ack: AtomicBool::new(input_seq_ack),
            last_accepted_seq: AtomicU64::new(0),
            ack: Mutex::new(AckState::default()),
        });
        let replay = st.scrollback.clone();
        if !attachment.enqueue_ready(session_id) {
            attachment.cancel();
            drop(st);
            drop(hub);
            if let Some(old) = superseded {
                old.close_now();
            }
            return Err("failed to queue ready frame".to_string());
        }
        if !enqueue_pty_replay(&attachment, &replay) {
            attachment.cancel();
            drop(st);
            drop(hub);
            if let Some(old) = superseded {
                old.close_now();
            }
            return Err("failed to queue replay frame".to_string());
        }
        st.attachments
            .insert(attachment_id, Arc::clone(&attachment));
        let should_apply_size = self.recompute_session_size_locked(&session, &mut st);
        let session_done = session.done.clone();
        drop(st);
        drop(hub);

        if let Some(old) = superseded {
            old.close_now();
        }
        if should_apply_size {
            self.apply_current_pty_size(&session);
        }
        Ok((attachment, session_done))
    }

    fn start_session_locked(
        self: &Arc<Self>,
        session_key: &SessionKey,
        session_id: &str,
        cols: i64,
        rows: i64,
        command: &str,
    ) -> Result<Arc<PtySession>, String> {
        let shell_path = resolve_pty_shell(&self.shell);
        let trimmed_command = command.trim();
        let mut cmd;
        let mut tmp_script: Option<PathBuf> = None;
        if trimmed_command.is_empty() {
            cmd = Command::new(&shell_path);
        } else if trimmed_command.len() > MAX_INLINE_STARTUP_SCRIPT_BYTES {
            // Startup script exceeds Linux's MAX_ARG_STRLEN (~128KB). Write to
            // a temp file and exec /bin/sh <file> to avoid E2BIG from execve.
            let path = crate::util::temp_dir()
                .join(format!("cmuxd-startup-{}.sh", crate::util::random_hex(8)));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .map_err(|err| format!("could not create startup script temp file: {err}"))?;
            if let Err(err) = file.write_all(trimmed_command.as_bytes()) {
                let _ = fs::remove_file(&path);
                return Err(format!("could not write startup script: {err}"));
            }
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o400));
            drop(file);
            cmd = Command::new("/bin/sh");
            cmd.arg(&path);
            tmp_script = Some(path);
        } else {
            cmd = Command::new("/bin/sh");
            cmd.arg("-c").arg(trimmed_command);
        }
        cmd.env_clear();
        for (key, value) in default_websocket_pty_env(&shell_path) {
            cmd.env(key, value);
        }
        let (pty, pid) = match self.start_pty_command(&mut cmd, cols, rows) {
            Ok(started) => started,
            Err(err) => {
                if let Some(path) = &tmp_script {
                    let _ = fs::remove_file(path);
                }
                self.log(&format!(
                    "pty session start failed session={session_id}: {err}\n"
                ));
                return Err(err);
            }
        };
        let (input_tx, input_rx) = flume::bounded(DEFAULT_PTY_INPUT_QUEUE_CAP);
        let session = Arc::new(PtySession {
            id: session_id.to_string(),
            key: session_key.clone(),
            pid: Some(pid),
            exited: AtomicBool::new(false),
            tmp_script,
            pty: Some(pty),
            tty: Mutex::new(None),
            input_tx: Some(input_tx),
            input_rx: Some(input_rx),
            input_enqueue_mu: Mutex::new(()),
            done: DoneSignal::new(),
            pty_write_mu: Mutex::new(()),
            st: Mutex::new(SessionMut {
                attachments: HashMap::new(),
                effective_cols: cols,
                effective_rows: rows,
                last_known_cols: cols,
                last_known_rows: rows,
                resize_confirms: 0,
                scrollback: Vec::new(),
                idle_timer: None,
                closed: false,
            }),
        });
        self.spawn_session_threads(&session);
        Ok(session)
    }

    fn spawn_session_threads(self: &Arc<Self>, session: &Arc<PtySession>) {
        {
            let session = Arc::clone(session);
            let pid = session.pid;
            std::thread::Builder::new()
                .name("cmuxd-pty-wait".to_string())
                .spawn(move || wait_session_process(&session, pid))
                .expect("spawn pty wait thread");
        }
        {
            let hub = Arc::clone(self);
            let session = Arc::clone(session);
            std::thread::Builder::new()
                .name("cmuxd-pty-pump".to_string())
                .spawn(move || hub.pump_session(&session))
                .expect("spawn pty pump thread");
        }
        {
            let hub = Arc::clone(self);
            let session = Arc::clone(session);
            std::thread::Builder::new()
                .name("cmuxd-pty-input".to_string())
                .spawn(move || hub.write_input_loop(&session))
                .expect("spawn pty input thread");
        }
    }

    fn start_pty_command(
        &self,
        cmd: &mut Command,
        cols: i64,
        rows: i64,
    ) -> Result<(Arc<PtyMaster>, i32), String> {
        let (master_fd, slave_fd) = {
            let opener = self.open_pty.lock().unwrap();
            match opener.as_ref() {
                Some(open) => open(),
                None => default_pty_opener()(),
            }
        }
        .map_err(|err| new_pty_allocation_error(&err))?;
        set_winsize(&slave_fd, cols, rows).map_err(|err| err.to_string())?;
        let stdin = slave_fd.try_clone().map_err(|err| err.to_string())?;
        let stdout = slave_fd.try_clone().map_err(|err| err.to_string())?;
        let stderr = slave_fd.try_clone().map_err(|err| err.to_string())?;
        cmd.stdin(Stdio::from(stdin))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        // SAFETY: the pre_exec closure only calls async-signal-safe functions.
        unsafe {
            use std::os::unix::process::CommandExt;
            cmd.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = cmd.spawn().map_err(|err| err.to_string())?;
        let pid = child.id() as i32;
        drop(slave_fd);
        // The wait thread owns the Child handle; keep it alive until it exits.
        CHILDREN.lock().unwrap().insert(pid, child);
        let master = PtyMaster::new(master_fd).map_err(|err| err.to_string())?;
        Ok((Arc::new(master), pid))
    }

    pub fn detach(self: &Arc<Self>, attachment: &Arc<PtyAttachment>) -> bool {
        let hub = self.lock();
        let session = match hub.sessions.get(&attachment.session_key) {
            Some(session) => Arc::clone(session),
            None => return false,
        };
        let mut st = session.state(&hub);
        match st.attachments.get(&attachment.id) {
            Some(current) if Arc::ptr_eq(current, attachment) => {}
            _ => return false,
        }
        st.attachments.remove(&attachment.id);
        attachment.cancel();
        let should_apply_size = self.recompute_session_size_locked(&session, &mut st);
        drop(st);
        drop(hub);
        if should_apply_size {
            self.apply_current_pty_size(&session);
        }
        true
    }

    pub fn drop_attachment(self: &Arc<Self>, attachment: &Arc<PtyAttachment>) {
        if attachment.persistent {
            self.detach(attachment);
        } else {
            self.close_session_for_attachment(attachment);
        }
        attachment.close_now();
    }

    pub fn close_session_for_attachment(&self, attachment: &Arc<PtyAttachment>) {
        let mut hub = self.lock();
        let session = match hub.sessions.get(&attachment.session_key) {
            Some(session) => Arc::clone(session),
            None => return,
        };
        {
            let mut st = session.state(&hub);
            match st.attachments.get(&attachment.id) {
                Some(current) if Arc::ptr_eq(current, attachment) => {}
                _ => return,
            }
            st.attachments.remove(&attachment.id);
            self.cancel_idle_reap_locked(&mut st);
        }
        hub.sessions.remove(&session.key);
        attachment.cancel();
        drop(hub);

        session.kill();
        session.close_pty_files();
    }

    pub fn close_all(&self) {
        let sessions: Vec<Arc<PtySession>> = {
            let mut hub = self.lock();
            let drained: Vec<Arc<PtySession>> =
                hub.sessions.drain().map(|(_, session)| session).collect();
            for session in &drained {
                let mut st = session.state(&hub);
                self.cancel_idle_reap_locked(&mut st);
            }
            drained
        };
        for session in sessions {
            session.kill();
            session.close_pty_files();
        }
    }

    pub fn write_input_by_id(
        &self,
        session_id: &str,
        attachment_id: &str,
        token: &str,
        payload: &[u8],
    ) -> InputWriteStatus {
        self.write_input_by_id_with_seq(session_id, attachment_id, token, payload, 0, false)
            .status
    }

    pub fn write_input_by_id_with_seq(
        &self,
        session_id: &str,
        attachment_id: &str,
        token: &str,
        payload: &[u8],
        seq: u64,
        has_seq: bool,
    ) -> InputWriteResult {
        match self.attachment_by_id(session_id, attachment_id, token) {
            Some(attachment) => self.write_input(&attachment, payload, seq, has_seq),
            None => InputWriteResult::status(InputWriteStatus::NotFound),
        }
    }

    pub fn resize_by_id(
        self: &Arc<Self>,
        session_id: &str,
        attachment_id: &str,
        token: &str,
        cols: i64,
        rows: i64,
    ) -> bool {
        match self.attachment_by_id(session_id, attachment_id, token) {
            Some(attachment) => {
                self.resize(&attachment, cols, rows);
                true
            }
            None => false,
        }
    }

    pub fn detach_by_id(
        self: &Arc<Self>,
        session_id: &str,
        attachment_id: &str,
        token: &str,
    ) -> bool {
        match self.attachment_by_id(session_id, attachment_id, token) {
            Some(attachment) => self.detach(&attachment),
            None => false,
        }
    }

    pub fn close_session_by_id(&self, session_id: &str) -> bool {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return false;
        }
        let key = persistent_pty_session_key(session_id);
        let session = {
            let mut hub = self.lock();
            let session = match hub.sessions.get(&key) {
                Some(session) => Arc::clone(session),
                None => return false,
            };
            {
                let mut st = session.state(&hub);
                if st.closed {
                    return false;
                }
                self.cancel_idle_reap_locked(&mut st);
                st.closed = true;
            }
            hub.sessions.remove(&key);
            session
        };
        session.kill();
        session.close_pty_files();
        true
    }

    pub fn session_snapshots(&self) -> Vec<Value> {
        let hub = self.lock();
        let mut keys: Vec<&SessionKey> = hub
            .sessions
            .iter()
            .filter(|(key, session)| {
                key.kind == SessionKind::Persistent && !session.state(&hub).closed
            })
            .map(|(key, _)| key)
            .collect();
        keys.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        keys.iter()
            .filter_map(|key| hub.sessions.get(*key))
            .map(|session| self.session_snapshot_locked(&hub, session))
            .collect()
    }

    pub fn attachment_by_id(
        &self,
        session_id: &str,
        attachment_id: &str,
        token: &str,
    ) -> Option<Arc<PtyAttachment>> {
        let session_id = session_id.trim();
        let attachment_id = attachment_id.trim();
        let token = token.trim();
        if session_id.is_empty() || attachment_id.is_empty() {
            return None;
        }
        let hub = self.lock();
        let session = hub.sessions.get(&persistent_pty_session_key(session_id))?;
        let st = session.state(&hub);
        if st.closed {
            return None;
        }
        let attachment = st.attachments.get(attachment_id)?;
        if !constant_time_eq(attachment.client_token.as_bytes(), token.as_bytes()) {
            return None;
        }
        Some(Arc::clone(attachment))
    }

    fn session_snapshot_locked(&self, hub: &HubGuard<'_>, session: &PtySession) -> Value {
        let st = session.state(hub);
        let mut ids: Vec<&String> = st.attachments.keys().collect();
        ids.sort();
        let attachments: Vec<Value> = ids
            .iter()
            .map(|id| {
                let attachment = &st.attachments[*id];
                json!({
                    "attachment_id": id,
                    "cols": attachment.cols(),
                    "rows": attachment.rows(),
                    "persistent": attachment.persistent,
                })
            })
            .collect();
        json!({
            "session_id": session.id,
            "attachments": attachments,
            "effective_cols": st.effective_cols,
            "effective_rows": st.effective_rows,
            "last_known_cols": st.last_known_cols,
            "last_known_rows": st.last_known_rows,
            "scrollback_bytes": st.scrollback.len(),
        })
    }

    pub fn active_session_count(&self) -> usize {
        self.lock().sessions.len()
    }

    pub fn max_scrollback_bytes(&self) -> usize {
        let hub = self.lock();
        hub.sessions
            .values()
            .map(|session| session.state(&hub).scrollback.len())
            .max()
            .unwrap_or(0)
    }

    pub fn pump_session(self: &Arc<Self>, session: &Arc<PtySession>) {
        let pty = match &session.pty {
            Some(pty) => Arc::clone(pty),
            None => {
                self.finish_session(session);
                return;
            }
        };
        let mut buffer = vec![0u8; 32768];
        loop {
            // Go's os.File.Read reports a 0-byte read as io.EOF. Linux signals
            // PTY hangup with EIO, but macOS/BSD return 0; treating 0 as
            // "keep polling" would spin forever on a hung-up master.
            match pty.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let chunk = buffer[..n].to_vec();
                    self.record_and_broadcast(session, &chunk);
                    self.confirm_pty_size_after_output(session);
                }
            }
        }
        self.finish_session(session);
    }

    fn finish_session(&self, session: &Arc<PtySession>) {
        session.close_pty_files();
        let mut hub = self.lock();
        if let Some(current) = hub.sessions.get(&session.key) {
            if Arc::ptr_eq(current, session) {
                hub.sessions.remove(&session.key);
            }
        }
        let mut st = session.state(&hub);
        self.cancel_idle_reap_locked(&mut st);
        st.closed = true;
        st.attachments.clear();
        session.done.close();
    }

    pub fn record_and_broadcast(self: &Arc<Self>, session: &Arc<PtySession>, data: &[u8]) {
        let attachments: Vec<Arc<PtyAttachment>> = {
            let hub = self.lock();
            let mut st = session.state(&hub);
            if st.closed {
                return;
            }
            self.append_scrollback_locked(&mut st, data);
            st.attachments.values().cloned().collect()
        };
        for attachment in attachments {
            if !attachment.enqueue_binary(data) {
                self.drop_attachment(&attachment);
            }
        }
    }

    pub fn append_scrollback_locked(&self, st: &mut SessionMut, data: &[u8]) {
        let limit = self.scrollback_limit;
        if limit == 0 || data.is_empty() {
            return;
        }
        if data.len() >= limit {
            let mut next = Vec::with_capacity(limit);
            next.extend_from_slice(&data[data.len() - limit..]);
            st.scrollback = next;
            return;
        }
        if st.scrollback.len() + data.len() > limit {
            let keep = (limit - data.len()).min(st.scrollback.len());
            let mut next = Vec::with_capacity(limit);
            if keep > 0 {
                let start = st.scrollback.len() - keep;
                next.extend_from_slice(&st.scrollback[start..]);
            }
            next.extend_from_slice(data);
            st.scrollback = next;
            return;
        }
        if st.scrollback.capacity() > limit {
            let mut next = Vec::with_capacity(limit);
            next.extend_from_slice(&st.scrollback);
            st.scrollback = next;
        }
        st.scrollback.extend_from_slice(data);
    }

    pub fn recompute_session_size_locked(
        self: &Arc<Self>,
        session: &Arc<PtySession>,
        st: &mut SessionMut,
    ) -> bool {
        if st.attachments.is_empty() {
            st.effective_cols = st.last_known_cols;
            st.effective_rows = st.last_known_rows;
            self.schedule_idle_reap_locked(session, st);
            return false;
        }
        self.cancel_idle_reap_locked(st);

        let mut min_cols = 0;
        let mut min_rows = 0;
        for attachment in st.attachments.values() {
            let cols = attachment.cols();
            let rows = attachment.rows();
            if min_cols == 0 || cols < min_cols {
                min_cols = cols;
            }
            if min_rows == 0 || rows < min_rows {
                min_rows = rows;
            }
        }
        if st.effective_cols == min_cols && st.effective_rows == min_rows {
            st.last_known_cols = min_cols;
            st.last_known_rows = min_rows;
            return false;
        }
        st.effective_cols = min_cols;
        st.effective_rows = min_rows;
        st.last_known_cols = min_cols;
        st.last_known_rows = min_rows;
        st.resize_confirms = 4;
        true
    }

    fn schedule_idle_reap_locked(self: &Arc<Self>, session: &Arc<PtySession>, st: &mut SessionMut) {
        if self.session_idle_ttl.is_zero() || st.closed || !st.attachments.is_empty() {
            return;
        }
        self.cancel_idle_reap_locked(st);
        let hub_ref = Arc::clone(self);
        let session_ref = Arc::clone(session);
        let handle = self.timers.schedule(
            self.session_idle_ttl,
            Box::new(move || hub_ref.reap_idle_session(&session_ref)),
        );
        st.idle_timer = Some(handle);
    }

    fn cancel_idle_reap_locked(&self, st: &mut SessionMut) {
        if let Some(handle) = st.idle_timer.take() {
            self.timers.cancel(handle);
        }
    }

    fn reap_idle_session(self: &Arc<Self>, session: &Arc<PtySession>) {
        {
            let mut hub = self.lock();
            let current = matches!(hub.sessions.get(&session.key), Some(current) if Arc::ptr_eq(current, session));
            let mut st = session.state(&hub);
            if !current || st.closed || !st.attachments.is_empty() {
                return;
            }
            st.idle_timer = None;
            drop(st);
            hub.sessions.remove(&session.key);
        }
        session.kill();
        session.close_pty_files();
    }

    fn confirm_pty_size_after_output(&self, session: &Arc<PtySession>) {
        {
            let hub = self.lock();
            let current = matches!(hub.sessions.get(&session.key), Some(current) if Arc::ptr_eq(current, session));
            let mut st = session.state(&hub);
            if !current || st.closed || st.resize_confirms <= 0 {
                return;
            }
            st.resize_confirms -= 1;
        }
        self.apply_current_pty_size(session);
    }

    pub fn apply_current_pty_size(&self, session: &Arc<PtySession>) -> bool {
        let _write_guard = session.pty_write_mu.lock().unwrap();
        let (current, cols, rows) = {
            let hub = self.lock();
            let st = session.state(&hub);
            let current = matches!(hub.sessions.get(&session.key), Some(current) if Arc::ptr_eq(current, session))
                && !st.closed
                && !st.attachments.is_empty();
            (current, st.effective_cols, st.effective_rows)
        };
        if !current || cols <= 0 || rows <= 0 {
            return false;
        }
        self.apply_pty_size_with_write_lock(session, cols, rows);
        true
    }

    fn apply_pty_size_with_write_lock(&self, session: &PtySession, cols: i64, rows: i64) -> bool {
        let pty = match &session.pty {
            Some(pty) => pty,
            None => return false,
        };
        let mut last_err: Option<String> = None;
        for _ in 0..8 {
            if let Err(err) = pty.set_size(cols, rows) {
                last_err = Some(err.to_string());
                continue;
            }
            match pty.get_size() {
                Ok((actual_cols, actual_rows)) => {
                    if actual_cols == cols && actual_rows == rows {
                        return true;
                    }
                    last_err = Some(format!(
                        "pty size remained {actual_cols}x{actual_rows} after resize to {cols}x{rows}"
                    ));
                }
                Err(err) => {
                    last_err = Some(err.to_string());
                }
            }
        }
        if let Some(err) = last_err {
            self.log(&format!(
                "ws pty resize failed session={}: {err}\n",
                session.id
            ));
        }
        false
    }

    pub fn write_input(
        &self,
        attachment: &Arc<PtyAttachment>,
        payload: &[u8],
        seq: u64,
        has_seq: bool,
    ) -> InputWriteResult {
        let session = match self.session_for_attachment(&attachment.session_key) {
            Some(session) => session,
            None => return InputWriteResult::status(InputWriteStatus::NotFound),
        };
        if payload.is_empty() {
            return InputWriteResult::status(InputWriteStatus::Ok);
        }
        let input_tx = match &session.input_tx {
            Some(tx) => tx.clone(),
            None => return InputWriteResult::status(InputWriteStatus::NotFound),
        };

        let is_current = |hub: &HubGuard<'_>| -> bool {
            matches!(hub.sessions.get(&attachment.session_key), Some(current) if Arc::ptr_eq(current, &session))
                && {
                    let st = session.state(hub);
                    !st.closed
                        && matches!(st.attachments.get(&attachment.id), Some(current) if Arc::ptr_eq(current, attachment))
                }
        };

        if !is_current(&self.lock()) {
            return InputWriteResult::status(InputWriteStatus::NotFound);
        }

        let mut chunks: Vec<InputChunk> =
            Vec::with_capacity(payload.len().div_ceil(DEFAULT_PTY_INPUT_CHUNK_BYTES));
        let mut remaining = payload.len();
        let mut offset = 0;
        while offset < payload.len() {
            let chunk_len = (payload.len() - offset).min(DEFAULT_PTY_INPUT_CHUNK_BYTES);
            remaining -= chunk_len;
            chunks.push(InputChunk {
                attachment_id: attachment.id.clone(),
                attachment: Some(Arc::clone(attachment)),
                payload: payload[offset..offset + chunk_len].to_vec(),
                seq,
                final_seq_chunk: attachment.input_seq_ack() && remaining == 0,
            });
            offset += chunk_len;
        }

        let _enqueue_guard = session.input_enqueue_mu.lock().unwrap();

        {
            let hub = self.lock();
            let current = is_current(&hub);
            if current && attachment.input_seq_ack() {
                let want = attachment.last_accepted_seq.load(Ordering::SeqCst) + 1;
                if !has_seq || seq != want {
                    return InputWriteResult {
                        status: InputWriteStatus::SeqGap,
                        got: seq,
                        want,
                    };
                }
            }
            if !current {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
        }
        let capacity = input_tx.capacity().unwrap_or(usize::MAX);
        if chunks.len() > capacity.saturating_sub(input_tx.len()) {
            self.log(&format!(
                "ws pty input queue full session={} attachment={}\n",
                session.id, attachment.id
            ));
            return InputWriteResult::status(InputWriteStatus::QueueFull);
        }
        if attachment.input_seq_ack() {
            let hub = self.lock();
            if !is_current(&hub) {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
            attachment.last_accepted_seq.store(seq, Ordering::SeqCst);
        }
        for chunk in chunks {
            if session.done.is_closed() {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
            let outcome = flume::Selector::new()
                .send(&input_tx, chunk, |result| result.is_ok())
                .recv(session.done.receiver(), |_| false)
                .wait();
            if !outcome {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
        }
        InputWriteResult::status(InputWriteStatus::Ok)
    }

    pub fn write_input_loop(self: &Arc<Self>, session: &Arc<PtySession>) {
        let input_rx = match &session.input_rx {
            Some(rx) => rx.clone(),
            None => return,
        };
        loop {
            if session.done.is_closed() {
                return;
            }
            let next = flume::Selector::new()
                .recv(session.done.receiver(), |_| None)
                .recv(&input_rx, |chunk| chunk.ok())
                .wait();
            match next {
                Some(chunk) => {
                    self.write_input_chunk(session, chunk);
                }
                None => {
                    if session.done.is_closed() || input_rx.is_disconnected() {
                        return;
                    }
                }
            }
        }
    }

    pub fn write_input_chunk(
        self: &Arc<Self>,
        session: &Arc<PtySession>,
        chunk: InputChunk,
    ) -> bool {
        let attachment = chunk.attachment.clone();
        let (written, ack_ok) = self.write_input_chunk_locked(session, &chunk);
        if !ack_ok {
            // enqueue_input_ack cancelled the attachment because its send
            // queue was saturated; finish the cleanup like the output path
            // does. Must run outside the PTY write lock: drop_attachment can
            // resize via apply_current_pty_size, which takes that lock.
            if let Some(attachment) = attachment {
                self.drop_attachment(&attachment);
            }
        }
        written
    }

    fn write_input_chunk_locked(
        &self,
        session: &Arc<PtySession>,
        chunk: &InputChunk,
    ) -> (bool, bool) {
        let _write_guard = session.pty_write_mu.lock().unwrap();
        // Deliberately session-scoped, not attachment-scoped: once write_input
        // accepted bytes they belong to the persistent session and are written
        // even if their attachment has since detached (tmux semantics: input
        // sent before detach still executes).
        let current = {
            let hub = self.lock();
            matches!(hub.sessions.get(&session.key), Some(current) if Arc::ptr_eq(current, session))
                && !session.state(&hub).closed
        };
        let pty = match &session.pty {
            Some(pty) if current && !pty.is_closed() => Arc::clone(pty),
            _ => return (false, true),
        };
        self.write_input_chunk_with_pty_file(&pty, chunk)
    }

    fn write_input_chunk_with_pty_file(&self, pty: &PtyMaster, chunk: &InputChunk) -> (bool, bool) {
        let mut total = 0;
        while total < chunk.payload.len() {
            match pty.write(&chunk.payload[total..]) {
                Ok(0) => return (false, true),
                Ok(n) => total += n,
                Err(_) => return (false, true),
            }
        }
        (true, self.ack_input_chunk(chunk))
    }

    fn ack_input_chunk(&self, chunk: &InputChunk) -> bool {
        match &chunk.attachment {
            Some(attachment) if chunk.final_seq_chunk && attachment.input_seq_ack() => {
                attachment.enqueue_input_ack(chunk.seq)
            }
            _ => true,
        }
    }

    fn session_for_attachment(&self, key: &SessionKey) -> Option<Arc<PtySession>> {
        let hub = self.lock();
        let session = hub.sessions.get(key)?;
        if session.state(&hub).closed {
            return None;
        }
        Some(Arc::clone(session))
    }

    pub fn resize(self: &Arc<Self>, attachment: &Arc<PtyAttachment>, cols: i64, rows: i64) {
        if cols <= 0 || rows <= 0 {
            return;
        }
        let (cols, rows) = normalize_pty_size(cols, rows);
        let session = {
            let hub = self.lock();
            let session = match hub.sessions.get(&attachment.session_key) {
                Some(session) => Arc::clone(session),
                None => return,
            };
            let mut st = session.state(&hub);
            if st.closed {
                return;
            }
            match st.attachments.get(&attachment.id) {
                Some(current) if Arc::ptr_eq(current, attachment) => {}
                _ => return,
            }
            attachment.cols.store(cols, Ordering::SeqCst);
            attachment.rows.store(rows, Ordering::SeqCst);
            if !self.recompute_session_size_locked(&session, &mut st) {
                return;
            }
            drop(st);
            session
        };
        self.apply_current_pty_size(&session);
    }

    // --- debug/test helpers mirroring the Go test file's hub methods ---

    #[doc(hidden)]
    pub fn session_debug_snapshot(&self, session_id: &str) -> Option<(usize, i64, i64)> {
        let hub = self.lock();
        let session = hub.sessions.get(&persistent_pty_session_key(session_id))?;
        let st = session.state(&hub);
        Some((st.attachments.len(), st.effective_cols, st.effective_rows))
    }

    #[doc(hidden)]
    pub fn debug_attachment(
        &self,
        session_id: &str,
        attachment_id: &str,
    ) -> Option<Arc<PtyAttachment>> {
        let hub = self.lock();
        let session = hub.sessions.get(&persistent_pty_session_key(session_id))?;
        let st = session.state(&hub);
        st.attachments.get(attachment_id).cloned()
    }

    #[doc(hidden)]
    pub fn session_pty_size(&self, session_id: &str) -> io::Result<Option<(i64, i64)>> {
        let session = {
            let hub = self.lock();
            match hub.sessions.get(&persistent_pty_session_key(session_id)) {
                Some(session) => Arc::clone(session),
                None => return Ok(None),
            }
        };
        let _write_guard = session.pty_write_mu.lock().unwrap();
        match &session.pty {
            Some(pty) => pty.get_size().map(Some),
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    pub fn session_attachment_ids(&self, key: &SessionKey) -> Vec<String> {
        let hub = self.lock();
        match hub.sessions.get(key) {
            Some(session) => session.state(&hub).attachments.keys().cloned().collect(),
            None => Vec::new(),
        }
    }
}

static CHILDREN: Mutex<std::collections::BTreeMap<i32, std::process::Child>> =
    Mutex::new(std::collections::BTreeMap::new());

fn wait_session_process(session: &Arc<PtySession>, pid: Option<i32>) {
    if let Some(pid) = pid {
        let child = CHILDREN.lock().unwrap().remove(&pid);
        if let Some(mut child) = child {
            let _ = child.wait();
        }
    }
    session.exited.store(true, Ordering::SeqCst);
    if let Some(path) = &session.tmp_script {
        let _ = fs::remove_file(path);
    }
    session.close_tty_file();
}

use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub fn normalize_pty_size(cols: i64, rows: i64) -> (i64, i64) {
    let mut cols = cols;
    let mut rows = rows;
    if cols <= 0 {
        cols = DEFAULT_PTY_COLS;
    }
    if rows <= 0 {
        rows = DEFAULT_PTY_ROWS;
    }
    if cols > MAX_PTY_DIMENSION {
        cols = MAX_PTY_DIMENSION;
    }
    if rows > MAX_PTY_DIMENSION {
        rows = MAX_PTY_DIMENSION;
    }
    (cols, rows)
}

pub fn resolve_pty_shell(explicit: &str) -> String {
    if !explicit.trim().is_empty() {
        return explicit.to_string();
    }
    if let Ok(shell) = crate::util::env_var("SHELL").ok_or(std::env::VarError::NotPresent) {
        let shell = shell.trim();
        if !shell.is_empty() && fs::metadata(shell).is_ok() {
            return shell.to_string();
        }
    }
    for candidate in ["/bin/bash", "/usr/bin/bash", "/bin/sh"] {
        if fs::metadata(candidate).is_ok() {
            return candidate.to_string();
        }
    }
    "/bin/sh".to_string()
}

/// Build the PTY child environment: inherit the daemon environment, force
/// terminal identity, and seed a UTF-8 locale when none is configured.
pub fn default_websocket_pty_env(shell_path: &str) -> Vec<(String, String)> {
    let raw: Vec<String> = crate::util::env_vars_os()
        .into_iter()
        .map(|(key, value)| format!("{}={}", key.to_string_lossy(), value.to_string_lossy()))
        .collect();
    let (mut env, mut order) = env_map_with_order(&raw);
    fn set(env: &mut HashMap<String, String>, order: &mut Vec<String>, key: &str, value: &str) {
        if !env.contains_key(key) {
            order.push(key.to_string());
        }
        env.insert(key.to_string(), value.to_string());
    }
    fn set_if_missing(
        env: &mut HashMap<String, String>,
        order: &mut Vec<String>,
        key: &str,
        value: &str,
    ) {
        if env.get(key).map(|v| v.trim().is_empty()).unwrap_or(true) {
            set(env, order, key, value);
        }
    }
    set(&mut env, &mut order, "TERM", "xterm-256color");
    set_if_missing(&mut env, &mut order, "COLORTERM", "truecolor");
    set_if_missing(&mut env, &mut order, "TERM_PROGRAM", "ghostty");
    set_if_missing(&mut env, &mut order, "SHELL", shell_path);
    set(&mut env, &mut order, "CMUX_REMOTE_TRANSPORT", "ws");
    if !env_has_utf8_locale(&env) {
        set(&mut env, &mut order, "LANG", "C.UTF-8");
        set(&mut env, &mut order, "LC_CTYPE", "C.UTF-8");
        set(&mut env, &mut order, "LC_ALL", "C.UTF-8");
    }
    let mut out = Vec::with_capacity(order.len());
    let mut seen = std::collections::HashSet::new();
    for key in order {
        if !seen.insert(key.clone()) {
            continue;
        }
        if let Some(value) = env.get(&key) {
            out.push((key, value.clone()));
        }
    }
    out
}

pub fn env_map_with_order(values: &[String]) -> (HashMap<String, String>, Vec<String>) {
    let mut env = HashMap::with_capacity(values.len());
    let mut order = Vec::with_capacity(values.len());
    for value in values {
        let Some((key, rest)) = value.split_once('=') else {
            continue;
        };
        if !env.contains_key(key) {
            order.push(key.to_string());
        }
        env.insert(key.to_string(), rest.to_string());
    }
    (env, order)
}

pub fn env_has_utf8_locale(env: &HashMap<String, String>) -> bool {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        let value = env
            .get(key)
            .map(|v| v.trim().to_uppercase())
            .unwrap_or_default();
        if value.is_empty() {
            continue;
        }
        return value.contains("UTF-8") || value.contains("UTF8");
    }
    false
}

/// Wrap a raw PTY-allocation failure with actionable diagnostics about the
/// remote devpts. Without this, a hardened mount (ptmxmode=000) surfaced only
/// a generic "remote PTY attach failed" with a 0-byte daemon log.
pub fn new_pty_allocation_error(err: &io::Error) -> String {
    let mut suffix = String::new();
    let detail = describe_devpts();
    if !detail.is_empty() {
        suffix = format!("; {detail}");
    }
    let hint = if is_permission_denied_err(err) {
        "; the remote devpts denies /dev/ptmx (e.g. mounted ptmxmode=000): remount it writable with `sudo mount -o remount,ptmxmode=0666 /dev/pts` or expose a writable /dev/ptmx so the cmux daemon can open a terminal"
    } else {
        ""
    };
    format!("could not allocate a remote PTY: {err}{suffix}{hint}")
}

pub fn describe_devpts() -> String {
    let mut parts = Vec::new();
    match fs::metadata("/dev/ptmx") {
        Ok(info) => parts.push(format!(
            "/dev/ptmx mode={:04o}",
            info.permissions().mode() & 0o777
        )),
        Err(err) => parts.push(format!("/dev/ptmx stat error: {err}")),
    }
    let opts = devpts_mount_options();
    if !opts.is_empty() {
        parts.push(format!("devpts ({opts})"));
    }
    parts.join("; ")
}

pub fn devpts_mount_options() -> String {
    let data = match fs::read_to_string("/proc/self/mountinfo") {
        Ok(data) => data,
        Err(_) => return String::new(),
    };
    for line in data.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 5 || fields[4] != "/dev/pts" {
            continue;
        }
        let Some(sep) = fields.iter().position(|f| *f == "-") else {
            continue;
        };
        if sep + 3 >= fields.len() {
            continue;
        }
        if fields[sep + 1] != "devpts" {
            continue;
        }
        return fields[sep + 3].to_string();
    }
    String::new()
}

/// Clamp a close reason to the 123-byte limit a WebSocket control frame allows
/// for its UTF-8 reason payload, trimming on a character boundary.
pub fn truncate_websocket_close_reason(reason: &str) -> String {
    const MAX_REASON_BYTES: usize = 123;
    if reason.len() <= MAX_REASON_BYTES {
        return reason.to_string();
    }
    let mut end = MAX_REASON_BYTES;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_string()
}

pub fn is_permission_denied_err(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::PermissionDenied
        || matches!(err.raw_os_error(), Some(code) if code == libc::EACCES || code == libc::EPERM)
}

/// Read helper used by tests to pull exactly `count` bytes from a pipe.
#[doc(hidden)]
pub fn read_exactly(reader: &mut impl Read, count: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; count];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Turn a raw file descriptor into an owned one (used by tests to wrap pipes).
#[doc(hidden)]
pub unsafe fn owned_fd_from_raw(fd: i32) -> OwnedFd {
    OwnedFd::from_raw_fd(fd)
}

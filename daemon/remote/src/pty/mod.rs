//! PTY session hub shared by the stdio RPC server, the persistent per-slot
//! daemon, and the WebSocket transport.
//!
//! A session owns a PTY master, the child process attached to its slave, a
//! bounded scrollback buffer, and any number of attachments (clients). Output
//! fans out to every attachment's bounded send queue; input from any
//! attachment is serialized through one FIFO so bytes accepted before a
//! detach still reach the PTY. Effective size is the smallest attached size.

pub mod open;

use std::collections::HashMap;
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use crossbeam_channel::{Receiver, Sender, bounded, select};
use rustix::event::{PollFd, PollFlags};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

use crate::logger::Logger;
use crate::rpc::RpcEvent;
use crate::signal::{Signal, SignalHandle, signal};
use crate::util::{io_error_text, quote};
use open::{PtyOpener, PtyPair};

pub const DEFAULT_PTY_COLS: usize = 80;
pub const DEFAULT_PTY_ROWS: usize = 24;
pub const MAX_PTY_DIMENSION: usize = 65535;
pub const DEFAULT_SCROLLBACK_CAP: usize = 1 << 20;
pub const DEFAULT_REPLAY_CHUNK_BYTES: usize = 48 * 1024;
pub const DEFAULT_WRITE_QUEUE_CAP: usize = 256;
pub const DEFAULT_INPUT_QUEUE_CAP: usize = 256;
pub const DEFAULT_INPUT_CHUNK_BYTES: usize = 16 * 1024;
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_SESSION_IDLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Read limit for `/rpc` WebSocket frames (mirrors the stdio frame cap).
pub const MAX_RPC_FRAME_BYTES_FOR_WS: usize = crate::rpc::MAX_RPC_FRAME_BYTES;
const RESIZE_CONFIRM_ATTEMPTS: u32 = 4;
const STARTUP_SCRIPT_ARG_LIMIT: usize = 120 * 1024;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Text,
    Binary,
}

#[derive(Debug, Clone)]
pub struct OutgoingFrame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
    pub input_ack: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionKind {
    Persistent = 0,
    Anonymous = 1,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub kind: SessionKind,
    pub session_id: String,
    pub anonymous_id: u64,
}

#[must_use]
pub fn persistent_session_key(session_id: &str) -> SessionKey {
    SessionKey {
        kind: SessionKind::Persistent,
        session_id: session_id.to_string(),
        anonymous_id: 0,
    }
}

#[must_use]
pub fn anonymous_session_key(session_id: &str, anonymous_id: u64) -> SessionKey {
    SessionKey { kind: SessionKind::Anonymous, session_id: session_id.to_string(), anonymous_id }
}

/// The transport connection behind an attachment (a WebSocket), so the hub
/// can tear it down when the attachment is dropped.
pub trait AttachmentConn: Send + Sync {
    fn close_now(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputWriteStatus {
    Ok,
    NotFound,
    QueueFull,
    SeqGap,
}

#[derive(Debug, Clone, Copy)]
pub struct InputWriteResult {
    pub status: InputWriteStatus,
    pub got: u64,
    pub want: u64,
}

impl InputWriteResult {
    fn status(status: InputWriteStatus) -> Self {
        Self { status, got: 0, want: 0 }
    }
}

pub struct Attachment {
    pub session_key: SessionKey,
    pub id: String,
    pub client_token: String,
    size: Mutex<(usize, usize)>,
    frames_tx: Sender<OutgoingFrame>,
    frames_rx: Receiver<OutgoingFrame>,
    cancel_handle: SignalHandle,
    ctx: Signal,
    conn: Option<Arc<dyn AttachmentConn>>,
    pub persistent: bool,
    pub input_seq_ack: bool,
    last_accepted_seq: Mutex<u64>,
    ack: Mutex<(bool, u64)>,
}

impl Attachment {
    #[allow(clippy::too_many_arguments)]
    fn new(
        session_key: SessionKey,
        id: String,
        client_token: String,
        cols: usize,
        rows: usize,
        conn: Option<Arc<dyn AttachmentConn>>,
        persistent: bool,
        input_seq_ack: bool,
    ) -> Arc<Self> {
        let (frames_tx, frames_rx) = bounded(DEFAULT_WRITE_QUEUE_CAP);
        let (cancel_handle, ctx) = signal();
        Arc::new(Self {
            session_key,
            id,
            client_token,
            size: Mutex::new((cols, rows)),
            frames_tx,
            frames_rx,
            cancel_handle,
            ctx,
            conn,
            persistent,
            input_seq_ack,
            last_accepted_seq: Mutex::new(0),
            ack: Mutex::new((false, 0)),
        })
    }

    /// Construct a detached attachment for tests that exercise queueing.
    #[doc(hidden)]
    #[must_use]
    pub fn detached_for_test(
        session_key: SessionKey,
        id: &str,
        client_token: &str,
        persistent: bool,
    ) -> Arc<Self> {
        Self::new(
            session_key,
            id.to_string(),
            client_token.to_string(),
            DEFAULT_PTY_COLS,
            DEFAULT_PTY_ROWS,
            None,
            persistent,
            false,
        )
    }

    #[must_use]
    pub fn cols_rows(&self) -> (usize, usize) {
        *lock(&self.size)
    }

    pub fn cancel(&self) {
        self.cancel_handle.fire();
    }

    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.cancel_handle.is_fired()
    }

    /// Cancellation signal (fires when the attachment is superseded, detached, or dropped).
    #[must_use]
    pub fn ctx(&self) -> Signal {
        self.ctx.clone()
    }

    /// Outgoing frame queue consumer side.
    #[must_use]
    pub fn frames(&self) -> &Receiver<OutgoingFrame> {
        &self.frames_rx
    }

    pub fn enqueue_binary(&self, payload: &[u8]) -> bool {
        self.enqueue(FrameKind::Binary, payload)
    }

    pub fn enqueue_json(&self, payload: &impl Serialize) -> bool {
        match serde_json::to_vec(payload) {
            Ok(data) => self.enqueue(FrameKind::Text, &data),
            Err(_) => {
                self.cancel();
                false
            }
        }
    }

    pub fn enqueue_ready(&self, session_id: &str) -> bool {
        self.enqueue_json(&PtyEventFrame {
            kind: "ready".to_string(),
            session_id: session_id.to_string(),
            attachment_id: self.id.clone(),
            message: String::new(),
        })
    }

    pub fn enqueue(&self, kind: FrameKind, payload: &[u8]) -> bool {
        let frame = OutgoingFrame { kind, payload: payload.to_vec(), input_ack: false };
        if self.frames_tx.try_send(frame).is_ok() {
            true
        } else {
            self.cancel();
            false
        }
    }

    pub fn enqueue_input_ack(&self, seq: u64) -> bool {
        {
            let mut ack = lock(&self.ack);
            if seq > ack.1 {
                ack.1 = seq;
            }
            if ack.0 {
                return true;
            }
            ack.0 = true;
        }
        let frame = OutgoingFrame { kind: FrameKind::Text, payload: Vec::new(), input_ack: true };
        if self.frames_tx.try_send(frame).is_ok() {
            true
        } else {
            lock(&self.ack).0 = false;
            self.cancel();
            false
        }
    }

    pub fn consume_input_ack(&self) -> u64 {
        let mut ack = lock(&self.ack);
        ack.0 = false;
        ack.1
    }

    pub fn close_now(&self) {
        if let Some(conn) = &self.conn {
            conn.close_now();
        }
    }
}

/// JSON text frame emitted to WebSocket PTY clients (`ready`, `error`, ...).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PtyEventFrame {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

pub struct InputChunk {
    attachment: Arc<Attachment>,
    payload: Vec<u8>,
    seq: u64,
    final_seq_chunk: bool,
}

struct SessionMut {
    attachments: HashMap<String, Arc<Attachment>>,
    effective_cols: usize,
    effective_rows: usize,
    last_known_cols: usize,
    last_known_rows: usize,
    resize_confirms: u32,
    scrollback: Vec<u8>,
    closed: bool,
    idle_gen: u64,
}

pub struct PtySession {
    pub id: String,
    pub key: SessionKey,
    pid: Option<rustix::process::Pid>,
    tmp_script: Option<PathBuf>,
    master: Mutex<Option<Arc<OwnedFd>>>,
    wake_tx: Mutex<Option<OwnedFd>>,
    st: Mutex<SessionMut>,
    idle_cv: Condvar,
    input_tx: Sender<InputChunk>,
    input_rx: Receiver<InputChunk>,
    input_enqueue_mu: Mutex<()>,
    done_handle: SignalHandle,
    done: Signal,
    pty_write_mu: Mutex<()>,
    pty_closed: AtomicBool,
}

impl PtySession {
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        key: SessionKey,
        pid: Option<rustix::process::Pid>,
        tmp_script: Option<PathBuf>,
        master: Option<OwnedFd>,
        wake_tx: Option<OwnedFd>,
        cols: usize,
        rows: usize,
    ) -> Arc<Self> {
        let (input_tx, input_rx) = bounded(DEFAULT_INPUT_QUEUE_CAP);
        let (done_handle, done) = signal();
        Arc::new(Self {
            id,
            key,
            pid,
            tmp_script,
            master: Mutex::new(master.map(Arc::new)),
            wake_tx: Mutex::new(wake_tx),
            st: Mutex::new(SessionMut {
                attachments: HashMap::new(),
                effective_cols: cols,
                effective_rows: rows,
                last_known_cols: cols,
                last_known_rows: rows,
                resize_confirms: 0,
                scrollback: Vec::new(),
                closed: false,
                idle_gen: 0,
            }),
            idle_cv: Condvar::new(),
            input_tx,
            input_rx,
            input_enqueue_mu: Mutex::new(()),
            done_handle,
            done,
            pty_write_mu: Mutex::new(()),
            pty_closed: AtomicBool::new(false),
        })
    }

    /// Session without a process, for tests of bookkeeping paths.
    #[doc(hidden)]
    #[must_use]
    pub fn detached_for_test(
        id: &str,
        attachments: Vec<Arc<Attachment>>,
        cols: usize,
        rows: usize,
    ) -> Arc<Self> {
        let session = Self::new(
            id.to_string(),
            persistent_session_key(id),
            None,
            None,
            None,
            None,
            cols,
            rows,
        );
        {
            let mut st = lock(&session.st);
            for attachment in attachments {
                st.attachments.insert(attachment.id.clone(), attachment);
            }
        }
        session
    }

    #[must_use]
    pub fn done(&self) -> Signal {
        self.done.clone()
    }

    #[must_use]
    pub fn attachment_count(&self) -> usize {
        lock(&self.st).attachments.len()
    }

    #[must_use]
    pub fn scrollback_len(&self) -> usize {
        lock(&self.st).scrollback.len()
    }

    fn kill(&self) {
        if let Some(pid) = self.pid {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
        }
    }

    fn close_pty_files(&self) {
        if self.pty_closed.swap(true, Ordering::SeqCst) {
            return;
        }
        lock(&self.master).take();
        lock(&self.wake_tx).take();
    }

    fn master(&self) -> Option<Arc<OwnedFd>> {
        lock(&self.master).clone()
    }
}

pub struct PtyHubConfig {
    pub shell: String,
    pub scrollback_limit: usize,
    pub session_idle_ttl: Duration,
}

impl Default for PtyHubConfig {
    fn default() -> Self {
        Self { shell: String::new(), scrollback_limit: 0, session_idle_ttl: Duration::ZERO }
    }
}

struct HubInner {
    sessions: HashMap<SessionKey, Arc<PtySession>>,
    next_attachment_id: u64,
    next_anonymous_id: u64,
}

pub struct PtyHub {
    self_weak: std::sync::Weak<PtyHub>,
    inner: Mutex<HubInner>,
    shell: String,
    logger: Arc<dyn Logger>,
    scrollback_limit: usize,
    session_idle_ttl: Duration,
    open_pty: Mutex<Arc<PtyOpener>>,
}

impl PtyHub {
    #[must_use]
    pub fn new(cfg: PtyHubConfig, logger: Arc<dyn Logger>) -> Arc<Self> {
        let limit =
            if cfg.scrollback_limit == 0 { DEFAULT_SCROLLBACK_CAP } else { cfg.scrollback_limit };
        let idle_ttl = if cfg.session_idle_ttl.is_zero() {
            DEFAULT_SESSION_IDLE_TTL
        } else {
            cfg.session_idle_ttl
        };
        Arc::new_cyclic(|weak| Self {
            self_weak: weak.clone(),
            inner: Mutex::new(HubInner {
                sessions: HashMap::new(),
                next_attachment_id: 0,
                next_anonymous_id: 0,
            }),
            shell: cfg.shell.trim().to_string(),
            logger,
            scrollback_limit: limit,
            session_idle_ttl: idle_ttl,
            open_pty: Mutex::new(Arc::new(open::open_pty)),
        })
    }

    /// Replace the PTY allocator (tests simulate a denied devpts).
    pub fn set_pty_opener(&self, opener: Arc<PtyOpener>) {
        *lock(&self.open_pty) = opener;
    }

    #[doc(hidden)]
    pub fn insert_session_for_test(&self, session: Arc<PtySession>) {
        lock(&self.inner).sessions.insert(session.key.clone(), session);
    }

    #[doc(hidden)]
    pub fn remove_session_for_test(&self, key: &SessionKey) {
        lock(&self.inner).sessions.remove(key);
    }

    #[doc(hidden)]
    #[must_use]
    pub fn session_for_test(&self, key: &SessionKey) -> Option<Arc<PtySession>> {
        lock(&self.inner).sessions.get(key).cloned()
    }

    /// Attach over RPC (persistent session semantics).
    #[allow(clippy::too_many_arguments)]
    pub fn attach_rpc(
        self: &Arc<Self>,
        session_id: &str,
        attachment_id: &str,
        cols: usize,
        rows: usize,
        command: &str,
        client_token: &str,
        require_existing: bool,
        input_seq_ack: bool,
    ) -> Result<(Arc<Attachment>, Signal, Signal), String> {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return Err("session_id is required".to_string());
        }
        let (cols, rows) = normalize_pty_size(cols, rows);
        self.prepare_attachment(
            None,
            session_id,
            attachment_id.trim(),
            cols,
            rows,
            true,
            command,
            client_token,
            require_existing,
            input_seq_ack,
        )
    }

    /// Shared attach path for RPC and WebSocket clients.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_attachment(
        self: &Arc<Self>,
        conn: Option<Arc<dyn AttachmentConn>>,
        session_id: &str,
        attachment_id: &str,
        cols: usize,
        rows: usize,
        persistent: bool,
        command: &str,
        client_token: &str,
        require_existing: bool,
        input_seq_ack: bool,
    ) -> Result<(Arc<Attachment>, Signal, Signal), String> {
        let mut inner = lock(&self.inner);
        let session_key = if persistent {
            persistent_session_key(session_id)
        } else {
            let key = anonymous_session_key(session_id, inner.next_anonymous_id);
            inner.next_anonymous_id += 1;
            key
        };
        let existing = inner.sessions.get(&session_key).filter(|s| !lock(&s.st).closed).cloned();
        let session = match existing {
            Some(session) => session,
            None => {
                if require_existing {
                    return Err(format!(
                        "persistent PTY session {} is not running",
                        quote(session_id)
                    ));
                }
                let session =
                    self.start_session_locked(&session_key, session_id, cols, rows, command)?;
                inner.sessions.insert(session_key.clone(), Arc::clone(&session));
                session
            }
        };

        let attachment_id = if attachment_id.is_empty() {
            let id = format!("att-{}", inner.next_attachment_id);
            inner.next_attachment_id += 1;
            id
        } else {
            attachment_id.to_string()
        };
        let mut st = lock(&session.st);
        // Supersede any existing attachment with this id. Input the old
        // attachment already had accepted stays queued and reaches the PTY
        // ahead of anything the replacement enqueues: the session input queue
        // is FIFO with the write loop as its only consumer, whole writes
        // enqueue atomically under input_enqueue_mu, and write_input_chunk
        // deliberately does not require the chunk's attachment to still be
        // registered. Never wait on that drain here: a wedged PTY must not
        // turn reattach into an indefinite hang.
        let superseded = st.attachments.remove(&attachment_id);
        if let Some(old) = &superseded {
            old.cancel();
        }
        let attachment = Attachment::new(
            session_key,
            attachment_id.clone(),
            client_token.trim().to_string(),
            cols,
            rows,
            conn,
            persistent,
            input_seq_ack,
        );
        let replay = st.scrollback.clone();
        if !attachment.enqueue_ready(session_id) {
            attachment.cancel();
            drop(st);
            drop(inner);
            if let Some(old) = superseded {
                old.close_now();
            }
            return Err("failed to queue ready frame".to_string());
        }
        if !enqueue_pty_replay(&attachment, &replay) {
            attachment.cancel();
            drop(st);
            drop(inner);
            if let Some(old) = superseded {
                old.close_now();
            }
            return Err("failed to queue replay frame".to_string());
        }
        st.attachments.insert(attachment_id, Arc::clone(&attachment));
        let should_apply_size = self.recompute_session_size_locked(&session, &mut st);
        let session_done = session.done.clone();
        let ctx = attachment.ctx();
        drop(st);
        drop(inner);

        if let Some(old) = superseded {
            old.close_now();
        }
        if should_apply_size {
            self.apply_current_pty_size(&session);
        }
        Ok((attachment, ctx, session_done))
    }

    fn start_session_locked(
        self: &Arc<Self>,
        session_key: &SessionKey,
        session_id: &str,
        cols: usize,
        rows: usize,
        command: &str,
    ) -> Result<Arc<PtySession>, String> {
        let shell_path = resolve_pty_shell(&self.shell);
        let trimmed = command.trim();
        let mut tmp_script: Option<PathBuf> = None;
        let mut cmd = if trimmed.is_empty() {
            Command::new(&shell_path)
        } else if trimmed.len() > STARTUP_SCRIPT_ARG_LIMIT {
            // Startup script exceeds Linux's MAX_ARG_STRLEN (~128KB). Write to
            // a temp file and exec /bin/sh <file> to avoid E2BIG from execve.
            let path = write_startup_script(trimmed)?;
            tmp_script = Some(path.clone());
            let mut cmd = Command::new("/bin/sh");
            cmd.arg(&path);
            cmd
        } else {
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-c").arg(trimmed);
            cmd
        };
        cmd.env_clear().envs(default_websocket_pty_env(&shell_path));
        let (master, child) = match self.start_pty_command(cmd, cols, rows) {
            Ok(v) => v,
            Err(err) => {
                if let Some(path) = &tmp_script {
                    let _ = std::fs::remove_file(path);
                }
                self.logger.log(&format!("pty session start failed session={session_id}: {err}\n"));
                return Err(err);
            }
        };
        let pid = rustix::process::Pid::from_child(&child);
        let (wake_rx, wake_tx) =
            crate::util::cloexec_pipe().map_err(|e| format!("pipe: {}", io_error_text(&e)))?;
        let session = PtySession::new(
            session_id.to_string(),
            session_key.clone(),
            Some(pid),
            tmp_script,
            Some(master),
            Some(wake_tx),
            cols,
            rows,
        );
        let waiter_session = Arc::clone(&session);
        std::thread::spawn(move || wait_session_process(child, &waiter_session));
        let hub = Arc::clone(self);
        let pump_session = Arc::clone(&session);
        std::thread::spawn(move || hub.pump_session(&pump_session, wake_rx));
        let hub = Arc::clone(self);
        let input_session = Arc::clone(&session);
        std::thread::spawn(move || hub.write_input_loop(&input_session));
        Ok(session)
    }

    fn start_pty_command(
        &self,
        cmd: Command,
        cols: usize,
        rows: usize,
    ) -> Result<(OwnedFd, std::process::Child), String> {
        let opener = Arc::clone(&lock(&self.open_pty));
        let PtyPair { master, slave } = opener().map_err(|e| new_pty_allocation_error(&e))?;
        open::set_winsize(&slave, cols, rows).map_err(|e| io_error_text(&e))?;
        rustix::fs::fcntl_setfl(&master, rustix::fs::OFlags::NONBLOCK)
            .map_err(|e| io_error_text(&e.into()))?;
        let child = open::spawn_with_controlling_tty(cmd, &slave).map_err(|e| io_error_text(&e))?;
        drop(slave);
        Ok((master, child))
    }

    pub fn detach(&self, attachment: &Arc<Attachment>) -> bool {
        let inner = lock(&self.inner);
        let Some(session) = inner.sessions.get(&attachment.session_key).cloned() else {
            return false;
        };
        let mut st = lock(&session.st);
        if !st.attachments.get(&attachment.id).is_some_and(|c| Arc::ptr_eq(c, attachment)) {
            return false;
        }
        st.attachments.remove(&attachment.id);
        attachment.cancel();
        let should_apply_size = self.recompute_session_size_locked(&session, &mut st);
        drop(st);
        drop(inner);
        if should_apply_size {
            self.apply_current_pty_size(&session);
        }
        true
    }

    pub fn drop_attachment(&self, attachment: &Arc<Attachment>) {
        if attachment.persistent {
            self.detach(attachment);
        } else {
            self.close_session_for_attachment(attachment);
        }
        attachment.close_now();
    }

    pub fn close_session_for_attachment(&self, attachment: &Arc<Attachment>) {
        let mut inner = lock(&self.inner);
        let Some(session) = inner.sessions.get(&attachment.session_key).cloned() else {
            return;
        };
        let mut st = lock(&session.st);
        if !st.attachments.get(&attachment.id).is_some_and(|c| Arc::ptr_eq(c, attachment)) {
            return;
        }
        inner.sessions.remove(&session.key);
        st.attachments.remove(&attachment.id);
        Self::cancel_idle_reap_locked(&session, &mut st);
        attachment.cancel();
        drop(st);
        drop(inner);
        session.kill();
        session.close_pty_files();
    }

    pub fn close_all(&self) {
        let sessions: Vec<Arc<PtySession>> = {
            let mut inner = lock(&self.inner);
            let sessions: Vec<Arc<PtySession>> = inner.sessions.drain().map(|(_, s)| s).collect();
            for session in &sessions {
                let mut st = lock(&session.st);
                Self::cancel_idle_reap_locked(session, &mut st);
            }
            sessions
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
        self.write_input_by_id_with_seq(session_id, attachment_id, token, payload, 0, false).status
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
        &self,
        session_id: &str,
        attachment_id: &str,
        token: &str,
        cols: usize,
        rows: usize,
    ) -> bool {
        match self.attachment_by_id(session_id, attachment_id, token) {
            Some(attachment) => {
                self.resize(&attachment, cols, rows);
                true
            }
            None => false,
        }
    }

    pub fn detach_by_id(&self, session_id: &str, attachment_id: &str, token: &str) -> bool {
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
        let key = persistent_session_key(session_id);
        let mut inner = lock(&self.inner);
        let Some(session) = inner.sessions.get(&key).cloned() else {
            return false;
        };
        let mut st = lock(&session.st);
        if st.closed {
            return false;
        }
        inner.sessions.remove(&key);
        Self::cancel_idle_reap_locked(&session, &mut st);
        st.closed = true;
        drop(st);
        drop(inner);
        session.kill();
        session.close_pty_files();
        true
    }

    /// Snapshots of every open persistent session, sorted by session id.
    #[must_use]
    pub fn session_snapshots(&self) -> Vec<Value> {
        let inner = lock(&self.inner);
        let mut keys: Vec<(&SessionKey, &Arc<PtySession>)> = inner
            .sessions
            .iter()
            .filter(|(key, session)| {
                key.kind == SessionKind::Persistent && !lock(&session.st).closed
            })
            .collect();
        keys.sort_by(|a, b| a.0.session_id.cmp(&b.0.session_id));
        keys.into_iter().map(|(_, session)| Self::session_snapshot_locked(session)).collect()
    }

    fn attachment_by_id(
        &self,
        session_id: &str,
        attachment_id: &str,
        token: &str,
    ) -> Option<Arc<Attachment>> {
        let session_id = session_id.trim();
        let attachment_id = attachment_id.trim();
        let token = token.trim();
        if session_id.is_empty() || attachment_id.is_empty() {
            return None;
        }
        let inner = lock(&self.inner);
        let session = inner.sessions.get(&persistent_session_key(session_id))?;
        let st = lock(&session.st);
        if st.closed {
            return None;
        }
        let attachment = st.attachments.get(attachment_id)?;
        if attachment.client_token.as_bytes().ct_eq(token.as_bytes()).into() {
            Some(Arc::clone(attachment))
        } else {
            None
        }
    }

    fn session_snapshot_locked(session: &PtySession) -> Value {
        let st = lock(&session.st);
        let mut ids: Vec<&String> = st.attachments.keys().collect();
        ids.sort();
        let attachments: Vec<Value> = ids
            .into_iter()
            .map(|id| {
                let attachment = &st.attachments[id];
                let (cols, rows) = attachment.cols_rows();
                json!({
                    "attachment_id": id,
                    "cols": cols,
                    "rows": rows,
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

    #[must_use]
    pub fn active_session_count(&self) -> usize {
        lock(&self.inner).sessions.len()
    }

    #[must_use]
    pub fn max_scrollback_bytes(&self) -> usize {
        lock(&self.inner).sessions.values().map(|s| lock(&s.st).scrollback.len()).max().unwrap_or(0)
    }

    fn pump_session(self: &Arc<Self>, session: &Arc<PtySession>, wake_rx: OwnedFd) {
        let master = session.master();
        if let Some(master) = master {
            let mut buffer = vec![0u8; 32768];
            loop {
                let mut fds =
                    [PollFd::new(&master, PollFlags::IN), PollFd::new(&wake_rx, PollFlags::IN)];
                match rustix::event::poll(&mut fds, None) {
                    Ok(_) => {}
                    Err(rustix::io::Errno::INTR) => continue,
                    Err(_) => break,
                }
                if !fds[1].revents().is_empty() || session.pty_closed.load(Ordering::SeqCst) {
                    break;
                }
                match rustix::io::read(&master, &mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        let chunk = buffer[..n].to_vec();
                        self.record_and_broadcast(session, &chunk);
                        self.confirm_pty_size_after_output(session);
                    }
                    Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => {}
                    Err(_) => break,
                }
            }
        }
        self.finish_session(session);
    }

    fn finish_session(&self, session: &Arc<PtySession>) {
        session.close_pty_files();
        let mut inner = lock(&self.inner);
        if inner.sessions.get(&session.key).is_some_and(|s| Arc::ptr_eq(s, session)) {
            inner.sessions.remove(&session.key);
        }
        let mut st = lock(&session.st);
        Self::cancel_idle_reap_locked(session, &mut st);
        st.closed = true;
        st.attachments.clear();
        session.done_handle.fire();
    }

    fn record_and_broadcast(&self, session: &Arc<PtySession>, data: &[u8]) {
        let attachments: Vec<Arc<Attachment>> = {
            let _inner = lock(&self.inner);
            let mut st = lock(&session.st);
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

    fn append_scrollback_locked(&self, st: &mut SessionMut, data: &[u8]) {
        let limit = self.scrollback_limit;
        if limit == 0 || data.is_empty() {
            return;
        }
        if data.len() >= limit {
            st.scrollback = data[data.len() - limit..].to_vec();
            return;
        }
        if st.scrollback.len() + data.len() > limit {
            let keep = (limit - data.len()).min(st.scrollback.len());
            let mut next = Vec::with_capacity(limit);
            if keep > 0 {
                next.extend_from_slice(&st.scrollback[st.scrollback.len() - keep..]);
            }
            next.extend_from_slice(data);
            st.scrollback = next;
            return;
        }
        st.scrollback.extend_from_slice(data);
    }

    fn recompute_session_size_locked(
        &self,
        session: &Arc<PtySession>,
        st: &mut SessionMut,
    ) -> bool {
        if st.attachments.is_empty() {
            st.effective_cols = st.last_known_cols;
            st.effective_rows = st.last_known_rows;
            self.schedule_idle_reap_locked(session, st);
            return false;
        }
        Self::cancel_idle_reap_locked(session, st);

        let mut min_cols = 0;
        let mut min_rows = 0;
        for attachment in st.attachments.values() {
            let (cols, rows) = attachment.cols_rows();
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
        st.resize_confirms = RESIZE_CONFIRM_ATTEMPTS;
        true
    }

    fn schedule_idle_reap_locked(&self, session: &Arc<PtySession>, st: &mut SessionMut) {
        if self.session_idle_ttl.is_zero() || st.closed || !st.attachments.is_empty() {
            return;
        }
        Self::cancel_idle_reap_locked(session, st);
        st.idle_gen += 1;
        let generation = st.idle_gen;
        let ttl = self.session_idle_ttl;
        let hub = self.hub_handle();
        let session = Arc::clone(session);
        std::thread::spawn(move || {
            let deadline = Instant::now() + ttl;
            let mut guard = lock(&session.st);
            loop {
                if guard.idle_gen != generation {
                    return;
                }
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (next, _) = session
                    .idle_cv
                    .wait_timeout(guard, deadline - now)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard = next;
            }
            drop(guard);
            hub.reap_idle_session(&session, generation);
        });
    }

    fn cancel_idle_reap_locked(session: &Arc<PtySession>, st: &mut SessionMut) {
        st.idle_gen += 1;
        session.idle_cv.notify_all();
    }

    fn reap_idle_session(&self, session: &Arc<PtySession>, generation: u64) {
        let mut inner = lock(&self.inner);
        let st = lock(&session.st);
        if !inner.sessions.get(&session.key).is_some_and(|s| Arc::ptr_eq(s, session))
            || st.closed
            || !st.attachments.is_empty()
            || st.idle_gen != generation
        {
            return;
        }
        inner.sessions.remove(&session.key);
        drop(st);
        drop(inner);
        session.kill();
        session.close_pty_files();
    }

    fn confirm_pty_size_after_output(&self, session: &Arc<PtySession>) {
        {
            let inner = lock(&self.inner);
            let mut st = lock(&session.st);
            if !inner.sessions.get(&session.key).is_some_and(|s| Arc::ptr_eq(s, session))
                || st.closed
                || st.resize_confirms == 0
            {
                return;
            }
            st.resize_confirms -= 1;
        }
        self.apply_current_pty_size(session);
    }

    fn apply_current_pty_size(&self, session: &Arc<PtySession>) -> bool {
        let _write_guard = lock(&session.pty_write_mu);
        let (current, cols, rows) = {
            let inner = lock(&self.inner);
            let st = lock(&session.st);
            let current = inner.sessions.get(&session.key).is_some_and(|s| Arc::ptr_eq(s, session))
                && !st.closed
                && !st.attachments.is_empty();
            (current, st.effective_cols, st.effective_rows)
        };
        if !current || cols == 0 || rows == 0 {
            return false;
        }
        self.apply_pty_size_with_write_lock(session, cols, rows)
    }

    fn apply_pty_size_with_write_lock(
        &self,
        session: &Arc<PtySession>,
        cols: usize,
        rows: usize,
    ) -> bool {
        let mut last_err = String::new();
        for _ in 0..8 {
            let Some(master) = session.master() else {
                last_err = "pty is closed".to_string();
                continue;
            };
            if let Err(e) = open::set_winsize(&master, cols, rows) {
                last_err = io_error_text(&e);
                continue;
            }
            match open::get_winsize(&master) {
                Ok((actual_cols, actual_rows)) => {
                    if actual_cols == cols && actual_rows == rows {
                        return true;
                    }
                    last_err = format!(
                        "pty size remained {actual_cols}x{actual_rows} after resize to {cols}x{rows}"
                    );
                }
                Err(e) => last_err = io_error_text(&e),
            }
        }
        if !last_err.is_empty() {
            self.logger.log(&format!("ws pty resize failed session={}: {last_err}\n", session.id));
        }
        false
    }

    pub fn write_input(
        &self,
        attachment: &Arc<Attachment>,
        payload: &[u8],
        seq: u64,
        has_seq: bool,
    ) -> InputWriteResult {
        let Some(session) = self.session_for_attachment(&attachment.session_key) else {
            return InputWriteResult::status(InputWriteStatus::NotFound);
        };
        if payload.is_empty() {
            return InputWriteResult::status(InputWriteStatus::Ok);
        }
        let is_current = |inner: &HubInner, st: &SessionMut| {
            inner.sessions.get(&attachment.session_key).is_some_and(|s| Arc::ptr_eq(s, &session))
                && !st.closed
                && st.attachments.get(&attachment.id).is_some_and(|c| Arc::ptr_eq(c, attachment))
        };
        {
            let inner = lock(&self.inner);
            let st = lock(&session.st);
            if !is_current(&inner, &st) {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
        }

        let mut chunks = Vec::with_capacity(payload.len().div_ceil(DEFAULT_INPUT_CHUNK_BYTES));
        let mut remaining = payload.len();
        let mut rest = payload;
        while !rest.is_empty() {
            let chunk_len = rest.len().min(DEFAULT_INPUT_CHUNK_BYTES);
            remaining -= chunk_len;
            chunks.push(InputChunk {
                attachment: Arc::clone(attachment),
                payload: rest[..chunk_len].to_vec(),
                seq,
                final_seq_chunk: attachment.input_seq_ack && remaining == 0,
            });
            rest = &rest[chunk_len..];
        }

        let _enqueue_guard = lock(&session.input_enqueue_mu);
        {
            let inner = lock(&self.inner);
            let st = lock(&session.st);
            let current = is_current(&inner, &st);
            if current && attachment.input_seq_ack {
                let want = *lock(&attachment.last_accepted_seq) + 1;
                if !has_seq || seq != want {
                    return InputWriteResult { status: InputWriteStatus::SeqGap, got: seq, want };
                }
            }
            if !current {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
        }
        let capacity = session.input_tx.capacity().unwrap_or(DEFAULT_INPUT_QUEUE_CAP);
        if chunks.len() > capacity.saturating_sub(session.input_tx.len()) {
            self.logger.log(&format!(
                "ws pty input queue full session={} attachment={}\n",
                session.id, attachment.id
            ));
            return InputWriteResult::status(InputWriteStatus::QueueFull);
        }
        if attachment.input_seq_ack {
            let inner = lock(&self.inner);
            let st = lock(&session.st);
            if !is_current(&inner, &st) {
                return InputWriteResult::status(InputWriteStatus::NotFound);
            }
            *lock(&attachment.last_accepted_seq) = seq;
        }
        for chunk in chunks {
            select! {
                send(session.input_tx, chunk) -> res => {
                    if res.is_err() {
                        return InputWriteResult::status(InputWriteStatus::NotFound);
                    }
                }
                recv(session.done.receiver()) -> _ => {
                    return InputWriteResult::status(InputWriteStatus::NotFound);
                }
            }
        }
        InputWriteResult::status(InputWriteStatus::Ok)
    }

    fn write_input_loop(self: &Arc<Self>, session: &Arc<PtySession>) {
        loop {
            select! {
                recv(session.done.receiver()) -> _ => return,
                recv(session.input_rx) -> chunk => {
                    let Ok(chunk) = chunk else { return };
                    self.write_input_chunk(session, chunk);
                }
            }
        }
    }

    fn write_input_chunk(&self, session: &Arc<PtySession>, chunk: InputChunk) -> bool {
        let attachment = Arc::clone(&chunk.attachment);
        let (written, ack_ok) = self.write_input_chunk_locked(session, chunk);
        if !ack_ok {
            // enqueue_input_ack canceled the attachment because its send queue
            // was saturated; finish the cleanup like the output path does.
            // Must run outside pty_write_mu: drop_attachment can resize via
            // apply_current_pty_size, which takes pty_write_mu.
            self.drop_attachment(&attachment);
        }
        written
    }

    fn write_input_chunk_locked(
        &self,
        session: &Arc<PtySession>,
        chunk: InputChunk,
    ) -> (bool, bool) {
        let _write_guard = lock(&session.pty_write_mu);
        // Deliberately session-scoped, not attachment-scoped: once write_input
        // accepted bytes they belong to the persistent session and are written
        // even if their attachment has since detached (tmux semantics: input
        // sent before detach still executes).
        let (current, master) = {
            let inner = lock(&self.inner);
            let st = lock(&session.st);
            let current = inner.sessions.get(&session.key).is_some_and(|s| Arc::ptr_eq(s, session))
                && !st.closed;
            (current, session.master())
        };
        let (Some(master), true) = (master, current) else {
            return (false, true);
        };
        Self::write_input_chunk_with_pty_file(&master, &chunk)
    }

    fn write_input_chunk_with_pty_file(master: &OwnedFd, chunk: &InputChunk) -> (bool, bool) {
        let mut total = 0;
        while total < chunk.payload.len() {
            match rustix::io::write(master, &chunk.payload[total..]) {
                Ok(0) => return (false, true),
                Ok(n) => total += n,
                Err(rustix::io::Errno::AGAIN) => {
                    let mut fds = [PollFd::new(master, PollFlags::OUT)];
                    if rustix::event::poll(&mut fds, None).is_err() {
                        return (false, true);
                    }
                }
                Err(rustix::io::Errno::INTR) => {}
                Err(_) => return (false, true),
            }
        }
        (true, Self::ack_input_chunk(chunk))
    }

    fn ack_input_chunk(chunk: &InputChunk) -> bool {
        if !chunk.final_seq_chunk || !chunk.attachment.input_seq_ack {
            return true;
        }
        chunk.attachment.enqueue_input_ack(chunk.seq)
    }

    fn session_for_attachment(&self, key: &SessionKey) -> Option<Arc<PtySession>> {
        let inner = lock(&self.inner);
        let session = inner.sessions.get(key)?;
        if lock(&session.st).closed {
            return None;
        }
        Some(Arc::clone(session))
    }

    pub fn resize(&self, attachment: &Arc<Attachment>, cols: usize, rows: usize) {
        if cols == 0 || rows == 0 {
            return;
        }
        let (cols, rows) = normalize_pty_size(cols, rows);
        let inner = lock(&self.inner);
        let Some(session) = inner.sessions.get(&attachment.session_key).cloned() else {
            return;
        };
        let mut st = lock(&session.st);
        if st.closed {
            return;
        }
        if !st.attachments.get(&attachment.id).is_some_and(|c| Arc::ptr_eq(c, attachment)) {
            return;
        }
        *lock(&attachment.size) = (cols, rows);
        let should_apply_size = self.recompute_session_size_locked(&session, &mut st);
        drop(st);
        drop(inner);
        if should_apply_size {
            self.apply_current_pty_size(&session);
        }
    }

    fn hub_handle(&self) -> Arc<Self> {
        self.self_weak.upgrade().expect("PtyHub is always constructed inside an Arc")
    }
}

fn wait_session_process(mut child: std::process::Child, session: &Arc<PtySession>) {
    let _ = child.wait();
    if let Some(path) = &session.tmp_script {
        let _ = std::fs::remove_file(path);
    }
}

fn write_startup_script(content: &str) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir();
    for _ in 0..16 {
        let path = dir.join(format!("cmuxd-startup-{}.sh", crate::util::random_hex(8)));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(content.as_bytes()) {
                    let _ = std::fs::remove_file(&path);
                    return Err(format!("could not write startup script: {}", io_error_text(&e)));
                }
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = file.set_permissions(std::fs::Permissions::from_mode(0o400));
                }
                return Ok(path);
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(format!(
                    "could not create startup script temp file: {}",
                    io_error_text(&e)
                ));
            }
        }
    }
    Err("could not create startup script temp file: too many collisions".to_string())
}

/// Wrap a raw PTY-allocation failure with actionable diagnostics about the
/// remote devpts. Without this, a hardened mount (ptmxmode=000) or a
/// non-writable /dev/ptmx surfaced only a generic "remote PTY attach failed".
#[must_use]
pub fn new_pty_allocation_error(err: &io::Error) -> String {
    let mut suffix = String::new();
    let detail = describe_devpts();
    if !detail.is_empty() {
        suffix = format!("; {detail}");
    }
    let mut hint = "";
    if is_permission_denied(err) {
        hint = "; the remote devpts denies /dev/ptmx (e.g. mounted ptmxmode=000): remount it writable with `sudo mount -o remount,ptmxmode=0666 /dev/pts` or expose a writable /dev/ptmx so the cmux daemon can open a terminal";
    }
    format!("could not allocate a remote PTY: {}{suffix}{hint}", io_error_text(err))
}

/// Best-effort summary of `/dev/ptmx` mode and the devpts mount options.
#[must_use]
pub fn describe_devpts() -> String {
    let mut parts = Vec::new();
    match std::fs::metadata("/dev/ptmx") {
        Ok(info) => {
            use std::os::unix::fs::PermissionsExt;
            parts.push(format!("/dev/ptmx mode={:04o}", info.permissions().mode() & 0o777));
        }
        Err(e) => parts.push(format!(
            "/dev/ptmx stat error: {}",
            crate::util::path_error("stat", "/dev/ptmx", &e)
        )),
    }
    let opts = devpts_mount_options();
    if !opts.is_empty() {
        parts.push(format!("devpts ({opts})"));
    }
    parts.join("; ")
}

/// Super-block options of the devpts filesystem mounted at `/dev/pts`, parsed
/// from `/proc/self/mountinfo`, or empty when unavailable.
#[must_use]
pub fn devpts_mount_options() -> String {
    let Ok(data) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return String::new();
    };
    devpts_mount_options_from(&data)
}

fn devpts_mount_options_from(data: &str) -> String {
    for line in data.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 5 || fields[4] != "/dev/pts" {
            continue;
        }
        let Some(sep) = fields.iter().position(|f| *f == "-") else { continue };
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

/// Clamp a close reason to the 123-byte limit of a WebSocket control frame,
/// trimming on a UTF-8 boundary.
#[must_use]
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

fn is_permission_denied(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::PermissionDenied
        || matches!(err.raw_os_error(), Some(code) if code == rustix::io::Errno::ACCESS.raw_os_error() || code == rustix::io::Errno::PERM.raw_os_error())
}

pub fn enqueue_pty_replay(attachment: &Attachment, replay: &[u8]) -> bool {
    for chunk in replay.chunks(DEFAULT_REPLAY_CHUNK_BYTES) {
        if !attachment.enqueue_binary(chunk) {
            return false;
        }
    }
    true
}

/// Translate an attachment frame into the RPC event delivered to stdio clients.
#[must_use]
pub fn rpc_pty_event_for_frame(attachment: &Attachment, frame: &OutgoingFrame) -> RpcEvent {
    if frame.input_ack {
        return RpcEvent {
            event: "pty.input_ack".to_string(),
            session_id: attachment.session_key.session_id.clone(),
            attachment_id: attachment.id.clone(),
            attachment_token: attachment.client_token.clone(),
            seq: attachment.consume_input_ack(),
            ..RpcEvent::default()
        };
    }
    let mut event = RpcEvent {
        event: "pty.data".to_string(),
        session_id: attachment.session_key.session_id.clone(),
        attachment_id: attachment.id.clone(),
        attachment_token: attachment.client_token.clone(),
        ..RpcEvent::default()
    };
    if frame.kind == FrameKind::Text {
        if let Ok(ws_event) = serde_json::from_slice::<PtyEventFrame>(&frame.payload)
            && !ws_event.kind.trim().is_empty()
        {
            event.event = format!("pty.{}", ws_event.kind.trim());
            event.message = ws_event.message;
            if !ws_event.session_id.trim().is_empty() {
                event.session_id = ws_event.session_id.trim().to_string();
            }
            if !ws_event.attachment_id.trim().is_empty() {
                event.attachment_id = ws_event.attachment_id.trim().to_string();
            }
            event.attachment_token = attachment.client_token.clone();
            return event;
        }
        event.event = "pty.message".to_string();
        event.message = String::from_utf8_lossy(&frame.payload).into_owned();
        return event;
    }
    event.data_base64 = BASE64.encode(&frame.payload);
    event
}

#[must_use]
pub fn rpc_pty_exit_event(attachment: &Attachment) -> RpcEvent {
    RpcEvent {
        event: "pty.exit".to_string(),
        session_id: attachment.session_key.session_id.clone(),
        attachment_id: attachment.id.clone(),
        attachment_token: attachment.client_token.clone(),
        ..RpcEvent::default()
    }
}

#[must_use]
pub fn normalize_pty_size(cols: usize, rows: usize) -> (usize, usize) {
    let cols = if cols == 0 { DEFAULT_PTY_COLS } else { cols.min(MAX_PTY_DIMENSION) };
    let rows = if rows == 0 { DEFAULT_PTY_ROWS } else { rows.min(MAX_PTY_DIMENSION) };
    (cols, rows)
}

/// Signed variant used by RPC params: non-positive values fall back to defaults.
#[must_use]
pub fn normalize_pty_size_i64(cols: i64, rows: i64) -> (usize, usize) {
    normalize_pty_size(usize::try_from(cols).unwrap_or(0), usize::try_from(rows).unwrap_or(0))
}

#[must_use]
pub fn resolve_pty_shell(explicit: &str) -> String {
    if !explicit.trim().is_empty() {
        return explicit.to_string();
    }
    if let Ok(shell) = std::env::var("SHELL") {
        let shell = shell.trim();
        if !shell.is_empty() && std::fs::metadata(shell).is_ok() {
            return shell.to_string();
        }
    }
    for candidate in ["/bin/bash", "/usr/bin/bash", "/bin/sh"] {
        if std::fs::metadata(candidate).is_ok() {
            return candidate.to_string();
        }
    }
    "/bin/sh".to_string()
}

/// Environment for PTY children: the daemon's environment plus terminal
/// identity and a UTF-8 locale when none is configured. Order is preserved.
#[must_use]
pub fn default_websocket_pty_env(shell_path: &str) -> Vec<(String, String)> {
    let (mut env, mut order) = env_map_with_order(std::env::vars());
    let set =
        |env: &mut HashMap<String, String>, order: &mut Vec<String>, key: &str, value: &str| {
            if !env.contains_key(key) {
                order.push(key.to_string());
            }
            env.insert(key.to_string(), value.to_string());
        };
    set(&mut env, &mut order, "TERM", "xterm-256color");
    for (key, value) in
        [("COLORTERM", "truecolor"), ("TERM_PROGRAM", "ghostty"), ("SHELL", shell_path)]
    {
        if env.get(key).is_none_or(|v| v.trim().is_empty()) {
            set(&mut env, &mut order, key, value);
        }
    }
    set(&mut env, &mut order, "CMUX_REMOTE_TRANSPORT", "ws");
    if !env_has_utf8_locale(&env) {
        set(&mut env, &mut order, "LANG", "C.UTF-8");
        set(&mut env, &mut order, "LC_CTYPE", "C.UTF-8");
        set(&mut env, &mut order, "LC_ALL", "C.UTF-8");
    }
    let mut seen = std::collections::HashSet::new();
    order
        .into_iter()
        .filter(|key| seen.insert(key.clone()))
        .map(|key| {
            let value = env[&key].clone();
            (key, value)
        })
        .collect()
}

fn env_map_with_order(
    values: impl Iterator<Item = (String, String)>,
) -> (HashMap<String, String>, Vec<String>) {
    let mut env = HashMap::new();
    let mut order = Vec::new();
    for (key, value) in values {
        if !env.contains_key(&key) {
            order.push(key.clone());
        }
        env.insert(key, value);
    }
    (env, order)
}

fn env_has_utf8_locale(env: &HashMap<String, String>) -> bool {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        let value = env.get(key).map(|v| v.trim().to_uppercase()).unwrap_or_default();
        if value.is_empty() {
            continue;
        }
        return value.contains("UTF-8") || value.contains("UTF8");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_size_defaults_and_caps() {
        assert_eq!(normalize_pty_size(0, 0), (80, 24));
        assert_eq!(normalize_pty_size(100, 30), (100, 30));
        assert_eq!(normalize_pty_size(70000, 70000), (65535, 65535));
        assert_eq!(normalize_pty_size_i64(-1, 5), (80, 5));
    }

    #[test]
    fn devpts_options_are_parsed_from_mountinfo() {
        let data = "36 25 0:31 / /dev/pts rw,nosuid,noexec,relatime shared:22 - devpts devpts rw,gid=5,mode=620,ptmxmode=000\n37 25 0:32 / /proc rw - proc proc rw\n";
        assert_eq!(devpts_mount_options_from(data), "rw,gid=5,mode=620,ptmxmode=000");
        assert_eq!(devpts_mount_options_from("garbage\n"), "");
        assert_eq!(devpts_mount_options_from("1 2 3 4 /dev/pts x - tmpfs t rw\n"), "");
    }

    #[test]
    fn close_reason_is_trimmed_on_char_boundary() {
        let reason = "é".repeat(100);
        let truncated = truncate_websocket_close_reason(&reason);
        assert!(truncated.len() <= 123);
        assert!(truncated.len().is_multiple_of(2));
        assert_eq!(truncate_websocket_close_reason("short"), "short");
    }

    #[test]
    fn scrollback_is_bounded_ring() {
        let hub = PtyHub::new(
            PtyHubConfig { scrollback_limit: 8, ..PtyHubConfig::default() },
            Arc::new(crate::logger::DiscardLogger),
        );
        let session = PtySession::detached_for_test("s", Vec::new(), 80, 24);
        let mut st = lock(&session.st);
        hub.append_scrollback_locked(&mut st, b"abc");
        hub.append_scrollback_locked(&mut st, b"defgh");
        assert_eq!(st.scrollback, b"abcdefgh");
        hub.append_scrollback_locked(&mut st, b"ij");
        assert_eq!(st.scrollback, b"cdefghij");
        hub.append_scrollback_locked(&mut st, b"0123456789");
        assert_eq!(st.scrollback, b"23456789");
        hub.append_scrollback_locked(&mut st, b"");
        assert_eq!(st.scrollback, b"23456789");
    }

    #[test]
    fn replay_is_chunked_below_rpc_frame_buffer() {
        const SWIFT_RPC_MAX_FRAME_BYTES: usize = 256 * 1024;
        let attachment = Attachment::detached_for_test(
            persistent_session_key("chunked"),
            "att-chunked",
            "",
            true,
        );
        let replay = vec![b'x'; DEFAULT_REPLAY_CHUNK_BYTES * 2 + 17];
        assert!(enqueue_pty_replay(&attachment, &replay));
        let mut joined = Vec::new();
        let mut frame_count = 0;
        let mut first_two = 0;
        while let Ok(frame) = attachment.frames().try_recv() {
            frame_count += 1;
            assert!(frame.payload.len() <= DEFAULT_REPLAY_CHUNK_BYTES);
            let event = rpc_pty_event_for_frame(&attachment, &frame);
            assert_eq!(event.event, "pty.data");
            let line = serde_json::to_vec(&event).unwrap();
            assert!(line.len() + 1 < SWIFT_RPC_MAX_FRAME_BYTES);
            if frame_count <= 2 {
                first_two += line.len() + 1;
                assert!(first_two < SWIFT_RPC_MAX_FRAME_BYTES);
            }
            assert_eq!(BASE64.decode(&event.data_base64).unwrap(), frame.payload);
            joined.extend_from_slice(&frame.payload);
        }
        assert!(frame_count >= 2);
        assert_eq!(joined, replay);
    }

    #[test]
    fn full_send_queue_cancels_attachment() {
        let attachment =
            Attachment::detached_for_test(persistent_session_key("full"), "a", "", true);
        for _ in 0..DEFAULT_WRITE_QUEUE_CAP {
            assert!(attachment.enqueue_binary(b"x"));
        }
        assert!(!attachment.is_canceled());
        assert!(!attachment.enqueue_binary(b"y"));
        assert!(attachment.is_canceled());
    }

    #[test]
    fn input_ack_coalesces_until_consumed() {
        let attachment =
            Attachment::detached_for_test(persistent_session_key("ack"), "a", "t", true);
        assert!(attachment.enqueue_input_ack(3));
        assert!(attachment.enqueue_input_ack(5));
        let frame = attachment.frames().try_recv().unwrap();
        assert!(frame.input_ack);
        assert!(attachment.frames().try_recv().is_err(), "second ack must coalesce");
        let event = rpc_pty_event_for_frame(&attachment, &frame);
        assert_eq!(event.event, "pty.input_ack");
        assert_eq!(event.seq, 5);
        assert_eq!(event.attachment_token, "t");
        assert!(attachment.enqueue_input_ack(6));
        assert!(attachment.frames().try_recv().is_ok());
    }

    #[test]
    fn text_frames_map_to_named_events() {
        let attachment =
            Attachment::detached_for_test(persistent_session_key("s"), "a", "tok", true);
        let frame = OutgoingFrame {
            kind: FrameKind::Text,
            payload: br#"{"type":"ready","session_id":"s","attachment_id":"a"}"#.to_vec(),
            input_ack: false,
        };
        let event = rpc_pty_event_for_frame(&attachment, &frame);
        assert_eq!(event.event, "pty.ready");
        assert_eq!(event.session_id, "s");
        assert_eq!(event.attachment_token, "tok");
        let frame =
            OutgoingFrame { kind: FrameKind::Text, payload: b"plain".to_vec(), input_ack: false };
        let event = rpc_pty_event_for_frame(&attachment, &frame);
        assert_eq!(event.event, "pty.message");
        assert_eq!(event.message, "plain");
        assert_eq!(rpc_pty_exit_event(&attachment).event, "pty.exit");
    }

    #[test]
    fn pty_env_sets_terminal_identity_and_utf8_locale() {
        let env = default_websocket_pty_env("/bin/sh");
        let map: HashMap<String, String> = env.iter().cloned().collect();
        assert_eq!(map["TERM"], "xterm-256color");
        assert_eq!(map["CMUX_REMOTE_TRANSPORT"], "ws");
        assert!(!map["COLORTERM"].is_empty());
        assert!(env_has_utf8_locale(&map));
        let keys: Vec<&String> = env.iter().map(|(k, _)| k).collect();
        let unique: std::collections::HashSet<&String> = keys.iter().copied().collect();
        assert_eq!(keys.len(), unique.len(), "no duplicate keys");
        let mut plain = HashMap::new();
        plain.insert("LANG".to_string(), "C".to_string());
        assert!(!env_has_utf8_locale(&plain));
        plain.insert("LC_ALL".to_string(), "en_US.utf8".to_string());
        assert!(env_has_utf8_locale(&plain));
    }

    #[test]
    fn shell_resolution_prefers_existing_paths() {
        assert_eq!(resolve_pty_shell(" /custom/shell "), " /custom/shell ");
        let resolved = resolve_pty_shell("");
        assert!(std::fs::metadata(&resolved).is_ok(), "{resolved}");
    }
}

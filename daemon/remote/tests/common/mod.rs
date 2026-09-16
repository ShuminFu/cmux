#![allow(dead_code, unsafe_code)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cmuxd_remote::rpc::{FrameWriter, RpcEvent, RpcResponse};
use crossbeam_channel::{Receiver, Sender, unbounded};
use serde_json::{Map, Value, json};

pub type Frame = Map<String, Value>;

pub fn b64(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

pub fn unb64(frame: &Frame) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(frame.get("data_base64").and_then(Value::as_str).unwrap_or(""))
        .unwrap_or_default()
}

pub fn parse_frame(line: &str) -> Frame {
    match serde_json::from_str::<Value>(line.trim()) {
        Ok(Value::Object(map)) => map,
        other => panic!("frame is not an object: {line:?} -> {other:?}"),
    }
}

pub fn is_ok(frame: &Frame) -> bool {
    frame.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

pub fn error_code(frame: &Frame) -> String {
    frame
        .get("error")
        .and_then(Value::as_object)
        .and_then(|e| e.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

pub fn error_message(frame: &Frame) -> String {
    frame
        .get("error")
        .and_then(Value::as_object)
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

pub fn result_obj(frame: &Frame) -> Frame {
    frame.get("result").and_then(Value::as_object).cloned().unwrap_or_default()
}

pub fn str_field(frame: &Frame, key: &str) -> String {
    frame.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

pub fn request(id: impl Into<Value>, method: &str, params: Value) -> String {
    json!({"id": id.into(), "method": method, "params": params}).to_string()
}

/// Reads newline-delimited JSON frames from a reader on a background thread.
pub struct FrameStream {
    rx: Receiver<Frame>,
    pending: Mutex<Vec<Frame>>,
}

impl FrameStream {
    pub fn spawn<R: Read + Send + 'static>(reader: R) -> Self {
        let (tx, rx) = unbounded();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        if tx.send(parse_frame(&line)).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self { rx, pending: Mutex::new(Vec::new()) }
    }

    pub fn next(&self, timeout: Duration) -> Option<Frame> {
        if let Some(frame) = self.pending.lock().unwrap().pop() {
            return Some(frame);
        }
        self.rx.recv_timeout(timeout).ok()
    }

    pub fn expect_next(&self) -> Frame {
        self.next(Duration::from_secs(5)).expect("timed out waiting for frame")
    }

    /// Wait for the first response (a frame without `event`), stashing events.
    pub fn response(&self) -> Frame {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stash = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let frame = self.rx.recv_timeout(remaining).expect("timed out waiting for response");
            if frame.contains_key("event") {
                stash.push(frame);
                continue;
            }
            let mut pending = self.pending.lock().unwrap();
            for f in stash.into_iter().rev() {
                pending.push(f);
            }
            return frame;
        }
    }

    pub fn event(&self, matches: impl Fn(&Frame) -> bool) -> Frame {
        self.event_within(Duration::from_secs(5), matches).expect("timed out waiting for event")
    }

    pub fn event_within(
        &self,
        timeout: Duration,
        matches: impl Fn(&Frame) -> bool,
    ) -> Option<Frame> {
        let deadline = Instant::now() + timeout;
        {
            let mut pending = self.pending.lock().unwrap();
            if let Some(idx) = pending.iter().rposition(|f| f.contains_key("event") && matches(f)) {
                return Some(pending.remove(idx));
            }
        }
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let Ok(frame) = self.rx.recv_timeout(remaining) else { return None };
            if frame.contains_key("event") && matches(&frame) {
                return Some(frame);
            }
            self.pending.lock().unwrap().insert(0, frame);
        }
    }

    pub fn drain(&self) -> Vec<Frame> {
        let mut out: Vec<Frame> = self.pending.lock().unwrap().drain(..).rev().collect();
        while let Ok(frame) = self.rx.try_recv() {
            out.push(frame);
        }
        out
    }
}

/// An in-process `serve --stdio` session driven over pipes.
pub struct StdioSession {
    stdin: Option<io::PipeWriter>,
    pub frames: FrameStream,
    handle: Option<std::thread::JoinHandle<(i32, String)>>,
}

impl StdioSession {
    pub fn start(args: &[&str]) -> Self {
        let (stdin_r, stdin_w) = io::pipe().unwrap();
        let (stdout_r, stdout_w) = io::pipe().unwrap();
        let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        let handle = std::thread::spawn(move || {
            let mut stderr = Vec::new();
            let code = cmuxd_remote::serve::run(&args, stdin_r, stdout_w, &mut stderr);
            (code, String::from_utf8_lossy(&stderr).into_owned())
        });
        Self { stdin: Some(stdin_w), frames: FrameStream::spawn(stdout_r), handle: Some(handle) }
    }

    pub fn send_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin closed");
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.write_all(b"\n").unwrap();
        stdin.flush().unwrap();
    }

    pub fn send_raw(&mut self, data: &[u8]) {
        let stdin = self.stdin.as_mut().expect("stdin closed");
        stdin.write_all(data).unwrap();
        stdin.flush().unwrap();
    }

    pub fn call(&mut self, id: impl Into<Value>, method: &str, params: Value) -> Frame {
        self.send_line(&request(id, method, params));
        self.frames.response()
    }

    pub fn close_stdin(&mut self) {
        self.stdin.take();
    }

    pub fn finish(mut self) -> (i32, String) {
        self.close_stdin();
        self.handle.take().unwrap().join().unwrap()
    }
}

/// Run `serve --stdio` to completion over a fixed input.
pub fn run_stdio(input: &str) -> (i32, Vec<Frame>, String) {
    let args = ["serve".to_string(), "--stdio".to_string()];
    let (mut out_r, out_w) = io::pipe().unwrap();
    let input = input.to_string();
    let collector = std::thread::spawn(move || {
        let mut buf = String::new();
        out_r.read_to_string(&mut buf).unwrap();
        buf
    });
    let mut stderr = Vec::new();
    let code =
        cmuxd_remote::serve::run(&args, io::Cursor::new(input.into_bytes()), out_w, &mut stderr);
    let output = collector.join().unwrap();
    let frames = output.lines().filter(|l| !l.trim().is_empty()).map(parse_frame).collect();
    (code, frames, String::from_utf8_lossy(&stderr).into_owned())
}

pub fn run_args(args: &[&str]) -> (i32, String, String) {
    let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    let (mut out_r, out_w) = io::pipe().unwrap();
    let collector = std::thread::spawn(move || {
        let mut buf = String::new();
        out_r.read_to_string(&mut buf).unwrap();
        buf
    });
    let mut stderr = Vec::new();
    let code = cmuxd_remote::serve::run(&args, io::empty(), out_w, &mut stderr);
    (code, collector.join().unwrap(), String::from_utf8_lossy(&stderr).into_owned())
}

/// FrameWriter that records frames and wakes waiters.
pub struct CaptureWriter {
    tx: Sender<Frame>,
    pub frames: FrameStream,
    pub fail_writes: std::sync::atomic::AtomicBool,
}

impl CaptureWriter {
    pub fn new() -> Arc<Self> {
        let (tx, rx) = unbounded();
        Arc::new(Self {
            tx,
            frames: FrameStream { rx, pending: Mutex::new(Vec::new()) },
            fail_writes: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn record(&self, value: &impl serde::Serialize) -> io::Result<()> {
        if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "capture closed"));
        }
        let frame = parse_frame(&serde_json::to_string(value).unwrap());
        let _ = self.tx.send(frame);
        Ok(())
    }
}

impl FrameWriter for CaptureWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.record(resp)
    }
    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.record(event)
    }
}

/// A newline-delimited JSON client over a Unix socket (persistent daemon).
pub struct UnixClient {
    pub conn: UnixStream,
    pub frames: FrameStream,
}

impl UnixClient {
    pub fn connect(path: &std::path::Path) -> Self {
        let conn = UnixStream::connect(path).expect("connect unix socket");
        let reader = conn.try_clone().unwrap();
        Self { conn, frames: FrameStream::spawn(reader) }
    }

    pub fn connect_and_auth(path: &std::path::Path, token: &str) -> Self {
        let mut client = Self::connect(path);
        let resp = client.call("auth", "daemon.auth", json!({"token": token}));
        assert!(is_ok(&resp), "auth failed: {resp:?}");
        client
    }

    pub fn send_line(&mut self, line: &str) {
        self.conn.write_all(line.as_bytes()).unwrap();
        self.conn.write_all(b"\n").unwrap();
        self.conn.flush().unwrap();
    }

    pub fn call(&mut self, id: impl Into<Value>, method: &str, params: Value) -> Frame {
        self.send_line(&request(id, method, params));
        self.frames.response()
    }
}

impl Drop for UnixClient {
    fn drop(&mut self) {
        // The reader thread holds a clone of the socket, so shut it down
        // explicitly to deliver EOF to the daemon like a closed connection.
        let _ = self.conn.shutdown(std::net::Shutdown::Both);
    }
}

pub fn temp_socket_dir() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("cmuxd-rs-test-").tempdir_in("/tmp").unwrap()
}

pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    cond()
}

/// Serialize tests that mutate process-wide state (environment variables).
pub static ENV_LOCK: Mutex<()> = Mutex::new(());

thread_local! {
    static ENV_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct EnvGuard {
    saved: Vec<(String, Option<String>)>,
    _lock: Option<std::sync::MutexGuard<'static, ()>>,
}

impl EnvGuard {
    /// Nested guards on the same thread share the outer lock.
    pub fn set(vars: &[(&str, Option<&str>)]) -> Self {
        let lock = if ENV_DEPTH.with(|d| d.get()) == 0 {
            Some(ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner()))
        } else {
            None
        };
        ENV_DEPTH.with(|d| d.set(d.get() + 1));
        let mut saved = Vec::new();
        for (key, value) in vars {
            saved.push(((*key).to_string(), std::env::var(key).ok()));
            // SAFETY: tests that touch the environment hold ENV_LOCK, and the
            // test binary runs no other threads reading the environment
            // concurrently at that point.
            unsafe {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
        Self { saved, _lock: lock }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        ENV_DEPTH.with(|d| d.set(d.get() - 1));
        for (key, value) in self.saved.drain(..) {
            unsafe {
                match value {
                    Some(v) => std::env::set_var(&key, v),
                    None => std::env::remove_var(&key),
                }
            }
        }
    }
}

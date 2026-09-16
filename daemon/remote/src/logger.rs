//! Diagnostic output sink (the Go code passed an `io.Writer`, usually
//! `os.Stderr` or `io.Discard`).

use std::io::Write;
use std::sync::{Arc, Mutex};

pub trait Logger: Send + Sync {
    fn log(&self, text: &str);
}

/// Writes to the process stderr.
pub struct StderrLogger;

impl Logger for StderrLogger {
    fn log(&self, text: &str) {
        let mut err = std::io::stderr().lock();
        let _ = err.write_all(text.as_bytes());
    }
}

/// Drops everything (`io.Discard`).
pub struct DiscardLogger;

impl Logger for DiscardLogger {
    fn log(&self, _text: &str) {}
}

/// Shared in-memory sink used by tests and by the persistent daemon log file.
#[derive(Clone, Default)]
pub struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn contents(&self) -> String {
        String::from_utf8_lossy(
            &self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_owned()
    }

    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Non-empty, newline-separated frames written so far.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.contents().lines().filter(|l| !l.trim().is_empty()).map(str::to_string).collect()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logger for SharedBuffer {
    fn log(&self, text: &str) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(text.as_bytes());
    }
}

/// Logger that appends to a file (the persistent daemon's `daemon.log`).
pub struct FileLogger {
    file: Mutex<std::fs::File>,
}

impl FileLogger {
    #[must_use]
    pub fn new(file: std::fs::File) -> Self {
        Self { file: Mutex::new(file) }
    }
}

impl Logger for FileLogger {
    fn log(&self, text: &str) {
        let mut file = self.file.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = file.write_all(text.as_bytes());
    }
}

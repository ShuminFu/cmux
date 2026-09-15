//! `cmux-vault` discovers local coding-agent session transcripts and syncs
//! them to cmux Vault cloud storage.
//!
//! The crate is a Rust port of the original Go CLI. Module boundaries, the
//! on-disk state format, the HTTP contract, and the command-line surface are
//! intentionally identical so existing sync state and scripts keep working.

pub mod agentdirs;
pub mod api;
pub mod authflow;
pub mod authstore;
pub mod cli;
pub mod environ;
pub mod flags;
pub mod gopath;
pub mod resume;
pub mod state;
pub mod syncer;
pub mod util;

/// Error type used throughout the crate. Messages are user-facing and mirror
/// the strings the Go implementation printed.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

/// Trait for the human-readable progress stream (stdout in the CLI, a buffer
/// in tests). Implementations must be `Sync` because upload workers report
/// from multiple threads.
pub trait Printer: Sync {
    fn print(&self, text: &str);
}

impl Printer for std::sync::Mutex<Vec<u8>> {
    fn print(&self, text: &str) {
        self.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(text.as_bytes());
    }
}

impl Printer for std::sync::Mutex<String> {
    fn print(&self, text: &str) {
        self.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push_str(text);
    }
}

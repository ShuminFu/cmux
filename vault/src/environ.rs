//! Process environment snapshot plus the deferred warning list that the CLI
//! prints on exit.

use std::collections::HashMap;
use std::sync::Mutex;

pub struct Environ {
    pub home_dir: String,
    pub vars: HashMap<String, String>,
    warnings: Mutex<Vec<String>>,
}

impl Environ {
    #[must_use]
    pub fn new(home_dir: impl Into<String>, vars: HashMap<String, String>) -> Self {
        Self { home_dir: home_dir.into(), vars, warnings: Mutex::new(Vec::new()) }
    }

    /// Snapshot of the real process environment, mirroring `os.UserHomeDir`.
    pub fn real() -> Result<Self, String> {
        let vars: HashMap<String, String> = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.to_str()?.to_string(), v.to_str()?.to_string())))
            .collect();
        let (home_key, label) =
            if cfg!(windows) { ("USERPROFILE", "%userprofile%") } else { ("HOME", "$HOME") };
        let home = std::env::var_os(home_key).and_then(|v| v.to_str().map(str::to_string));
        match home {
            Some(home) if !home.is_empty() => Ok(Self::new(home, vars)),
            _ => Err(format!("{label} is not defined")),
        }
    }

    /// Trimmed environment variable, or empty when unset.
    #[must_use]
    pub fn get(&self, key: &str) -> String {
        self.vars.get(key).map(|v| v.trim().to_string()).unwrap_or_default()
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.warnings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message.into());
    }

    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        self.warnings.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    pub fn take_warnings(&self) -> Vec<String> {
        std::mem::take(
            &mut *self.warnings.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

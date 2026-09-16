//! Discovery of local coding-agent transcripts (Claude Code, Codex, pi) and
//! the per-agent restore and resume contracts.

mod claude;
mod codex;
mod open;
mod pi;
mod walk;

use std::io::{BufRead, BufReader};
use std::sync::LazyLock;
use std::time::SystemTime;

use regex::Regex;
use serde::Serialize;

pub use claude::Claude;
pub use codex::Codex;
pub use open::{open_regular_file_nofollow, regular_file_info_nofollow};
pub use pi::Pi;

use crate::environ::Environ;
use crate::gopath;
use crate::util::{path_error, quote};
use crate::{Error, Result};

pub const UUID_PATTERN: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

pub static UUID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("(?i)^{UUID_PATTERN}$")).expect("valid regex"));

/// Maximum bytes of a single JSONL line inspected while recovering metadata
/// (matches the Go scanner buffer cap).
const MAX_LINE_BYTES: usize = 1024 * 1024;

pub trait Agent: Sync {
    fn name(&self) -> &'static str;
    fn discover(&self, env: &Environ) -> Result<Vec<Session>>;
    fn restore_path(&self, env: &Environ, s: &SessionRef) -> Result<String>;
    fn resume_hint(&self, s: &SessionRef) -> String;
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Session {
    #[serde(rename = "agent")]
    pub agent_name: String,
    #[serde(rename = "agentSessionId")]
    pub agent_session_id: String,
    #[serde(rename = "path")]
    pub abs_path: String,
    #[serde(rename = "relPath")]
    pub rel_path: String,
    #[serde(rename = "cwd", skip_serializing_if = "String::is_empty")]
    pub cwd: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: i64,
    #[serde(rename = "modTime", with = "crate::util::serde_rfc3339_nano")]
    pub mod_time: SystemTime,
}

impl Session {
    #[must_use]
    pub fn as_ref(&self) -> SessionRef {
        SessionRef {
            agent_name: self.agent_name.clone(),
            agent_session_id: self.agent_session_id.clone(),
            rel_path: self.rel_path.clone(),
            cwd: self.cwd.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct SessionRef {
    #[serde(rename = "agent")]
    pub agent_name: String,
    #[serde(rename = "agentSessionId")]
    pub agent_session_id: String,
    #[serde(rename = "relPath")]
    pub rel_path: String,
    #[serde(rename = "cwd", skip_serializing_if = "String::is_empty")]
    pub cwd: String,
}

#[must_use]
pub fn all() -> Vec<Box<dyn Agent>> {
    vec![Box::new(Claude), Box::new(Codex), Box::new(Pi)]
}

#[must_use]
pub fn by_name(name: &str) -> Option<Box<dyn Agent>> {
    let normalized = name.trim().to_lowercase();
    all().into_iter().find(|agent| agent.name() == normalized)
}

pub fn discover_all(env: &Environ, agent_filter: &str) -> Result<Vec<Session>> {
    let agents: Vec<Box<dyn Agent>> = if agent_filter.trim().is_empty() {
        all()
    } else {
        match by_name(agent_filter) {
            Some(agent) => vec![agent],
            None => return Err(format!("unknown agent {}", quote(agent_filter)).into()),
        }
    };
    let mut sessions = Vec::new();
    for agent in &agents {
        sessions.extend(agent.discover(env)?);
    }
    sessions
        .sort_by(|a, b| a.agent_name.cmp(&b.agent_name).then_with(|| a.rel_path.cmp(&b.rel_path)));
    Ok(sessions)
}

pub(crate) fn path_under_home(env: &Environ, parts: &[&str]) -> Result<String> {
    if env.home_dir.trim().is_empty() {
        return Err("home directory is empty".into());
    }
    let mut all: Vec<&str> = vec![env.home_dir.as_str()];
    all.extend_from_slice(parts);
    Ok(gopath::join(&all))
}

pub(crate) fn rel_path(root: &str, path: &str) -> Result<String> {
    let rel = gopath::rel(root, path).map_err(Error::from)?;
    if rel == "." || rel.starts_with(&format!("..{}", gopath::SEP)) || rel == ".." {
        return Err(format!("{path} is not under {root}").into());
    }
    Ok(gopath::to_slash(&rel))
}

pub(crate) fn clean_restore_path(root: &str, rel: &str) -> Result<String> {
    let rel = gopath::from_slash(rel.trim());
    if rel.is_empty() || gopath::is_abs(&rel) {
        return Err(format!("invalid relative path {}", quote(&rel)).into());
    }
    let cleaned = gopath::clean(&rel);
    if cleaned == "." || cleaned.starts_with(&format!("..{}", gopath::SEP)) || cleaned == ".." {
        return Err(format!("invalid relative path {}", quote(&rel)).into());
    }
    Ok(gopath::join(&[root, &cleaned]))
}

/// Resolve symlinks in a walk root so the traversal below never follows a
/// link, while keeping the literal root for relative-path bookkeeping.
pub(crate) fn resolve_walk_root(env: &Environ, agent_name: &str, path: &str) -> String {
    match std::fs::canonicalize(path) {
        Ok(resolved) => {
            if let Some(resolved) = resolved.to_str() {
                strip_verbatim_prefix(resolved).to_string()
            } else {
                env.warn(format!(
                    "{agent_name}: using literal walk root {path} after symlink resolution produced a non-UTF-8 path"
                ));
                path.to_string()
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => path.to_string(),
        Err(e) => {
            env.warn(format!(
                "{agent_name}: using literal walk root {path} after symlink resolution failed: {}",
                path_error("lstat", path, &e)
            ));
            path.to_string()
        }
    }
}

/// `std::fs::canonicalize` returns verbatim (`\\?\`) paths on Windows, which
/// the lexical helpers would mangle; Go's `EvalSymlinks` returned plain drive
/// paths, so normalize drive-letter paths back to that shape. Verbatim UNC
/// paths are left untouched. No-op on other platforms.
fn strip_verbatim_prefix(path: &str) -> &str {
    if cfg!(windows)
        && !path.starts_with(r"\\?\UNC\")
        && let Some(rest) = path.strip_prefix(r"\\?\")
    {
        return rest;
    }
    path
}

pub(crate) fn logical_walk_path(literal_root: &str, resolved_root: &str, path: &str) -> String {
    match gopath::rel(resolved_root, path) {
        Ok(rel) => gopath::join(&[literal_root, &rel]),
        Err(_) => path.to_string(),
    }
}

pub(crate) fn stat_session_with_logical_path(
    agent_name: &str,
    root: &str,
    path: &str,
    logical_path: &str,
    id: String,
    cwd: String,
) -> Result<Session> {
    let info = regular_file_info_nofollow(path)?;
    let rel = rel_path(root, logical_path)?;
    Ok(Session {
        agent_name: agent_name.to_string(),
        agent_session_id: id,
        abs_path: path.to_string(),
        rel_path: rel,
        cwd,
        size_bytes: i64::try_from(info.len()).unwrap_or(i64::MAX),
        mod_time: info.modified().unwrap_or(SystemTime::UNIX_EPOCH),
    })
}

/// Decides whether a walked file (`name`, `path`) is a session, returning its
/// agent session id and working directory.
pub(crate) type Classify<'a> = dyn Fn(&str, &str) -> Option<(String, String)> + 'a;

/// Shared walk used by every agent: visit regular `.jsonl` candidates under
/// `literal_walk_root`, skipping symlinks and unreadable subtrees with a
/// warning, and let `classify` decide whether a file is a session.
pub(crate) fn discover_sessions(
    env: &Environ,
    agent_name: &str,
    root: &str,
    literal_walk_root: &str,
    classify: &Classify<'_>,
) -> Vec<Session> {
    let walk_root = resolve_walk_root(env, agent_name, literal_walk_root);
    let walk_root_clean = gopath::clean(&walk_root);
    let mut sessions = Vec::new();
    walk::walk_dir(&walk_root, &mut |path, entry, err| {
        if let Some(err) = err {
            if gopath::clean(path) == walk_root_clean {
                return walk::Flow::Continue;
            }
            env.warn(format!(
                "{agent_name}: skipping unreadable path {path}: {}",
                path_error("open", path, &err)
            ));
            if entry.is_some_and(|e| e.is_dir) {
                return walk::Flow::SkipDir;
            }
            return walk::Flow::Continue;
        }
        let Some(entry) = entry else {
            return walk::Flow::Continue;
        };
        if entry.is_dir {
            return walk::Flow::Continue;
        }
        if entry.is_symlink {
            env.warn(format!("{agent_name}: skipping symlinked session {path}"));
            return walk::Flow::Continue;
        }
        let Some((id, cwd)) = classify(&entry.name, path) else {
            return walk::Flow::Continue;
        };
        let logical_path = logical_walk_path(literal_walk_root, &walk_root, path);
        match stat_session_with_logical_path(agent_name, root, path, &logical_path, id, cwd) {
            Ok(session) => sessions.push(session),
            Err(e) => {
                env.warn(format!("{agent_name}: skipping session {path} after stat failed: {e}"));
            }
        }
        walk::Flow::Continue
    });
    sessions
}

pub(crate) enum LineRead {
    Eof,
    Line,
    TooLong,
}

/// Read one line (without its terminator) into `buf`, giving up once the line
/// exceeds `max` bytes so a corrupt transcript cannot exhaust memory.
pub(crate) fn read_line_capped(
    reader: &mut impl BufRead,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<LineRead> {
    buf.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(if buf.is_empty() { LineRead::Eof } else { LineRead::Line });
        }
        if let Some(idx) = available.iter().position(|b| *b == b'\n') {
            buf.extend_from_slice(&available[..idx]);
            reader.consume(idx + 1);
            if buf.len() >= max {
                return Ok(LineRead::TooLong);
            }
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(LineRead::Line);
        }
        let len = available.len();
        buf.extend_from_slice(available);
        reader.consume(len);
        if buf.len() >= max {
            return Ok(LineRead::TooLong);
        }
    }
}

pub(crate) fn recover_cwd_from_jsonl(path: &str) -> String {
    let Ok((file, _)) = open_regular_file_nofollow(path) else {
        return String::new();
    };
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = Vec::new();
    let mut lines = 0;
    loop {
        match read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES) {
            Ok(LineRead::Line) => {}
            Ok(LineRead::Eof | LineRead::TooLong) | Err(_) => return String::new(),
        }
        lines += 1;
        if let Some(cwd) = cwd_from_json(&buf) {
            return cwd;
        }
        if lines >= 128 {
            return String::new();
        }
    }
}

pub(crate) fn cwd_from_json(data: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(data).ok()?;
    find_string_key(&value, "cwd")
}

fn find_string_key(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(s)) = map.get(key)
                && !s.trim().is_empty()
            {
                return Some(s.clone());
            }
            map.values().find_map(|child| find_string_key(child, key))
        }
        serde_json::Value::Array(items) => {
            items.iter().find_map(|child| find_string_key(child, key))
        }
        _ => None,
    }
}

pub(crate) fn cwd_from_munged(name: &str) -> String {
    name.trim().to_string()
}

#[must_use]
pub fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests;

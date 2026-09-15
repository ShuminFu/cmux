use std::io::BufReader;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

use super::{
    Agent, LineRead, Session, SessionRef, UUID_PATTERN, UUID_RE, clean_restore_path,
    discover_sessions, open_regular_file_nofollow, path_under_home, read_line_capped,
    recover_cwd_from_jsonl,
};
use crate::Result;
use crate::environ::Environ;
use crate::gopath;

pub struct Codex;

static CODEX_ROLLOUT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"(?i)^rollout-.+-({UUID_PATTERN})\.jsonl$")).expect("valid regex")
});

fn codex_root(env: &Environ) -> Result<String> {
    let root = env.get("CODEX_HOME");
    if !root.is_empty() {
        return Ok(root);
    }
    path_under_home(env, &[".codex"])
}

impl Agent for Codex {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn discover(&self, env: &Environ) -> Result<Vec<Session>> {
        let root = codex_root(env)?;
        let mut sessions = Vec::new();
        for dir in ["sessions", "archived_sessions"] {
            let walk_root = gopath::join(&[&root, dir]);
            sessions.extend(discover_sessions(
                env,
                self.name(),
                &root,
                &walk_root,
                &|name, path| {
                    let mut id = codex_id_from_filename(name);
                    if id.is_empty() {
                        return None;
                    }
                    let (meta_id, cwd) = codex_meta(path);
                    if !meta_id.is_empty() {
                        id = meta_id;
                    }
                    Some((id, cwd))
                },
            ));
        }
        Ok(sessions)
    }

    fn restore_path(&self, env: &Environ, s: &SessionRef) -> Result<String> {
        let root = codex_root(env)?;
        clean_restore_path(&root, &s.rel_path)
    }

    fn resume_hint(&self, s: &SessionRef) -> String {
        format!("codex resume {}", s.agent_session_id)
    }
}

pub(crate) fn codex_id_from_filename(name: &str) -> String {
    match CODEX_ROLLOUT_RE.captures(name) {
        Some(caps) => caps[1].to_lowercase(),
        None => String::new(),
    }
}

/// Look up an object key the way `encoding/json` does: exact match first,
/// then case-insensitive.
fn get_ci<'a>(map: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.get(key).or_else(|| map.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v))
}

/// Decode a string field with Go struct semantics: missing or `null` is
/// empty, and any other non-string value is a decode error.
fn string_field(map: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    match get_ci(map, key) {
        None | Some(Value::Null) => Some(String::new()),
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => None,
    }
}

/// Read the first line of a Codex rollout. Returns `(id, cwd)` from a
/// `session_meta` record, or `("", recovered cwd)` for any other first line.
pub(crate) fn codex_meta(path: &str) -> (String, String) {
    let Ok((file, _)) = open_regular_file_nofollow(path) else {
        return (String::new(), String::new());
    };
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = Vec::new();
    match read_line_capped(&mut reader, &mut buf, super::MAX_LINE_BYTES) {
        Ok(LineRead::Line) => {}
        Ok(LineRead::Eof | LineRead::TooLong) | Err(_) => return (String::new(), String::new()),
    }
    let Some((line_type, id, cwd)) = parse_meta_line(&buf) else {
        return (String::new(), String::new());
    };
    if line_type != "session_meta" {
        return (String::new(), recover_cwd_from_jsonl(path));
    }
    let id = id.trim();
    let id = if UUID_RE.is_match(id) { id.to_lowercase() } else { String::new() };
    (id, cwd.trim().to_string())
}

/// Parse `{"type": ..., "payload": {"id": ..., "cwd": ...}}` with the
/// tolerance of `json.Unmarshal` into a Go struct. `None` means the line is
/// not decodable into that shape.
fn parse_meta_line(data: &[u8]) -> Option<(String, String, String)> {
    let value: Value = serde_json::from_slice(data).ok()?;
    let map = match value {
        Value::Null => return Some((String::new(), String::new(), String::new())),
        Value::Object(map) => map,
        _ => return None,
    };
    let line_type = string_field(&map, "type")?;
    let (id, cwd) = match get_ci(&map, "payload") {
        None | Some(Value::Null) => (String::new(), String::new()),
        Some(Value::Object(payload)) => {
            (string_field(payload, "id")?, string_field(payload, "cwd")?)
        }
        Some(_) => return None,
    };
    Some((line_type, id, cwd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollout_filename_extracts_lowercase_uuid() {
        assert_eq!(
            codex_id_from_filename(
                "rollout-2026-07-04T00-00-00-019D60BC-B684-7A01-B4AC-52FEFFC5FCB5.jsonl"
            ),
            "019d60bc-b684-7a01-b4ac-52feffc5fcb5"
        );
        assert_eq!(codex_id_from_filename("junk-019d60bc-b684-7a01-b4ac-52feffc5fcb5.jsonl"), "");
        assert_eq!(
            codex_id_from_filename("rollout-x-019d60bc-b684-7a01-b4ac-52feffc5fcb5.txt"),
            ""
        );
    }

    #[test]
    fn meta_line_parsing_tolerates_go_struct_shapes() {
        let ok = parse_meta_line(br#"{"type":"session_meta","payload":{"id":"abc","cwd":"/r"}}"#);
        assert_eq!(ok, Some(("session_meta".into(), "abc".into(), "/r".into())));
        let ci = parse_meta_line(br#"{"Type":"session_meta","PAYLOAD":{"ID":"abc"}}"#);
        assert_eq!(ci, Some(("session_meta".into(), "abc".into(), String::new())));
        assert_eq!(parse_meta_line(b"null"), Some((String::new(), String::new(), String::new())));
        assert_eq!(
            parse_meta_line(br#"{"type":"x","payload":null}"#),
            Some(("x".into(), String::new(), String::new()))
        );
        assert_eq!(parse_meta_line(br#"{"type":123}"#), None);
        assert_eq!(parse_meta_line(br#"{"type":"session_meta","payload":{"id":5}}"#), None);
        assert_eq!(parse_meta_line(br#"{"type":"session_meta","payload":[]}"#), None);
        assert_eq!(parse_meta_line(b"[1,2]"), None);
        assert_eq!(parse_meta_line(b""), None);
        assert_eq!(parse_meta_line(b"{} trailing"), None);
    }
}

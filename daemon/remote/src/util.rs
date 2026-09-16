//! Small helpers shared across modules.

use std::fmt::Write as _;
use std::io;

/// Go-style `%q` quoting for error messages.
#[must_use]
pub fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Strip Rust's ` (os error N)` suffix and lower-case the first letter so
/// errors read like Go's strerror strings.
#[must_use]
pub fn io_error_text(err: &io::Error) -> String {
    let text = err.to_string();
    let text = match text.find(" (os error ") {
        Some(idx) => &text[..idx],
        None => text.as_str(),
    };
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// `<op> <path>: <error>` in the shape of Go's `*os.PathError`.
#[must_use]
pub fn path_error(op: &str, path: &str, err: &io::Error) -> String {
    format!("{op} {path}: {}", io_error_text(err))
}

/// First non-empty string.
#[must_use]
pub fn first_non_empty<'a>(values: &[&'a str]) -> &'a str {
    values.iter().copied().find(|v| !v.is_empty()).unwrap_or("")
}

/// Cryptographically random bytes.
pub fn random_bytes(n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    getrandom::fill(&mut buf).map_err(|e| io::Error::other(e.to_string()))?;
    Ok(buf)
}

/// Random lowercase hex string of `n` bytes.
pub fn random_hex(n: usize) -> String {
    random_bytes(n).map(hex::encode).unwrap_or_default()
}

/// `os.UserHomeDir` equivalent.
#[must_use]
pub fn user_home_dir() -> Option<String> {
    std::env::var("HOME").ok().filter(|h| !h.is_empty())
}

/// `os.TempDir` equivalent.
#[must_use]
pub fn temp_dir() -> String {
    std::env::temp_dir().to_string_lossy().into_owned()
}

/// Effective uid.
#[must_use]
pub fn getuid() -> u32 {
    rustix::process::getuid().as_raw()
}

/// Go's `fmt.Sscanf(s, "%d", &n)` for the tmux shim: leading whitespace,
/// optional sign, decimal digits; anything else yields 0.
#[must_use]
pub fn parse_int_lenient(s: &str) -> i64 {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    let value = digits.parse::<i64>().unwrap_or(i64::MAX);
    if negative { -value } else { value }
}

/// Go's `fmt.Sscanf(s, "%f", &f)`: leading whitespace, optional sign,
/// digits with optional fraction and exponent; anything else yields 0.
#[must_use]
pub fn parse_float_lenient(s: &str) -> f64 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut end = 0;
    if end < bytes.len() && (bytes[end] == b'-' || bytes[end] == b'+') {
        end += 1;
    }
    let digits_start = end;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    let mut saw_digits = end > digits_start;
    if end < bytes.len() && bytes[end] == b'.' {
        let frac_start = end + 1;
        let mut cursor = frac_start;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor > frac_start || saw_digits {
            saw_digits = saw_digits || cursor > frac_start;
            end = cursor;
        }
    }
    if !saw_digits {
        return 0.0;
    }
    if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
        let mut cursor = end + 1;
        if cursor < bytes.len() && (bytes[cursor] == b'-' || bytes[cursor] == b'+') {
            cursor += 1;
        }
        let exp_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor > exp_start {
            end = cursor;
        }
    }
    s[..end].parse::<f64>().unwrap_or(0.0)
}

/// FNV-1a 64-bit hash (Go's `hash/fnv` `New64a`).
#[must_use]
pub fn fnv1a_64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Go's `time.Now().UTC().Format(time.RFC3339Nano)`: fractional seconds
/// without trailing zeros and a literal `Z`.
#[must_use]
pub fn rfc3339_nano_utc(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = dt.timestamp_subsec_nanos();
    if nanos == 0 {
        return format!("{base}Z");
    }
    let frac = format!("{nanos:09}");
    let frac = frac.trim_end_matches('0');
    format!("{base}.{frac}Z")
}

/// Go's `time.Duration.String()` for the durations that appear in messages.
#[must_use]
pub fn go_duration(d: std::time::Duration) -> String {
    let nanos = d.as_nanos();
    if nanos == 0 {
        return "0s".to_string();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format!("{}µs", trim_frac(nanos, 1_000));
    }
    if nanos < 1_000_000_000 {
        return format!("{}ms", trim_frac(nanos, 1_000_000));
    }
    let total_secs = nanos / 1_000_000_000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs_nanos = nanos - (hours * 3600 + mins * 60) * 1_000_000_000;
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if hours > 0 || mins > 0 {
        out.push_str(&format!("{mins}m"));
    }
    out.push_str(&format!("{}s", trim_frac(secs_nanos, 1_000_000_000)));
    out
}

fn trim_frac(value: u128, unit: u128) -> String {
    let whole = value / unit;
    let frac = value % unit;
    if frac == 0 {
        return whole.to_string();
    }
    let width = unit.to_string().len() - 1;
    let frac = format!("{frac:0width$}");
    format!("{whole}.{}", frac.trim_end_matches('0'))
}

/// Create a uniquely named file in `dir` with mode 0600, like `os.CreateTemp`
/// with pattern `<prefix>*<suffix>`.
pub fn create_temp_file(
    dir: &std::path::Path,
    prefix: &str,
    suffix: &str,
) -> io::Result<(std::fs::File, std::path::PathBuf)> {
    use std::os::unix::fs::OpenOptionsExt;
    for _ in 0..10_000 {
        let path = dir.join(format!("{prefix}{}{suffix}", random_hex(8)));
        match std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    path_error("open", &path.to_string_lossy(), &e),
                ));
            }
        }
    }
    Err(io::Error::other(format!(
        "open {}: file exists",
        dir.join(format!("{prefix}*{suffix}")).display()
    )))
}

/// Metadata without following symlinks, with Go's `lstat <path>: <err>` text.
pub fn lstat(path: &std::path::Path) -> io::Result<std::fs::Metadata> {
    std::fs::symlink_metadata(path)
        .map_err(|e| io::Error::new(e.kind(), path_error("lstat", &path.to_string_lossy(), &e)))
}

/// Whether `metadata` belongs to the current user.
#[must_use]
pub fn owned_by_current_user(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.uid() == getuid()
}

/// Re-encode a JSON value the way Go's `encoding/json` would after a
/// `map[string]any` round trip: sorted keys, integral floats without a
/// fraction, and HTML-sensitive characters escaped.
#[must_use]
pub fn go_json(value: &serde_json::Value) -> String {
    let normalized = normalize_go_numbers(value.clone());
    go_escape(&serde_json::to_string(&normalized).unwrap_or_default())
}

/// `json.MarshalIndent(value, "", "  ")`.
#[must_use]
pub fn go_json_indent(value: &serde_json::Value) -> String {
    let normalized = normalize_go_numbers(value.clone());
    go_escape(&serde_json::to_string_pretty(&normalized).unwrap_or_default())
}

/// Go decodes every JSON number into `float64`; integral values re-encode
/// without a fractional part.
#[must_use]
pub fn normalize_go_numbers(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => {
            if n.is_f64()
                && let Some(f) = n.as_f64()
                && f.fract() == 0.0
                && f.abs() < 9_007_199_254_740_992.0
            {
                #[allow(clippy::cast_possible_truncation)]
                return Value::from(f as i64);
            }
            Value::Number(n)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(normalize_go_numbers).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, normalize_go_numbers(v))).collect())
        }
        other => other,
    }
}

/// Apply Go's default JSON escaping (`<`, `>`, `&`, U+2028, U+2029, and the
/// long forms of `\b` and `\f`) to serialized JSON text.
#[must_use]
pub fn go_escape(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let mut chars = json.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('b') => out.push_str("\\u0008"),
                Some('f') => out.push_str("\\u000c"),
                Some(next) => {
                    out.push('\\');
                    out.push(next);
                }
                None => out.push('\\'),
            },
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(c),
        }
    }
    out
}

/// Port of Go's `filepath.Clean` for Unix paths.
#[must_use]
pub fn clean_path(path: &str) -> String {
    let bytes = path.as_bytes();
    if bytes.is_empty() {
        return ".".to_string();
    }
    let rooted = bytes[0] == b'/';
    let n = bytes.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        out.push(b'/');
        r = 1;
        dotdot = 1;
    }
    while r < n {
        if bytes[r] == b'/' || (bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/')) {
            // Empty or `.` element.
            r += 1;
        } else if bytes[r] == b'.' && bytes[r + 1] == b'.' && (r + 2 == n || bytes[r + 2] == b'/') {
            r += 2;
            if out.len() > dotdot {
                let mut removed = out.pop();
                while out.len() > dotdot && removed != Some(b'/') {
                    removed = out.pop();
                }
            } else if !rooted {
                if !out.is_empty() {
                    out.push(b'/');
                }
                out.extend_from_slice(b"..");
                dotdot = out.len();
            }
        } else {
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push(b'/');
            }
            while r < n && bytes[r] != b'/' {
                out.push(bytes[r]);
                r += 1;
            }
        }
    }
    if out.is_empty() {
        out.push(b'.');
    }
    String::from_utf8(out).expect("clean preserves UTF-8")
}

/// A pipe whose ends are close-on-exec (`pipe2(O_CLOEXEC)` on Linux; `pipe`
/// plus `FD_CLOEXEC` elsewhere). Returns `(read, write)`.
pub fn cloexec_pipe() -> io::Result<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    #[cfg(target_os = "linux")]
    {
        Ok(rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let (read, write) = rustix::pipe::pipe()?;
        rustix::io::fcntl_setfd(&read, rustix::io::FdFlags::CLOEXEC)?;
        rustix::io::fcntl_setfd(&write, rustix::io::FdFlags::CLOEXEC)?;
        Ok((read, write))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenient_int_parsing_matches_sscanf() {
        assert_eq!(parse_int_lenient("10%"), 10);
        assert_eq!(parse_int_lenient(" -3"), -3);
        assert_eq!(parse_int_lenient("+4x"), 4);
        assert_eq!(parse_int_lenient("abc"), 0);
        assert_eq!(parse_int_lenient(""), 0);
    }

    #[test]
    fn lenient_float_parsing_matches_sscanf() {
        assert!((parse_float_lenient("2.5s") - 2.5).abs() < f64::EPSILON);
        assert!((parse_float_lenient("30") - 30.0).abs() < f64::EPSILON);
        assert!((parse_float_lenient("1e2") - 100.0).abs() < f64::EPSILON);
        assert!((parse_float_lenient(".5") - 0.5).abs() < f64::EPSILON);
        assert_eq!(parse_float_lenient("x"), 0.0);
    }

    #[test]
    fn fnv_matches_go_reference_values() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x85944171f73967e8);
    }
}

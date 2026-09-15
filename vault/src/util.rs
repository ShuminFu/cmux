//! Small formatting helpers shared across modules.

use std::fmt::Write as _;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Local, Timelike};

/// Format an I/O error the way Go's `*os.PathError` renders it:
/// `<op> <path>: <lowercase strerror>`.
#[must_use]
pub fn path_error(op: &str, path: &str, err: &io::Error) -> String {
    format!("{op} {path}: {}", io_error_text(err))
}

/// Strip Rust's ` (os error N)` suffix and lower-case the leading letter so
/// the text matches the libc strerror strings Go prints.
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

/// Go-style `%q` quoting: double quotes with backslash escapes for the
/// characters that matter in error messages.
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

/// Nanoseconds since the Unix epoch, mirroring Go's `time.Time.UnixNano`.
#[must_use]
pub fn unix_nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

/// Go `time.RFC3339` formatting in the local time zone.
#[must_use]
pub fn rfc3339_local(t: SystemTime) -> String {
    format_local(t, false)
}

/// Go `time.RFC3339Nano` formatting in the local time zone (the encoding
/// `encoding/json` uses for `time.Time`).
#[must_use]
pub fn rfc3339_nano_local(t: SystemTime) -> String {
    format_local(t, true)
}

fn format_local(t: SystemTime, with_fraction: bool) -> String {
    let dt: DateTime<Local> = DateTime::from(t);
    let mut out = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    if with_fraction {
        let nanos = dt.nanosecond() % 1_000_000_000;
        if nanos != 0 {
            let digits = format!("{nanos:09}");
            out.push('.');
            out.push_str(digits.trim_end_matches('0'));
        }
    }
    let offset = dt.offset().local_minus_utc();
    if offset == 0 {
        out.push('Z');
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let abs = offset.abs();
        let _ = write!(out, "{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60);
    }
    out
}

pub mod serde_rfc3339_nano {
    use std::time::SystemTime;

    use serde::Serializer;

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::rfc3339_nano_local(*t))
    }
}

/// Deserialize a field that may be JSON `null`, treating it as the type's
/// default (the tolerance `encoding/json` shows for Go zero values).
pub fn nullable<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    Ok(<Option<T> as serde::Deserialize>::deserialize(deserializer)?.unwrap_or_default())
}

/// Create every missing directory in `path` with mode `0700` (mirrors
/// `os.MkdirAll(path, 0o700)`).
pub fn mkdir_all_private(path: &str) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

/// Restrict a file to owner read/write (mirrors `chmod 0600`).
pub fn chmod_private(file: &std::fs::File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok(())
    }
}

/// Write `data` to `path` atomically: temp file in the same directory with
/// mode `0600`, then rename over the destination.
pub fn write_file_atomic(
    path: &str,
    data: &[u8],
    tmp_prefix: &str,
    tmp_suffix: &str,
) -> crate::Result<()> {
    use std::io::Write;

    let dir = crate::gopath::dir(path);
    mkdir_all_private(&dir).map_err(|e| path_error("mkdir", &dir, &e))?;
    let mut tmp = tempfile::Builder::new()
        .prefix(tmp_prefix)
        .suffix(tmp_suffix)
        .tempfile_in(&dir)
        .map_err(|e| path_error("open", &dir, &e))?;
    let tmp_path = tmp.path().to_string_lossy().into_owned();
    tmp.write_all(data).map_err(|e| path_error("write", &tmp_path, &e))?;
    chmod_private(tmp.as_file()).map_err(|e| path_error("chmod", &tmp_path, &e))?;
    tmp.persist(path).map_err(|e| path_error("rename", path, &e.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn io_error_text_matches_go_strerror_style() {
        let err = io::Error::from_raw_os_error(13);
        assert_eq!(io_error_text(&err), "permission denied");
        let err = io::Error::other("Custom Message");
        assert_eq!(io_error_text(&err), "custom Message");
        assert_eq!(
            path_error("open", "/x", &io::Error::from_raw_os_error(2)),
            "open /x: no such file or directory"
        );
    }

    #[test]
    fn quote_escapes_like_go() {
        assert_eq!(quote("plain"), "\"plain\"");
        assert_eq!(quote("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
    }

    #[test]
    fn unix_nanos_round_trips_epoch_offsets() {
        assert_eq!(unix_nanos(UNIX_EPOCH), 0);
        assert_eq!(unix_nanos(UNIX_EPOCH + Duration::new(1, 5)), 1_000_000_005);
        assert_eq!(unix_nanos(UNIX_EPOCH - Duration::new(1, 0)), -1_000_000_000);
    }

    #[test]
    fn rfc3339_formats_trim_fraction_like_go() {
        // The test process may run in any zone; only assert structure that is
        // zone independent.
        let t = UNIX_EPOCH + Duration::new(1_700_000_000, 120_000_000);
        let nano = rfc3339_nano_local(t);
        assert!(nano.contains(".12"), "{nano}");
        assert!(!nano.contains(".120"), "{nano}");
        let plain = rfc3339_local(t);
        assert!(!plain.contains('.'), "{plain}");
        let whole = rfc3339_nano_local(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        assert!(!whole.contains('.'), "{whole}");
        assert!(plain.ends_with('Z') || plain.as_bytes()[plain.len() - 3] == b':', "{plain}");
    }
}

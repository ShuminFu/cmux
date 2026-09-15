//! A tiny command-line flag parser with the semantics of Go's `flag`
//! package, so `-json`, `--json`, `--agent codex`, `--agent=codex`, and
//! `--limit=25` all keep working exactly as they did.

use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Bool(bool),
    Str(String),
    Int(i64),
}

#[derive(Debug, Clone)]
struct Spec {
    name: &'static str,
    usage: &'static str,
    default: Value,
    value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// `-h` / `-help` was given: usage has been rendered into `usage`.
    Help { usage: String },
    /// Any other failure: the message followed by the usage text, as Go's
    /// `flag.ContinueOnError` prints them.
    Failure { message: String, usage: String },
}

impl ParseError {
    /// Text to write to stderr, matching Go's `FlagSet` output.
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::Help { usage } => usage.clone(),
            Self::Failure { message, usage } => format!("{message}\n{usage}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlagSet {
    name: &'static str,
    specs: Vec<Spec>,
}

impl FlagSet {
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self { name, specs: Vec::new() }
    }

    pub fn bool(&mut self, name: &'static str, default: bool, usage: &'static str) {
        self.specs.push(Spec {
            name,
            usage,
            default: Value::Bool(default),
            value: Value::Bool(default),
        });
    }

    pub fn string(&mut self, name: &'static str, default: &str, usage: &'static str) {
        self.specs.push(Spec {
            name,
            usage,
            default: Value::Str(default.to_string()),
            value: Value::Str(default.to_string()),
        });
    }

    pub fn int(&mut self, name: &'static str, default: i64, usage: &'static str) {
        self.specs.push(Spec {
            name,
            usage,
            default: Value::Int(default),
            value: Value::Int(default),
        });
    }

    #[must_use]
    pub fn get_bool(&self, name: &str) -> bool {
        match self.lookup(name) {
            Some(Spec { value: Value::Bool(v), .. }) => *v,
            _ => false,
        }
    }

    #[must_use]
    pub fn get_string(&self, name: &str) -> String {
        match self.lookup(name) {
            Some(Spec { value: Value::Str(v), .. }) => v.clone(),
            _ => String::new(),
        }
    }

    #[must_use]
    pub fn get_int(&self, name: &str) -> i64 {
        match self.lookup(name) {
            Some(Spec { value: Value::Int(v), .. }) => *v,
            _ => 0,
        }
    }

    fn lookup(&self, name: &str) -> Option<&Spec> {
        self.specs.iter().find(|s| s.name == name)
    }

    /// Parse `args`, returning the positional arguments that follow the flags.
    pub fn parse(&mut self, args: &[String]) -> Result<Vec<String>, ParseError> {
        let mut rest: &[String] = args;
        while let Some(s) = rest.first() {
            if s.len() < 2 || !s.starts_with('-') {
                break;
            }
            let mut num_minuses = 1;
            if s.as_bytes()[1] == b'-' {
                num_minuses += 1;
                if s.len() == 2 {
                    // "--" terminates the flags.
                    rest = &rest[1..];
                    break;
                }
            }
            let name = &s[num_minuses..];
            if name.is_empty() || name.starts_with('-') || name.starts_with('=') {
                return Err(self.failure(format!("bad flag syntax: {s}")));
            }
            rest = &rest[1..];
            let (name, mut value) = match name.split_once('=') {
                Some((n, v)) => (n.to_string(), Some(v.to_string())),
                None => (name.to_string(), None),
            };
            let Some(idx) = self.specs.iter().position(|spec| spec.name == name) else {
                if name == "help" || name == "h" {
                    return Err(ParseError::Help { usage: self.usage() });
                }
                return Err(self.failure(format!("flag provided but not defined: -{name}")));
            };
            match &self.specs[idx].value {
                Value::Bool(_) => {
                    let parsed = match &value {
                        Some(v) => match parse_bool(v) {
                            Some(b) => b,
                            None => {
                                return Err(self.failure(format!(
                                    "invalid boolean value {} for -{name}: parse error",
                                    crate::util::quote(v)
                                )));
                            }
                        },
                        None => true,
                    };
                    self.specs[idx].value = Value::Bool(parsed);
                }
                Value::Str(_) | Value::Int(_) => {
                    if value.is_none()
                        && let Some(next) = rest.first()
                    {
                        value = Some(next.clone());
                        rest = &rest[1..];
                    }
                    let Some(v) = value else {
                        return Err(self.failure(format!("flag needs an argument: -{name}")));
                    };
                    match &self.specs[idx].value {
                        Value::Str(_) => self.specs[idx].value = Value::Str(v),
                        _ => match parse_int(&v) {
                            Some(n) => self.specs[idx].value = Value::Int(n),
                            None => {
                                return Err(self.failure(format!(
                                    "invalid value {} for flag -{name}: parse error",
                                    crate::util::quote(&v)
                                )));
                            }
                        },
                    }
                }
            }
        }
        Ok(rest.to_vec())
    }

    fn failure(&self, message: String) -> ParseError {
        ParseError::Failure { message, usage: self.usage() }
    }

    /// Render the flag list the way Go's `FlagSet.PrintDefaults` does.
    #[must_use]
    pub fn usage(&self) -> String {
        let mut out = format!("Usage of {}:\n", self.name);
        let mut specs: Vec<&Spec> = self.specs.iter().collect();
        specs.sort_by(|a, b| a.name.cmp(b.name));
        for spec in specs {
            let _ = write!(out, "  -{}", spec.name);
            match &spec.default {
                Value::Bool(_) => {}
                Value::Str(_) => out.push_str(" string"),
                Value::Int(_) => out.push_str(" int"),
            }
            out.push_str("\n    \t");
            out.push_str(spec.usage);
            match &spec.default {
                Value::Bool(true) => out.push_str(" (default true)"),
                Value::Str(v) if !v.is_empty() => {
                    let _ = write!(out, " (default {})", crate::util::quote(v));
                }
                Value::Int(v) if *v != 0 => {
                    let _ = write!(out, " (default {v})");
                }
                _ => {}
            }
            out.push('\n');
        }
        out
    }
}

/// `strconv.ParseBool`.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `strconv.ParseInt(value, 0, 64)`: optional sign, then decimal or a
/// `0x`/`0o`/`0b`/leading-`0` prefixed literal, with `_` digit separators
/// allowed only in prefixed forms.
fn parse_int(value: &str) -> Option<i64> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    if digits.is_empty() {
        return None;
    }
    let lower = digits.to_ascii_lowercase();
    let (radix, body, allow_underscore) = if let Some(rest) = lower.strip_prefix("0x") {
        (16, rest.to_string(), true)
    } else if let Some(rest) = lower.strip_prefix("0b") {
        (2, rest.to_string(), true)
    } else if let Some(rest) = lower.strip_prefix("0o") {
        (8, rest.to_string(), true)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, lower[1..].to_string(), true)
    } else {
        (10, lower, false)
    };
    let cleaned = if allow_underscore {
        if body.starts_with('_') || body.ends_with('_') || body.contains("__") {
            return None;
        }
        body.replace('_', "")
    } else {
        body
    };
    if cleaned.is_empty() {
        return None;
    }
    let magnitude = u64::from_str_radix(&cleaned, radix).ok()?;
    if negative {
        if magnitude > i64::MAX as u64 + 1 {
            return None;
        }
        Some(magnitude.cast_signed().wrapping_neg())
    } else {
        i64::try_from(magnitude).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_string()).collect()
    }

    fn sync_flags() -> FlagSet {
        let mut fs = FlagSet::new("sync");
        fs.string("agent", "", "agent to sync (claude, codex, pi)");
        fs.bool("dry-run", false, "scan and diff without uploading");
        fs.int("limit", 0, "maximum changed sessions to upload");
        fs.bool("json", false, "write JSON output");
        fs
    }

    #[test]
    fn accepts_go_flag_spellings() {
        let mut fs = sync_flags();
        let rest = fs
            .parse(&args(&[
                "--agent",
                "codex",
                "-dry-run",
                "--limit=25",
                "-json=true",
                "pos",
                "-x",
            ]))
            .unwrap();
        assert_eq!(fs.get_string("agent"), "codex");
        assert!(fs.get_bool("dry-run"));
        assert_eq!(fs.get_int("limit"), 25);
        assert!(fs.get_bool("json"));
        assert_eq!(rest, args(&["pos", "-x"]));

        let mut fs = sync_flags();
        let rest = fs.parse(&args(&["--json=false", "--", "--agent", "codex"])).unwrap();
        assert!(!fs.get_bool("json"));
        assert_eq!(rest, args(&["--agent", "codex"]));

        let mut fs = sync_flags();
        let rest = fs.parse(&args(&["-", "--json"])).unwrap();
        assert_eq!(rest, args(&["-", "--json"]));

        let mut fs = sync_flags();
        fs.parse(&args(&["--limit", "-3", "--limit=0x10"])).unwrap();
        assert_eq!(fs.get_int("limit"), 16);
    }

    #[test]
    fn reports_errors_like_go() {
        let mut fs = sync_flags();
        let err = fs.parse(&args(&["--nope"])).unwrap_err();
        assert!(
            matches!(&err, ParseError::Failure { message, .. } if message == "flag provided but not defined: -nope")
        );
        assert!(err.render().starts_with("flag provided but not defined: -nope\nUsage of sync:\n  -agent string\n    \tagent to sync"));

        let mut fs = sync_flags();
        assert!(
            matches!(fs.parse(&args(&["--agent"])).unwrap_err(), ParseError::Failure { message, .. } if message == "flag needs an argument: -agent")
        );
        let mut fs = sync_flags();
        assert!(
            matches!(fs.parse(&args(&["--limit", "abc"])).unwrap_err(), ParseError::Failure { message, .. } if message == "invalid value \"abc\" for flag -limit: parse error")
        );
        let mut fs = sync_flags();
        assert!(
            matches!(fs.parse(&args(&["--json=maybe"])).unwrap_err(), ParseError::Failure { message, .. } if message == "invalid boolean value \"maybe\" for -json: parse error")
        );
        let mut fs = sync_flags();
        assert!(
            matches!(fs.parse(&args(&["---x"])).unwrap_err(), ParseError::Failure { message, .. } if message == "bad flag syntax: ---x")
        );
        let mut fs = sync_flags();
        assert!(matches!(fs.parse(&args(&["-h"])).unwrap_err(), ParseError::Help { .. }));
        let mut fs = sync_flags();
        assert!(matches!(fs.parse(&args(&["--help"])).unwrap_err(), ParseError::Help { .. }));
    }

    #[test]
    fn usage_matches_print_defaults() {
        let mut fs = FlagSet::new("cmux-vault");
        fs.string("api-base", "https://cmux.com", "cmux web API base URL");
        fs.bool("json", false, "write JSON output where supported");
        assert_eq!(
            fs.usage(),
            "Usage of cmux-vault:\n  -api-base string\n    \tcmux web API base URL (default \"https://cmux.com\")\n  -json\n    \twrite JSON output where supported\n"
        );
        let mut fs = FlagSet::new("scan");
        fs.bool("json", true, "write JSON output");
        fs.int("limit", 5, "n");
        assert_eq!(
            fs.usage(),
            "Usage of scan:\n  -json\n    \twrite JSON output (default true)\n  -limit int\n    \tn (default 5)\n"
        );
    }

    #[test]
    fn go_int_parsing() {
        assert_eq!(parse_int("10"), Some(10));
        assert_eq!(parse_int("-10"), Some(-10));
        assert_eq!(parse_int("+7"), Some(7));
        assert_eq!(parse_int("0x1f"), Some(31));
        assert_eq!(parse_int("0o17"), Some(15));
        assert_eq!(parse_int("017"), Some(15));
        assert_eq!(parse_int("0b101"), Some(5));
        assert_eq!(parse_int("1_000"), None);
        assert_eq!(parse_int("0x1_0"), Some(16));
        assert_eq!(parse_int(""), None);
        assert_eq!(parse_int("1.5"), None);
        assert_eq!(parse_int("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int("9223372036854775808"), None);
    }
}

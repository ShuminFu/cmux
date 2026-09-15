use std::sync::LazyLock;

use regex::Regex;

use super::{
    Agent, Session, SessionRef, UUID_PATTERN, clean_restore_path, cwd_from_munged,
    discover_sessions, path_under_home, recover_cwd_from_jsonl, shell_quote,
};
use crate::Result;
use crate::environ::Environ;
use crate::gopath;

pub struct Pi;

static PI_FILE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"(?i)^.+_({UUID_PATTERN})\.jsonl$")).expect("valid regex")
});

fn pi_root(env: &Environ) -> Result<String> {
    path_under_home(env, &[".pi", "agent", "sessions"])
}

impl Agent for Pi {
    fn name(&self) -> &'static str {
        "pi"
    }

    fn discover(&self, env: &Environ) -> Result<Vec<Session>> {
        let root = pi_root(env)?;
        Ok(discover_sessions(env, self.name(), &root, &root, &|name, path| {
            let caps = PI_FILE_RE.captures(name)?;
            let mut cwd = recover_cwd_from_jsonl(path);
            if cwd.is_empty() {
                cwd = cwd_from_munged(&gopath::base(&gopath::dir(path)));
            }
            Some((caps[1].to_lowercase(), cwd))
        }))
    }

    fn restore_path(&self, env: &Environ, s: &SessionRef) -> Result<String> {
        let root = pi_root(env)?;
        clean_restore_path(&root, &s.rel_path)
    }

    fn resume_hint(&self, s: &SessionRef) -> String {
        // CWD can be a lossy munged-directory fallback; only emit a cd for a
        // real absolute path.
        if !gopath::is_abs(s.cwd.trim()) {
            return format!("open pi and resume session {}", s.agent_session_id);
        }
        format!("cd {} && open pi to resume session {}", shell_quote(&s.cwd), s.agent_session_id)
    }
}

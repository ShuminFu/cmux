use super::{
    Agent, Session, SessionRef, UUID_RE, clean_restore_path, cwd_from_munged, discover_sessions,
    path_under_home, recover_cwd_from_jsonl, shell_quote,
};
use crate::Result;
use crate::environ::Environ;
use crate::gopath;

pub struct Claude;

fn claude_root(env: &Environ) -> Result<String> {
    let root = env.get("CLAUDE_CONFIG_DIR");
    if !root.is_empty() {
        return Ok(root);
    }
    path_under_home(env, &[".claude"])
}

impl Agent for Claude {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn discover(&self, env: &Environ) -> Result<Vec<Session>> {
        let root = claude_root(env)?;
        let projects_root = gopath::join(&[&root, "projects"]);
        Ok(discover_sessions(env, self.name(), &root, &projects_root, &|name, path| {
            if gopath::ext(name) != ".jsonl" {
                return None;
            }
            let id = name.strip_suffix(".jsonl").unwrap_or(name);
            if !UUID_RE.is_match(id) {
                return None;
            }
            let project_name = gopath::base(&gopath::dir(path));
            let mut cwd = recover_cwd_from_jsonl(path);
            if cwd.is_empty() {
                cwd = cwd_from_munged(&project_name);
            }
            Some((id.to_string(), cwd))
        }))
    }

    fn restore_path(&self, env: &Environ, s: &SessionRef) -> Result<String> {
        let root = claude_root(env)?;
        clean_restore_path(&root, &s.rel_path)
    }

    fn resume_hint(&self, s: &SessionRef) -> String {
        // CWD can be a lossy munged-directory fallback; only emit a cd for a
        // real absolute path.
        if !gopath::is_abs(s.cwd.trim()) {
            return format!("claude --resume {}", s.agent_session_id);
        }
        format!("cd {} && claude --resume {}", shell_quote(&s.cwd), s.agent_session_id)
    }
}

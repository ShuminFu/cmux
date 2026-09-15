//! Restore a missing transcript from cmux Vault and print the command the
//! agent expects for resuming it.

use std::io::Read;

use crate::agentdirs::{self, Session, UUID_RE};
use crate::api::Client;
use crate::environ::Environ;
use crate::gopath;
use crate::util::{chmod_private, mkdir_all_private, path_error, quote};
use crate::{Error, Printer, Result};

/// Lowercase UUID-shaped ids so a session id copied with uppercase hex (e.g.
/// from a dashboard) still matches the lowercase UUIDs agents use in transcript
/// filenames. Non-UUID ids are left untouched.
#[must_use]
pub fn normalize_session_id(id: &str) -> String {
    if UUID_RE.is_match(id) { id.to_lowercase() } else { id.to_string() }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Options<'a> {
    pub agent: &'a str,
    pub force: bool,
}

pub struct Restorer<'a> {
    pub env: &'a Environ,
    pub client: &'a Client,
    pub out: Option<&'a dyn Printer>,
}

impl Restorer<'_> {
    pub fn resume(&self, session_id: &str, opts: Options<'_>) -> Result<String> {
        let session_id = normalize_session_id(session_id.trim());
        if session_id.is_empty() {
            return Err("session id is required".into());
        }
        let local = self.find_local(&session_id, opts.agent)?;
        // --force means "replace whatever is on disk from the vault", so skip
        // the local fast path and let the cloud restore overwrite it.
        if let Some(local) = &local
            && !opts.force
        {
            let agent = agentdirs::by_name(&local.agent_name).ok_or_else(|| {
                Error::from(format!("unknown agent {}", quote(&local.agent_name)))
            })?;
            let hint = agent.resume_hint(&local.as_ref());
            self.print(&format!("{hint}\n"));
            return Ok(hint);
        }

        let cloud_session = self.client.find_session(opts.agent, &session_id)?;
        let Some(cloud_session) = cloud_session else {
            if local.is_some() {
                return Err(format!(
                    "session {session_id} exists locally but was not found in cmux vault; rerun without --force to use the local transcript"
                )
                .into());
            }
            return Err(format!("session {session_id} not found locally or in cmux vault").into());
        };
        let detail = self.client.get_session(&cloud_session.id)?;
        if detail.session.download_url.is_empty() {
            return Err("server did not return a download URL".into());
        }
        let Some(agent) = agentdirs::by_name(&detail.session.agent) else {
            return Err(
                format!("unknown agent {} from server", quote(&detail.session.agent)).into()
            );
        };
        let reference = agentdirs::SessionRef {
            agent_name: detail.session.agent.clone(),
            agent_session_id: detail.session.agent_session_id.clone(),
            rel_path: detail.session.rel_path.clone(),
            cwd: detail.session.cwd.clone(),
        };
        let restore_path = agent.restore_path(self.env, &reference)?;
        match std::fs::metadata(&restore_path) {
            Ok(_) if !opts.force => {
                return Err(
                    format!("{restore_path} already exists; pass --force to overwrite").into()
                );
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(path_error("stat", &restore_path, &e).into()),
        }
        let parent = gopath::dir(&restore_path);
        mkdir_all_private(&parent).map_err(|e| path_error("mkdir", &parent, &e))?;
        let reader = self.client.download(&detail.session.download_url)?;
        decompress_to_path(reader, &restore_path, opts.force)?;
        let hint = agent.resume_hint(&reference);
        self.print(&format!("restored {restore_path}\n{hint}\n"));
        Ok(hint)
    }

    fn find_local(&self, session_id: &str, agent_filter: &str) -> Result<Option<Session>> {
        let sessions = agentdirs::discover_all(self.env, agent_filter)?;
        let mut found: Option<Session> = None;
        for session in sessions {
            if normalize_session_id(&session.agent_session_id) != session_id {
                continue;
            }
            if found.is_some() {
                return Err(format!(
                    "session id {session_id} exists for multiple agents; pass --agent"
                )
                .into());
            }
            found = Some(session);
        }
        Ok(found)
    }

    fn print(&self, text: &str) {
        if let Some(out) = self.out {
            out.print(text);
        }
    }
}

fn decompress_to_path(
    reader: Box<dyn Read + Send + Sync>,
    target: &str,
    force: bool,
) -> Result<()> {
    let dir = gopath::dir(target);
    let mut tmp = tempfile::Builder::new()
        .prefix(".restore-")
        .suffix(".jsonl")
        .tempfile_in(&dir)
        .map_err(|e| path_error("open", &dir, &e))?;
    let tmp_path = tmp.path().to_string_lossy().into_owned();
    let mut decoder =
        zstd::stream::read::Decoder::new(reader).map_err(|e| Error::from(e.to_string()))?;
    std::io::copy(&mut decoder, tmp.as_file_mut()).map_err(|e| Error::from(e.to_string()))?;
    chmod_private(tmp.as_file()).map_err(|e| path_error("chmod", &tmp_path, &e))?;
    if force {
        tmp.persist(target).map_err(|e| path_error("rename", target, &e.error))?;
        return Ok(());
    }
    // hard_link fails if target exists, closing the window between the earlier
    // metadata pre-check and this write (a file created during the download
    // must not be silently clobbered without --force). Dropping `tmp` removes
    // the temp path, leaving target as the only link.
    match std::fs::hard_link(&tmp_path, target) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(format!("{target} already exists; pass --force to overwrite").into())
        }
        Err(e) => Err(path_error("link", target, &e).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_only_uuid_shaped_ids() {
        assert_eq!(
            normalize_session_id("019D60BC-B684-7A01-B4AC-52FEFFC5FCB5"),
            "019d60bc-b684-7a01-b4ac-52feffc5fcb5"
        );
        assert_eq!(normalize_session_id("Custom-ID"), "Custom-ID");
    }
}

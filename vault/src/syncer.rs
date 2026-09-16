//! Incremental upload engine: discover sessions, skip unchanged files by size
//! and mtime, hash the rest, zstd-compress changed transcripts, and upload
//! them through presigned URLs in batches.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::agentdirs::{self, Session};
use crate::api::{Client, CommitResult, UploadItem, UploadResult};
use crate::environ::Environ;
use crate::state::{self, Store};
use crate::util::{mkdir_all_private, path_error, unix_nanos};
use crate::{Error, Printer, Result};

pub const MAX_UPLOAD_BATCH: usize = 25;
const UPLOAD_WORKERS: usize = 4;

#[derive(Debug, Clone, Copy, Default)]
pub struct Options<'a> {
    pub agent: &'a str,
    pub dry_run: bool,
    pub limit: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub uploaded: i64,
    pub skipped: i64,
    pub failed: i64,
    #[serde(rename = "bytesUploaded")]
    pub bytes_uploaded: i64,
    #[serde(rename = "compressedBytesUploaded")]
    pub compressed_bytes_uploaded: i64,
}

pub struct Engine<'a> {
    pub env: &'a Environ,
    pub state: &'a mut Store,
    pub client: &'a Client,
    /// Directory for compressed temp files; empty means the system temp dir.
    pub temp_dir: String,
    pub out: Option<&'a dyn Printer>,
}

#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub(crate) session: Session,
    /// Plaintext digest sent to the server. After `prepare_batch` it is
    /// recomputed from the exact bytes that were compressed, so a transcript
    /// that changes between scan and upload can never commit a stale hash.
    pub(crate) sha256: String,
    pub(crate) plain_size: i64,
    pub(crate) compressed: String,
    pub(crate) compressed_size: i64,
}

impl Candidate {
    pub(crate) fn new(session: Session, sha256: String) -> Self {
        let plain_size = session.size_bytes;
        Self { session, sha256, plain_size, compressed: String::new(), compressed_size: 0 }
    }

    fn key(&self) -> String {
        state::key(&self.session.agent_name, &self.session.rel_path)
    }

    fn remove_compressed(&self) {
        if !self.compressed.is_empty() {
            let _ = std::fs::remove_file(&self.compressed);
        }
    }
}

fn count(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

impl Engine<'_> {
    /// Run one sync pass. The summary is returned even when the pass fails so
    /// the CLI can report partial progress.
    pub fn sync(&mut self, opts: Options<'_>) -> (Summary, Option<Error>) {
        let mut summary = Summary::default();
        let candidates = match self.scan_candidates(opts, &mut summary) {
            Ok(candidates) => candidates,
            Err(err) => return (summary, Some(err)),
        };

        if opts.dry_run {
            for c in &candidates {
                self.print(&format!(
                    "would upload {} {} ({} bytes)\n",
                    c.session.agent_name, c.session.rel_path, c.session.size_bytes
                ));
            }
            summary.skipped += count(candidates.len());
            // Dry run must not advance sync bookkeeping, so skip state.save even
            // though the scan loop may have reconciled entries in memory.
            return (summary, None);
        }

        for batch in candidates.chunks(MAX_UPLOAD_BATCH) {
            self.process_batch(batch, &mut summary);
        }

        if let Err(err) = self.state.save() {
            return (summary, Some(err));
        }
        if summary.failed > 0 {
            let failed = summary.failed;
            return (summary, Some(format!("{failed} upload(s) failed").into()));
        }
        (summary, None)
    }

    /// Discover sessions and decide which need uploading, reconciling local
    /// bookkeeping for files whose content is already known to the vault.
    fn scan_candidates(
        &mut self,
        opts: Options<'_>,
        summary: &mut Summary,
    ) -> Result<Vec<Candidate>> {
        let sessions = agentdirs::discover_all(self.env, opts.agent)?;
        let mut candidates: Vec<Candidate> = Vec::new();
        for session in sessions {
            let key = state::key(&session.agent_name, &session.rel_path);
            let entry = self.state.entries.get(&key).cloned().unwrap_or_default();
            let mtime = unix_nanos(session.mod_time);
            if entry.size_bytes == session.size_bytes && entry.mtime_unix_ns == mtime {
                summary.skipped += 1;
                self.print(&format!(
                    "skip unchanged {} {}\n",
                    session.agent_name, session.rel_path
                ));
                continue;
            }
            let hash = match sha256_file(&session.abs_path) {
                Ok(hash) => hash,
                Err(err) => {
                    summary.failed += 1;
                    self.print(&format!(
                        "fail hash {} {}: {err}\n",
                        session.agent_name, session.rel_path
                    ));
                    continue;
                }
            };
            if hash == entry.remote_sha256 {
                self.state.entries.insert(
                    key,
                    state::Entry {
                        size_bytes: session.size_bytes,
                        mtime_unix_ns: mtime,
                        sha256: hash,
                        remote_sha256: entry.remote_sha256,
                    },
                );
                summary.skipped += 1;
                self.print(&format!(
                    "skip already uploaded {} {}\n",
                    session.agent_name, session.rel_path
                ));
                continue;
            }
            candidates.push(Candidate::new(session, hash));
            if opts.limit > 0 && count(candidates.len()) >= opts.limit {
                break;
            }
        }
        Ok(candidates)
    }

    /// Compress, presign, upload, and commit one batch of at most
    /// `MAX_UPLOAD_BATCH` candidates, accounting every outcome in `summary`.
    fn process_batch(&mut self, batch: &[Candidate], summary: &mut Summary) {
        let prepared = match self.prepare_batch(batch) {
            Ok(prepared) => prepared,
            Err(err) => {
                summary.failed += count(batch.len());
                self.print(&format!("fail prepare batch: {err}\n"));
                return;
            }
        };
        let results = match self.client.request_uploads(&upload_items(&prepared)) {
            Ok(results) => results,
            Err(err) => {
                summary.failed += count(prepared.len());
                self.print(&format!("fail presign batch: {err}\n"));
                cleanup(&prepared);
                return;
            }
        };
        let result_by_key: HashMap<String, UploadResult> =
            results.items.into_iter().map(|r| (state::key(&r.agent, &r.rel_path), r)).collect();

        let mut to_upload = Vec::new();
        for c in prepared {
            let result = result_by_key.get(&c.key()).cloned().unwrap_or_default();
            match result.status.as_str() {
                "unchanged" => {
                    self.mark_uploaded(&c);
                    summary.skipped += 1;
                    self.print(&format!(
                        "skip cloud unchanged {} {}\n",
                        c.session.agent_name, c.session.rel_path
                    ));
                    c.remove_compressed();
                }
                "upload" => {
                    if result.put_url.is_empty() {
                        summary.failed += 1;
                        self.print(&format!(
                            "fail presign {} {}: missing putUrl\n",
                            c.session.agent_name, c.session.rel_path
                        ));
                        c.remove_compressed();
                        continue;
                    }
                    to_upload.push(c);
                }
                _ => {
                    summary.failed += 1;
                    let error = if result.error.is_empty() {
                        "unexpected_presign_status"
                    } else {
                        &result.error
                    };
                    self.print(&format!(
                        "fail presign {} {}: {error}\n",
                        c.session.agent_name, c.session.rel_path
                    ));
                    c.remove_compressed();
                }
            }
        }

        let (successes, failures) = self.upload_batch(to_upload, &result_by_key);
        summary.failed += failures;
        if successes.is_empty() {
            return;
        }
        let commit = match self.client.commit_sessions(&upload_items(&successes)) {
            Ok(commit) => commit,
            Err(err) => {
                summary.failed += count(successes.len());
                self.print(&format!("fail commit batch: {err}\n"));
                cleanup(&successes);
                return;
            }
        };
        let commit_by_key: HashMap<String, CommitResult> =
            commit.items.into_iter().map(|r| (state::key(&r.agent, &r.rel_path), r)).collect();
        for c in successes {
            let result = commit_by_key.get(&c.key()).cloned().unwrap_or_default();
            if result.status != "committed" && result.status != "unchanged" {
                summary.failed += 1;
                let error = if result.error.is_empty() { "commit_failed" } else { &result.error };
                self.print(&format!(
                    "fail commit {} {}: {error}\n",
                    c.session.agent_name, c.session.rel_path
                ));
                c.remove_compressed();
                continue;
            }
            self.mark_uploaded(&c);
            summary.uploaded += 1;
            summary.bytes_uploaded += c.session.size_bytes;
            summary.compressed_bytes_uploaded += c.compressed_size;
            self.print(&format!(
                "uploaded {} {} ({} -> {} bytes)\n",
                c.session.agent_name, c.session.rel_path, c.session.size_bytes, c.compressed_size
            ));
            c.remove_compressed();
        }
    }

    pub(crate) fn prepare_batch(&self, batch: &[Candidate]) -> Result<Vec<Candidate>> {
        let mut prepared: Vec<Candidate> = Vec::with_capacity(batch.len());
        for c in batch {
            let result = match compress_file(&c.session.abs_path, &self.temp_dir) {
                Ok(result) => result,
                Err(err) => {
                    cleanup(&prepared);
                    return Err(err);
                }
            };
            // Re-anchor the hash and size to the snapshot that will actually be
            // uploaded; the scan-time hash may be stale if the agent kept writing.
            let mut c = c.clone();
            c.sha256 = result.plain_sha256;
            c.plain_size = result.plain_size;
            c.compressed = result.path;
            c.compressed_size = result.compressed_size;
            prepared.push(c);
        }
        Ok(prepared)
    }

    /// PUT every prepared candidate with a small worker pool, returning the
    /// candidates that reached storage and the number that did not.
    fn upload_batch(
        &self,
        batch: Vec<Candidate>,
        results: &HashMap<String, UploadResult>,
    ) -> (Vec<Candidate>, i64) {
        if batch.is_empty() {
            return (Vec::new(), 0);
        }
        let jobs = Mutex::new(batch.into_iter());
        let successes: Mutex<Vec<Candidate>> = Mutex::new(Vec::new());
        let failures = AtomicI64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..UPLOAD_WORKERS {
                scope.spawn(|| {
                    loop {
                        let next =
                            jobs.lock().unwrap_or_else(std::sync::PoisonError::into_inner).next();
                        let Some(c) = next else { break };
                        let result = results.get(&c.key()).cloned().unwrap_or_default();
                        let outcome = File::open(&c.compressed)
                            .map_err(|e| Error::from(path_error("open", &c.compressed, &e)))
                            .and_then(|mut file| {
                                self.client
                                    .put_object(&result.put_url, &mut file, c.compressed_size)
                                    .map_err(Error::from)
                            });
                        match outcome {
                            Err(err) => {
                                failures.fetch_add(1, Ordering::SeqCst);
                                self.print(&format!(
                                    "fail upload {} {}: {err}\n",
                                    c.session.agent_name, c.session.rel_path
                                ));
                                c.remove_compressed();
                            }
                            Ok(()) => {
                                successes
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .push(c);
                            }
                        }
                    }
                });
            }
        });
        (
            successes.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner),
            failures.into_inner(),
        )
    }

    fn mark_uploaded(&mut self, c: &Candidate) {
        self.state.entries.insert(
            state::key(&c.session.agent_name, &c.session.rel_path),
            state::Entry {
                size_bytes: c.session.size_bytes,
                mtime_unix_ns: unix_nanos(c.session.mod_time),
                sha256: c.sha256.clone(),
                remote_sha256: c.sha256.clone(),
            },
        );
    }

    fn print(&self, text: &str) {
        if let Some(out) = self.out {
            out.print(text);
        }
    }
}

fn cleanup(batch: &[Candidate]) {
    for c in batch {
        c.remove_compressed();
    }
}

fn upload_items(candidates: &[Candidate]) -> Vec<UploadItem> {
    candidates
        .iter()
        .map(|c| UploadItem {
            agent: c.session.agent_name.clone(),
            agent_session_id: c.session.agent_session_id.clone(),
            rel_path: c.session.rel_path.clone(),
            cwd: c.session.cwd.clone(),
            sha256: c.sha256.clone(),
            size_bytes: c.plain_size,
            compressed_size_bytes: c.compressed_size,
        })
        .collect()
}

pub fn sha256_file(path: &str) -> Result<String> {
    let (mut file, _) = agentdirs::open_regular_file_nofollow(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| path_error("read", path, &e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) struct CompressResult {
    pub(crate) path: String,
    pub(crate) compressed_size: i64,
    pub(crate) plain_sha256: String,
    pub(crate) plain_size: i64,
}

/// Compress `path` into a fresh temp file, hashing the plaintext as it streams
/// so the digest always matches the exact bytes inside the uploaded snapshot.
pub(crate) fn compress_file(path: &str, temp_dir: &str) -> Result<CompressResult> {
    let temp_dir = if temp_dir.trim().is_empty() {
        std::env::temp_dir().to_string_lossy().into_owned()
    } else {
        temp_dir.to_string()
    };
    mkdir_all_private(&temp_dir).map_err(|e| path_error("mkdir", &temp_dir, &e))?;
    let (mut input, _) = agentdirs::open_regular_file_nofollow(path)?;

    let tmp = tempfile::Builder::new()
        .prefix("cmux-vault-")
        .suffix(".jsonl.zst")
        .tempfile_in(&temp_dir)
        .map_err(|e| path_error("open", &temp_dir, &e))?;
    let (file, tmp_path) = tmp.keep().map_err(|e| Error::from(e.error.to_string()))?;
    let tmp_path = tmp_path.to_string_lossy().into_owned();

    let attempt = (|| -> Result<(String, i64)> {
        let mut encoder = zstd::stream::write::Encoder::new(file, zstd::DEFAULT_COMPRESSION_LEVEL)
            .map_err(|e| Error::from(e.to_string()))?;
        encoder.include_checksum(true).map_err(|e| Error::from(e.to_string()))?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        let mut plain_size: i64 = 0;
        loop {
            let n = input.read(&mut buf).map_err(|e| path_error("read", path, &e))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            encoder.write_all(&buf[..n]).map_err(|e| path_error("write", &tmp_path, &e))?;
            plain_size += count(n);
        }
        let mut file = encoder.finish().map_err(|e| path_error("write", &tmp_path, &e))?;
        file.flush().map_err(|e| path_error("write", &tmp_path, &e))?;
        drop(file);
        Ok((hex::encode(hasher.finalize()), plain_size))
    })();
    let (plain_sha256, plain_size) = match attempt {
        Ok(v) => v,
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    };
    let info = match std::fs::metadata(&tmp_path) {
        Ok(info) => info,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(path_error("stat", &tmp_path, &e).into());
        }
    };
    Ok(CompressResult {
        path: tmp_path,
        compressed_size: i64::try_from(info.len()).unwrap_or(i64::MAX),
        plain_sha256,
        plain_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn prepare_batch_rejects_symlinked_session_file() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().to_str().unwrap().to_string();
        let target = format!("{home}/secret.jsonl");
        let link = format!(
            "{home}/.codex/sessions/2026/07/04/rollout-2026-07-04T00-00-00-11111111-1111-4111-8111-111111111111.jsonl"
        );
        std::fs::write(&target, "{\"message\":\"secret\"}\n").unwrap();
        std::fs::create_dir_all(crate::gopath::dir(&link)).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let env = Environ::new(&home, HashMap::new());
        let mut store = state::load(
            &home,
            &HashMap::from([("CMUX_VAULT_STATE_DIR".to_string(), format!("{home}/state"))]),
        )
        .unwrap();
        let client = Client::new("http://127.0.0.1:9", None);
        let engine = Engine {
            env: &env,
            state: &mut store,
            client: &client,
            temp_dir: format!("{home}/tmp"),
            out: None,
        };
        let candidate = Candidate::new(
            Session {
                agent_name: "codex".into(),
                agent_session_id: "11111111-1111-4111-8111-111111111111".into(),
                abs_path: link.clone(),
                rel_path: "sessions/2026/07/04/rollout-2026-07-04T00-00-00-11111111-1111-4111-8111-111111111111.jsonl".into(),
                cwd: String::new(),
                size_bytes: 1,
                mod_time: std::time::SystemTime::now(),
            },
            String::new(),
        );
        let err = engine.prepare_batch(&[candidate]).unwrap_err().to_string();
        assert!(err.starts_with(&format!("open {link}: ")), "{err}");
        let leftovers = std::fs::read_dir(format!("{home}/tmp")).unwrap().count();
        assert_eq!(leftovers, 0, "no compressed temp file may survive a rejected batch");
    }

    #[test]
    fn compress_file_hashes_plaintext_and_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        let path = format!("{dir}/in.jsonl");
        let content = "{\"a\":1}\n".repeat(2000);
        std::fs::write(&path, &content).unwrap();
        let result = compress_file(&path, &format!("{dir}/tmp")).unwrap();
        assert_eq!(result.plain_size, count(content.len()));
        assert_eq!(result.plain_sha256, sha256_file(&path).unwrap());
        assert_eq!(result.plain_sha256, hex::encode(Sha256::digest(content.as_bytes())));
        let compressed = std::fs::read(&result.path).unwrap();
        assert_eq!(compressed.len(), usize::try_from(result.compressed_size).unwrap());
        assert!(compressed.len() < content.len());
        assert_eq!(zstd::stream::decode_all(&compressed[..]).unwrap(), content.as_bytes());
        assert!(result.path.starts_with(&format!("{dir}/tmp/cmux-vault-")));
        assert!(result.path.ends_with(".jsonl.zst"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&result.path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert!(compress_file(&format!("{dir}/missing.jsonl"), &format!("{dir}/tmp")).is_err());
    }
}

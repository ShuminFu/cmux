//! Repository evidence verification.
//!
//! A diagram that cites source paths pins a repository URL and a commit.
//! Verification runs against a local checkout: the checkout's `origin` must
//! be that repository, the commit must exist, and every cited path must be a
//! file (not a directory) at that commit, with any cited line inside it.

use std::path::Path;
use std::process::Command;

use serde_json::json;

use crate::diag::Diagnostic;
use crate::spec::Spec;

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Canonical form for comparing remote URLs: scheme-less, lower-case host,
/// no trailing slash or `.git`, and `git@host:path` folded into `host/path`.
pub fn normalize_remote(url: &str) -> String {
    let mut u = url.trim().to_string();
    if let Some(rest) = u.strip_prefix("git@") {
        u = rest.replacen(':', "/", 1);
    }
    for prefix in ["https://", "http://", "ssh://git@", "ssh://", "git://"] {
        if let Some(rest) = u.strip_prefix(prefix) {
            u = rest.to_string();
            break;
        }
    }
    let u = u
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .trim_end_matches('/');
    let (host, path) = u.split_once('/').unwrap_or((u, ""));
    format!("{}/{}", host.to_ascii_lowercase(), path)
}

pub fn verify(spec: &Spec, repo_root: Option<&Path>) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    let Spec::Architecture(a) = spec else {
        return out;
    };
    if !spec.declares_evidence() {
        return out;
    }
    let Some(root) = repo_root else {
        out.push(
            Diagnostic::error("repository-evidence/root-required", "This diagram declares source evidence. Pass --repo-root <repository> so archify-rs can verify it before rendering.")
                .subject(json!({ "surface": "repository-evidence", "path": "/meta/repository" }))
                .fix("pass --repo-root with the matching local Git checkout"),
        );
        return out;
    };
    let Some(repo) = &a.meta.repository else {
        out.push(
            Diagnostic::error(
                "repository-evidence/repository-required",
                "components cite sources but meta.repository is missing",
            )
            .subject(json!({ "surface": "repository-evidence", "path": "/meta/repository" }))
            .fix("add meta.repository with url and the pinned revision"),
        );
        return out;
    };
    let origin = match git(root, &["remote", "get-url", "origin"]) {
        Ok(o) => o,
        Err(e) => {
            out.push(Diagnostic::error("repository-evidence/origin-unreadable", format!("cannot read origin of {}: {e}", root.display())).subject(json!({ "surface": "repository-evidence", "repoRoot": root.display().to_string() })));
            return out;
        }
    };
    if normalize_remote(&origin) != normalize_remote(&repo.url) {
        out.push(
            Diagnostic::error("repository-evidence/origin-mismatch", format!("Evidence repository origin {origin:?} does not match {:?}.", repo.url))
                .subject(json!({ "surface": "repository-evidence", "repoRoot": root.display().to_string(), "localOrigin": origin, "authoredRepository": repo.url }))
                .fix("use the matching local checkout or correct the authored repository URL"),
        );
        return out;
    }
    if git(
        root,
        &["cat-file", "-e", &format!("{}^{{commit}}", repo.revision)],
    )
    .is_err()
    {
        out.push(
            Diagnostic::error(
                "repository-evidence/revision-missing",
                format!(
                    "revision {} is not present in {}",
                    repo.revision,
                    root.display()
                ),
            )
            .subject(
                json!({ "surface": "repository-evidence", "path": "/meta/repository/revision" }),
            )
            .fix("fetch the pinned revision into the checkout or correct meta.repository.revision"),
        );
        return out;
    }
    for (ci, c) in a.components.iter().enumerate() {
        for (si, s) in c.sources.iter().flatten().enumerate() {
            let spec_path = format!("/components/{ci}/sources/{si}/path");
            let object = format!("{}:{}", repo.revision, s.path);
            match git(root, &["cat-file", "-t", &object]) {
                Ok(t) if t == "blob" => {}
                _ => {
                    out.push(
                        Diagnostic::error("repository-evidence/file-missing", format!("{spec_path} does not identify a file at revision {}.", repo.revision))
                            .subject(json!({ "surface": "repository-evidence", "path": spec_path, "componentId": c.id, "sourcePath": s.path, "revision": repo.revision }))
                            .fix("use a file path that exists at the pinned revision"),
                    );
                    continue;
                }
            }
            if let Some(line) = s.line.or(s.end_line) {
                let want = s.end_line.unwrap_or(line).max(line);
                let count = git(root, &["cat-file", "-p", &object])
                    .map(|body| body.lines().count() as u32)
                    .unwrap_or(0);
                if want > count {
                    out.push(
                        Diagnostic::error("repository-evidence/line-out-of-range", format!("{spec_path} cites line {want} but the file has {count} lines at revision {}.", repo.revision))
                            .subject(json!({ "surface": "repository-evidence", "path": spec_path, "componentId": c.id, "sourcePath": s.path }))
                            .fix("cite a line inside the file"),
                    );
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::normalize_remote;

    #[test]
    fn remote_urls_normalise_across_schemes_and_suffixes() {
        assert_eq!(
            normalize_remote("https://github.com/manaflow-ai/cmux"),
            "github.com/manaflow-ai/cmux"
        );
        assert_eq!(
            normalize_remote("https://GitHub.com/manaflow-ai/cmux.git/"),
            "github.com/manaflow-ai/cmux"
        );
        assert_eq!(
            normalize_remote("git@github.com:manaflow-ai/cmux.git"),
            "github.com/manaflow-ai/cmux"
        );
    }
}

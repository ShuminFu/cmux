//! End-to-end tests that drive the `cmux-vault` executable exactly as a user
//! or script would, covering every command and exit code.

#![allow(clippy::too_many_lines)]

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{Home, MockServer, UUID_A, UUID_B, UUID_C, compress, decompress, run};

const CODEX_REL: &str =
    "sessions/2026/07/04/rollout-2026-07-04T00-00-00-11111111-1111-4111-8111-111111111111.jsonl";

fn codex_transcript() -> String {
    format!(
        "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{UUID_A}\",\"cwd\":\"/repo\"}}}}\n{{\"message\":\"hello\"}}\n"
    )
}

fn seed_agents(home: &Home) {
    home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    home.write(
        &format!(".claude/projects/-Users-me-work/{UUID_B}.jsonl"),
        "{\"type\":\"user\",\"cwd\":\"/Users/me/work\"}\n",
    );
    home.write(
        &format!(".pi/agent/sessions/-Users-me-work/2026-07-04T00-00-00_{UUID_C}.jsonl"),
        "{}\n",
    );
    home.write(".claude/projects/-Users-me-work/notes.jsonl", "{}\n");
}

fn mtime_rfc3339(path: &std::path::Path) -> String {
    let modified = std::fs::metadata(path).unwrap().modified().unwrap();
    DateTime::<Utc>::from(modified).format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[test]
fn version_and_help_surface() {
    let home = Home::new();
    let out = run(&home, "", &["version"]);
    assert_eq!((out.code, out.stdout.as_str()), (0, concat!(env!("CARGO_PKG_VERSION"), "\n")));
    let out = run(&home, "", &["--json", "version"]);
    assert_eq!(out.code, 0);
    assert_eq!(out.json()["version"], env!("CARGO_PKG_VERSION"));

    let out = run(&home, "", &[]);
    assert_eq!(out.code, 2);
    assert!(out.stdout.is_empty());
    assert!(out.stderr.starts_with(
        "Usage: cmux-vault [--api-base URL] [--json] <command> [options]\n\nCommands:\n"
    ));

    let out = run(&home, "", &["help"]);
    assert_eq!(out.code, 0);
    assert!(out.stdout.contains(
        "  resume     Restore a missing session from cmux Vault and print the resume command\n"
    ));
    assert!(out.stderr.is_empty());

    let out = run(&home, "", &["bogus"]);
    assert_eq!(out.code, 2);
    assert!(
        out.stderr.starts_with("cmux-vault: unknown command \"bogus\"\nUsage: cmux-vault"),
        "{}",
        out.stderr
    );

    let out = run(&home, "", &["--help"]);
    assert_eq!(out.code, 2);
    assert!(out.stderr.starts_with("Usage of cmux-vault:\n  -api-base string\n"), "{}", out.stderr);

    let out = run(&home, "", &["scan", "--bogus"]);
    assert_eq!(out.code, 2);
    assert!(
        out.stderr.starts_with("flag provided but not defined: -bogus\nUsage of scan:\n"),
        "{}",
        out.stderr
    );

    let out = run(&home, "", &["--api-base"]);
    assert_eq!(out.code, 2);
    assert!(out.stderr.starts_with("flag needs an argument: -api-base\n"), "{}", out.stderr);
}

#[test]
fn scan_lists_every_agent_in_text_and_json() {
    let home = Home::new();
    seed_agents(&home);
    #[cfg(unix)]
    {
        home.write("elsewhere/secret.jsonl", "{\"cwd\":\"/secret\"}\n");
        std::os::unix::fs::symlink(
            home.join("elsewhere/secret.jsonl"),
            home.join(format!(".claude/projects/-Users-me-work/{UUID_A}.jsonl")),
        )
        .unwrap();
    }

    let out = run(&home, "", &["scan"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    let claude_path = home.join(format!(".claude/projects/-Users-me-work/{UUID_B}.jsonl"));
    let codex_path = home.join(format!(".codex/{CODEX_REL}"));
    let pi_path =
        home.join(format!(".pi/agent/sessions/-Users-me-work/2026-07-04T00-00-00_{UUID_C}.jsonl"));
    let expected = format!(
        "claude\t{UUID_B}\t39\t{}\t{}\ncodex\t{UUID_A}\t{}\t{}\t{}\npi\t{UUID_C}\t3\t{}\t{}\n",
        mtime_rfc3339(&claude_path),
        claude_path.display(),
        codex_transcript().len(),
        mtime_rfc3339(&codex_path),
        codex_path.display(),
        mtime_rfc3339(&pi_path),
        pi_path.display(),
    );
    assert_eq!(out.stdout, expected);
    #[cfg(unix)]
    assert_eq!(
        out.stderr,
        format!(
            "warning: claude: skipping symlinked session {}\n",
            home.join(format!(".claude/projects/-Users-me-work/{UUID_A}.jsonl")).display()
        )
    );

    let out = run(&home, "", &["--json", "scan"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    let json = out.json();
    let sessions = json["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 3);
    assert_eq!(sessions[0]["agent"], "claude");
    assert_eq!(sessions[0]["cwd"], "/Users/me/work");
    assert_eq!(sessions[0]["relPath"], format!("projects/-Users-me-work/{UUID_B}.jsonl"));
    assert_eq!(sessions[0]["sizeBytes"], 39);
    assert!(sessions[0]["modTime"].as_str().unwrap().ends_with('Z'));
    assert_eq!(sessions[1]["agent"], "codex");
    assert_eq!(sessions[1]["cwd"], "/repo");
    assert_eq!(sessions[2]["agent"], "pi");
    assert_eq!(sessions[2]["cwd"], "-Users-me-work");
    let agent_idx = out.stdout.find("\"agent\"").unwrap();
    let id_idx = out.stdout.find("\"agentSessionId\"").unwrap();
    let path_idx = out.stdout.find("\"path\"").unwrap();
    let mod_idx = out.stdout.find("\"modTime\"").unwrap();
    assert!(
        agent_idx < id_idx && id_idx < path_idx && path_idx < mod_idx,
        "field order must follow the Go struct"
    );

    let out = run(&home, "", &["scan", "--agent", "codex", "-json"]);
    assert_eq!(out.code, 0);
    assert_eq!(out.json()["sessions"].as_array().unwrap().len(), 1);

    let out = run(&home, "", &["scan", "--agent=nope"]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "scan failed: unknown agent \"nope\"\n");

    let empty = Home::new();
    let out = run(&empty, "", &["--json", "scan"]);
    assert_eq!(out.code, 0);
    assert_eq!(out.stdout, "{\n  \"sessions\": []\n}\n");
}

#[test]
fn status_and_logout() {
    let home = Home::new();
    let out = run(&home, "", &["status"]);
    assert_eq!((out.code, out.stdout.as_str()), (0, "Not logged in.\nTracked files: 0\n"));
    let out = run(&home, "", &["--json", "status"]);
    assert_eq!(out.stdout, "{\n  \"loggedIn\": false,\n  \"trackedFiles\": 0\n}\n");

    home.login();
    home.write(
        "state/state.json",
        "{\"entries\":{\"codex\\u0000a\":{\"sizeBytes\":1},\"pi\\u0000b\":{}}}",
    );
    let out = run(&home, "", &["status"]);
    assert_eq!(out.stdout, "Logged in.\nTracked files: 2\n");
    let out = run(&home, "", &["status", "--json"]);
    assert_eq!(out.json()["loggedIn"], true);
    assert_eq!(out.json()["trackedFiles"], 2);

    home.write("state/state.json", "{broken");
    let out = run(&home, "", &["status"]);
    assert_eq!(out.code, 1);
    assert!(out.stderr.starts_with("loading state failed: "), "{}", out.stderr);

    let out = run(&home, "", &["logout"]);
    assert_eq!((out.code, out.stdout.as_str()), (0, "Logged out.\n"));
    assert!(!home.join("cfg/auth.json").exists());
    let out = run(&home, "", &["--json", "logout"]);
    assert_eq!(out.stdout, "{\n  \"ok\": true\n}\n");
}

#[test]
fn sync_requires_login_except_dry_run() {
    let home = Home::new();
    seed_agents(&home);
    let out = run(&home, "", &["sync"]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "not logged in; run cmux-vault login\n");
    assert!(out.stdout.is_empty());

    let out = run(&home, "", &["sync", "--dry-run", "--agent", "codex"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(
        out.stdout,
        format!(
            "would upload codex {CODEX_REL} ({} bytes)\nsummary: uploaded=0 skipped=1 failed=0 bytes=0 compressedBytes=0\n",
            codex_transcript().len()
        )
    );
    assert!(!home.join("state/state.json").exists(), "dry run must not write state");

    let out = run(&home, "", &["sync", "--dry-run", "--json"]);
    assert_eq!(out.code, 0);
    assert_eq!(
        out.stdout,
        "{\n  \"uploaded\": 0,\n  \"skipped\": 3,\n  \"failed\": 0,\n  \"bytesUploaded\": 0,\n  \"compressedBytesUploaded\": 0\n}\n"
    );

    let out = run(&home, "", &["sync", "--dry-run", "--limit", "1"]);
    assert_eq!(out.code, 0);
    assert_eq!(out.stdout.lines().count(), 2, "{}", out.stdout);
    assert!(out.stdout.starts_with("would upload claude "), "{}", out.stdout);
}

#[test]
fn sync_uploads_incrementally_and_compresses() {
    let home = Home::new();
    home.login();
    let session_path = home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    let server = MockServer::start();

    let first = run(&home, &server.base, &["sync", "--agent", "codex"]);
    assert_eq!(first.code, 0, "{}{}", first.stdout, first.stderr);
    let blob =
        server.state().blobs.get(&format!("codex/{CODEX_REL}")).cloned().expect("uploaded blob");
    assert_eq!(decompress(&blob), codex_transcript().as_bytes());
    assert_eq!(
        first.stdout,
        format!(
            "uploaded codex {CODEX_REL} ({} -> {} bytes)\nsummary: uploaded=1 skipped=0 failed=0 bytes={} compressedBytes={}\n",
            codex_transcript().len(),
            blob.len(),
            codex_transcript().len(),
            blob.len()
        )
    );
    assert!(first.stderr.is_empty());

    let put = &server.requests_to("/put")[0];
    assert_eq!(put.method, "PUT");
    assert_eq!(put.headers["content-type"], "application/zstd");
    assert_eq!(put.headers["content-length"], blob.len().to_string());
    assert!(!put.headers.contains_key("transfer-encoding"));
    let uploads = &server.requests_to("/api/vault/uploads")[0];
    assert_eq!(uploads.headers["authorization"], "Bearer access-token");
    assert_eq!(uploads.headers["x-stack-refresh-token"], "refresh-token");
    assert_eq!(uploads.headers["content-type"], "application/json");
    assert_eq!(uploads.headers["accept"], "application/json");
    let body: serde_json::Value = serde_json::from_slice(&uploads.body).unwrap();
    let item = &body["items"][0];
    assert_eq!(item["agent"], "codex");
    assert_eq!(item["agentSessionId"], UUID_A);
    assert_eq!(item["cwd"], "/repo");
    assert_eq!(item["sizeBytes"], codex_transcript().len());
    assert_eq!(item["compressedSizeBytes"], blob.len());
    assert_eq!(item["sha256"].as_str().unwrap().len(), 64);

    let state = home.state_json();
    let entry = &state["entries"][format!("codex\u{0}{CODEX_REL}")];
    assert_eq!(entry["sizeBytes"], codex_transcript().len());
    assert_eq!(entry["sha256"], item["sha256"]);
    assert_eq!(entry["remoteSha256"], item["sha256"]);
    assert!(entry["mtimeUnixNs"].as_i64().unwrap() > 0);
    let tmp_leftovers = std::fs::read_dir(home.join("tmp")).map(Iterator::count).unwrap_or(0);
    assert_eq!(tmp_leftovers, 0, "compressed temp files must be cleaned up");

    let second = run(&home, &server.base, &["sync", "--agent", "codex"]);
    assert_eq!(second.code, 0);
    assert_eq!(
        second.stdout,
        format!(
            "skip unchanged codex {CODEX_REL}\nsummary: uploaded=0 skipped=1 failed=0 bytes=0 compressedBytes=0\n"
        )
    );
    assert_eq!(
        server.requests_to("/api/vault/uploads").len(),
        1,
        "unchanged file must not hit the API"
    );

    // Same content, new mtime: hashed, recognized as already uploaded.
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(&session_path, codex_transcript()).unwrap();
    let third = run(&home, &server.base, &["sync", "--agent", "codex"]);
    assert_eq!(third.code, 0);
    assert!(
        third.stdout.starts_with(&format!("skip already uploaded codex {CODEX_REL}\n")),
        "{}",
        third.stdout
    );
    assert_eq!(server.requests_to("/api/vault/uploads").len(), 1);

    let updated = format!("{}{{\"message\":\"updated\"}}\n", codex_transcript());
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(&session_path, &updated).unwrap();
    let fourth = run(&home, &server.base, &["--json", "sync", "--agent", "codex"]);
    assert_eq!(fourth.code, 0, "{}", fourth.stderr);
    let summary = fourth.json();
    assert_eq!(summary["uploaded"], 1);
    assert_eq!(summary["failed"], 0);
    assert_eq!(summary["bytesUploaded"], updated.len());
    assert!(fourth.stdout.starts_with("{\n  \"uploaded\": 1,"), "{}", fourth.stdout);
    let blob = server.state().blobs.get(&format!("codex/{CODEX_REL}")).cloned().unwrap();
    assert_eq!(decompress(&blob), updated.as_bytes());

    // Re-syncing after the server already has this content: cloud-unchanged path.
    home.write("state/state.json", "{}");
    let fifth = run(&home, &server.base, &["sync", "--agent", "codex"]);
    assert_eq!(fifth.code, 0);
    assert!(
        fifth.stdout.starts_with(&format!("skip cloud unchanged codex {CODEX_REL}\n")),
        "{}",
        fifth.stdout
    );
    assert_eq!(server.requests_to("/put").len(), 2);
}

#[test]
fn sync_reports_server_failures_and_retries_5xx_once() {
    let home = Home::new();
    home.login();
    home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    let server = MockServer::start();
    server.state().fail_uploads = 2;

    let out = run(&home, &server.base, &["sync"]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stdout, "fail presign batch: api request failed: status 500: boom\n");
    assert_eq!(out.stderr, "sync failed: 1 upload(s) failed\n");
    assert_eq!(
        server.requests_to("/api/vault/uploads").len(),
        2,
        "5xx must be retried exactly once"
    );
    assert!(home.join("state/state.json").exists(), "state is saved even when uploads fail");
    assert!(home.state_json()["entries"].as_object().unwrap().is_empty());

    server.state().fail_uploads = 1;
    let out = run(&home, &server.base, &["sync"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(out.stdout.starts_with("uploaded codex "), "{}", out.stdout);
    assert_eq!(server.requests_to("/api/vault/uploads").len(), 4);

    server.state().api_override = Some((401, "{\"error\":\"unauthorized\"}".into()));
    home.write("state/state.json", "{}");
    let out = run(&home, &server.base, &["--json", "sync"]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "sync failed: 1 upload(s) failed\n");
    let summary: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(summary["failed"], 1);
    assert_eq!(server.requests_to("/api/vault/uploads").len(), 5, "4xx must not be retried");

    let out = run(&home, "http://127.0.0.1:9", &["sync"]);
    assert_eq!(out.code, 1);
    assert!(out.stdout.starts_with("fail presign batch: "), "{}", out.stdout);
    assert_eq!(out.stderr, "sync failed: 1 upload(s) failed\n");
}

#[test]
fn sync_reports_storage_and_commit_failures_per_item() {
    let home = Home::new();
    home.login();
    home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    let server = MockServer::start();
    server.state().reject_puts = true;
    let out = run(&home, &server.base, &["sync"]);
    assert_eq!(out.code, 1);
    assert_eq!(
        out.stdout,
        format!("fail upload codex {CODEX_REL}: storage PUT failed: status 403: denied\n")
    );
    assert_eq!(out.stderr, "sync failed: 1 upload(s) failed\n");
    assert!(
        server.requests_to("/api/vault/sessions/commit").is_empty(),
        "nothing to commit after a failed PUT"
    );
    assert!(home.state_json()["entries"].as_object().unwrap().is_empty());

    server.state().reject_puts = false;
    server.state().drop_puts = true;
    let out = run(&home, &server.base, &["sync"]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stdout, format!("fail commit codex {CODEX_REL}: object_missing\n"));
    assert_eq!(server.requests_to("/api/vault/sessions/commit").len(), 1);
    assert!(home.state_json()["entries"].as_object().unwrap().is_empty());

    server.state().drop_puts = false;
    let out = run(&home, &server.base, &["sync"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(home.state_json()["entries"].as_object().unwrap().len(), 1);
    let leftovers = std::fs::read_dir(home.join("tmp")).map(Iterator::count).unwrap_or(0);
    assert_eq!(leftovers, 0, "compressed temp files must be cleaned up after failures too");
}

#[test]
fn resume_uses_local_transcript_then_cloud_restore() {
    let home = Home::new();
    home.login();
    let session_path = home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    let server = MockServer::start();
    let plain = codex_transcript();
    {
        let mut st = server.state();
        st.sessions.push(cmux_vault::api::Session {
            id: "cloud-session".into(),
            agent: "codex".into(),
            agent_session_id: UUID_A.into(),
            rel_path: CODEX_REL.into(),
            cwd: "/repo".into(),
            ..Default::default()
        });
        st.objects.insert("cloud-session".into(), compress(plain.as_bytes()));
    }

    let out = run(&home, &server.base, &["resume"]);
    assert_eq!((out.code, out.stderr.as_str()), (2, "resume requires a session id\n"));

    let out = run(&home, &server.base, &["resume", UUID_A.to_uppercase().as_str()]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stdout, format!("codex resume {UUID_A}\n"));
    assert!(server.requests_to("/api").is_empty(), "local hit must not call the API");

    let out = run(&home, &server.base, &["--json", "resume", UUID_A]);
    assert_eq!(out.stdout, format!("{{\n  \"hint\": \"codex resume {UUID_A}\"\n}}\n"));

    std::fs::remove_file(&session_path).unwrap();
    let out = run(&home, &server.base, &["resume", "--agent", "codex", UUID_A]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stdout, format!("restored {}\ncodex resume {UUID_A}\n", session_path.display()));
    assert_eq!(std::fs::read(&session_path).unwrap(), plain.as_bytes());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&session_path).unwrap().permissions().mode() & 0o777, 0o600);
    }
    let lookup = &server.requests_to("/api/vault/sessions?")[0];
    assert_eq!(
        lookup.url,
        format!("/api/vault/sessions?agent=codex&agentSessionId={UUID_A}&limit=2")
    );
    assert_eq!(lookup.headers["authorization"], "Bearer access-token");
    let leftovers: Vec<String> = std::fs::read_dir(session_path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".restore-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    // Cloud says the session lives at a path that already exists locally but
    // is not a discovered session: refuse to clobber without --force.
    let overwrite_rel = "sessions/2026/07/04/not-a-discovered-session.jsonl";
    let existing = home.write(&format!(".codex/{overwrite_rel}"), "existing\n");
    {
        let mut st = server.state();
        st.sessions.clear();
        st.sessions.push(cmux_vault::api::Session {
            id: "cloud-overwrite".into(),
            agent: "codex".into(),
            agent_session_id: UUID_B.into(),
            rel_path: overwrite_rel.into(),
            cwd: "/repo".into(),
            ..Default::default()
        });
        st.objects.insert("cloud-overwrite".into(), compress(b"from cloud\n"));
    }
    let out = run(&home, &server.base, &["resume", UUID_B]);
    assert_eq!(out.code, 1);
    assert_eq!(
        out.stderr,
        format!(
            "resume failed: {} already exists; pass --force to overwrite\n",
            existing.display()
        )
    );
    assert_eq!(std::fs::read(&existing).unwrap(), b"existing\n");

    let out = run(&home, &server.base, &["resume", "--force", UUID_B]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(std::fs::read(&existing).unwrap(), b"from cloud\n");
    assert_eq!(out.stdout, format!("restored {}\ncodex resume {UUID_B}\n", existing.display()));

    // --force on a session that exists locally but not in the vault.
    server.state().sessions.clear();
    home.write(&format!(".codex/{CODEX_REL}"), codex_transcript());
    let out = run(&home, &server.base, &["resume", "--force", UUID_A]);
    assert_eq!(out.code, 1);
    assert_eq!(
        out.stderr,
        format!(
            "resume failed: session {UUID_A} exists locally but was not found in cmux vault; rerun without --force to use the local transcript\n"
        )
    );
    let out = run(&home, &server.base, &["resume", UUID_C]);
    assert_eq!(out.code, 1);
    assert_eq!(
        out.stderr,
        format!("resume failed: session {UUID_C} not found locally or in cmux vault\n")
    );

    // Traversal in a server-supplied relPath is rejected before any write.
    {
        let mut st = server.state();
        st.sessions.push(cmux_vault::api::Session {
            id: "evil".into(),
            agent: "codex".into(),
            agent_session_id: UUID_C.into(),
            rel_path: "../../escape.jsonl".into(),
            ..Default::default()
        });
        st.objects.insert("evil".into(), compress(b"evil\n"));
    }
    let out = run(&home, &server.base, &["resume", UUID_C]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "resume failed: invalid relative path \"../../escape.jsonl\"\n");
    assert!(!home.join("escape.jsonl").exists());

    let logged_out = Home::new();
    let out = run(&logged_out, &server.base, &["resume", UUID_A]);
    assert_eq!((out.code, out.stderr.as_str()), (1, "not logged in; run cmux-vault login\n"));
}

#[test]
fn resume_reports_ambiguous_and_unknown_agent_from_server() {
    let home = Home::new();
    home.login();
    let server = MockServer::start();
    {
        let mut st = server.state();
        for (id, agent) in [("one", "codex"), ("two", "claude")] {
            st.sessions.push(cmux_vault::api::Session {
                id: id.into(),
                agent: agent.into(),
                agent_session_id: UUID_A.into(),
                rel_path: "x.jsonl".into(),
                ..Default::default()
            });
        }
    }
    let out = run(&home, &server.base, &["resume", UUID_A]);
    assert_eq!(out.code, 1);
    assert_eq!(
        out.stderr,
        format!(
            "resume failed: session {UUID_A} exists for multiple agents in cmux vault; pass --agent to disambiguate\n"
        )
    );

    {
        let mut st = server.state();
        st.sessions.clear();
        st.sessions.push(cmux_vault::api::Session {
            id: "g".into(),
            agent: "gemini".into(),
            agent_session_id: UUID_A.into(),
            rel_path: "x.jsonl".into(),
            ..Default::default()
        });
    }
    let out = run(&home, &server.base, &["resume", UUID_A]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "resume failed: unknown agent \"gemini\" from server\n");

    server.state().api_override = Some((503, "down".into()));
    let out = run(&home, &server.base, &["resume", UUID_A]);
    assert_eq!(out.code, 1);
    assert_eq!(out.stderr, "resume failed: api request failed: status 503: down\n");
}

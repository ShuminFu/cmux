//! Ported from the Go `agentdirs` tests, plus coverage for the helpers whose
//! behavior the sync contract depends on.

use std::collections::HashMap;
use std::fs;

use super::*;

const UUID_A: &str = "019d60bc-b684-7a01-b4ac-52feffc5fcb5";
const UUID_B: &str = "019d60bc-b685-7a02-b4ac-52feffc5fcb6";
const UUID_C: &str = "019d60bc-b686-7a03-b4ac-52feffc5fcb7";
const UUID_D: &str = "019f21d4-161e-7d25-a342-8a426f41d8a4";

fn write_file(path: &str, content: &str) {
    fs::create_dir_all(gopath::dir(path)).unwrap();
    fs::write(path, content).unwrap();
}

fn env_with(home: &str, vars: &[(&str, &str)]) -> Environ {
    Environ::new(
        home,
        vars.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect::<HashMap<_, _>>(),
    )
}

fn find_session<'a>(sessions: &'a [Session], id: &str) -> &'a Session {
    sessions
        .iter()
        .find(|s| s.agent_session_id == id)
        .unwrap_or_else(|| panic!("session {id} not found in {sessions:#?}"))
}

fn tmp() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    (dir, path)
}

#[cfg(unix)]
fn running_as_root() -> bool {
    use std::os::unix::fs::MetadataExt;
    fs::metadata("/").map(|m| m.uid() == 0).unwrap_or(false)
        && fs::metadata(std::env::temp_dir()).map(|m| m.uid() == 0).unwrap_or(false)
}

#[cfg(unix)]
#[test]
fn claude_discover() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let (_keep, base) = tmp();
    let root = format!("{base}/claude-config");
    let secret_path = format!("{base}/elsewhere/secret.jsonl");
    write_file(&secret_path, "{\"cwd\":\"/secret\"}\n");
    let session_path = format!("{root}/projects/-Users-lawrence-work-cmux/{UUID_A}.jsonl");
    write_file(&session_path, "{\"type\":\"message\",\"cwd\":\"/Users/lawrence/work/cmux\"}\n");
    write_file(&format!("{root}/projects/-Users-lawrence-work-cmux/not-a-session.jsonl"), "{}\n");
    write_file(&format!("{root}/projects/-Users-lawrence-work-cmux/{UUID_B}.txt"), "{}\n");
    write_file(&format!("{root}/projects/nested/{UUID_B}.jsonl"), "{\"cwd\":\"/nested\"}\n");
    symlink(
        format!("{root}/missing-target.jsonl"),
        format!("{root}/projects/nested/{UUID_C}.jsonl"),
    )
    .unwrap();
    symlink(&secret_path, format!("{root}/projects/nested/{UUID_D}.jsonl")).unwrap();
    let unreadable = format!("{root}/projects/unreadable");
    fs::create_dir_all(&unreadable).unwrap();
    write_file(&format!("{unreadable}/{UUID_C}.jsonl"), "{}\n");
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();

    let env = env_with(&base, &[("CLAUDE_CONFIG_DIR", &root)]);
    let got = Claude.discover(&env);
    fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o700)).unwrap();
    let got = got.unwrap();
    let warnings = env.warnings();
    if running_as_root() {
        // Root ignores directory modes, so the unreadable subtree is visible.
        assert_eq!(got.len(), 3, "{got:#?}");
    } else {
        assert_eq!(got.len(), 2, "{got:#?}");
        assert!(warnings.iter().any(|w| w.contains("skipping unreadable path")), "{warnings:?}");
    }
    let first = find_session(&got, UUID_A);
    assert_eq!(first.cwd, "/Users/lawrence/work/cmux");
    assert_eq!(first.rel_path, format!("projects/-Users-lawrence-work-cmux/{UUID_A}.jsonl"));
    assert_eq!(first.abs_path, session_path);
    assert_eq!(first.size_bytes, 53);
    assert_eq!(find_session(&got, UUID_B).cwd, "/nested");
    assert!(!warnings.is_empty(), "expected skip warning");
    assert!(
        warnings.iter().any(|w| w
            == &format!(
                "claude: skipping symlinked session {root}/projects/nested/{UUID_D}.jsonl"
            )),
        "{warnings:?}"
    );
    assert!(warnings.iter().any(|w| w.contains(&format!("nested/{UUID_C}.jsonl"))), "{warnings:?}");
    assert!(!got.iter().any(|s| s.cwd == "/secret"));
}

#[cfg(unix)]
#[test]
fn codex_discover_skips_symlinked_session_files() {
    use std::os::unix::fs::symlink;

    let (_keep, home) = tmp();
    let root = format!("{home}/.codex");
    let secret_path = format!("{home}/elsewhere/secret.jsonl");
    write_file(
        &secret_path,
        &format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{UUID_D}\",\"cwd\":\"/secret\"}}}}\n"
        ),
    );
    let session_dir = format!("{root}/sessions/2026/07/04");
    fs::create_dir_all(&session_dir).unwrap();
    symlink(&secret_path, format!("{session_dir}/rollout-2026-07-04T00-00-00-{UUID_D}.jsonl"))
        .unwrap();

    let env = env_with(&home, &[]);
    let got = Codex.discover(&env).unwrap();
    assert!(got.is_empty(), "expected symlinked session to be skipped, got {got:#?}");
    assert_eq!(env.warnings().len(), 1);
}

#[cfg(unix)]
#[test]
fn discover_symlinked_roots() {
    use std::os::unix::fs::symlink;

    let (_keep, base) = tmp();
    let shared = format!("{base}/shared");

    let claude_root = format!("{base}/claude-config");
    let claude_projects = format!("{shared}/claude-projects");
    write_file(&format!("{claude_projects}/-repo/{UUID_A}.jsonl"), "{\"cwd\":\"/repo\"}\n");
    fs::create_dir_all(&claude_root).unwrap();
    symlink(&claude_projects, format!("{claude_root}/projects")).unwrap();
    let env = env_with(&base, &[("CLAUDE_CONFIG_DIR", &claude_root)]);
    let claude_sessions = Claude.discover(&env).unwrap();
    let claude_session = find_session(&claude_sessions, UUID_A);
    assert_eq!(claude_session.rel_path, format!("projects/-repo/{UUID_A}.jsonl"));
    assert_eq!(
        claude_session.abs_path,
        fs::canonicalize(format!("{claude_projects}/-repo/{UUID_A}.jsonl"))
            .unwrap()
            .to_str()
            .unwrap()
    );
    assert!(env.warnings().is_empty(), "{:?}", env.warnings());

    let codex_home = format!("{base}/.codex");
    let codex_sessions = format!("{shared}/codex-sessions");
    write_file(
        &format!("{codex_sessions}/2026/04/05/rollout-2026-04-05T20-01-13-{UUID_B}.jsonl"),
        &format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{UUID_B}\",\"cwd\":\"/repo\"}}}}\n"
        ),
    );
    fs::create_dir_all(&codex_home).unwrap();
    symlink(&codex_sessions, format!("{codex_home}/sessions")).unwrap();
    let codex_found = Codex.discover(&env_with(&base, &[])).unwrap();
    let codex_session = find_session(&codex_found, UUID_B);
    assert_eq!(
        codex_session.rel_path,
        format!("sessions/2026/04/05/rollout-2026-04-05T20-01-13-{UUID_B}.jsonl")
    );

    let pi_home = format!("{base}/pi-home");
    let pi_sessions = format!("{shared}/pi-sessions");
    write_file(
        &format!("{pi_sessions}/-repo/2026-07-02T07-56-15-262Z_{UUID_D}.jsonl"),
        "{\"cwd\":\"/repo\"}\n",
    );
    fs::create_dir_all(format!("{pi_home}/.pi/agent")).unwrap();
    symlink(&pi_sessions, format!("{pi_home}/.pi/agent/sessions")).unwrap();
    let pi_found = Pi.discover(&env_with(&pi_home, &[])).unwrap();
    let pi_session = find_session(&pi_found, UUID_D);
    assert_eq!(pi_session.rel_path, format!("-repo/2026-07-02T07-56-15-262Z_{UUID_D}.jsonl"));
}

#[test]
fn codex_discover_sessions_and_archived() {
    let (_keep, home) = tmp();
    let root = format!("{home}/.codex");
    write_file(
        &format!("{root}/sessions/2026/07/04/rollout-2026-07-04T00-00-00-{UUID_A}.jsonl"),
        &format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{UUID_B}\",\"cwd\":\"/repo/from/meta\"}}}}\n"
        ),
    );
    write_file(
        &format!("{root}/archived_sessions/2026/07/03/rollout-2026-07-03T00-00-00-{UUID_C}.jsonl"),
        "{\"type\":\"other\",\"payload\":{}}\n{\"nested\":{\"deep\":{\"cwd\":\"/from/body\"}}}\n",
    );
    write_file(&format!("{root}/sessions/2026/07/04/junk-{UUID_D}.jsonl"), "{}\n");
    // Uppercase filename id, meta without a usable id: the lowercased filename id wins.
    write_file(
        &format!(
            "{root}/sessions/2026/07/05/rollout-2026-07-05T00-00-00-{}.jsonl",
            UUID_D.to_uppercase()
        ),
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"not-a-uuid\",\"cwd\":\" /meta/cwd \"}}\n",
    );

    let env = env_with(&home, &[]);
    let got = Codex.discover(&env).unwrap();
    assert_eq!(got.len(), 3, "{got:#?}");
    let meta = find_session(&got, UUID_B);
    assert_eq!(meta.cwd, "/repo/from/meta");
    let fallback = find_session(&got, UUID_C);
    assert_eq!(
        fallback.rel_path,
        format!("archived_sessions/2026/07/03/rollout-2026-07-03T00-00-00-{UUID_C}.jsonl")
    );
    assert_eq!(fallback.cwd, "/from/body");
    let upper = find_session(&got, UUID_D);
    assert_eq!(upper.cwd, "/meta/cwd");
    assert!(env.warnings().is_empty(), "{:?}", env.warnings());
}

#[test]
fn pi_discover() {
    let (_keep, home) = tmp();
    let session_path = format!(
        "{home}/.pi/agent/sessions/-Users-lawrence-work-cmux/2026-07-04T00-00-00_{UUID_D}.jsonl"
    );
    write_file(&session_path, "{\"cwd\":\"/Users/lawrence/work/cmux\"}\n");
    write_file(&format!("{home}/.pi/agent/sessions/-Users-lawrence-work-cmux/junk.jsonl"), "{}\n");
    write_file(&format!("{home}/.pi/agent/sessions/-munged-dir/x_{UUID_A}.jsonl"), "not json\n");

    let got = Pi.discover(&env_with(&home, &[])).unwrap();
    assert_eq!(got.len(), 2, "{got:#?}");
    let real = find_session(&got, UUID_D);
    assert_eq!(real.cwd, "/Users/lawrence/work/cmux");
    let munged = find_session(&got, UUID_A);
    assert_eq!(munged.cwd, "-munged-dir");
    assert_eq!(Pi.resume_hint(&munged.as_ref()), format!("open pi and resume session {UUID_A}"));
    assert_eq!(
        Pi.resume_hint(&real.as_ref()),
        format!("cd '/Users/lawrence/work/cmux' && open pi to resume session {UUID_D}")
    );
}

#[test]
fn claude_discover_ignores_case_variants_and_keeps_filename_case() {
    let (_keep, home) = tmp();
    let upper = UUID_A.to_uppercase();
    write_file(&format!("{home}/.claude/projects/p/{upper}.jsonl"), "");
    write_file(&format!("{home}/.claude/projects/p/{UUID_B}.JSONL"), "{}\n");
    write_file(
        &format!("{home}/.claude/projects/p/{UUID_C}.jsonl"),
        "{\"cwd\":\"\"}\n{\"a\":[{\"cwd\":\"/arr\"}]}\n",
    );
    let got = Claude.discover(&env_with(&home, &[])).unwrap();
    assert_eq!(got.len(), 2, "{got:#?}");
    let first = find_session(&got, &upper);
    assert_eq!(first.cwd, "p");
    assert_eq!(first.size_bytes, 0);
    assert_eq!(Claude.resume_hint(&first.as_ref()), format!("claude --resume {upper}"));
    let arr = find_session(&got, UUID_C);
    assert_eq!(arr.cwd, "/arr");
    assert_eq!(Claude.resume_hint(&arr.as_ref()), format!("cd '/arr' && claude --resume {UUID_C}"));
}

#[test]
fn discover_all_sorts_and_rejects_unknown_agent() {
    let (_keep, home) = tmp();
    write_file(&format!("{home}/.pi/agent/sessions/z/x_{UUID_A}.jsonl"), "{}\n");
    write_file(&format!("{home}/.pi/agent/sessions/a/x_{UUID_B}.jsonl"), "{}\n");
    write_file(&format!("{home}/.claude/projects/p/{UUID_C}.jsonl"), "{}\n");
    let env = env_with(&home, &[]);
    let got = discover_all(&env, "").unwrap();
    let order: Vec<(&str, &str)> =
        got.iter().map(|s| (s.agent_name.as_str(), s.rel_path.as_str())).collect();
    assert_eq!(order[0].0, "claude");
    assert_eq!(order[1], ("pi", &*format!("a/x_{UUID_B}.jsonl")));
    assert_eq!(order[2], ("pi", &*format!("z/x_{UUID_A}.jsonl")));
    assert_eq!(discover_all(&env, " PI ").unwrap().len(), 2);
    assert_eq!(discover_all(&env, "gemini").unwrap_err().to_string(), "unknown agent \"gemini\"");
    assert!(
        discover_all(&env_with("", &[]), "")
            .unwrap_err()
            .to_string()
            .contains("home directory is empty")
    );
    assert!(discover_all(&env_with(&format!("{home}/nonexistent"), &[]), "").unwrap().is_empty());
    assert!(env.warnings().is_empty());
}

#[test]
fn restore_paths_reject_traversal_and_absolute_input() {
    let env = env_with("/home/u", &[("CODEX_HOME", "/custom/codex")]);
    let make = |rel: &str| SessionRef { rel_path: rel.to_string(), ..SessionRef::default() };
    assert_eq!(
        Codex.restore_path(&env, &make("sessions/a/b.jsonl")).unwrap(),
        "/custom/codex/sessions/a/b.jsonl"
    );
    assert_eq!(
        Claude.restore_path(&env, &make(" projects/x/./y.jsonl ")).unwrap(),
        "/home/u/.claude/projects/x/y.jsonl"
    );
    assert_eq!(
        Pi.restore_path(&env, &make("a/b.jsonl")).unwrap(),
        "/home/u/.pi/agent/sessions/a/b.jsonl"
    );
    for bad in ["", "   ", "/abs.jsonl", "../escape.jsonl", "..", "a/../../escape.jsonl", "."] {
        let err = Codex.restore_path(&env, &make(bad)).unwrap_err().to_string();
        assert!(err.starts_with("invalid relative path"), "{bad:?}: {err}");
    }
}

#[test]
fn cwd_recovery_caps_lines_and_handles_bad_json() {
    let (_keep, dir) = tmp();
    let path = format!("{dir}/f.jsonl");
    let mut content = String::new();
    for _ in 0..128 {
        content.push_str("{\"x\":1}\n");
    }
    content.push_str("{\"cwd\":\"/late\"}\n");
    write_file(&path, &content);
    assert_eq!(recover_cwd_from_jsonl(&path), "");
    let mut content = String::new();
    for _ in 0..127 {
        content.push_str("garbage\n");
    }
    content.push_str("{\"cwd\":\"/last\"}");
    write_file(&path, &content);
    assert_eq!(recover_cwd_from_jsonl(&path), "/last");
    write_file(&path, "\r\n{\"cwd\":\"/crlf\"}\r\n");
    assert_eq!(recover_cwd_from_jsonl(&path), "/crlf");
    let long = format!("{{\"pad\":\"{}\"}}\n{{\"cwd\":\"/after\"}}\n", "x".repeat(1024 * 1024));
    write_file(&path, &long);
    assert_eq!(recover_cwd_from_jsonl(&path), "", "over-long line must stop the scan");
    assert_eq!(recover_cwd_from_jsonl(&format!("{dir}/missing.jsonl")), "");
    assert_eq!(cwd_from_json(b"{\"cwd\": 5, \"inner\": {\"cwd\": \"/n\"}}"), Some("/n".into()));
    assert_eq!(
        cwd_from_json(b"[{\"cwd\": \"  \"}, {\"cwd\": \"/second\"}]"),
        Some("/second".into())
    );
    assert_eq!(cwd_from_json(b"\"cwd\""), None);
}

#[test]
fn shell_quote_escapes_single_quotes() {
    assert_eq!(shell_quote(""), "''");
    assert_eq!(shell_quote("/a b"), "'/a b'");
    assert_eq!(shell_quote("it's"), "'it'\\''s'");
}

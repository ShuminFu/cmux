//! End-to-end tests that drive the compiled binary the way a user would.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_archify-rs"))
}

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name)
}

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("archify-rs-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn run(args: &[&str]) -> (bool, serde_json::Value, String) {
    let out = Command::new(bin())
        .args(args)
        .output()
        .expect("binary runs");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let json = serde_json::from_str(&stdout).unwrap_or(serde_json::Value::Null);
    (
        out.status.success(),
        json,
        stdout + &String::from_utf8_lossy(&out.stderr),
    )
}

#[test]
fn workflow_example_passes_showcase_validation_with_all_nine_checks() {
    let (ok, receipt, raw) = run(&[
        "validate",
        "workflow",
        example("catchup-runbook.workflow.json").to_str().unwrap(),
        "--quality",
        "showcase",
        "--json",
    ]);
    assert!(ok, "{raw}");
    assert_eq!(receipt["ok"], true);
    let checks = receipt["checks"].as_array().unwrap();
    assert_eq!(checks.len(), 9);
    assert!(checks.iter().all(|c| c["ok"] == true));
    assert!(receipt["diagnostics"].as_array().unwrap().is_empty());
}

#[test]
fn architecture_example_requires_a_repo_root_because_it_cites_sources() {
    let (ok, receipt, _) = run(&[
        "validate",
        "architecture",
        example("upstream-map.architecture.json").to_str().unwrap(),
        "--json",
    ]);
    assert!(!ok);
    assert_eq!(receipt["stage"], "evidence");
    assert_eq!(
        receipt["diagnostics"][0]["code"],
        "repository-evidence/root-required"
    );
}

#[test]
fn architecture_example_rejects_a_checkout_with_the_wrong_origin() {
    let root = tmp("wrong-origin");
    std::fs::create_dir_all(&root).unwrap();
    let git = |args: &[&str]| {
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(args)
                .status()
                .unwrap()
                .success()
        )
    };
    if !root.join(".git").exists() {
        git(&["init", "-q", "."]);
        git(&["remote", "add", "origin", "https://example.com/other/repo"]);
    }
    let (ok, receipt, _) = run(&[
        "validate",
        "architecture",
        example("upstream-map.architecture.json").to_str().unwrap(),
        "--repo-root",
        root.to_str().unwrap(),
        "--json",
    ]);
    assert!(!ok);
    assert_eq!(
        receipt["diagnostics"][0]["code"],
        "repository-evidence/origin-mismatch"
    );
}

#[test]
fn deliver_writes_html_with_receipts_and_refuses_on_failure() {
    let out = tmp("runbook.html");
    let (ok, receipt, raw) = run(&[
        "deliver",
        "workflow",
        example("catchup-runbook.workflow.json").to_str().unwrap(),
        out.to_str().unwrap(),
        "--quality",
        "showcase",
        "--json",
    ]);
    assert!(ok, "{raw}");
    assert_eq!(receipt["committed"], true);
    let html = std::fs::read_to_string(&out).unwrap();
    assert_eq!(html.matches("<svg").count(), 1, "exactly one inline svg");
    assert!(html.contains("data-vb-w="));
    assert!(html.contains("Upstream catch-up runbook"));
    assert!(
        !html.contains("<script src="),
        "self-contained: no external scripts"
    );
    let sha = receipt["artifact"]["sha256"].as_str().unwrap();
    assert_eq!(sha.len(), 64);
    assert!(out.with_extension("deliver.json").is_file());
    assert!(out.with_extension("spec-snapshot.json").is_file());

    // A broken spec must not touch the previously committed output.
    let before = std::fs::read(&out).unwrap();
    let bad = tmp("bad.workflow.json");
    std::fs::write(&bad, r#"{"schema_version":2,"diagram_type":"workflow","meta":{"title":"x"},"lanes":[{"id":"l","label":"L"}],"nodes":[{"id":"a","lane":"l","col":9,"type":"backend","label":"A"}]}"#).unwrap();
    let (ok, receipt, _) = run(&[
        "deliver",
        "workflow",
        bad.to_str().unwrap(),
        out.to_str().unwrap(),
        "--json",
    ]);
    assert!(!ok);
    assert_eq!(receipt["committed"], false);
    assert_eq!(std::fs::read(&out).unwrap(), before);
}

#[test]
fn schema_errors_name_the_offending_path() {
    let bad = tmp("unknown-field.architecture.json");
    std::fs::write(&bad, r#"{"schema_version":1,"diagram_type":"architecture","meta":{"title":"x"},"components":[{"id":"a","type":"backend","label":"A","colour":"red"}]}"#).unwrap();
    let (ok, receipt, _) = run(&["validate", "architecture", bad.to_str().unwrap(), "--json"]);
    assert!(!ok);
    assert_eq!(receipt["stage"], "schema");
    assert!(receipt["error"].as_str().unwrap().contains("colour"));
}

#[test]
fn layout_json_exposes_the_compiler_receipt() {
    let (ok, receipt, _) = run(&[
        "validate",
        "workflow",
        example("catchup-runbook.workflow.json").to_str().unwrap(),
        "--layout-json",
        "--json",
    ]);
    assert!(ok);
    assert_eq!(receipt["layout"]["contract"], "readable-v2");
    assert_eq!(receipt["layout"]["nodes"].as_array().unwrap().len(), 8);
    assert!(
        receipt["layout"]["routes"][0]["points"]
            .as_array()
            .unwrap()
            .len()
            >= 2
    );
}

#[test]
fn desktop_readability_rejects_a_diagram_that_is_too_wide_for_its_text() {
    let wide = tmp("wide.architecture.json");
    std::fs::write(&wide, r#"{"schema_version":1,"diagram_type":"architecture","meta":{"title":"wide"},
      "components":[{"id":"a","type":"backend","label":"A","sublabel":"context","pos":[40,40],"size":[120,60]},
                    {"id":"b","type":"backend","label":"B","sublabel":"context","pos":[1600,40],"size":[120,60]}],
      "connections":[{"from":"a","to":"b"}]}"#).unwrap();
    let (ok, receipt, _) = run(&["validate", "architecture", wide.to_str().unwrap(), "--json"]);
    assert!(!ok);
    assert!(
        receipt["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "composition/desktop-readability")
    );
}

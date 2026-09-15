//! archify-rs — validate, deliver, and visually check self-contained HTML
//! diagrams authored as typed JSON.
//!
//! Pipeline: `spec` (parse + schema) → `evidence` (git-verified sources) →
//! `arch` / `workflow` (layout into a `scene`) → `checks` (composition) →
//! `render` (HTML) → `receipt` (hashes) → `visual` (headless Chromium).

mod arch;
mod checks;
mod diag;
mod evidence;
mod geom;
mod receipt;
mod render;
mod route;
mod scene;
mod spec;
mod visual;
mod workflow;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::{Value, json};

use diag::{Check, Diagnostic};
use spec::{DiagramType, Quality, Spec};

const USAGE: &str = "archify-rs — typed JSON in, verified interactive HTML diagram out

USAGE
  archify-rs validate <architecture|workflow> <spec.json> [--quality standard|showcase] [--repo-root DIR] [--json] [--layout-json]
  archify-rs deliver  <architecture|workflow> <spec.json> <out.html> [--quality standard|showcase] [--repo-root DIR] [--json]
  archify-rs visual-check <out.html> [--chrome PATH] [--out-dir DIR] [--json]
  archify-rs doctor [--chrome PATH]

  validate      schema, evidence, layout, and the nine composition checks; nothing is written
  deliver       validate, then render and atomically commit the HTML with SHA-256 receipts
  visual-check  load the delivered HTML in headless Chromium at four desktop viewports, light and dark
  doctor        report what this build can do on this machine

EXIT CODES  0 ok · 1 the artifact did not pass · 2 usage error";

struct Args {
    command: String,
    positional: Vec<String>,
    quality: Option<Quality>,
    repo_root: Option<PathBuf>,
    json: bool,
    layout_json: bool,
    chrome: Option<String>,
    out_dir: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let command = it.next().ok_or_else(|| USAGE.to_string())?;
    let mut a = Args {
        command,
        positional: vec![],
        quality: None,
        repo_root: None,
        json: false,
        layout_json: false,
        chrome: None,
        out_dir: None,
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--json" => a.json = true,
            "--layout-json" => a.layout_json = true,
            "--quality" => {
                let v = it.next().ok_or("--quality needs a value")?;
                a.quality = Some(
                    Quality::parse(&v).ok_or_else(|| format!("unknown quality profile {v:?}"))?,
                );
            }
            "--repo-root" => {
                a.repo_root = Some(PathBuf::from(
                    it.next().ok_or("--repo-root needs a directory")?,
                ))
            }
            "--chrome" => a.chrome = Some(it.next().ok_or("--chrome needs a path")?),
            "--out-dir" => {
                a.out_dir = Some(PathBuf::from(
                    it.next().ok_or("--out-dir needs a directory")?,
                ))
            }
            "-h" | "--help" => return Err(USAGE.to_string()),
            s if s.starts_with("--") => return Err(format!("unknown flag {s}\n\n{USAGE}")),
            _ => a.positional.push(arg),
        }
    }
    Ok(a)
}

struct Validation {
    ok: bool,
    stage: &'static str,
    quality: Quality,
    checks: Vec<Check>,
    diagnostics: Vec<Diagnostic>,
    scene: Option<scene::Scene>,
    spec_bytes: Vec<u8>,
    layout: Option<Value>,
}

fn validate(
    kind: DiagramType,
    input: &Path,
    quality: Option<Quality>,
    repo_root: Option<&Path>,
    want_layout: bool,
) -> Validation {
    let (spec, bytes) = match spec::load(input, kind) {
        Ok(v) => v,
        Err(d) => {
            return Validation {
                ok: false,
                stage: "schema",
                quality: quality.unwrap_or(Quality::Standard),
                checks: vec![],
                diagnostics: d,
                scene: None,
                spec_bytes: vec![],
                layout: None,
            };
        }
    };
    let q = spec.quality(quality);
    let ev = evidence::verify(&spec, repo_root);
    if ev.iter().any(Diagnostic::is_error) {
        return Validation {
            ok: false,
            stage: "evidence",
            quality: q,
            checks: vec![],
            diagnostics: ev,
            scene: None,
            spec_bytes: bytes,
            layout: None,
        };
    }
    let (scene, mut diags) = match &spec {
        Spec::Architecture(a) => arch::compile(a, q),
        Spec::Workflow(w) => workflow::compile(w, q),
    };
    let outcome = checks::run(&scene, q);
    diags.extend(outcome.diagnostics);
    let ok =
        !diags.iter().any(Diagnostic::is_error) && (q == Quality::Standard || diags.is_empty());
    let layout = want_layout.then(|| {
        json!({
            "contract": match spec.diagram_type() { DiagramType::Architecture => "authored-v1", DiagramType::Workflow => "readable-v2" },
            "viewBox": [scene.view_box.0, scene.view_box.1],
            "nodes": scene.nodes.iter().map(|n| json!({ "id": n.id, "x": n.rect.x, "y": n.rect.y, "width": n.rect.w, "height": n.rect.h, "labelFont": n.label_font, "sublabelFont": n.sublabel_font })).collect::<Vec<_>>(),
            "routes": scene.routes.iter().map(|r| json!({ "id": r.id, "from": r.from, "to": r.to, "fromSide": r.from_side.name(), "toSide": r.to_side.name(), "points": r.points.iter().map(|p| [p.x, p.y]).collect::<Vec<_>>(), "label": r.label.as_ref().map(|l| json!({ "text": l.text, "rect": [l.rect.x, l.rect.y, l.rect.w, l.rect.h] })) })).collect::<Vec<_>>(),
        })
    });
    Validation {
        ok,
        stage: "render",
        quality: q,
        checks: outcome.checks,
        diagnostics: diags,
        scene: Some(scene),
        spec_bytes: bytes,
        layout,
    }
}

fn error_summary(v: &Validation) -> Option<String> {
    if v.ok {
        return None;
    }
    let lines: Vec<String> = v
        .diagnostics
        .iter()
        .map(|d| format!("- {}", d.message))
        .collect();
    Some(match v.stage {
        "schema" => format!("schema validation failed:\n{}", lines.join("\n")),
        "evidence" => lines.join("\n").trim_start_matches("- ").to_string(),
        _ => format!(
            "{} layout validation failed:\n{}",
            v.scene
                .as_ref()
                .map_or("diagram", |s| s.diagram_type.name()),
            lines.join("\n")
        ),
    })
}

fn print_validation(
    v: &Validation,
    command: &str,
    kind: DiagramType,
    input: &Path,
    json_out: bool,
    extra: Value,
) {
    let mut receipt = json!({
        "schemaVersion": 1,
        "ok": v.ok,
        "command": command,
        "stage": v.stage,
        "type": kind.name(),
        "input": input.display().to_string(),
        "quality": v.quality.name(),
        "checks": v.checks,
        "diagnostics": v.diagnostics,
    });
    if let Some(s) = &v.scene {
        receipt["viewBox"] = json!([s.view_box.0, s.view_box.1]);
    }
    if let Some(e) = error_summary(v) {
        receipt["error"] = Value::String(e);
    }
    if let Some(l) = &v.layout {
        receipt["layout"] = l.clone();
    }
    if let Value::Object(map) = extra {
        for (k, val) in map {
            receipt[k] = val;
        }
    }
    if json_out {
        println!("{}", serde_json::to_string_pretty(&receipt).unwrap());
        return;
    }
    println!(
        "archify-rs {command} · {} · {}",
        kind.name(),
        input.display()
    );
    println!("quality: {} · stage: {}", v.quality.name(), v.stage);
    for c in &v.checks {
        println!(
            "[{}] {}{}",
            if c.ok { "ok" } else { "FAIL" },
            c.name,
            if c.details.is_empty() {
                String::new()
            } else {
                format!(" — {}", c.details.join("; "))
            }
        );
    }
    for d in &v.diagnostics {
        println!(
            "{}: [{}] {}",
            match d.severity {
                diag::Severity::Error => "error",
                diag::Severity::Warning => "warning",
            },
            d.code,
            d.message
        );
        for f in &d.supported_fixes {
            println!("    fix: {f}");
        }
    }
    if let Some(s) = &v.scene {
        println!("viewBox: {} × {}", s.view_box.0, s.view_box.1);
    }
    if let Some(Value::Object(m)) = receipt.get("artifact").cloned() {
        println!("artifact: {} bytes, sha256 {}", m["bytes"], m["sha256"]);
    }
    println!("{}", if v.ok { "PASS" } else { "FAIL" });
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default(),
        std::process::id()
    ));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    match args.command.as_str() {
        "validate" | "deliver" => {
            let is_deliver = args.command == "deliver";
            let need = if is_deliver { 3 } else { 2 };
            if args.positional.len() < need {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
            let Some(kind) = DiagramType::parse(&args.positional[0]) else {
                eprintln!(
                    "unknown diagram type {:?}; use architecture or workflow",
                    args.positional[0]
                );
                return ExitCode::from(2);
            };
            let input = PathBuf::from(&args.positional[1]);
            let v = validate(
                kind,
                &input,
                args.quality,
                args.repo_root.as_deref(),
                args.layout_json,
            );
            if !is_deliver {
                print_validation(&v, "validate", kind, &input, args.json, json!({}));
                return if v.ok {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(1)
                };
            }
            let output = PathBuf::from(&args.positional[2]);
            if !v.ok {
                print_validation(
                    &v,
                    "deliver",
                    kind,
                    &input,
                    args.json,
                    json!({ "output": output.display().to_string(), "committed": false }),
                );
                return ExitCode::from(1);
            }
            let html = render::html(v.scene.as_ref().unwrap());
            let html_bytes = html.into_bytes();
            let spec_receipt = receipt::FileReceipt::of(&v.spec_bytes);
            let artifact_receipt = receipt::FileReceipt::of(&html_bytes);
            if let Err(e) = write_atomic(&output, &html_bytes) {
                eprintln!("cannot write {}: {e}", output.display());
                return ExitCode::from(1);
            }
            let snapshot = output.with_extension("spec-snapshot.json");
            let _ = write_atomic(&snapshot, &v.spec_bytes);
            let extra = json!({
                "output": output.display().to_string(),
                "committed": true,
                "specification": spec_receipt,
                "artifact": artifact_receipt,
                "snapshot": snapshot.display().to_string(),
            });
            let receipt_path = output.with_extension("deliver.json");
            let _ = write_atomic(&receipt_path, serde_json::to_string_pretty(&json!({ "schemaVersion": 1, "ok": true, "command": "deliver", "type": kind.name(), "input": input.display().to_string(), "quality": v.quality.name(), "checks": v.checks, "output": output.display().to_string(), "specification": extra["specification"], "artifact": extra["artifact"] })).unwrap().as_bytes());
            print_validation(&v, "deliver", kind, &input, args.json, extra);
            ExitCode::SUCCESS
        }
        "visual-check" => {
            let Some(html) = args.positional.first() else {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            };
            let report = visual::run(
                Path::new(html),
                args.chrome.as_deref(),
                args.out_dir.as_deref(),
            );
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report.receipt).unwrap());
            } else {
                let r = &report.receipt;
                println!("archify-rs visual-check · {html}");
                println!("chrome: {}", r["chrome"]["status"]);
                if let Some(vs) = r["containment"]["viewports"].as_array() {
                    for v in vs {
                        println!(
                            "  {}x{} {:<5} scroll {}x{} in {}x{} · text {:.2}px · {}",
                            v["width"],
                            v["height"],
                            v["theme"].as_str().unwrap_or(""),
                            v["scrollWidth"],
                            v["scrollHeight"],
                            v["innerWidth"],
                            v["innerHeight"],
                            v["minimumProjectedNodeTextPx"].as_f64().unwrap_or(0.0),
                            if v["ok"].as_bool().unwrap_or(false)
                                && v["readabilityOk"].as_bool().unwrap_or(false)
                            {
                                "ok"
                            } else {
                                "FAIL"
                            }
                        );
                    }
                }
                if let Some(ds) = r["diagnostics"].as_array() {
                    for d in ds {
                        println!(
                            "{}: [{}] {}",
                            d["severity"].as_str().unwrap_or(""),
                            d["code"].as_str().unwrap_or(""),
                            d["message"].as_str().unwrap_or("")
                        );
                    }
                }
                if let Some(ss) = r["captures"]["screenshots"].as_array() {
                    for s in ss {
                        println!("screenshot: {}", s["path"].as_str().unwrap_or(""));
                    }
                }
                println!("{}", if report.ok { "PASS" } else { "FAIL" });
            }
            if report.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        "doctor" => {
            println!("archify-rs doctor");
            println!(
                "[ok] archify-rs {} ({} diagram types: architecture, workflow)",
                env!("CARGO_PKG_VERSION"),
                2
            );
            println!(
                "[ok] renderer, viewer runtime, and stylesheet are compiled in ({} bytes)",
                include_str!("assets/viewer.css").len() + include_str!("assets/viewer.js").len()
            );
            match std::process::Command::new("git").arg("--version").output() {
                Ok(o) if o.status.success() => println!(
                    "[ok] git available for repository evidence: {}",
                    String::from_utf8_lossy(&o.stdout).trim()
                ),
                _ => println!("[warn] git not found; repository evidence cannot be verified"),
            }
            match visual::find_chrome(args.chrome.as_deref()) {
                Some(p) => println!("[ok] Chromium for visual-check: {}", p.display()),
                None => println!(
                    "[warn] no Chrome/Chromium found; set ARCHIFY_CHROME to enable visual-check"
                ),
            }
            println!("archify-rs is ready.");
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("unknown command {:?}\n\n{USAGE}", args.command);
            ExitCode::from(2)
        }
    }
}

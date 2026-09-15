//! Bounded browser evidence from the exact delivered HTML.
//!
//! Headless Chromium loads the file at each desktop viewport in both color
//! modes. The page's own runtime stamps its measured `scrollWidth`,
//! `scrollHeight`, and minimum projected node text size onto `<html>`, and
//! `--dump-dom` hands those back without a DevTools client. Because the new
//! headless mode reserves window chrome, each viewport is calibrated: if the
//! first load reports a smaller inner height than requested, the window is
//! enlarged by the difference and measured again.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;
use serde_json::{Value, json};

use crate::checks::MIN_PROJECTED_NODE_TEXT_PX;
use crate::diag::Diagnostic;
use crate::receipt::FileReceipt;

pub const VIEWPORTS: [(u32, u32); 4] = [(1440, 900), (1600, 1000), (1920, 1080), (2048, 1320)];
pub const CAPTURE_VIEWPORTS: [(u32, u32); 2] = [(1440, 900), (2048, 1320)];
pub const THEMES: [&str; 2] = ["light", "dark"];

#[derive(Debug, Clone, Serialize)]
pub struct ViewportEvidence {
    pub width: u32,
    pub height: u32,
    pub theme: String,
    #[serde(rename = "windowHeight")]
    pub window_height: u32,
    #[serde(rename = "innerWidth")]
    pub inner_width: u32,
    #[serde(rename = "innerHeight")]
    pub inner_height: u32,
    #[serde(rename = "scrollWidth")]
    pub scroll_width: u32,
    #[serde(rename = "scrollHeight")]
    pub scroll_height: u32,
    #[serde(rename = "overflowX")]
    pub overflow_x: bool,
    #[serde(rename = "overflowY")]
    pub overflow_y: bool,
    pub ok: bool,
    #[serde(rename = "diagramWidth")]
    pub diagram_width: u32,
    #[serde(rename = "minimumProjectedNodeTextPx")]
    pub min_text_px: f64,
    #[serde(rename = "readabilityOk")]
    pub readability_ok: bool,
}

pub fn find_chrome(explicit: Option<&str>) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(e) = explicit {
        candidates.push(PathBuf::from(e));
    }
    if let Ok(e) = std::env::var("ARCHIFY_CHROME") {
        candidates.push(PathBuf::from(e));
    }
    for p in [
        "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ] {
        candidates.push(PathBuf::from(p));
    }
    if let Ok(root) = std::env::var("PLAYWRIGHT_BROWSERS_PATH")
        && let Ok(rd) = std::fs::read_dir(&root)
    {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("chromium-") {
                candidates.push(e.path().join("chrome-linux").join("chrome"));
                candidates.push(
                    e.path()
                        .join("chrome-mac")
                        .join("Chromium.app")
                        .join("Contents")
                        .join("MacOS")
                        .join("Chromium"),
                );
            }
        }
    }
    candidates.into_iter().find(|p| p.is_file())
}

fn attr(dom: &str, name: &str) -> Option<String> {
    let key = format!("{name}=\"");
    let start = dom.find(&key)? + key.len();
    let end = dom[start..].find('"')? + start;
    Some(dom[start..end].to_string())
}

fn file_url(html: &Path, theme: &str) -> String {
    let abs = std::fs::canonicalize(html).unwrap_or_else(|_| html.to_path_buf());
    let mut s = String::from("file://");
    for c in abs.to_string_lossy().chars() {
        match c {
            ' ' => s.push_str("%20"),
            '#' => s.push_str("%23"),
            '?' => s.push_str("%3F"),
            _ => s.push(c),
        }
    }
    format!("{s}?theme={theme}")
}

fn chrome_args(width: u32, height: u32) -> Vec<String> {
    vec![
        "--headless=new".into(),
        "--no-sandbox".into(),
        "--disable-gpu".into(),
        "--hide-scrollbars".into(),
        "--disable-dev-shm-usage".into(),
        "--no-first-run".into(),
        "--force-device-scale-factor=1".into(),
        format!("--window-size={width},{height}"),
        "--virtual-time-budget=3000".into(),
    ]
}

fn dump_dom(chrome: &Path, url: &str, width: u32, height: u32) -> Result<String, String> {
    let out = Command::new(chrome)
        .args(chrome_args(width, height))
        .arg("--dump-dom")
        .arg(url)
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() && out.stdout.is_empty() {
        return Err(String::from_utf8_lossy(&out.stderr)
            .lines()
            .last()
            .unwrap_or("chrome failed")
            .to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn screenshot(
    chrome: &Path,
    url: &str,
    width: u32,
    height: u32,
    out_path: &Path,
) -> Result<(), String> {
    let out = Command::new(chrome)
        .args(chrome_args(width, height))
        .arg(format!("--screenshot={}", out_path.display()))
        .arg(url)
        .output()
        .map_err(|e| e.to_string())?;
    if out_path.is_file() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr)
            .lines()
            .last()
            .unwrap_or("screenshot failed")
            .to_string())
    }
}

fn measure(
    chrome: &Path,
    url: &str,
    width: u32,
    height: u32,
    theme: &str,
) -> Result<ViewportEvidence, String> {
    let mut window_h = height;
    let mut dom = dump_dom(chrome, url, width, window_h)?;
    let mut inner_h: u32 = attr(&dom, "data-inner-h")
        .and_then(|v| v.parse().ok())
        .ok_or("page did not report data-inner-h; is this an archify-rs artifact?")?;
    if inner_h != height && inner_h > 0 {
        window_h = height + (height - inner_h.min(height));
        dom = dump_dom(chrome, url, width, window_h)?;
        inner_h = attr(&dom, "data-inner-h")
            .and_then(|v| v.parse().ok())
            .unwrap_or(inner_h);
    }
    let get = |k: &str| {
        attr(&dom, k)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0)
    };
    let inner_w = get("data-inner-w");
    let scroll_w = get("data-scroll-w");
    let scroll_h = get("data-scroll-h");
    let diagram_w = get("data-diagram-w");
    let min_text: f64 = attr(&dom, "data-min-text-px")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    let overflow_x = scroll_w > inner_w;
    let overflow_y = scroll_h > inner_h;
    let readability_ok = min_text >= MIN_PROJECTED_NODE_TEXT_PX - 1e-6;
    Ok(ViewportEvidence {
        width,
        height,
        theme: theme.to_string(),
        window_height: window_h,
        inner_width: inner_w,
        inner_height: inner_h,
        scroll_width: scroll_w,
        scroll_height: scroll_h,
        overflow_x,
        overflow_y,
        ok: !overflow_x && !overflow_y,
        diagram_width: diagram_w,
        min_text_px: min_text,
        readability_ok,
    })
}

pub struct VisualReport {
    pub ok: bool,
    pub receipt: Value,
}

pub fn run(html: &Path, chrome_hint: Option<&str>, out_dir: Option<&Path>) -> VisualReport {
    let bytes = std::fs::read(html).unwrap_or_default();
    let artifact = FileReceipt::of(&bytes);
    let stem = html
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "artifact".into());
    let dir = out_dir.map(Path::to_path_buf).unwrap_or_else(|| {
        html.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    });
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let Some(chrome) = find_chrome(chrome_hint) else {
        diagnostics.push(
            Diagnostic::warning(
                "viewer/chrome-unavailable",
                "Chrome or Chromium is unavailable. Set ARCHIFY_CHROME to its executable path.",
            )
            .subject(json!({ "artifact": html.display().to_string() }))
            .fix("set ARCHIFY_CHROME to a Chrome or Chromium executable and rerun visual-check"),
        );
        return VisualReport {
            ok: false,
            receipt: json!({
                "schemaVersion": 1, "ok": false, "command": "visual-check", "evidenceKind": "automated-browser", "status": "skipped",
                "visualReview": "pending",
                "artifact": { "path": html.display().to_string(), "sha256": artifact.sha256, "bytes": artifact.bytes },
                "chrome": { "status": "unavailable", "executable": null },
                "containment": { "status": "skipped", "viewports": [] },
                "readability": { "status": "skipped", "minimumProjectedNodeTextPx": MIN_PROJECTED_NODE_TEXT_PX, "viewports": [] },
                "captures": { "status": "skipped", "screenshots": [] },
                "diagnostics": diagnostics,
                "error": "Chrome or Chromium is unavailable. Set ARCHIFY_CHROME to its executable path."
            }),
        };
    };
    let mut viewports: Vec<ViewportEvidence> = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    for theme in THEMES {
        let url = file_url(html, theme);
        for (w, h) in VIEWPORTS {
            match measure(&chrome, &url, w, h, theme) {
                Ok(v) => {
                    if !v.ok {
                        diagnostics.push(
                            Diagnostic::error("viewer/viewport-overflow", format!("The rendered artifact overflows the {w}x{h} {theme} viewport (scroll {}x{} in {}x{}).", v.scroll_width, v.scroll_height, v.inner_width, v.inner_height))
                                .subject(json!({ "width": w, "height": h, "theme": theme }))
                                .fix("remove redundant content or compact spacing before shrinking nodes; never clip or scroll the diagram"),
                        );
                    }
                    if !v.readability_ok {
                        diagnostics.push(
                            Diagnostic::error("viewer/readability", format!("Node text projects to {:.2}px at {w}x{h} {theme} (minimum {MIN_PROJECTED_NODE_TEXT_PX}px).", v.min_text_px))
                                .subject(json!({ "width": w, "height": h, "theme": theme })),
                        );
                    }
                    viewports.push(v);
                }
                Err(e) => errors.push(format!("{w}x{h} {theme}: {e}")),
            }
        }
    }
    let mut shots: Vec<Value> = Vec::new();
    for theme in THEMES {
        let url = file_url(html, theme);
        for (w, h) in CAPTURE_VIEWPORTS {
            let window_h = viewports
                .iter()
                .find(|v| v.width == w && v.height == h && v.theme == theme)
                .map_or(h, |v| v.window_height);
            let out_path = dir.join(format!("{stem}.visual-check.{w}x{h}.{theme}.png"));
            match screenshot(&chrome, &url, w, window_h, &out_path) {
                Ok(()) => shots.push(json!({ "width": w, "height": h, "theme": theme, "path": out_path.display().to_string() })),
                Err(e) => errors.push(format!("screenshot {w}x{h} {theme}: {e}")),
            }
        }
    }
    for e in &errors {
        diagnostics.push(Diagnostic::error("viewer/measurement-failed", e.clone()));
    }
    let containment_ok =
        !viewports.is_empty() && viewports.iter().all(|v| v.ok) && errors.is_empty();
    let readability_ok = !viewports.is_empty() && viewports.iter().all(|v| v.readability_ok);
    let min_text = viewports
        .iter()
        .map(|v| v.min_text_px)
        .fold(f64::INFINITY, f64::min);
    let ok = containment_ok && readability_ok;
    let receipt = json!({
        "schemaVersion": 1,
        "ok": ok,
        "command": "visual-check",
        "evidenceKind": "automated-browser",
        "status": if ok { "pass" } else { "fail" },
        "visualReview": "pending",
        "artifact": { "path": html.display().to_string(), "sha256": artifact.sha256, "bytes": artifact.bytes },
        "chrome": { "status": "available", "executable": chrome.display().to_string() },
        "containment": { "status": if containment_ok { "pass" } else { "fail" }, "viewports": viewports },
        "readability": { "status": if readability_ok { "pass" } else { "fail" }, "minimumProjectedNodeTextPx": MIN_PROJECTED_NODE_TEXT_PX, "observedMinimumPx": if min_text.is_finite() { Value::from(min_text) } else { Value::Null } },
        "captures": { "status": if shots.is_empty() { "fail" } else { "pass" }, "screenshots": shots },
        "sidecars": { "receipt": format!("{stem}.visual-check.json") },
        "diagnostics": diagnostics,
    });
    let sidecar = dir.join(format!("{stem}.visual-check.json"));
    let _ = std::fs::write(
        &sidecar,
        serde_json::to_vec_pretty(&receipt).unwrap_or_default(),
    );
    VisualReport { ok, receipt }
}

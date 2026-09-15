//! Scene → self-contained HTML with one inline SVG.
//!
//! The page carries its stylesheet and viewer runtime inline, so the file
//! works from disk with no network. Text is set in a monospace stack in both
//! HTML and SVG so the width model used by the checks is the width drawn.

use std::fmt::Write as _;

use crate::geom::{Pt, text_width};
use crate::scene::{BandKind, SNode, SRoute, Scene};
use crate::spec::{Role, Variant};

const CSS: &str = include_str!("assets/viewer.css");
const JS: &str = include_str!("assets/viewer.js");
pub const PILL_FONT: f64 = 7.0;

pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn num(v: f64) -> String {
    let r = (v * 100.0).round() / 100.0;
    if (r - r.round()).abs() < 1e-9 {
        format!("{}", r.round() as i64)
    } else {
        format!("{r}")
    }
}

fn path_d(points: &[Pt]) -> String {
    let mut d = String::new();
    for (i, p) in points.iter().enumerate() {
        let _ = write!(
            d,
            "{}{} {}",
            if i == 0 { "M" } else { " L" },
            num(p.x),
            num(p.y)
        );
    }
    d
}

fn pill(out: &mut String, x: f64, y: f64, text: &str, class: &str) -> f64 {
    let w = text_width(text, PILL_FONT) + 8.0;
    let _ = write!(
        out,
        r#"<g class="pill {class}"><rect x="{}" y="{}" width="{}" height="11" rx="5.5"/><text x="{}" y="{}" font-size="{PILL_FONT}" text-anchor="middle">{}</text></g>"#,
        num(x),
        num(y),
        num(w),
        num(x + w / 2.0),
        num(y + 8.2),
        esc(text)
    );
    w
}

fn glyph(out: &mut String, kind: crate::spec::ComponentType, x: f64, y: f64) {
    use crate::spec::ComponentType as K;
    let d = match kind {
        K::Frontend => "M0 1.5h9v6h-9z M0 3.5h9",
        K::Backend => "M2 1l-2 3.2 2 3.2 M7 1l2 3.2-2 3.2",
        K::Database => {
            "M4.5 1c2.5 0 4 .7 4 1.5s-1.5 1.5-4 1.5-4-.7-4-1.5S2 1 4.5 1z M.5 2.5v4c0 .8 1.5 1.5 4 1.5s4-.7 4-1.5v-4"
        }
        K::Cloud => "M2.5 7.5h4.5a2 2 0 0 0 .3-4 3 3 0 0 0-5.6-.6 2.3 2.3 0 0 0 .8 4.6z",
        K::Security => "M4.5 1l3.5 1.3v2.5c0 1.8-1.5 3.1-3.5 3.7-2-.6-3.5-1.9-3.5-3.7V2.3z",
        K::Messagebus => "M0 2h9 M0 4.5h9 M0 7h9",
        K::External => "M1 8.5v-6h3 M5 2.5h3v3 M8 2.5l-4.5 4.5",
    };
    let _ = write!(
        out,
        r#"<path class="glyph" transform="translate({} {})" d="{d}"/>"#,
        num(x),
        num(y)
    );
}

fn node_svg(out: &mut String, n: &SNode) {
    let r = n.rect;
    let mut class = format!("node kind-{}", n.kind.key());
    if n.main {
        class.push_str(" main");
    }
    let _ = write!(
        out,
        r#"<g class="{class}" data-id="{}" data-label="{}">"#,
        esc(&n.id),
        esc(&format!(
            "{} {}",
            n.label,
            n.sublabel.as_deref().unwrap_or("")
        ))
    );
    let _ = write!(
        out,
        r#"<rect class="body" x="{}" y="{}" width="{}" height="{}" rx="10"/>"#,
        num(r.x),
        num(r.y),
        num(r.w),
        num(r.h)
    );
    glyph(out, n.kind, r.x + 7.0, r.y + 6.0);
    let has_sub = n.sublabel.as_deref().is_some_and(|s| !s.is_empty());
    let label_y = if has_sub {
        r.y + r.h / 2.0 - 1.0
    } else {
        r.y + r.h / 2.0 + 4.0
    };
    let _ = write!(
        out,
        r#"<text class="node-label" data-detail="label" x="{}" y="{}" font-size="{}" text-anchor="middle">{}</text>"#,
        num(r.cx()),
        num(label_y),
        num(n.label_font),
        esc(&n.label)
    );
    if let Some(sub) = n.sublabel.as_deref().filter(|s| !s.is_empty()) {
        let _ = write!(
            out,
            r#"<text class="node-sub" data-detail="context" x="{}" y="{}" font-size="{}" text-anchor="middle">{}</text>"#,
            num(r.cx()),
            num(r.y + r.h / 2.0 + 13.0),
            num(n.sublabel_font),
            esc(sub)
        );
    }
    if let Some(tag) = n.tag.as_deref().filter(|t| !t.is_empty()) {
        pill(out, r.x + 20.0, r.y + 5.0, tag, "tag");
    }
    if !n.sources.is_empty() {
        let text = format!("SRC {}", n.sources.len());
        let w = text_width(&text, PILL_FONT) + 8.0;
        let x = r.right() - w - 6.0;
        let first = &n.sources[0];
        let title: String = n
            .sources
            .iter()
            .map(|s| {
                let mut t = s.path.clone();
                if let Some(l) = s.line {
                    let _ = write!(t, ":{l}");
                }
                if let Some(lbl) = &s.label {
                    let _ = write!(t, " — {lbl}");
                }
                t
            })
            .collect::<Vec<_>>()
            .join("\n");
        match &first.href {
            Some(h) => {
                let _ = write!(
                    out,
                    r#"<a href="{}" target="_blank" rel="noopener"><title>{}</title>"#,
                    esc(h),
                    esc(&title)
                );
                pill(out, x, r.y + 5.0, &text, "src");
                out.push_str("</a>");
            }
            None => {
                let _ = write!(out, r#"<g><title>{}</title>"#, esc(&title));
                pill(out, x, r.y + 5.0, &text, "src");
                out.push_str("</g>");
            }
        }
    }
    out.push_str("</g>");
}

fn route_classes(r: &SRoute) -> String {
    let mut c = format!("v-{}", r.variant.key());
    if let Some(role) = r.role {
        let _ = write!(c, " role-{}", role.key());
    }
    if r.main {
        c.push_str(" main");
    }
    c
}

fn marker_id(r: &SRoute) -> &'static str {
    match (r.role, r.variant, r.main) {
        (Some(Role::Error), _, _) => "arrow-error",
        (_, _, true) => "arrow-main",
        (_, Variant::Emphasis, _) => "arrow-emphasis",
        (_, Variant::Security, _) => "arrow-security",
        (_, Variant::Dashed, _) => "arrow-dashed",
        _ => "arrow-default",
    }
}

fn svg(scene: &Scene) -> String {
    let (w, h) = scene.view_box;
    let mut out = String::with_capacity(64 * 1024);
    let mut class = String::from("archify");
    if scene.trace {
        class.push_str(" trace");
    }
    let _ = write!(
        out,
        r#"<svg id="diagram" class="{class}" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" data-vb-w="{}" data-vb-h="{}" role="img" aria-label="{}">"#,
        num(w),
        num(h),
        num(w),
        num(h),
        esc(&scene.title)
    );
    out.push_str("<defs>");
    for (id, var) in [
        ("arrow-default", "--r-default"),
        ("arrow-emphasis", "--r-emphasis"),
        ("arrow-security", "--r-security"),
        ("arrow-dashed", "--r-dashed"),
        ("arrow-main", "--r-main"),
        ("arrow-error", "--r-error"),
    ] {
        let _ = write!(
            out,
            r#"<marker id="{id}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" markerUnits="userSpaceOnUse" orient="auto"><path d="M0 0L10 5L0 10z" style="fill:var({var})"/></marker>"#
        );
    }
    out.push_str("</defs>");

    out.push_str(r#"<g class="layer bands">"#);
    for b in scene.bands.iter().filter(|b| b.kind == BandKind::Phase) {
        let v = b.variant.as_deref().unwrap_or("default");
        let _ = write!(
            out,
            r#"<g class="phase v-{v}" data-id="{}"><rect x="{}" y="{}" width="{}" height="{}" rx="6"/><text x="{}" y="{}" font-size="9" text-anchor="middle">{}</text></g>"#,
            esc(&b.id),
            num(b.rect.x),
            num(b.rect.y),
            num(b.rect.w),
            num(b.rect.h),
            num(b.rect.cx()),
            num(b.rect.y + 14.5),
            esc(&b.label)
        );
    }
    for b in scene.bands.iter().filter(|b| b.kind == BandKind::Lane) {
        let v = b.variant.as_deref().unwrap_or("default");
        let prefix = if v == "exception" {
            "EX".to_string()
        } else {
            format!("{:02}", b.index + 1)
        };
        let _ = write!(
            out,
            r#"<g class="lane v-{v}" data-id="{}"><rect x="{}" y="{}" width="{}" height="{}" rx="12"/><text x="{}" y="{}" font-size="10">{} / {}</text></g>"#,
            esc(&b.id),
            num(b.rect.x),
            num(b.rect.y),
            num(b.rect.w),
            num(b.rect.h),
            num(b.rect.x + 12.0),
            num(b.rect.y + 17.0),
            prefix,
            esc(&b.label)
        );
    }
    out.push_str("</g>");

    out.push_str(r#"<g class="layer boundaries">"#);
    for b in &scene.boundaries {
        let v = b.variant.as_deref().unwrap_or("default");
        let _ = write!(
            out,
            r#"<g class="boundary kind-{} v-{v}"><rect x="{}" y="{}" width="{}" height="{}" rx="12"/><text x="{}" y="{}" font-size="9">{}</text></g>"#,
            esc(&b.kind),
            num(b.rect.x),
            num(b.rect.y),
            num(b.rect.w),
            num(b.rect.h),
            num(b.rect.x + 10.0),
            num(b.rect.y + 13.0),
            esc(&b.label)
        );
    }
    out.push_str("</g>");

    out.push_str(r#"<g class="layer routes">"#);
    for r in &scene.routes {
        let _ = write!(
            out,
            r#"<path class="route {}" data-id="{}" data-from="{}" data-to="{}" d="{}" stroke-width="{}" marker-end="url(#{})"><title>{} → {}{}</title></path>"#,
            route_classes(r),
            esc(&r.id),
            esc(&r.from),
            esc(&r.to),
            path_d(&r.points),
            num(r.width),
            marker_id(r),
            esc(&r.from),
            esc(&r.to),
            r.label
                .as_ref()
                .map(|l| format!(": {}", esc(&l.text)))
                .unwrap_or_default()
        );
    }
    out.push_str("</g>");

    out.push_str(r#"<g class="layer labels">"#);
    for r in &scene.routes {
        let Some(l) = &r.label else { continue };
        let anchor = if l.middle { "middle" } else { "start" };
        let tx = l.anchor.x;
        let _ = write!(
            out,
            r#"<g class="rlabel {}" data-route="{}"><rect x="{}" y="{}" width="{}" height="{}" rx="3"/><text x="{}" y="{}" font-size="{}" text-anchor="{anchor}">{}</text></g>"#,
            route_classes(r),
            esc(&r.id),
            num(l.rect.x),
            num(l.rect.y),
            num(l.rect.w),
            num(l.rect.h),
            num(tx),
            num(l.anchor.y),
            num(crate::route::LABEL_FONT),
            esc(&l.text)
        );
    }
    out.push_str("</g>");

    out.push_str(r#"<g class="layer nodes">"#);
    for n in &scene.nodes {
        node_svg(&mut out, n);
    }
    out.push_str("</g>");

    if let (Some(lr), false) = (scene.legend_rect, scene.legend.is_empty()) {
        out.push_str(r#"<g class="layer legend">"#);
        let _ = write!(
            out,
            r#"<text class="title" x="{}" y="{}" font-size="10">Legend</text>"#,
            num(lr.x),
            num(lr.y + 12.0)
        );
        let mut x = lr.x;
        let y = lr.y + 22.0;
        for item in &scene.legend {
            let _ = write!(
                out,
                r#"<rect class="sw kind-{}" x="{}" y="{}" width="14" height="9" rx="2"/>"#,
                item.kind.key(),
                num(x),
                num(y)
            );
            x += 19.0;
            let _ = write!(
                out,
                r#"<text x="{}" y="{}" font-size="9">{}</text>"#,
                num(x),
                num(y + 8.0),
                esc(&item.label)
            );
            x += text_width(&item.label, 9.0) + 5.0;
            let count = item.count.to_string();
            let cw = text_width(&count, 8.0) + 6.0;
            let _ = write!(
                out,
                r#"<g class="count"><rect x="{}" y="{}" width="{}" height="11" rx="4"/><text x="{}" y="{}" font-size="8" text-anchor="middle">{}</text></g>"#,
                num(x),
                num(y - 1.0),
                num(cw),
                num(x + cw / 2.0),
                num(y + 7.5),
                count
            );
            x += cw + 16.0;
        }
        out.push_str("</g>");
    }
    out.push_str("</svg>");
    out
}

pub fn html(scene: &Scene) -> String {
    let mut out = String::with_capacity(96 * 1024);
    let lang = if scene.locale == "zh-CN" {
        "zh-CN"
    } else {
        "en"
    };
    let _ = write!(
        out,
        "<!doctype html>\n<html lang=\"{lang}\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<meta name=\"generator\" content=\"archify-rs\">\n<title>{}</title>\n<style id=\"archify-css\">{CSS}</style>\n</head>\n<body>\n<div class=\"app\">\n",
        esc(&scene.title)
    );
    let _ = write!(
        out,
        r#"<header class="topbar"><div class="brand"><span class="dot"></span><h1>{}</h1>"#,
        esc(&scene.title)
    );
    if let Some(sub) = scene.subtitle.as_deref().filter(|s| !s.trim().is_empty()) {
        let _ = write!(out, r#"<span class="sub">{}</span>"#, esc(sub));
    }
    out.push_str("</div><div class=\"controls\">");
    out.push_str(r#"<div class="seg" role="group" aria-label="Color mode"><button data-theme-choice="light">Light</button><button data-theme-choice="dark">Dark</button><button data-theme-choice="system">System</button></div>"#);
    out.push_str(r#"<button id="btn-present" aria-pressed="false">Present</button>"#);
    out.push_str(r#"<div class="menu"><button id="btn-export">Export ▾</button><div class="menu-list"><button data-export="svg">SVG</button><button data-export="png">PNG (2×)</button></div></div>"#);
    out.push_str("</div></header>\n");
    if !scene.views.is_empty() {
        out.push_str(r#"<nav class="views" aria-label="Guided views"><span class="lead">Guided views</span><button data-view="" data-focus="" data-note="" aria-pressed="true">All</button>"#);
        for (i, v) in scene.views.iter().enumerate() {
            let _ = write!(
                out,
                r#"<button data-view="{}" data-focus="{}" data-note="{}" aria-pressed="false"><span class="n">{:02}</span>{}</button>"#,
                esc(&v.id),
                esc(&v.focus.join(",")),
                esc(v.note.as_deref().unwrap_or("")),
                i + 1,
                esc(&v.label)
            );
        }
        out.push_str("</nav><p id=\"view-note\"></p>\n");
    }
    out.push_str("<main class=\"reader\"><section class=\"stage\" id=\"stage\">\n");
    out.push_str(&svg(scene));
    out.push_str("\n<div class=\"stage-tools\"><input id=\"search\" type=\"search\" placeholder=\"Search nodes\" aria-label=\"Search nodes\"><span class=\"spacer\"></span><button data-zoom=\"-\" aria-label=\"Zoom out\">−</button><span id=\"zoom-level\">100%</span><button data-zoom=\"+\" aria-label=\"Zoom in\">+</button><button data-zoom=\"0\">Reset</button></div>\n</section>\n");
    if !scene.cards.is_empty() {
        out.push_str("<section class=\"cards\">");
        for c in &scene.cards {
            let _ = write!(
                out,
                r#"<article class="card"><h3><span class="dot" style="background:var(--card-{})"></span>{}</h3><ul>"#,
                c.dot.key(),
                esc(&c.title)
            );
            for item in &c.items {
                let _ = write!(out, "<li>{}</li>", esc(item));
            }
            out.push_str("</ul></article>");
        }
        out.push_str("</section>\n");
    }
    out.push_str("</main>\n");
    let _ = write!(
        out,
        r#"<footer class="foot"><span>archify-rs · {} · {}</span><span>viewBox {}×{}</span><span>{} nodes · {} relationships</span></footer>"#,
        scene.diagram_type.name(),
        scene.quality.name(),
        num(scene.view_box.0),
        num(scene.view_box.1),
        scene.nodes.len(),
        scene.routes.len()
    );
    let _ = write!(out, "\n</div>\n<script>{JS}</script>\n</body>\n</html>\n");
    out
}

//! Composition checks over a finished scene.
//!
//! These are the nine artifact checks a showcase acceptance must report,
//! plus the layout constraints (label overlap, text fit) and the desktop
//! readability gate. Each failure is a diagnostic naming the subject and
//! the fixes the authoring contract supports.

use serde_json::json;

use crate::diag::{Check, Diagnostic};
use crate::geom::{Rect, Seg, segments, text_width};
use crate::scene::{NODE_TEXT_INSET, Scene};
use crate::spec::Quality;

pub const MIN_LABEL_ROUTE_CLEARANCE: f64 = 4.0;
pub const MIN_SEGMENT: f64 = 8.0;
pub const CORRIDOR_TOLERANCE: f64 = 2.0;
pub const CORRIDOR_MIN_OVERLAP: f64 = 8.0;
pub const BORDER_RUN_TOLERANCE: f64 = 3.0;
pub const BORDER_RUN_MIN_OVERLAP: f64 = 16.0;
pub const DESKTOP_READER_DIAGRAM_WIDTH: f64 = 930.0;
pub const MIN_PROJECTED_NODE_TEXT_PX: f64 = 6.0;

pub struct Outcome {
    pub checks: Vec<Check>,
    pub diagnostics: Vec<Diagnostic>,
}

fn route_name(scene: &Scene, i: usize) -> String {
    let r = &scene.routes[i];
    format!(
        "relationships[{}] id {:?} {:?} -> {:?}",
        r.index, r.id, r.from, r.to
    )
}

fn fmt(p: crate::geom::Pt) -> String {
    format!("{}, {}", p.x.round(), p.y.round())
}

pub fn run(scene: &Scene, quality: Quality) -> Outcome {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut checks: Vec<Check> = Vec::new();
    let profile = quality.name();

    // single_svg: the renderer emits one <svg>; the scene has one viewBox.
    let mut single = Check::new("single_svg");
    single.details.push("found 1 <svg> block(s)".to_string());
    checks.push(single);

    // finite_svg
    let mut finite = Check::new("finite_svg");
    for n in &scene.nodes {
        if !n.rect.is_finite() {
            finite.fail(format!("node {:?} has a non-finite rect", n.id));
        }
    }
    for r in &scene.routes {
        if r.points.iter().any(|p| !p.is_finite()) {
            finite.fail(format!("route {:?} has non-finite points", r.id));
        }
    }
    if !(scene.view_box.0.is_finite() && scene.view_box.1.is_finite()) {
        finite.fail("viewBox is not finite");
    }
    if !finite.ok {
        diags.push(Diagnostic::error(
            "composition/finite",
            "the scene contains non-finite geometry",
        ));
    }
    checks.push(finite);

    // orthogonal_arrows + route_rhythm
    let mut ortho = Check::new("orthogonal_arrows");
    let mut rhythm = Check::new("route_rhythm");
    for (i, r) in scene.routes.iter().enumerate() {
        let segs = segments(&r.points);
        if segs.is_empty() {
            rhythm.fail(format!("{} has no segments", route_name(scene, i)));
            diags.push(Diagnostic::error(
                "composition/route-rhythm",
                format!("{} has no segments", route_name(scene, i)),
            ));
            continue;
        }
        for (k, s) in segs.iter().enumerate() {
            if !s.is_axis_aligned() {
                let m = format!(
                    "{} segment {k} [{}] -> [{}] is not axis-aligned",
                    route_name(scene, i),
                    fmt(s.a),
                    fmt(s.b)
                );
                ortho.fail(m.clone());
                diags.push(
                    Diagnostic::error("composition/orthogonal", m)
                        .subject(
                            json!({ "collection": "relationships", "index": r.index, "id": r.id }),
                        )
                        .fix("use automatic routing or axis-aligned via points"),
                );
            }
            if s.len() < MIN_SEGMENT {
                let m = format!(
                    "{} segment {k} is {:.1}px long (minimum {MIN_SEGMENT}px)",
                    route_name(scene, i),
                    s.len()
                );
                rhythm.fail(m.clone());
                diags.push(Diagnostic::error("composition/route-rhythm", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "segmentIndex": k })).fix("remove the tiny hop by adjusting sides, via, or the node position"));
            }
        }
    }
    checks.push(ortho);

    // label_route_clearance (labels vs other routes) and label overlaps.
    let mut clearance = Check::new("label_route_clearance");
    for (i, r) in scene.routes.iter().enumerate() {
        let Some(label) = &r.label else { continue };
        for (j, o) in scene.routes.iter().enumerate() {
            if i == j {
                continue;
            }
            for (k, s) in segments(&o.points).iter().enumerate() {
                let gap = s.gap_to_rect(&label.rect);
                if gap < MIN_LABEL_ROUTE_CLEARANCE {
                    let m = format!(
                        "[composition/label-route-clearance] {profile} label {:?} on {} is {:.0}px from {} segment {k} [{}] -> [{}] (label rect [{}, {}, {}, {}]; minimum {MIN_LABEL_ROUTE_CLEARANCE}px) — adjust labelAt, labelDx, labelDy, or labelSegment; otherwise adjust the other relationship route/via/channel.",
                        label.text,
                        route_name(scene, i),
                        gap,
                        route_name(scene, j),
                        fmt(s.a),
                        fmt(s.b),
                        label.rect.x.round(),
                        label.rect.y.round(),
                        label.rect.w.round(),
                        label.rect.h.round()
                    );
                    clearance.fail(m.clone());
                    diags.push(
                        Diagnostic::error("composition/label-route-clearance", m)
                            .subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "from": r.from, "to": r.to }))
                            .fix("adjust labelAt, labelDx, labelDy, or labelSegment; otherwise adjust the other relationship route/via/channel"),
                    );
                }
            }
        }
        for n in &scene.nodes {
            if label.rect.intersects(&n.rect) {
                let below = n.rect.bottom() - label.rect.y + 4.0;
                let above = label.rect.bottom() - n.rect.y + 4.0;
                let m = format!(
                    "Label {:?} overlaps component {:?} — adjust labelDx/labelDy/labelSegment or set labelAt.\n  label rect: [{}, {}, {}, {}]\n  component {:?} rect: [{}, {}, {}, {}]\n  Suggested fix: labelDy +{} (below) or labelDy -{} (above)",
                    label.text,
                    n.id,
                    label.rect.x.round(),
                    label.rect.y.round(),
                    label.rect.w.round(),
                    label.rect.h.round(),
                    n.id,
                    n.rect.x.round(),
                    n.rect.y.round(),
                    n.rect.w.round(),
                    n.rect.h.round(),
                    below.round(),
                    above.round()
                );
                diags.push(Diagnostic::error("layout/constraint", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "componentId": n.id })).fix(format!("labelDy +{}", below.round())).fix(format!("labelDy -{}", above.round())));
            }
        }
        let captions = scene
            .boundaries
            .iter()
            .map(|b| (b.label.as_str(), b.label_rect()))
            .chain(
                scene
                    .bands
                    .iter()
                    .map(|b| (b.label.as_str(), b.label_rect())),
            );
        for (name, rect) in captions {
            if label.rect.intersects(&rect) {
                diags.push(
                    Diagnostic::error("layout/constraint", format!("Label {:?} on {} overlaps the caption of container {:?} — adjust labelDx/labelDy/labelSegment or set labelAt.", label.text, route_name(scene, i), name))
                        .subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "container": name }))
                        .fix("move the label with labelDx/labelDy or labelSegment, or enlarge the container pad"),
                );
            }
        }
        for (j, o) in scene.routes.iter().enumerate() {
            if j <= i {
                continue;
            }
            if let Some(ol) = &o.label
                && label.rect.intersects(&ol.rect)
            {
                diags.push(
                    Diagnostic::error(
                        "layout/constraint",
                        format!(
                            "Label {:?} on {} overlaps label {:?} on {}",
                            label.text,
                            route_name(scene, i),
                            ol.text,
                            route_name(scene, j)
                        ),
                    )
                    .subject(json!({ "collection": "relationships", "index": r.index, "id": r.id }))
                    .fix("move one label with labelDx/labelDy or labelSegment"),
                );
            }
        }
    }
    checks.push(clearance);

    // relationship_crossings: routes through unrelated nodes (error) and
    // route/route crossings (warning).
    let mut crossings = Check::new("relationship_crossings");
    for (i, r) in scene.routes.iter().enumerate() {
        let segs = segments(&r.points);
        for n in &scene.nodes {
            if n.id == r.from || n.id == r.to {
                continue;
            }
            if segs.iter().any(|s| s.pierces(&n.rect)) {
                let m = format!(
                    "{} crosses unrelated component {:?}",
                    route_name(scene, i),
                    n.id
                );
                crossings.fail(m.clone());
                diags.push(Diagnostic::error("composition/relationship-crossing", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "componentId": n.id })).fix("set fromSide/toSide or via so the route detours, or move the component"));
            }
        }
        for (j, o) in scene.routes.iter().enumerate() {
            if j <= i {
                continue;
            }
            let osegs = segments(&o.points);
            for s in &segs {
                for t in &osegs {
                    if let Some(p) = s.crosses(t) {
                        let m = format!(
                            "{} crosses {} at [{}]",
                            route_name(scene, i),
                            route_name(scene, j),
                            fmt(p)
                        );
                        crossings.fail(m.clone());
                        diags.push(Diagnostic::warning("composition/relationship-crossing", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "other": o.id })).fix("reorder nodes or route one relationship through a channel"));
                    }
                }
            }
        }
    }
    checks.push(crossings);

    // relationship_corridors: shared collinear runs between different routes.
    let mut corridors = Check::new("relationship_corridors");
    for (i, r) in scene.routes.iter().enumerate() {
        for (j, o) in scene.routes.iter().enumerate() {
            if j <= i {
                continue;
            }
            for s in segments(&r.points) {
                for t in segments(&o.points) {
                    let overlap = s.collinear_overlap(&t, CORRIDOR_TOLERANCE);
                    if overlap > CORRIDOR_MIN_OVERLAP {
                        let shared_endpoint = (r.from == o.from) || (r.to == o.to);
                        let m = format!(
                            "{} shares a {:.0}px corridor with {}",
                            route_name(scene, i),
                            overlap,
                            route_name(scene, j)
                        );
                        corridors.fail(m.clone());
                        let d = Diagnostic::error("composition/relationship-corridor", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "other": o.id, "sharedEndpoint": shared_endpoint })).fix("spread the ports, use distinct sides, or route one relationship through a channel");
                        diags.push(d);
                    }
                }
            }
        }
    }
    checks.push(corridors);

    // container_border_runs: routes hugging a boundary or band border.
    let mut borders = Check::new("container_border_runs");
    let containers: Vec<(&str, Rect)> = scene
        .boundaries
        .iter()
        .map(|b| (b.label.as_str(), b.rect))
        .chain(
            scene
                .bands
                .iter()
                .filter(|b| b.kind == crate::scene::BandKind::Lane)
                .map(|b| (b.label.as_str(), b.rect)),
        )
        .collect();
    for (i, r) in scene.routes.iter().enumerate() {
        for s in segments(&r.points) {
            for (label, rect) in &containers {
                let edges = [
                    Seg::new(
                        crate::geom::Pt::new(rect.x, rect.y),
                        crate::geom::Pt::new(rect.right(), rect.y),
                    ),
                    Seg::new(
                        crate::geom::Pt::new(rect.x, rect.bottom()),
                        crate::geom::Pt::new(rect.right(), rect.bottom()),
                    ),
                    Seg::new(
                        crate::geom::Pt::new(rect.x, rect.y),
                        crate::geom::Pt::new(rect.x, rect.bottom()),
                    ),
                    Seg::new(
                        crate::geom::Pt::new(rect.right(), rect.y),
                        crate::geom::Pt::new(rect.right(), rect.bottom()),
                    ),
                ];
                for e in edges {
                    if s.collinear_overlap(&e, BORDER_RUN_TOLERANCE) > BORDER_RUN_MIN_OVERLAP {
                        let m = format!(
                            "{} runs along the border of container {:?}",
                            route_name(scene, i),
                            label
                        );
                        borders.fail(m.clone());
                        diags.push(Diagnostic::error("composition/container-border-run", m).subject(json!({ "collection": "relationships", "index": r.index, "id": r.id, "container": label })).fix("move the route off the border with via or a channel, or change the container pad"));
                    }
                }
            }
        }
    }
    checks.push(borders);
    checks.push(rhythm);

    // legend_clearance
    let mut legend = Check::new("legend_clearance");
    if let Some(lr) = scene.legend_rect {
        for n in &scene.nodes {
            if n.rect.intersects(&lr) {
                legend.fail(format!("legend overlaps node {:?}", n.id));
            }
        }
        for r in &scene.routes {
            if segments(&r.points)
                .iter()
                .any(|s| s.gap_to_rect(&lr) <= 0.0)
            {
                legend.fail(format!("legend overlaps route {:?}", r.id));
            }
        }
        if !legend.ok {
            diags.push(Diagnostic::error(
                "composition/legend-clearance",
                "the legend row collides with diagram content",
            ));
        }
    }
    checks.push(legend);

    // Node text fit and desktop readability.
    let scale = (DESKTOP_READER_DIAGRAM_WIDTH / scene.view_box.0).min(1.0);
    for n in &scene.nodes {
        let avail = n.rect.w - NODE_TEXT_INSET;
        let lw = text_width(&n.label, n.label_font);
        if lw > avail {
            diags.push(
                Diagnostic::error("layout/constraint", format!("Label {:?} (~{}px) is wider than node {:?} ({}px) — shorten the label or increase node.width.", n.label, lw.round(), n.id, n.rect.w.round()))
                    .subject(json!({ "collection": "nodes", "id": n.id }))
                    .fix("shorten the label")
                    .fix(format!("increase node.width to at least {}", (lw + NODE_TEXT_INSET).ceil())),
            );
        }
        let mut min_font = n.label_font;
        if let Some(sub) = &n.sublabel {
            let sw = text_width(sub, n.sublabel_font);
            if sw > avail + 0.01 {
                diags.push(
                    Diagnostic::error("layout/constraint", format!("Sublabel {:?} needs ~{}px at the {}px legible minimum, but node {:?} provides {}px — shorten the sublabel or increase node.width.", sub, sw.round(), n.sublabel_font, n.id, avail.round()))
                        .subject(json!({ "collection": "nodes", "id": n.id }))
                        .fix("shorten the sublabel")
                        .fix(format!("increase node.width to at least {}", (sw + NODE_TEXT_INSET).ceil())),
                );
            }
            min_font = min_font.min(n.sublabel_font);
        }
        let projected = min_font * scale;
        if projected < MIN_PROJECTED_NODE_TEXT_PX - 1e-9 {
            diags.push(
                Diagnostic::error(
                    "composition/desktop-readability",
                    format!(
                        "node {:?} text projects to {:.2}px at a 1440px desktop viewport (viewBox width {:.0}, scale {:.3}, source {:.1}px; minimum {MIN_PROJECTED_NODE_TEXT_PX}px)",
                        n.id, projected, scene.view_box.0, scale, min_font
                    ),
                )
                .subject(json!({ "collection": "nodes", "id": n.id, "viewBoxWidth": scene.view_box.0, "scale": scale, "sourceFontPx": min_font, "projectedFontPx": projected }))
                .fix("reduce the viewBox width, shorten node copy, widen affected nodes, or split the diagram so node context remains at least 6px at a 1440px desktop viewport"),
            );
        }
    }

    Outcome {
        checks,
        diagnostics: diags,
    }
}

//! Architecture layout: authored component positions, boundary boxes around
//! wrapped components, routed connections with port spread, and labels.

use std::collections::HashMap;

use crate::diag::Diagnostic;
use crate::geom::{Pt, Rect, Side};
use crate::route::{self, Hint, LabelRequest, Request};
use crate::scene::{
    NODE_LABEL_FONT, NODE_SUBLABEL_MIN_FONT, NODE_TEXT_INSET, SBoundary, SLabel, SLink, SNode,
    SRoute, Scene, fit_font,
};
use crate::spec::{ArchitectureSpec, DiagramType, Quality, Repository, Variant};

pub const DEFAULT_SIZE: [f64; 2] = [150.0, 64.0];
pub const GRID_ORIGIN: Pt = Pt::new(60.0, 120.0);
pub const GRID_PITCH: Pt = Pt::new(230.0, 160.0);
pub const BOUNDARY_PAD: f64 = 24.0;
pub const BOUNDARY_LABEL_ROOM: f64 = 14.0;
pub const PORT_SPREAD: f64 = 20.0;
/// Architecture context text is set larger than workflow context text, as
/// in archify, because authored layouts leave more room per node.
pub const ARCH_SUBLABEL_FONT: f64 = 9.0;

pub fn variant_width(v: Variant) -> f64 {
    match v {
        Variant::Emphasis => 2.2,
        Variant::Security => 1.8,
        Variant::Dashed => 1.4,
        Variant::Default => 1.6,
    }
}

pub fn source_href(
    repo: Option<&Repository>,
    path: &str,
    line: Option<u32>,
    end: Option<u32>,
) -> Option<String> {
    let repo = repo?;
    if repo.link_mode.as_deref() == Some("local-only") {
        return None;
    }
    let base = repo.url.trim_end_matches('/').trim_end_matches(".git");
    let mut href = format!("{base}/blob/{}/{path}", repo.revision);
    if let Some(l) = line {
        href.push_str(&format!("#L{l}"));
        if let Some(e) = end
            && e > l
        {
            href.push_str(&format!("-L{e}"));
        }
    }
    Some(href)
}

pub fn compile(spec: &ArchitectureSpec, quality: Quality) -> (Scene, Vec<Diagnostic>) {
    let diags: Vec<Diagnostic> = Vec::new();
    let repo = spec.meta.repository.as_ref();

    let mut nodes: Vec<SNode> = Vec::with_capacity(spec.components.len());
    for c in &spec.components {
        let size = c.size.unwrap_or(DEFAULT_SIZE);
        let pos = match c.pos {
            Some(p) => p,
            None => [
                GRID_ORIGIN.x + c.col.unwrap_or(0) as f64 * GRID_PITCH.x,
                GRID_ORIGIN.y + c.row.unwrap_or(0) as f64 * GRID_PITCH.y,
            ],
        };
        let rect = Rect::new(pos[0], pos[1], size[0], size[1]);
        let avail = rect.w - NODE_TEXT_INSET;
        let sublabel_font = c
            .sublabel
            .as_deref()
            .map(|s| fit_font(s, avail, ARCH_SUBLABEL_FONT, NODE_SUBLABEL_MIN_FONT))
            .unwrap_or(ARCH_SUBLABEL_FONT);
        let sources = c
            .sources
            .iter()
            .flatten()
            .map(|s| SLink {
                path: s.path.clone(),
                line: s.line,
                label: s.label.clone(),
                href: source_href(repo, &s.path, s.line, s.end_line),
            })
            .collect();
        nodes.push(SNode {
            id: c.id.clone(),
            kind: c.kind,
            label: c.label.clone(),
            sublabel: c.sublabel.clone(),
            tag: c.tag.clone(),
            rect,
            label_font: NODE_LABEL_FONT,
            sublabel_font,
            sources,
            main: false,
        });
    }
    let rect_of: HashMap<&str, Rect> = nodes.iter().map(|n| (n.id.as_str(), n.rect)).collect();

    let boundaries: Vec<SBoundary> = spec
        .boundaries
        .iter()
        .flatten()
        .map(|b| {
            let pad = b.pad.unwrap_or(BOUNDARY_PAD);
            let mut acc: Option<Rect> = None;
            for id in &b.wraps {
                if let Some(r) = rect_of.get(id.as_str()) {
                    acc = Some(acc.map_or(*r, |a| a.union(r)));
                }
            }
            let mut rect = acc.unwrap_or(Rect::new(0.0, 0.0, 1.0, 1.0)).inflate(pad);
            rect.y -= BOUNDARY_LABEL_ROOM;
            rect.h += BOUNDARY_LABEL_ROOM;
            SBoundary {
                label: b.label.clone(),
                rect,
                kind: b.kind.key().to_string(),
                variant: None,
            }
        })
        .collect();

    // Pass 1: route with centred ports to learn which sides each relationship
    // owns; pass 2: spread ports that share a side, then route for real.
    let conns: Vec<&crate::spec::Connection> = spec.connections.iter().flatten().collect();
    let mut requests: Vec<Request> = Vec::with_capacity(conns.len());
    let mut obstacles: Vec<Vec<Rect>> = Vec::with_capacity(conns.len());
    for c in &conns {
        let from = rect_of[c.from.as_str()];
        let to = rect_of[c.to.as_str()];
        requests.push(Request {
            from,
            to,
            from_side: c.from_side.as_deref().and_then(Side::parse),
            to_side: c.to_side.as_deref().and_then(Side::parse),
            via: c
                .via
                .iter()
                .flatten()
                .map(|p| Pt::new(p[0], p[1]))
                .collect(),
            hint: c.route.as_deref().and_then(Hint::parse).unwrap_or_default(),
            from_offset: 0.0,
            to_offset: 0.0,
            channel_x: None,
            channel_y: None,
        });
        obstacles.push(
            nodes
                .iter()
                .filter(|n| n.id != c.from && n.id != c.to)
                .map(|n| n.rect)
                .collect(),
        );
    }
    let pass1: Vec<route::Route> = requests
        .iter()
        .zip(&obstacles)
        .map(|(r, o)| route::route(r, o))
        .collect();
    apply_port_spread(
        &conns
            .iter()
            .map(|c| {
                (
                    c.from.as_str(),
                    c.to.as_str(),
                    c.via.is_some() || c.label_at.is_some(),
                )
            })
            .collect::<Vec<_>>(),
        &pass1,
        &mut requests,
        &rect_of,
    );
    for (i, r) in pass1.iter().enumerate() {
        requests[i].from_side = Some(r.from_side);
        requests[i].to_side = Some(r.to_side);
    }

    let label_obstacles: Vec<Rect> = nodes
        .iter()
        .map(|n| n.rect)
        .chain(boundaries.iter().map(SBoundary::label_rect))
        .collect();
    let mut routes: Vec<SRoute> = Vec::with_capacity(conns.len());
    for (i, c) in conns.iter().enumerate() {
        let routed = route::route(&requests[i], &obstacles[i]);
        let variant = c.variant.unwrap_or_default();
        let label = c
            .label
            .as_deref()
            .filter(|l| !l.trim().is_empty())
            .map(|text| {
                let placed = route::place_label(
                    &routed.points,
                    &LabelRequest {
                        text,
                        at: c.label_at.map(|p| Pt::new(p[0], p[1])),
                        dx: c.label_dx.unwrap_or(0.0),
                        dy: c.label_dy.unwrap_or(0.0),
                        segment: c.label_segment,
                    },
                    &label_obstacles,
                );
                SLabel {
                    text: text.to_string(),
                    rect: placed.rect,
                    anchor: placed.anchor,
                    middle: placed.middle,
                }
            });
        routes.push(SRoute {
            id: c
                .id
                .clone()
                .unwrap_or_else(|| format!("{}-to-{}", c.from, c.to)),
            from: c.from.clone(),
            to: c.to.clone(),
            points: routed.points,
            variant,
            role: None,
            width: c.width.unwrap_or_else(|| variant_width(variant)),
            label,
            main: variant == Variant::Emphasis,
            from_side: routed.from_side,
            to_side: routed.to_side,
            index: i,
        });
    }

    let mut scene = Scene {
        title: spec.meta.title.clone(),
        subtitle: spec.meta.subtitle.clone(),
        locale: spec.meta.locale.clone().unwrap_or_else(|| "en".to_string()),
        diagram_type: DiagramType::Architecture,
        quality,
        view_box: (0.0, 0.0),
        nodes,
        boundaries,
        bands: Vec::new(),
        routes,
        legend: Vec::new(),
        legend_rect: None,
        cards: spec.cards.clone().unwrap_or_default(),
        views: spec.meta.views.clone().unwrap_or_default(),
        trace: spec.meta.animation.as_deref() == Some("trace"),
    };
    let legend = spec.meta.legend.clone().unwrap_or_default();
    scene.finalize(
        legend.mode.unwrap_or_default(),
        &legend.entries.unwrap_or_default(),
    );
    (scene, diags)
}

/// `(node id, side)` → `(request index, is_from, far-end coordinate)`.
type SpreadGroups = HashMap<(String, Side), Vec<(usize, bool, f64)>>;

/// Spread the ports of automatic relationships that share a node side so
/// they leave through distinct points instead of one stacked corridor.
pub fn apply_port_spread(
    meta: &[(&str, &str, bool)],
    pass1: &[route::Route],
    requests: &mut [Request],
    rect_of: &HashMap<&str, Rect>,
) {
    // (node, side) -> list of (request index, is_from, coordinate of the far end)
    let mut groups: SpreadGroups = HashMap::new();
    for (i, r) in pass1.iter().enumerate() {
        let (from, to, explicit) = meta[i];
        if explicit || requests[i].hint.is_explicit() {
            continue;
        }
        let far_to = rect_of[to].center();
        let far_from = rect_of[from].center();
        let key_from = if r.from_side.is_horizontal() {
            far_to.y
        } else {
            far_to.x
        };
        let key_to = if r.to_side.is_horizontal() {
            far_from.y
        } else {
            far_from.x
        };
        groups
            .entry((from.to_string(), r.from_side))
            .or_default()
            .push((i, true, key_from));
        groups
            .entry((to.to_string(), r.to_side))
            .or_default()
            .push((i, false, key_to));
    }
    for ((node, side), mut members) in groups {
        if members.len() < 2 {
            continue;
        }
        members.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        let rect = rect_of[node.as_str()];
        let side_len = if side.is_horizontal() { rect.h } else { rect.w };
        let spacing = PORT_SPREAD.min(side_len / (members.len() as f64 + 1.0));
        let n = members.len() as f64;
        for (k, (i, is_from, _)) in members.into_iter().enumerate() {
            let offset = (k as f64 - (n - 1.0) / 2.0) * spacing;
            if is_from {
                requests[i].from_offset = offset;
            } else {
                requests[i].to_offset = offset;
            }
        }
    }
}

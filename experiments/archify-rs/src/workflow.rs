//! Workflow layout: lanes and columns compile into a readable grid, phases
//! and groups become bands and boxes, and edges route through the shared
//! router with the main path emphasised.

use std::collections::{HashMap, HashSet};

use crate::diag::Diagnostic;
use crate::geom::{Pt, Rect, Side};
use crate::route::{self, Hint, LabelRequest, Request};
use crate::scene::{
    BandKind, NODE_LABEL_FONT, NODE_SUBLABEL_FONT, NODE_SUBLABEL_MIN_FONT, NODE_TEXT_INSET, SBand,
    SBoundary, SLabel, SNode, SRoute, Scene, fit_font,
};
use crate::spec::{DiagramType, Quality, Role, WorkflowSpec};

pub const DEFAULT_NODE_W: f64 = 92.0;
pub const DEFAULT_NODE_H: f64 = 52.0;
pub const LEFT_MARGIN: f64 = 24.0;
pub const TOP_MARGIN: f64 = 24.0;
pub const PHASE_H: f64 = 22.0;
pub const PHASE_GAP: f64 = 18.0;
pub const LANE_INNER_X: f64 = 20.0;
pub const LANE_LABEL_H: f64 = 26.0;
pub const LANE_PAD_Y: f64 = 22.0;
pub const LANE_GAP: f64 = 18.0;
pub const COL_GAP: f64 = 46.0;
pub const LABEL_GAP_ROOM: f64 = 16.0;
pub const GROUP_INSET: f64 = 10.0;

pub fn compile(spec: &WorkflowSpec, quality: Quality) -> (Scene, Vec<Diagnostic>) {
    let diags: Vec<Diagnostic> = Vec::new();
    let ncols = (crate::spec::MAX_WORKFLOW_COL + 1) as usize;
    let used_cols = spec
        .nodes
        .iter()
        .map(|n| n.col as usize)
        .max()
        .map_or(1, |m| m + 1)
        .min(ncols);

    let mut col_w = vec![DEFAULT_NODE_W; used_cols];
    for n in &spec.nodes {
        let c = n.col as usize;
        col_w[c] = col_w[c].max(n.width.unwrap_or(DEFAULT_NODE_W));
    }
    // Gaps between adjacent columns reserve room for the labels of same-lane
    // edges that cross them, so a label never has to sit on a node.
    let mut gap = vec![COL_GAP; used_cols.saturating_sub(1).max(1)];
    let lane_of: HashMap<&str, &str> = spec
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.lane.as_str()))
        .collect();
    let col_of: HashMap<&str, usize> = spec
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.col as usize))
        .collect();
    for e in spec.edges.iter().flatten() {
        let (Some(text), Some(&a), Some(&b)) = (
            e.label.as_deref(),
            col_of.get(e.from.as_str()),
            col_of.get(e.to.as_str()),
        ) else {
            continue;
        };
        if lane_of.get(e.from.as_str()) != lane_of.get(e.to.as_str())
            || a.abs_diff(b) != 1
            || e.label_at.is_some()
        {
            continue;
        }
        let need =
            crate::geom::text_width(text, route::LABEL_FONT) + route::LABEL_PAD + LABEL_GAP_ROOM;
        let g = a.min(b);
        gap[g] = gap[g].max(need);
    }
    let mut col_x = vec![0.0; used_cols];
    let mut x = LEFT_MARGIN + LANE_INNER_X;
    for c in 0..used_cols {
        col_x[c] = x;
        x += col_w[c] + gap.get(c).copied().unwrap_or(COL_GAP);
    }
    let total_w = x - gap
        .get(used_cols.saturating_sub(1))
        .copied()
        .unwrap_or(COL_GAP)
        + LANE_INNER_X;

    let lane_index: HashMap<&str, usize> = spec
        .lanes
        .iter()
        .enumerate()
        .map(|(i, l)| (l.id.as_str(), i))
        .collect();
    let mut lane_node_h = vec![DEFAULT_NODE_H; spec.lanes.len()];
    for n in &spec.nodes {
        let li = lane_index[n.lane.as_str()];
        lane_node_h[li] = lane_node_h[li].max(n.height.unwrap_or(DEFAULT_NODE_H));
    }
    let has_phases = spec.phases.as_ref().is_some_and(|p| !p.is_empty());
    let mut lane_y = vec![0.0; spec.lanes.len()];
    let mut lane_h = vec![0.0; spec.lanes.len()];
    let mut y = TOP_MARGIN + if has_phases { PHASE_H + PHASE_GAP } else { 0.0 };
    for (i, _) in spec.lanes.iter().enumerate() {
        lane_y[i] = y;
        lane_h[i] = LANE_LABEL_H + lane_node_h[i] + 2.0 * LANE_PAD_Y;
        y += lane_h[i] + LANE_GAP;
    }

    let main_path: Vec<&str> = spec
        .main_path
        .iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let main_nodes: HashSet<&str> = main_path.iter().copied().collect();
    let main_edges: HashSet<(&str, &str)> = main_path.windows(2).map(|w| (w[0], w[1])).collect();

    let mut nodes: Vec<SNode> = Vec::with_capacity(spec.nodes.len());
    for n in &spec.nodes {
        let li = lane_index[n.lane.as_str()];
        let c = n.col as usize;
        let w = n.width.unwrap_or(DEFAULT_NODE_W);
        let h = n.height.unwrap_or(DEFAULT_NODE_H);
        let rect = Rect::new(
            col_x[c] + (col_w[c] - w) / 2.0,
            lane_y[li]
                + LANE_LABEL_H
                + LANE_PAD_Y
                + (lane_node_h[li] - h) / 2.0
                + n.y_offset.unwrap_or(0.0),
            w,
            h,
        );
        let sublabel_font = n
            .sublabel
            .as_deref()
            .map(|s| {
                fit_font(
                    s,
                    rect.w - NODE_TEXT_INSET,
                    NODE_SUBLABEL_FONT,
                    NODE_SUBLABEL_MIN_FONT,
                )
            })
            .unwrap_or(NODE_SUBLABEL_FONT);
        nodes.push(SNode {
            id: n.id.clone(),
            kind: n.kind,
            label: n.label.clone(),
            sublabel: n.sublabel.clone(),
            tag: n.tag.clone(),
            rect,
            label_font: NODE_LABEL_FONT,
            sublabel_font,
            sources: Vec::new(),
            main: main_nodes.contains(n.id.as_str()),
        });
    }
    let rect_of: HashMap<&str, Rect> = nodes.iter().map(|n| (n.id.as_str(), n.rect)).collect();

    let mut bands: Vec<SBand> = Vec::new();
    for (i, l) in spec.lanes.iter().enumerate() {
        bands.push(SBand {
            kind: BandKind::Lane,
            id: l.id.clone(),
            label: l.label.clone(),
            rect: Rect::new(LEFT_MARGIN, lane_y[i], total_w, lane_h[i]),
            variant: l.variant.clone(),
            index: i,
        });
    }
    for (i, p) in spec.phases.iter().flatten().enumerate() {
        let a = (p.from_col as usize).min(used_cols - 1);
        let b = (p.to_col as usize).min(used_cols - 1);
        let x0 = col_x[a] - 8.0;
        let x1 = col_x[b] + col_w[b] + 8.0;
        bands.push(SBand {
            kind: BandKind::Phase,
            id: p.id.clone(),
            label: p.label.clone(),
            rect: Rect::new(x0, TOP_MARGIN, x1 - x0, PHASE_H),
            variant: p.variant.clone(),
            index: i,
        });
    }
    let boundaries: Vec<SBoundary> = spec
        .groups
        .iter()
        .flatten()
        .map(|g| {
            let li = lane_index[g.lane.as_str()];
            let a = (g.from_col as usize).min(used_cols - 1);
            let b = (g.to_col as usize).min(used_cols - 1);
            let x0 = col_x[a] - GROUP_INSET;
            let x1 = col_x[b] + col_w[b] + GROUP_INSET;
            let y0 = lane_y[li] + LANE_LABEL_H + 4.0;
            let y1 = lane_y[li] + lane_h[li] - 6.0;
            SBoundary {
                label: g.label.clone(),
                rect: Rect::new(x0, y0, x1 - x0, y1 - y0),
                kind: "group".to_string(),
                variant: g.variant.clone(),
            }
        })
        .collect();

    let edges: Vec<&crate::spec::WorkflowEdge> = spec.edges.iter().flatten().collect();
    let mut requests: Vec<Request> = Vec::with_capacity(edges.len());
    let mut obstacles: Vec<Vec<Rect>> = Vec::with_capacity(edges.len());
    for e in &edges {
        requests.push(Request {
            from: rect_of[e.from.as_str()],
            to: rect_of[e.to.as_str()],
            from_side: e.from_side.as_deref().and_then(Side::parse),
            to_side: e.to_side.as_deref().and_then(Side::parse),
            via: e
                .via
                .iter()
                .flatten()
                .map(|p| Pt::new(p[0], p[1]))
                .collect(),
            hint: e.route.as_deref().and_then(Hint::parse).unwrap_or_default(),
            from_offset: 0.0,
            to_offset: 0.0,
            channel_x: e.channel_x,
            channel_y: e.channel_y,
        });
        obstacles.push(
            nodes
                .iter()
                .filter(|n| n.id != e.from && n.id != e.to)
                .map(|n| n.rect)
                .collect(),
        );
    }
    let pass1: Vec<route::Route> = requests
        .iter()
        .zip(&obstacles)
        .map(|(r, o)| route::route(r, o))
        .collect();
    let meta: Vec<(&str, &str, bool)> = edges
        .iter()
        .map(|e| {
            (
                e.from.as_str(),
                e.to.as_str(),
                e.via.is_some()
                    || e.label_at.is_some()
                    || e.channel_x.is_some()
                    || e.channel_y.is_some(),
            )
        })
        .collect();
    crate::arch::apply_port_spread(&meta, &pass1, &mut requests, &rect_of);
    for (i, r) in pass1.iter().enumerate() {
        requests[i].from_side = Some(r.from_side);
        requests[i].to_side = Some(r.to_side);
    }

    let label_obstacles: Vec<Rect> = nodes
        .iter()
        .map(|n| n.rect)
        .chain(boundaries.iter().map(SBoundary::label_rect))
        .chain(bands.iter().map(SBand::label_rect))
        .collect();
    let mut routes: Vec<SRoute> = Vec::with_capacity(edges.len());
    for (i, e) in edges.iter().enumerate() {
        let routed = route::route(&requests[i], &obstacles[i]);
        let variant = e.variant.unwrap_or_default();
        let main = main_edges.contains(&(e.from.as_str(), e.to.as_str()));
        let label = e
            .label
            .as_deref()
            .filter(|l| !l.trim().is_empty())
            .map(|text| {
                let placed = route::place_label(
                    &routed.points,
                    &LabelRequest {
                        text,
                        at: e.label_at.map(|p| Pt::new(p[0], p[1])),
                        dx: e.label_dx.unwrap_or(0.0),
                        dy: e.label_dy.unwrap_or(0.0),
                        segment: e.label_segment,
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
        let width = e.width.unwrap_or(if main {
            2.4
        } else {
            crate::arch::variant_width(variant)
        });
        routes.push(SRoute {
            id: e
                .id
                .clone()
                .unwrap_or_else(|| format!("{}-to-{}", e.from, e.to)),
            from: e.from.clone(),
            to: e.to.clone(),
            points: routed.points,
            variant,
            role: e.role.or(if main { Some(Role::Main) } else { None }),
            width,
            label,
            main,
            from_side: routed.from_side,
            to_side: routed.to_side,
            index: i,
        });
    }

    let mut scene = Scene {
        title: spec.meta.title.clone(),
        subtitle: spec.meta.subtitle.clone(),
        locale: spec.meta.locale.clone().unwrap_or_else(|| "en".to_string()),
        diagram_type: DiagramType::Workflow,
        quality,
        view_box: (0.0, 0.0),
        nodes,
        boundaries,
        bands,
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

//! The renderer-neutral scene every diagram type compiles into.
//!
//! Layout stages produce a `Scene`; the composition checks and the HTML
//! renderer both consume it. Nothing downstream knows whether the scene came
//! from an architecture map or a workflow.

use crate::geom::{Pt, Rect, Side};
use crate::spec::{Card, ComponentType, DiagramType, LegendMode, Quality, Role, Variant, View};

pub const NODE_LABEL_FONT: f64 = 11.0;
pub const NODE_SUBLABEL_FONT: f64 = 8.0;
pub const NODE_SUBLABEL_MIN_FONT: f64 = 6.0;
pub const NODE_TEXT_INSET: f64 = 8.0;
pub const CANVAS_PAD: f64 = 40.0;
pub const LEGEND_HEIGHT: f64 = 46.0;

#[derive(Debug, Clone)]
pub struct SLink {
    pub path: String,
    pub line: Option<u32>,
    pub label: Option<String>,
    pub href: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SNode {
    pub id: String,
    pub kind: ComponentType,
    pub label: String,
    pub sublabel: Option<String>,
    pub tag: Option<String>,
    pub rect: Rect,
    pub label_font: f64,
    pub sublabel_font: f64,
    pub sources: Vec<SLink>,
    pub main: bool,
}

#[derive(Debug, Clone)]
pub struct SBoundary {
    pub label: String,
    pub rect: Rect,
    pub kind: String,
    pub variant: Option<String>,
}

pub const CONTAINER_LABEL_FONT: f64 = 9.0;
pub const LANE_LABEL_FONT: f64 = 10.0;

impl SBoundary {
    /// Where the boundary's caption is drawn (top-left, inside the box).
    pub fn label_rect(&self) -> Rect {
        Rect::new(
            self.rect.x + 8.0,
            self.rect.y + 3.0,
            crate::geom::text_width(&self.label, CONTAINER_LABEL_FONT) + 6.0,
            13.0,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandKind {
    Lane,
    Phase,
}

#[derive(Debug, Clone)]
pub struct SBand {
    pub kind: BandKind,
    pub id: String,
    pub label: String,
    pub rect: Rect,
    pub variant: Option<String>,
    pub index: usize,
}

impl SBand {
    /// Where the lane caption is drawn; phases centre their caption instead.
    pub fn label_rect(&self) -> Rect {
        match self.kind {
            BandKind::Lane => Rect::new(
                self.rect.x + 10.0,
                self.rect.y + 6.0,
                crate::geom::text_width(&format!("00 / {}", self.label), LANE_LABEL_FONT) + 6.0,
                14.0,
            ),
            BandKind::Phase => {
                let w = crate::geom::text_width(&self.label, 9.0) + 6.0;
                Rect::new(self.rect.cx() - w / 2.0, self.rect.y + 4.0, w, 13.0)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SLabel {
    pub text: String,
    pub rect: Rect,
    pub anchor: Pt,
    pub middle: bool,
}

#[derive(Debug, Clone)]
pub struct SRoute {
    pub id: String,
    pub from: String,
    pub to: String,
    pub points: Vec<Pt>,
    pub variant: Variant,
    pub role: Option<Role>,
    pub width: f64,
    pub label: Option<SLabel>,
    pub main: bool,
    pub from_side: Side,
    pub to_side: Side,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct LegendItem {
    pub kind: ComponentType,
    pub label: String,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct Scene {
    pub title: String,
    pub subtitle: Option<String>,
    pub locale: String,
    pub diagram_type: DiagramType,
    pub quality: Quality,
    pub view_box: (f64, f64),
    pub nodes: Vec<SNode>,
    pub boundaries: Vec<SBoundary>,
    pub bands: Vec<SBand>,
    pub routes: Vec<SRoute>,
    pub legend: Vec<LegendItem>,
    pub legend_rect: Option<Rect>,
    pub cards: Vec<Card>,
    pub views: Vec<View>,
    pub trace: bool,
}

/// Largest font in `[minimum, preferred]` at which `text` fits `avail` px;
/// returns `minimum` when even that overflows so the checks can report it.
pub fn fit_font(text: &str, avail: f64, preferred: f64, minimum: f64) -> f64 {
    let chars = text.chars().count().max(1) as f64;
    let ideal = avail / (chars * crate::geom::CHAR_ADVANCE_EM);
    ideal.min(preferred).max(minimum)
}

impl Scene {
    /// Bounding box of everything drawn except the legend.
    pub fn content_bounds(&self) -> Rect {
        let mut acc: Option<Rect> = None;
        let mut push = |r: Rect| {
            acc = Some(match acc {
                Some(a) => a.union(&r),
                None => r,
            });
        };
        for n in &self.nodes {
            push(n.rect);
        }
        for b in &self.boundaries {
            push(b.rect);
        }
        for b in &self.bands {
            push(b.rect);
        }
        for r in &self.routes {
            for w in r.points.windows(2) {
                push(Rect::from_points(w[0], w[1]));
            }
            if let Some(l) = &r.label {
                push(l.rect);
            }
        }
        acc.unwrap_or(Rect::new(0.0, 0.0, 1.0, 1.0))
    }

    /// Build the legend from the node kinds in use and the authored overrides,
    /// then size the viewBox around content, padding, and the legend row.
    pub fn finalize(
        &mut self,
        mode: LegendMode,
        overrides: &std::collections::BTreeMap<String, crate::spec::LegendEntry>,
    ) {
        let mut legend = Vec::new();
        if mode != LegendMode::Hidden {
            for kind in ComponentType::ALL {
                let count = self.nodes.iter().filter(|n| n.kind == kind).count();
                let entry = overrides.get(kind.key());
                let visible = entry.and_then(|e| e.visible).unwrap_or(true);
                if !visible || (mode == LegendMode::Auto && count == 0) {
                    continue;
                }
                let label = entry
                    .and_then(|e| e.label.clone())
                    .unwrap_or_else(|| kind.default_label().to_string());
                legend.push(LegendItem { kind, label, count });
            }
        }
        self.legend = legend;
        let b = self.content_bounds();
        let width = b.right() + CANVAS_PAD;
        let mut height = b.bottom() + CANVAS_PAD;
        if !self.legend.is_empty() {
            self.legend_rect = Some(Rect::new(
                CANVAS_PAD,
                height - 8.0,
                width - 2.0 * CANVAS_PAD,
                LEGEND_HEIGHT,
            ));
            height += LEGEND_HEIGHT;
        }
        self.view_box = (width.max(320.0).ceil(), height.max(200.0).ceil());
    }
}

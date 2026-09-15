//! Orthogonal relationship routing and label placement.
//!
//! A route leaves its source through one side and enters its target through
//! one side; the first and last segments are perpendicular to those sides.
//! When a side is not authored, candidates are ranked by how many unrelated
//! nodes they pierce, then by bends, then by length, so the router prefers
//! a clean detour over a short line through a stranger.

use crate::geom::{Pt, Rect, Seg, Side, has_reversal, segments, simplify, text_width};

pub const STUB: f64 = 20.0;
pub const CHANNEL_MARGIN: f64 = 36.0;
pub const LABEL_FONT: f64 = 8.0;
pub const LABEL_HEIGHT: f64 = 14.0;
pub const LABEL_PAD: f64 = 6.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Hint {
    #[default]
    Auto,
    Straight,
    OrthogonalH,
    OrthogonalV,
    Drop,
    OutsideRight,
    ReturnLeft,
    BottomChannel,
    UpChannel,
}

impl Hint {
    pub fn parse(s: &str) -> Option<Hint> {
        match s {
            "auto" => Some(Hint::Auto),
            "straight" => Some(Hint::Straight),
            "orthogonal-h" => Some(Hint::OrthogonalH),
            "orthogonal-v" => Some(Hint::OrthogonalV),
            "drop" => Some(Hint::Drop),
            "outside-right" => Some(Hint::OutsideRight),
            "return-left" => Some(Hint::ReturnLeft),
            "bottom-channel" => Some(Hint::BottomChannel),
            "up-channel" => Some(Hint::UpChannel),
            _ => None,
        }
    }

    /// Explicit geometry controls opt a relationship out of port spread.
    pub fn is_explicit(self) -> bool {
        !matches!(self, Hint::Auto)
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub from: Rect,
    pub to: Rect,
    pub from_side: Option<Side>,
    pub to_side: Option<Side>,
    pub via: Vec<Pt>,
    pub hint: Hint,
    pub from_offset: f64,
    pub to_offset: f64,
    pub channel_x: Option<f64>,
    pub channel_y: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct Route {
    pub points: Vec<Pt>,
    pub from_side: Side,
    pub to_side: Side,
}

pub fn port(rect: &Rect, side: Side, offset: f64) -> Pt {
    match side {
        Side::Left => Pt::new(rect.x, rect.cy() + offset),
        Side::Right => Pt::new(rect.right(), rect.cy() + offset),
        Side::Top => Pt::new(rect.cx() + offset, rect.y),
        Side::Bottom => Pt::new(rect.cx() + offset, rect.bottom()),
    }
}

fn axis_gaps(from: &Rect, to: &Rect) -> (f64, f64) {
    let x_gap = if to.x >= from.right() {
        to.x - from.right()
    } else if from.x >= to.right() {
        from.x - to.right()
    } else {
        -1.0
    };
    let y_gap = if to.y >= from.bottom() {
        to.y - from.bottom()
    } else if from.y >= to.bottom() {
        from.y - to.bottom()
    } else {
        -1.0
    };
    (x_gap, y_gap)
}

/// The side pair a relationship would use with no authored sides.
pub fn auto_sides(from: &Rect, to: &Rect, hint: Hint) -> (Side, Side) {
    let dx = to.cx() - from.cx();
    let dy = to.cy() - from.cy();
    let horizontal = || {
        if dx >= 0.0 {
            (Side::Right, Side::Left)
        } else {
            (Side::Left, Side::Right)
        }
    };
    let vertical = || {
        if dy >= 0.0 {
            (Side::Bottom, Side::Top)
        } else {
            (Side::Top, Side::Bottom)
        }
    };
    match hint {
        Hint::OutsideRight => (Side::Right, Side::Right),
        Hint::ReturnLeft => (Side::Left, Side::Left),
        Hint::BottomChannel => (Side::Bottom, Side::Bottom),
        Hint::UpChannel => (Side::Top, Side::Top),
        Hint::Drop | Hint::OrthogonalV => vertical(),
        Hint::OrthogonalH => horizontal(),
        Hint::Auto | Hint::Straight => {
            let (xg, yg) = axis_gaps(from, to);
            if xg >= 0.0 && (yg < 0.0 || xg >= yg) {
                horizontal()
            } else if yg >= 0.0 {
                vertical()
            } else if dx.abs() >= dy.abs() {
                horizontal()
            } else {
                vertical()
            }
        }
    }
}

fn elbow(cur: Pt, next: Pt, heading_horizontal: bool) -> Pt {
    if heading_horizontal {
        Pt::new(next.x, cur.y)
    } else {
        Pt::new(cur.x, next.y)
    }
}

fn via_path(p0: Pt, fs: Side, via: &[Pt], p1: Pt, ts: Side) -> Vec<Pt> {
    let mut pts = vec![p0];
    let mut cur = p0;
    let mut heading_h = fs.is_horizontal();
    for &v in via {
        let mid = elbow(cur, v, heading_h);
        pts.push(mid);
        pts.push(v);
        let last = Seg::new(mid, v);
        if last.len() > 1e-6 {
            heading_h = last.is_horizontal();
        }
        cur = v;
    }
    let mid = if ts.is_horizontal() {
        Pt::new(cur.x, p1.y)
    } else {
        Pt::new(p1.x, cur.y)
    };
    pts.push(mid);
    pts.push(p1);
    simplify(&pts)
}

fn candidate_paths(req: &Request, fs: Side, ts: Side) -> Vec<Vec<Pt>> {
    let p0 = port(&req.from, fs, req.from_offset);
    let p1 = port(&req.to, ts, req.to_offset);
    if !req.via.is_empty() {
        return vec![via_path(p0, fs, &req.via, p1, ts)];
    }
    let o0 = fs.outward();
    let o1 = ts.outward();
    let q0 = Pt::new(p0.x + o0.x * STUB, p0.y + o0.y * STUB);
    let q1 = Pt::new(p1.x + o1.x * STUB, p1.y + o1.y * STUB);
    let mut out: Vec<Vec<Pt>> = Vec::new();
    let both_h = fs.is_horizontal() && ts.is_horizontal();
    let both_v = !fs.is_horizontal() && !ts.is_horizontal();
    match req.hint {
        Hint::Straight => {
            out.push(vec![p0, p1]);
            return out;
        }
        Hint::OutsideRight | Hint::ReturnLeft => {
            let cx = req.channel_x.unwrap_or(if req.hint == Hint::OutsideRight {
                req.from.right().max(req.to.right()) + CHANNEL_MARGIN
            } else {
                req.from.x.min(req.to.x) - CHANNEL_MARGIN
            });
            out.push(simplify(&[p0, Pt::new(cx, p0.y), Pt::new(cx, p1.y), p1]));
            return out;
        }
        Hint::BottomChannel | Hint::UpChannel => {
            let cy = req.channel_y.unwrap_or(if req.hint == Hint::BottomChannel {
                req.from.bottom().max(req.to.bottom()) + CHANNEL_MARGIN
            } else {
                req.from.y.min(req.to.y) - CHANNEL_MARGIN
            });
            out.push(simplify(&[p0, Pt::new(p0.x, cy), Pt::new(p1.x, cy), p1]));
            return out;
        }
        _ => {}
    }
    // Facing sides on one axis: straight when aligned, otherwise a Z at the
    // midpoint (or at the authored channel).
    if both_h && fs != ts {
        if (p0.y - p1.y).abs() < 0.5 {
            out.push(vec![p0, p1]);
        } else {
            let mx = req.channel_x.unwrap_or((p0.x + p1.x) / 2.0);
            out.push(simplify(&[p0, Pt::new(mx, p0.y), Pt::new(mx, p1.y), p1]));
        }
    }
    if both_v && fs != ts {
        if (p0.x - p1.x).abs() < 0.5 {
            out.push(vec![p0, p1]);
        } else {
            let my = req.channel_y.unwrap_or((p0.y + p1.y) / 2.0);
            out.push(simplify(&[p0, Pt::new(p0.x, my), Pt::new(p1.x, my), p1]));
        }
    }
    // Two elbow orders through the stubs.
    out.push(simplify(&[
        p0,
        q0,
        elbow(q0, q1, fs.is_horizontal()),
        q1,
        p1,
    ]));
    out.push(simplify(&[
        p0,
        q0,
        elbow(q0, q1, !fs.is_horizontal()),
        q1,
        p1,
    ]));
    // Double elbow through a shared channel between the stubs.
    if both_h {
        let cx = req.channel_x.unwrap_or((q0.x + q1.x) / 2.0);
        out.push(simplify(&[
            p0,
            q0,
            Pt::new(cx, q0.y),
            Pt::new(cx, q1.y),
            q1,
            p1,
        ]));
        let cy = req.channel_y.unwrap_or((q0.y + q1.y) / 2.0);
        out.push(simplify(&[
            p0,
            q0,
            Pt::new(q0.x, cy),
            Pt::new(q1.x, cy),
            q1,
            p1,
        ]));
    } else {
        let cy = req.channel_y.unwrap_or((q0.y + q1.y) / 2.0);
        out.push(simplify(&[
            p0,
            q0,
            Pt::new(q0.x, cy),
            Pt::new(q1.x, cy),
            q1,
            p1,
        ]));
        let cx = req.channel_x.unwrap_or((q0.x + q1.x) / 2.0);
        out.push(simplify(&[
            p0,
            q0,
            Pt::new(cx, q0.y),
            Pt::new(cx, q1.y),
            q1,
            p1,
        ]));
    }
    // Same-side U around the outside.
    if fs == ts {
        let (a, b) = match fs {
            Side::Right => {
                let cx = req.from.right().max(req.to.right()) + CHANNEL_MARGIN;
                (Pt::new(cx, p0.y), Pt::new(cx, p1.y))
            }
            Side::Left => {
                let cx = req.from.x.min(req.to.x) - CHANNEL_MARGIN;
                (Pt::new(cx, p0.y), Pt::new(cx, p1.y))
            }
            Side::Bottom => {
                let cy = req.from.bottom().max(req.to.bottom()) + CHANNEL_MARGIN;
                (Pt::new(p0.x, cy), Pt::new(p1.x, cy))
            }
            Side::Top => {
                let cy = req.from.y.min(req.to.y) - CHANNEL_MARGIN;
                (Pt::new(p0.x, cy), Pt::new(p1.x, cy))
            }
        };
        out.push(simplify(&[p0, a, b, p1]));
    }
    out
}

fn leaves_and_enters_correctly(points: &[Pt], fs: Side, ts: Side) -> bool {
    let segs = segments(points);
    let Some(first) = segs.first() else {
        return false;
    };
    let Some(last) = segs.last() else {
        return false;
    };
    let o0 = fs.outward();
    let o1 = ts.outward();
    let d0 = first.dir();
    let d1 = last.dir();
    let leaves = d0.x * o0.x + d0.y * o0.y > 0.0;
    let enters = d1.x * o1.x + d1.y * o1.y < 0.0;
    leaves && enters
}

pub fn pierce_count(points: &[Pt], obstacles: &[Rect]) -> usize {
    let segs = segments(points);
    obstacles
        .iter()
        .filter(|r| segs.iter().any(|s| s.pierces(r)))
        .count()
}

/// Lower is better: pierced nodes and sub-8px hops first, then whether the
/// candidate abandons the automatic side pair, then bends, then length.
/// `(pierces + short hops, abandoned auto pair, bends, length)`.
type Score = (usize, usize, usize, i64);

fn score(points: &[Pt], obstacles: &[Rect], auto_pair: bool) -> Score {
    let segs = segments(points);
    let bends = segs.len().saturating_sub(1);
    let len: f64 = segs.iter().map(Seg::len).sum();
    let short = segs.iter().filter(|s| s.len() < 8.0).count();
    (
        pierce_count(points, obstacles) + short,
        usize::from(!auto_pair),
        bends,
        len.round() as i64,
    )
}

/// Route one relationship. Unauthored sides are chosen by trying the
/// automatic pair first and then every alternative, keeping the cleanest.
pub fn route(req: &Request, obstacles: &[Rect]) -> Route {
    let (afs, ats) = auto_sides(&req.from, &req.to, req.hint);
    let from_options: Vec<Side> = match req.from_side {
        Some(s) => vec![s],
        None => ordered_sides(afs),
    };
    let to_options: Vec<Side> = match req.to_side {
        Some(s) => vec![s],
        None => ordered_sides(ats),
    };
    let mut best: Option<(Route, Score, usize)> = None;
    let mut rank = 0usize;
    for &fs in &from_options {
        for &ts in &to_options {
            for path in candidate_paths(req, fs, ts) {
                rank += 1;
                if path.len() < 2
                    || has_reversal(&path)
                    || !leaves_and_enters_correctly(&path, fs, ts)
                {
                    continue;
                }
                let s = score(&path, obstacles, fs == afs && ts == ats);
                let better = match &best {
                    None => true,
                    Some((_, bs, _)) => s < *bs,
                };
                if better {
                    best = Some((
                        Route {
                            points: path,
                            from_side: fs,
                            to_side: ts,
                        },
                        s,
                        rank,
                    ));
                }
            }
        }
    }
    match best {
        Some((r, _, _)) => r,
        None => {
            // Nothing satisfied the side contract; fall back to the plain
            // elbow so validation can name the problem.
            let p0 = port(&req.from, afs, req.from_offset);
            let p1 = port(&req.to, ats, req.to_offset);
            Route {
                points: simplify(&[p0, elbow(p0, p1, afs.is_horizontal()), p1]),
                from_side: afs,
                to_side: ats,
            }
        }
    }
}

fn ordered_sides(preferred: Side) -> Vec<Side> {
    let mut v = vec![preferred];
    for s in [Side::Right, Side::Bottom, Side::Left, Side::Top] {
        if s != preferred {
            v.push(s);
        }
    }
    v
}

#[derive(Debug, Clone)]
pub struct LabelRequest<'a> {
    pub text: &'a str,
    pub at: Option<Pt>,
    pub dx: f64,
    pub dy: f64,
    pub segment: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct PlacedLabel {
    pub rect: Rect,
    pub anchor: Pt,
    pub middle: bool,
}

fn label_rect(anchor: Pt, middle: bool, tw: f64) -> Rect {
    if middle {
        Rect::new(
            anchor.x - tw / 2.0,
            anchor.y - LABEL_HEIGHT + 3.0,
            tw,
            LABEL_HEIGHT,
        )
    } else {
        Rect::new(
            anchor.x - 4.0,
            anchor.y - LABEL_HEIGHT + 3.0,
            tw,
            LABEL_HEIGHT,
        )
    }
}

/// Place a relationship label beside its longest (or authored) segment.
/// Horizontal segments get the label above the line, vertical segments get
/// it to the right; when that side collides with an obstacle (a node or a
/// container label) the opposite side is used instead. `labelDx`/`labelDy`
/// shift the chosen position and `labelAt` replaces it outright.
pub fn place_label(points: &[Pt], req: &LabelRequest, obstacles: &[Rect]) -> PlacedLabel {
    let segs = segments(points);
    let longest = segs
        .iter()
        .enumerate()
        .max_by(|a, b| {
            a.1.len()
                .partial_cmp(&b.1.len())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(i, _)| i)
        .unwrap_or(0);
    let idx = req
        .segment
        .unwrap_or(longest)
        .min(segs.len().saturating_sub(1));
    let tw = text_width(req.text, LABEL_FONT) + LABEL_PAD;
    if let Some(p) = req.at {
        let anchor = Pt::new(p.x + req.dx, p.y + req.dy);
        return PlacedLabel {
            rect: label_rect(anchor, true, tw),
            anchor,
            middle: true,
        };
    }
    let s = segs
        .get(idx)
        .copied()
        .unwrap_or(Seg::new(points[0], points[0]));
    let mid = Pt::new((s.a.x + s.b.x) / 2.0, (s.a.y + s.b.y) / 2.0);
    let candidates: [(Pt, bool); 2] = if s.is_vertical() {
        // right of the line, then left of the line
        [
            (Pt::new(mid.x + 6.0, mid.y + 3.0), false),
            (Pt::new(mid.x - 6.0 - tw + 4.0, mid.y + 3.0), false),
        ]
    } else {
        // above the line, then below it
        [
            (Pt::new(mid.x, mid.y - 7.0), true),
            (Pt::new(mid.x, mid.y + LABEL_HEIGHT + 2.0), true),
        ]
    };
    let clear = |anchor: Pt, middle: bool| {
        let r = label_rect(anchor, middle, tw);
        !obstacles.iter().any(|o| o.intersects(&r))
    };
    let (base, middle) = candidates
        .iter()
        .copied()
        .find(|(a, m)| clear(*a, *m))
        .unwrap_or(candidates[0]);
    let anchor = Pt::new(base.x + req.dx, base.y + req.dy);
    PlacedLabel {
        rect: label_rect(anchor, middle, tw),
        anchor,
        middle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(from: Rect, to: Rect) -> Request {
        Request {
            from,
            to,
            from_side: None,
            to_side: None,
            via: vec![],
            hint: Hint::Auto,
            from_offset: 0.0,
            to_offset: 0.0,
            channel_x: None,
            channel_y: None,
        }
    }

    #[test]
    fn aligned_neighbours_get_a_straight_line() {
        let r = route(
            &req(
                Rect::new(0.0, 0.0, 100.0, 50.0),
                Rect::new(200.0, 0.0, 100.0, 50.0),
            ),
            &[],
        );
        assert_eq!(r.points.len(), 2);
        assert_eq!(r.from_side, Side::Right);
        assert_eq!(r.to_side, Side::Left);
    }

    #[test]
    fn offset_neighbours_get_a_midpoint_z() {
        let r = route(
            &req(
                Rect::new(0.0, 0.0, 100.0, 50.0),
                Rect::new(200.0, 120.0, 100.0, 50.0),
            ),
            &[],
        );
        assert_eq!(r.points.len(), 4);
        assert!((r.points[1].x - 150.0).abs() < 1e-9);
    }

    #[test]
    fn router_detours_around_an_unrelated_node() {
        let from = Rect::new(300.0, 260.0, 170.0, 64.0);
        let to = Rect::new(60.0, 400.0, 130.0, 64.0);
        let blocker = Rect::new(60.0, 260.0, 130.0, 64.0);
        let r = route(
            &Request {
                to_side: Some(Side::Top),
                ..req(from, to)
            },
            &[blocker],
        );
        assert_eq!(pierce_count(&r.points, &[blocker]), 0, "{:?}", r.points);
        assert_eq!(r.to_side, Side::Top);
    }

    #[test]
    fn via_points_bend_the_route_and_respect_the_target_side() {
        let from = Rect::new(40.0, 110.0, 120.0, 64.0);
        let to = Rect::new(670.0, 300.0, 130.0, 60.0);
        let r = route(
            &Request {
                from_side: Some(Side::Right),
                to_side: Some(Side::Top),
                via: vec![
                    Pt::new(620.0, 142.0),
                    Pt::new(620.0, 246.0),
                    Pt::new(735.0, 246.0),
                ],
                ..req(from, to)
            },
            &[],
        );
        assert_eq!(r.points.last().copied(), Some(Pt::new(735.0, 300.0)));
        assert!(segments(&r.points).iter().all(Seg::is_axis_aligned));
        assert_eq!(r.points.len(), 5);
    }

    #[test]
    fn labels_sit_above_horizontal_and_beside_vertical_segments() {
        let h = place_label(
            &[Pt::new(0.0, 10.0), Pt::new(100.0, 10.0)],
            &LabelRequest {
                text: "go",
                at: None,
                dx: 0.0,
                dy: 0.0,
                segment: None,
            },
            &[],
        );
        assert!(h.middle && h.rect.bottom() <= 10.0);
        let v = place_label(
            &[Pt::new(10.0, 0.0), Pt::new(10.0, 100.0)],
            &LabelRequest {
                text: "go",
                at: None,
                dx: 0.0,
                dy: 0.0,
                segment: None,
            },
            &[],
        );
        assert!(!v.middle && v.rect.x > 10.0);
    }

    #[test]
    fn labels_swap_sides_to_avoid_an_obstacle() {
        let blocker = Rect::new(12.0, 30.0, 80.0, 40.0);
        let v = place_label(
            &[Pt::new(10.0, 0.0), Pt::new(10.0, 100.0)],
            &LabelRequest {
                text: "go",
                at: None,
                dx: 0.0,
                dy: 0.0,
                segment: None,
            },
            &[blocker],
        );
        assert!(v.rect.right() <= 10.0, "{:?}", v.rect);
        let above = Rect::new(0.0, 0.0, 100.0, 9.0);
        let h = place_label(
            &[Pt::new(0.0, 10.0), Pt::new(100.0, 10.0)],
            &LabelRequest {
                text: "go",
                at: None,
                dx: 0.0,
                dy: 0.0,
                segment: None,
            },
            &[above],
        );
        assert!(h.rect.y >= 10.0, "{:?}", h.rect);
    }
}

//! Plain geometry: points, rectangles, axis-aligned segments, and the text
//! width model every layout decision depends on.
//!
//! Text is measured with a fixed monospace advance so the numbers the
//! validator reasons about match what the renderer draws: the HTML uses a
//! monospace font stack, and a monospace glyph advances 0.62em.

pub const CHAR_ADVANCE_EM: f64 = 0.60;

/// Sign with a true zero, unlike `f64::signum`, which maps 0.0 to 1.0.
pub fn sgn(v: f64) -> f64 {
    if v > 1e-6 {
        1.0
    } else if v < -1e-6 {
        -1.0
    } else {
        0.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pt {
    pub x: f64,
    pub y: f64,
}

impl Pt {
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub fn is_finite(&self) -> bool {
        self.x.is_finite() && self.y.is_finite()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rect {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

impl Rect {
    pub const fn new(x: f64, y: f64, w: f64, h: f64) -> Self {
        Self { x, y, w, h }
    }

    pub fn from_points(a: Pt, b: Pt) -> Self {
        let x = a.x.min(b.x);
        let y = a.y.min(b.y);
        Self {
            x,
            y,
            w: (a.x - b.x).abs(),
            h: (a.y - b.y).abs(),
        }
    }

    pub fn right(&self) -> f64 {
        self.x + self.w
    }

    pub fn bottom(&self) -> f64 {
        self.y + self.h
    }

    pub fn cx(&self) -> f64 {
        self.x + self.w / 2.0
    }

    pub fn cy(&self) -> f64 {
        self.y + self.h / 2.0
    }

    pub fn center(&self) -> Pt {
        Pt::new(self.cx(), self.cy())
    }

    pub fn inflate(&self, d: f64) -> Self {
        Self {
            x: self.x - d,
            y: self.y - d,
            w: self.w + 2.0 * d,
            h: self.h + 2.0 * d,
        }
    }

    pub fn union(&self, o: &Rect) -> Self {
        let x = self.x.min(o.x);
        let y = self.y.min(o.y);
        let r = self.right().max(o.right());
        let b = self.bottom().max(o.bottom());
        Self {
            x,
            y,
            w: r - x,
            h: b - y,
        }
    }

    pub fn intersects(&self, o: &Rect) -> bool {
        self.x < o.right() && o.x < self.right() && self.y < o.bottom() && o.y < self.bottom()
    }

    /// Euclidean gap between two rectangles; zero when they overlap.
    pub fn gap(&self, o: &Rect) -> f64 {
        let dx = (o.x - self.right()).max(self.x - o.right()).max(0.0);
        let dy = (o.y - self.bottom()).max(self.y - o.bottom()).max(0.0);
        (dx * dx + dy * dy).sqrt()
    }

    pub fn is_finite(&self) -> bool {
        [self.x, self.y, self.w, self.h]
            .iter()
            .all(|v| v.is_finite())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Left,
    Right,
    Top,
    Bottom,
}

impl Side {
    pub fn parse(s: &str) -> Option<Side> {
        match s {
            "left" => Some(Side::Left),
            "right" => Some(Side::Right),
            "top" => Some(Side::Top),
            "bottom" => Some(Side::Bottom),
            _ => None,
        }
    }

    pub fn is_horizontal(self) -> bool {
        matches!(self, Side::Left | Side::Right)
    }

    /// Unit vector pointing away from a node through this side.
    pub fn outward(self) -> Pt {
        match self {
            Side::Left => Pt::new(-1.0, 0.0),
            Side::Right => Pt::new(1.0, 0.0),
            Side::Top => Pt::new(0.0, -1.0),
            Side::Bottom => Pt::new(0.0, 1.0),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Side::Left => "left",
            Side::Right => "right",
            Side::Top => "top",
            Side::Bottom => "bottom",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Seg {
    pub a: Pt,
    pub b: Pt,
}

impl Seg {
    pub const fn new(a: Pt, b: Pt) -> Self {
        Self { a, b }
    }

    pub fn len(&self) -> f64 {
        ((self.b.x - self.a.x).powi(2) + (self.b.y - self.a.y).powi(2)).sqrt()
    }

    pub fn is_horizontal(&self) -> bool {
        (self.a.y - self.b.y).abs() < 1e-6
    }

    pub fn is_vertical(&self) -> bool {
        (self.a.x - self.b.x).abs() < 1e-6
    }

    pub fn is_axis_aligned(&self) -> bool {
        self.is_horizontal() || self.is_vertical()
    }

    pub fn bounds(&self) -> Rect {
        Rect::from_points(self.a, self.b)
    }

    /// Direction as a unit-ish vector (sign only matters for reversal checks).
    pub fn dir(&self) -> Pt {
        Pt::new(sgn(self.b.x - self.a.x), sgn(self.b.y - self.a.y))
    }

    /// Distance from an axis-aligned segment to a rectangle; zero on overlap.
    pub fn gap_to_rect(&self, r: &Rect) -> f64 {
        self.bounds().gap(r)
    }

    /// True when the segment passes through the rectangle's interior, not
    /// merely touching its border.
    pub fn pierces(&self, r: &Rect) -> bool {
        let inner = r.inflate(-0.5);
        if inner.w <= 0.0 || inner.h <= 0.0 {
            return false;
        }
        let b = self.bounds();
        // A degenerate bounds rect (horizontal or vertical) still intersects.
        let bx = Rect::new(b.x, b.y, b.w.max(1e-9), b.h.max(1e-9));
        bx.intersects(&inner)
    }

    /// Proper crossing point of two perpendicular axis-aligned segments.
    pub fn crosses(&self, o: &Seg) -> Option<Pt> {
        let (h, v) = if self.is_horizontal() && o.is_vertical() {
            (self, o)
        } else if self.is_vertical() && o.is_horizontal() {
            (o, self)
        } else {
            return None;
        };
        let hx0 = h.a.x.min(h.b.x);
        let hx1 = h.a.x.max(h.b.x);
        let vy0 = v.a.y.min(v.b.y);
        let vy1 = v.a.y.max(v.b.y);
        let x = v.a.x;
        let y = h.a.y;
        let eps = 0.5;
        if x > hx0 + eps && x < hx1 - eps && y > vy0 + eps && y < vy1 - eps {
            Some(Pt::new(x, y))
        } else {
            None
        }
    }

    /// Length of overlap between two parallel segments that lie within
    /// `tolerance` of the same line; zero otherwise.
    pub fn collinear_overlap(&self, o: &Seg, tolerance: f64) -> f64 {
        if self.is_horizontal() && o.is_horizontal() {
            if (self.a.y - o.a.y).abs() > tolerance {
                return 0.0;
            }
            let a0 = self.a.x.min(self.b.x);
            let a1 = self.a.x.max(self.b.x);
            let b0 = o.a.x.min(o.b.x);
            let b1 = o.a.x.max(o.b.x);
            (a1.min(b1) - a0.max(b0)).max(0.0)
        } else if self.is_vertical() && o.is_vertical() {
            if (self.a.x - o.a.x).abs() > tolerance {
                return 0.0;
            }
            let a0 = self.a.y.min(self.b.y);
            let a1 = self.a.y.max(self.b.y);
            let b0 = o.a.y.min(o.b.y);
            let b1 = o.a.y.max(o.b.y);
            (a1.min(b1) - a0.max(b0)).max(0.0)
        } else {
            0.0
        }
    }
}

/// Width in px of `text` set in a monospace face at `font_px`.
pub fn text_width(text: &str, font_px: f64) -> f64 {
    text.chars().count() as f64 * font_px * CHAR_ADVANCE_EM
}

/// Split a polyline into its segments.
pub fn segments(points: &[Pt]) -> Vec<Seg> {
    points.windows(2).map(|w| Seg::new(w[0], w[1])).collect()
}

/// Remove zero-length hops and merge collinear runs. Reversals are kept so
/// the caller can detect them.
pub fn simplify(points: &[Pt]) -> Vec<Pt> {
    let mut out: Vec<Pt> = Vec::with_capacity(points.len());
    for &p in points {
        if let Some(last) = out.last()
            && (last.x - p.x).abs() < 1e-6
            && (last.y - p.y).abs() < 1e-6
        {
            continue;
        }
        out.push(p);
    }
    let mut i = 1;
    while i + 1 < out.len() {
        let a = out[i - 1];
        let b = out[i];
        let c = out[i + 1];
        let ab = Seg::new(a, b);
        let bc = Seg::new(b, c);
        let same_axis =
            (ab.is_horizontal() && bc.is_horizontal()) || (ab.is_vertical() && bc.is_vertical());
        let d1 = ab.dir();
        let d2 = bc.dir();
        let same_dir = d1.x * d2.x + d1.y * d2.y > 0.0;
        if same_axis && same_dir {
            out.remove(i);
        } else {
            i += 1;
        }
    }
    out
}

/// True when any two consecutive segments run back along the same axis.
pub fn has_reversal(points: &[Pt]) -> bool {
    let segs = segments(points);
    segs.windows(2).any(|w| {
        let d1 = w[0].dir();
        let d2 = w[1].dir();
        d1.x * d2.x + d1.y * d2.y < 0.0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rect_gap_is_zero_on_overlap_and_euclidean_otherwise() {
        let a = Rect::new(0.0, 0.0, 10.0, 10.0);
        assert_eq!(a.gap(&Rect::new(5.0, 5.0, 10.0, 10.0)), 0.0);
        assert_eq!(a.gap(&Rect::new(20.0, 0.0, 10.0, 10.0)), 10.0);
        assert!((a.gap(&Rect::new(13.0, 14.0, 1.0, 1.0)) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn perpendicular_segments_cross_only_in_their_interiors() {
        let h = Seg::new(Pt::new(0.0, 5.0), Pt::new(10.0, 5.0));
        let v = Seg::new(Pt::new(5.0, 0.0), Pt::new(5.0, 10.0));
        assert_eq!(h.crosses(&v), Some(Pt::new(5.0, 5.0)));
        let touching = Seg::new(Pt::new(10.0, 0.0), Pt::new(10.0, 10.0));
        assert_eq!(h.crosses(&touching), None);
    }

    #[test]
    fn simplify_merges_collinear_runs_but_keeps_reversals() {
        let pts = [
            Pt::new(0.0, 0.0),
            Pt::new(5.0, 0.0),
            Pt::new(10.0, 0.0),
            Pt::new(10.0, 4.0),
        ];
        assert_eq!(simplify(&pts).len(), 3);
        let back = [Pt::new(0.0, 0.0), Pt::new(10.0, 0.0), Pt::new(4.0, 0.0)];
        assert!(has_reversal(&simplify(&back)));
    }
}

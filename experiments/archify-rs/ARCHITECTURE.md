# archify-rs — design

This document is the map of the crate: what each module owns, the shape of
the data that flows between them, the algorithms that decide geometry, the
checks that gate delivery, and the invariants that make the output
deterministic. It ends with an honest account of where this rewrite departs
from the Node.js original.

## 1. Pipeline

```
                ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────┐
 spec.json ────▶│  spec    │───▶│ evidence │───▶│ arch/workflow│───▶│  checks  │
                │ parse +  │    │ git-     │    │ layout into  │    │ 9 named  │
                │ schema   │    │ verified │    │ a Scene      │    │ checks   │
                └──────────┘    └──────────┘    └──────────────┘    └────┬─────┘
                                                                         │ ok
                ┌──────────┐    ┌──────────┐    ┌──────────────┐         ▼
 out.html ◀─────│  main    │◀───│ receipt  │◀───│   render     │◀── Scene (frozen)
 receipts       │ atomic   │    │ SHA-256  │    │ HTML + SVG   │
                │ commit   │    └──────────┘    └──────────────┘
                └────┬─────┘
                     │ visual-check
                     ▼
                ┌──────────┐
                │  visual  │  headless Chromium, 4 viewports × 2 themes,
                │          │  containment + readability + screenshots
                └──────────┘
```

Every stage speaks `Diagnostic`. A stage that finds an error stops the
pipeline at a named `stage` (`schema`, `evidence`, `render`), and the
receipt names that stage so a caller knows how far the document got.

## 2. Module map

| Module | Owns | Depends on |
|---|---|---|
| `diag` | `Diagnostic { code, severity, message, subject, supportedFixes }`, `Check { name, ok, details }` | — |
| `geom` | `Pt`, `Rect`, `Seg`, `Side`; gap/pierce/cross/collinear-overlap; polyline `simplify` and `has_reversal`; the text width model | — |
| `spec` | serde types for both diagram types with `deny_unknown_fields`; enums for every closed vocabulary; `load()` and `validate_semantics()` | `diag`, `geom::Side` |
| `route` | side selection, candidate orthogonal paths, scoring, `route()`; `place_label()` | `geom` |
| `scene` | the renderer-neutral `Scene` and its parts (`SNode`, `SRoute`, `SLabel`, `SBoundary`, `SBand`, `LegendItem`); `fit_font`; `finalize()` (legend + viewBox) | `geom`, `spec` enums |
| `arch` | architecture layout: authored rects, boundary boxes, two-pass routing with port spread, evidence hrefs | `route`, `scene`, `spec` |
| `workflow` | workflow layout: label-aware column compiler, lane/phase bands, groups, main path | `route`, `scene`, `spec`, `arch::apply_port_spread` |
| `checks` | the nine artifact checks, layout constraints, desktop readability | `scene`, `geom`, `diag` |
| `evidence` | repository evidence verification through `git` | `spec`, `diag` |
| `render` | `Scene → String` HTML with one inline SVG; includes `assets/viewer.css` and `assets/viewer.js` | `scene`, `geom::text_width` |
| `receipt` | `FileReceipt { sha256, bytes }` | `sha2` |
| `visual` | headless Chromium driver, viewport calibration, receipt sidecar | `receipt`, `diag` |
| `main` | argument parsing, stage orchestration, atomic writes, JSON and human output | everything |

Dependencies point one way. `render` and `checks` never see a `Spec`; they
see a `Scene`. `arch` and `workflow` never see HTML. `visual` never sees a
`Scene`; it only knows the delivered file and what the page reports about
itself.

## 3. Data model

### Spec (input)

`spec::Spec` is an enum over `ArchitectureSpec` and `WorkflowSpec`. Both
share `Meta` (title, locale, quality profile, views, legend, repository) and
`Card`. Every struct is `#[serde(deny_unknown_fields)]`, so a misspelled key
is a schema error rather than a silently ignored field, mirroring archify's
`additionalProperties: false`. Closed vocabularies (`ComponentType`,
`Variant`, `CardDot`, `LegendMode`, `Role`, `BoundaryKind`) are Rust enums;
an invalid value fails at parse time with the path in the message.

`validate_semantics` then checks what a JSON schema cannot: unique ids, that
every reference (`from`, `to`, `wraps`, `focus`, `lane`, `mainPath`)
resolves, that `mainPath` steps are backed by edges, that no two workflow
nodes share a lane × column slot, the column budget (`col ≤ 5`), the view
budget (≤ 5), phase non-overlap, and source-list bounds (1–3 per node).

### Scene (intermediate)

```rust
Scene {
  title, subtitle, locale, diagram_type, quality,
  view_box: (w, h),
  nodes:      Vec<SNode>,     // id, kind, label, sublabel, tag, rect, fonts, sources, main
  boundaries: Vec<SBoundary>, // label, rect, kind (region | security-group | group)
  bands:      Vec<SBand>,     // lanes and phases (workflow only)
  routes:     Vec<SRoute>,    // id, from, to, points, variant, role, width, label, sides
  legend:     Vec<LegendItem>, legend_rect,
  cards, views, trace,
}
```

Everything downstream is a pure function of a `Scene`. That is what makes
`checks` and `render` diagram-type-agnostic and what makes the output
deterministic: the same spec bytes always produce the same `Scene`, the same
SVG, and the same SHA-256.

## 4. Geometry

### 4.1 Text model

Every width the validator reasons about must be the width the browser
draws, or the checks are theatre. archify-rs pins both sides to one rule:
text is set in a monospace stack (`ui-monospace, SFMono-Regular, Menlo,
Consolas, …`) in the HTML **and** the SVG, and a monospace glyph advances
`0.60em` (`geom::CHAR_ADVANCE_EM`). `text_width(text, px) = chars × px × 0.60`.

Fonts: node label 11px, node context 9px (architecture) / 8px (workflow),
route label 8px, pills 7px. Context text is fitted downward to a 6px floor
when it does not fit its node (`scene::fit_font`); anything that still does
not fit is a `layout/constraint` error.

### 4.2 Routing (`route`)

A relationship leaves its source through one **side** and enters its
target through one side. The first segment must leave perpendicular to the
from-side and the last must enter perpendicular to the to-side; this is the
"side is a direction contract" rule and `leaves_and_enters_correctly`
rejects any candidate that breaks it.

1. **Automatic sides** (`auto_sides`): if the rectangles are separated
   horizontally and that gap is at least the vertical gap, use
   `Right → Left` (or mirrored); else if separated vertically, `Bottom →
   Top`; otherwise by the larger centre delta. Route hints override
   (`drop`, `outside-right`, `return-left`, `bottom-channel`, `up-channel`,
   `orthogonal-h/v`).
2. **Candidates** (`candidate_paths`) for a given side pair: straight line
   when aligned; a Z through the midpoint (or authored channel) for facing
   sides; two elbow orders through 20px stubs; a double elbow through a
   shared channel; a U around the outside for same-side pairs; and a
   `via` path that walks authored waypoints with heading-aware elbows.
   `simplify` merges collinear runs; `has_reversal` rejects paths that
   double back.
3. **Scoring** picks the lowest tuple
   `(pierced unrelated nodes + sub-8px hops, !auto_pair, bends, length)`.
   An authored side is fixed; an unauthored side tries the automatic choice
   first and then the other three, so a route only abandons its natural
   side to avoid crossing a stranger.
4. **Port spread** (`arch::apply_port_spread`) runs after a first routing
   pass: relationships that share a node side are spread along it at up to
   20px spacing, ordered by where their far end lies so they do not cross.
   Single relationships and anything with explicit geometry (`via`,
   `labelAt`, channels, non-`auto` route) are left centred, as in archify.
   The second pass routes with the learned sides pinned and the offsets
   applied.

### 4.3 Labels (`route::place_label`)

The label goes beside the longest segment (or the authored `labelSegment`):
above a horizontal segment, to the right of a vertical one. If that
position intersects an obstacle — a node or a container caption — the
opposite side is tried. `labelDx`/`labelDy` shift the chosen anchor;
`labelAt` replaces it. The label's **mask** (text width + 6px, 14px tall)
is what the clearance and overlap checks measure.

### 4.4 Architecture layout (`arch`)

Positions are authored (`pos`/`size`, default 150×64) or fall back to a
`row`/`col` grid (origin 60,120; pitch 230×160). A boundary is the union of
its wrapped rects inflated by `pad` (default 24) plus 14px of caption room
at the top. Evidence links are built from `meta.repository`:
`{url}/blob/{revision}/{path}#L{line}[-L{end}]`, or omitted for
`link_mode: local-only`.

### 4.5 Workflow layout (`workflow`)

Columns are as wide as their widest node (default 92). The gap between two
adjacent columns is 46px **or wide enough for any labelled same-lane edge
that crosses it** (`label width + 16`), so a label never has to sit on a
node — this is what archify calls "labels reserve clearance". Lanes stack
with a 26px caption row and 22px padding around the tallest node in the
lane; phases sit above the lanes spanning their column ranges; groups are
dashed boxes inside a lane across a column range. Nodes on `mainPath` and
the edges between consecutive `mainPath` nodes are emphasised (2.4px, main
colour). Schema v1 and v2 compile identically here; v1's "fixed legacy
geometry" has no separate engine.

### 4.6 viewBox (`scene::finalize`)

The viewBox is the content bounds plus 40px, plus a 46px legend row when
the legend is visible, rounded up to whole units. There is no authored
`viewBox` override; readability is governed by width alone (see §5).

## 5. Checks (`checks`)

Nine named checks feed the receipt; each failure is also a `Diagnostic`.

| Check | Rule | Code |
|---|---|---|
| `single_svg` | exactly one `<svg>` | — |
| `finite_svg` | every coordinate finite | `composition/finite` |
| `orthogonal_arrows` | every segment axis-aligned | `composition/orthogonal` |
| `label_route_clearance` | a label mask is ≥ 4px from every *other* route's segments | `composition/label-route-clearance` |
| `relationship_crossings` | no route pierces an unrelated node (error); route/route crossings (warning) | `composition/relationship-crossing` |
| `relationship_corridors` | no two routes share > 8px of a collinear run within 2px | `composition/relationship-corridor` |
| `container_border_runs` | no segment runs > 16px along a boundary or lane border within 3px | `composition/container-border-run` |
| `route_rhythm` | no segment shorter than 8px | `composition/route-rhythm` |
| `legend_clearance` | the legend row touches no node or route | `composition/legend-clearance` |

Layout constraints (`layout/constraint`): a label mask may not overlap a
node, another label, or a container caption; node labels must fit at 11px;
context text must fit at its fitted size. Each overlap diagnostic proposes
concrete `labelDy` values.

**Desktop readability** (`composition/desktop-readability`): at a 1440px
viewport the reader is 960px wide and the diagram 930px, so text projects
at `font × min(1, 930 / viewBoxWidth)`. The smallest node text must project
to ≥ 6px. This is why viewBox *width* is the number to watch: a wider
diagram shrinks every glyph.

**Quality profiles**: `standard` passes with warnings; `showcase` requires
zero diagnostics of any severity.

## 6. Evidence (`evidence`)

An architecture diagram that cites `sources` must pin `meta.repository`
and be validated with `--repo-root`. Verification is four `git` calls:

1. `git remote get-url origin` must equal the authored URL after
   normalisation (scheme, `.git`, trailing slash, host case, `git@` form).
2. `git cat-file -e <revision>^{commit}` — the pinned commit exists.
3. `git cat-file -t <revision>:<path>` must be `blob` — a file, not a
   directory, at that revision.
4. If a line is cited, `git cat-file -p` line count must cover it.

The checkout's working tree is never read; only the object at the pinned
revision counts, so a dirty tree cannot fake evidence.

## 7. Delivery (`main`, `receipt`)

`deliver` validates; on success it renders, writes the HTML to a temp file
in the target directory and `rename`s it into place (atomic on POSIX), writes
the exact spec bytes as `<out>.spec-snapshot.json`, and a
`<out>.deliver.json` receipt with SHA-256 and byte counts of both. On
failure nothing is written, so a previously delivered artifact is never
replaced by a broken one — and a `visual-check` after a failed deliver would
inspect the last good file, which is why the receipt says `committed:false`.

## 8. Browser evidence (`visual`)

The page's own runtime, after layout, stamps `data-inner-w/h`,
`data-scroll-w/h`, `data-diagram-w`, and `data-min-text-px` on `<html>`.
`visual-check` loads the file with `chrome --headless=new --dump-dom
--window-size=W,H` and reads those attributes back — real layout numbers
without a DevTools client. New-headless Chromium reserves window chrome
(1440×900 yields an 813px inner height), so each viewport is **calibrated**:
if the first load reports a smaller inner height than requested, the window
is enlarged by the shortfall and measured again. Containment requires
`scrollWidth ≤ innerWidth` and `scrollHeight ≤ innerHeight` at 1440×900,
1600×1000, 1920×1080 and 2048×1320 in light and dark; readability requires
the reported minimum text ≥ 6px. Screenshots are taken at the smallest and
largest viewports in both themes. The receipt is written beside the artifact
as `<stem>.visual-check.json`.

Three claims stay separate, as in archify: `deliver` proves deterministic
artifact checks, `visual-check` proves bounded behaviour in a real browser,
and perceptual review is a human's or an image-capable reviewer's job — the
receipt's `visualReview` is always `pending`.

## 9. Viewer runtime (`assets/viewer.js`, `assets/viewer.css`)

~250 lines of dependency-free JavaScript and one stylesheet, inlined at
render time. Theme tokens are CSS custom properties defined for light on
`:root`, overridden under `prefers-color-scheme: dark` guarded by
`:root:not([data-theme="light"])`, and again under `:root[data-theme="dark"]`
so the toggle wins both ways. The runtime provides theme choice (persisted;
`?theme=` overrides), pan/zoom by rewriting the SVG `viewBox`, guided views
(nodes and the routes between focused nodes get `is-focus`, the rest dim),
search, presentation mode, SVG export (tokens resolved into the clone so the
file stands alone) and 2× PNG export via canvas, and trace motion. The
**adaptive reader width** picks the widest stage between 960px and 1440px
whose height, together with the header, views, cards and footer, fits the
viewport — the same height-budgeted shell archify uses.

## 10. Invariants

- Same spec bytes ⇒ same `Scene` ⇒ same HTML ⇒ same SHA-256. No clocks,
  no randomness, no HashMap iteration order reaching the output (groups in
  port spread are independent; nodes and routes keep authored order).
- The validator's text widths are the renderer's text widths (§4.1).
- A route's first and last segments respect the side contract.
- A failed `deliver` writes nothing.
- The evidence checkout's working tree is never consulted.
- No external resource is referenced by the artifact.

## 11. Testing

- `geom`: gap, proper crossing, simplification, reversal detection.
- `route`: straight/Z selection, obstacle detour, `via` walking, label
  sides, obstacle-driven label side swap.
- `spec`: dangling references, unknown fields, column budget, `mainPath`
  edge backing.
- `evidence`: remote URL normalisation.
- `receipt`: SHA-256 against the known digest of `"abc"`.
- `tests/pipeline.rs`: the compiled binary on the bundled examples —
  showcase pass with 9/9 checks, evidence root required, origin mismatch,
  deliver receipts and the no-clobber guarantee, schema error paths, the
  layout receipt, and the readability gate.

`visual-check` is exercised manually (it needs Chromium); both bundled
examples pass it at all eight viewport/theme combinations.

## 12. Relationship to archify (Node.js)

Same JSON contract, same nine check names, same receipt vocabulary, same
readability formula (930px at 1440), same evidence rules. Differences:

- **Scope.** `architecture` and `workflow` are implemented; `sequence`,
  `dataflow` and `lifecycle` are not. Brand marks (`brand`), the
  `deployment-ownership` engineering profile, share cards, guided stories
  with motion, delta/compare, the update checker, and `preview` are not.
- **Geometry engine.** The router, label placer and workflow compiler are
  new implementations of the same contracts, not ports of archify's
  solver. Automatic placement differs in detail, so archify specs may need
  their geometry controls (`labelDy`, `via`, `toSide`) re-tuned here — the
  bundled examples are archify's own specs and validate unchanged, but that
  is evidence, not a guarantee.
- **Artifact size.** ~40 KB per artifact against ~800 KB, because the viewer
  runtime is a fraction of archify's.
- **Browser driver.** DOM instrumentation + `--dump-dom` instead of a
  DevTools session; the page reports its own measurements.

## 13. Extending

A new diagram type is a new layout module that produces a `Scene`:
add serde types to `spec`, semantic rules to `validate_semantics`, a
`compile(&SpecType, Quality) -> (Scene, Vec<Diagnostic>)` module, and one
match arm in `main::validate`. Checks and rendering come for free. A new
check is a function over `&Scene` in `checks::run` that pushes a `Check`
and diagnostics; add it to the table in §5.

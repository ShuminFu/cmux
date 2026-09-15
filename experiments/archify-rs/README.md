# archify-rs

A Rust rewrite of the [archify](https://github.com/tt-a1i/archify) diagram
pipeline: a typed JSON specification goes in, a validated, self-contained,
interactive HTML diagram comes out, with deterministic receipts and real
browser evidence.

```
spec.json ──▶ validate ──▶ deliver ──▶ out.html ──▶ visual-check
              schema        render      one inline    headless Chromium,
              evidence      atomic      SVG, inline   4 desktop viewports,
              layout        write       CSS + JS      light and dark
              9 checks      SHA-256
```

Two diagram types are implemented: `architecture` (authored positions,
boundaries, git-verified source evidence) and `workflow` (lanes × columns
compiled into a readable grid, phases, groups, main path). The JSON contract
is archify's, so archify specs validate here unchanged apart from the
geometry controls (`labelDy`, `via`, …) that are tuned per engine.

## Build

```bash
cd experiments/archify-rs
cargo build --release
./target/release/archify-rs doctor
```

Three crates only: `serde`, `serde_json`, `sha2`. No runtime dependencies;
`visual-check` shells out to a Chrome/Chromium binary if one is found (or
`ARCHIFY_CHROME` / `--chrome` names one).

## Commands

```bash
# Schema, evidence, layout, and the nine composition checks. Writes nothing.
archify-rs validate workflow examples/catchup-runbook.workflow.json --quality showcase --json

# Architecture diagrams that cite sources are verified against a checkout
# whose `origin` is the authored repository at the pinned revision.
archify-rs validate architecture examples/upstream-map.architecture.json \
  --repo-root /path/to/manaflow-ai/cmux --quality showcase --json

# Validate, render, and atomically commit the HTML with receipts.
archify-rs deliver workflow examples/catchup-runbook.workflow.json out/runbook.html --quality showcase --json

# Load the delivered HTML in headless Chromium at 1440×900, 1600×1000,
# 1920×1080 and 2048×1320 in light and dark; screenshots + JSON sidecar.
archify-rs visual-check out/runbook.html --json

# Per-node and per-route geometry as a stable compiler receipt.
archify-rs validate workflow examples/catchup-runbook.workflow.json --layout-json --json
```

Exit codes: `0` the artifact passed · `1` it did not (diagnostics say why) ·
`2` usage error. A failed `deliver` never touches a previously written output.

## Output

`deliver out/x.html` writes:

| File | What it is |
|---|---|
| `out/x.html` | the artifact: one page, one `<svg>`, inline stylesheet and viewer runtime, no network |
| `out/x.deliver.json` | receipt: checks, SHA-256 and byte counts of spec and artifact |
| `out/x.spec-snapshot.json` | the exact specification bytes that were rendered |

`visual-check out/x.html` writes `out/x.visual-check.json` and four
screenshots (`1440x900` and `2048x1320`, light and dark).

## What the artifact can do

Theme switch (light / dark / system, `?theme=` override), pan and zoom,
guided views that dim everything outside a chapter's focus set, node search,
presentation mode, SVG and 2× PNG export, and `meta.animation: "trace"`
motion on the main path. Source badges (`SRC n`) link to the pinned revision
on GitHub or Gitee.

## Design

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the module map, the routing and
label-placement algorithms, the check catalogue, the text model the checks
and the renderer share, and how this rewrite differs from archify.

## Tests

```bash
cargo test
```

Unit tests cover geometry, routing, label placement, schema semantics,
remote-URL normalisation, and hashing. `tests/pipeline.rs` drives the
compiled binary through validate / deliver / failure paths on the bundled
examples.

# Upstream review — 2026-09-13

Review of `manaflow-ai/cmux` (upstream) against this fork, `ShuminFu/cmux`.

| | commit | subject | date |
|---|---|---|---|
| Fork `main` | `af987b6` | Add stable cmux-tui split IDs (#8023) | 2026-07-19 |
| Upstream `main` | `ff4428f3` | Make Cloud Bash prompts follow VM names (#12502) | 2026-09-13 |

## Headline

`git merge-base main upstream/main` returns `af987b6` — the fork's own HEAD. The fork
carries **zero local commits**; it is an unmodified mirror that has gone stale. The gap is
**6,883 commits / 1,317 merged PRs** over roughly eight weeks, and closing it is a plain
fast-forward with no conflicts to resolve.

## Where the work landed

Files touched across the gap:

| Area | File touches |
|---|---|
| `Packages/macOS` | 13,554 |
| `Packages/iOS` | 8,722 |
| `cmux-tui/bindings` | 5,013 |
| `cmux-tui/crates` | 4,936 |
| `Packages/Shared` | 3,893 |
| `Resources/markdown-viewer` | 2,185 |
| `web/app` | 2,048 |
| `web/tests` | 1,434 |
| `Resources/ghostty` | 1,392 |
| `web/services` | 1,355 |
| `Sources/Panels` | 1,351 |
| `web/messages` | 1,058 |
| `Sources/Cloud` | 809 |

Conventional-commit scopes, most active first: `tui` (881), `relay` (214), `remote` (186),
`ios` (141), `cmux-tui` (129), `iroh` (109), `push` (87), `cli` (54), `cloud` (43), `ssh` (28).
By type the gap is overwhelmingly maintenance: ~1,825 `fix` and ~1,237 `test` commits
against 60 `feat`.

## Structural changes

New top-level entries: `cmux-browser`, `TunnelExtension`, `schemas`, `KeyboardPinningLab`,
`CLA.md`, `PR-10599-AUDIT.md`. Removed: `daemon/`.

New `Packages/macOS` modules: `CmuxAgentJournal`, `CmuxAgentSessionStore`,
`CmuxCloudBannerCore`, `CmuxCloudMachines`, `CmuxDiffComments`, `CmuxFilePreviewCore`,
`CmuxPhonePush`, `CmuxSimulator`, `CmuxSudoBroker`, `CmuxSudoBrokerUI`.

## Most recent upstream activity (2026-09-03 → 09-13, 647 commits)

- **Cloud terminal readiness lifecycle** dominates: readiness rearm and retry bounds,
  loader retirement, attachment recovery, terminal identity preservation across rebind,
  workspace name authority, and provenance for the Cloud sidebar.
- **coderouter revocable API-key authentication** (`web/services/coderouter`, with the Swift
  client at `Sources/Cloud/CoderouterClient.swift` and CLI passthrough).
- Browser download history, workspace-switch hang and ghosting fixes, machine resource metrics.

## Artifacts

Both diagrams were generated with the open-source [archify](https://github.com/tt-a1i/archify)
agent skill (v2.17, MIT). Open the HTML files directly in a browser; they are self-contained.

- `upstream-map.html` — subsystem map of upstream cmux at `ff4428f3`, annotated with where the
  gap's changes landed. Every node carries repository evidence verified against the pinned
  revision. Spec: `upstream-map.architecture.json`.
- `catchup-runbook.html` — the follow-up runbook for closing the gap. Spec:
  `catchup-runbook.workflow.json`.

Both passed archify `validate --quality showcase` (9/9 artifact checks, 0 errors, 0 warnings),
`deliver`, and `visual-check` (0 overflow at 1440×900, 1600×1000, 1920×1080 and 2048×1320 in
both light and dark). Receipts are in the `*.visual-check.json` sidecars.

## Recommended follow-up

1. Fast-forward the fork: `git fetch upstream main && git merge --ff-only upstream/main`.
   Nothing local is at risk, so there is no rebase or conflict work.
2. Re-check the `ghostty` submodule pointer after the fast-forward; the submodule moved
   during the gap.
3. Expect the first tagged build after the sync to be slow — `cmux-browser`, `TunnelExtension`
   and ten new macOS packages arrive at once, and `daemon/` disappears.
4. Run the repository gates before dogfooding: `scripts/lint-pbxproj-test-wiring.sh`,
   `scripts/check-package-resolved-policy.py`, `scripts/check-workspace-package-groups.py --check`.

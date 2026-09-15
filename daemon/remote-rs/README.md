# cmuxd-remote (Rust)

Rust port of the Go remote daemon in `daemon/remote`. It speaks the same
newline-delimited JSON-RPC protocol over stdio, runs the same persistent
per-slot daemon, WebSocket transport, cloud CLI bridge, `cmux` CLI relay,
tmux compatibility layer, and agent launch shims, and is validated against the
Go binary byte for byte by a differential parity harness.

The Go implementation remains the release artifact; this crate ships in
parallel so the two can be compared and the cut-over can happen deliberately.

## Layout

| Path | Mirrors (Go) | Contents |
| --- | --- | --- |
| `src/rpc.rs` | `main.go` | Frame reader/writer, `hello`/`ping`, `proxy.*`, `session.*`, `pty.*` handlers, attachment pump |
| `src/pty_hub.rs` | `ws_pty.go` (hub) | PTY sessions, attachments, scrollback, input queue and seq acks, smallest-wins resize, idle reap |
| `src/ws.rs` | `ws_pty.go` (HTTP) | `/healthz`, `/terminal`, `/rpc`, `/admin/leases`, lease files, close codes |
| `src/persistent.rs` | `persistent_lifecycle.go` | Slot paths, auth token, socket dir, lock file, stdio proxy, stop and lease reaping |
| `src/cloud_cli_bridge.rs` | `cloud_cli_bridge.go` | `cli.request` / `cli.response` bridge socket |
| `src/cli.rs`, `src/commands.rs` | `cli.go`, `commands.go`, `cli_overrides.go` | `cmux` relay: command table, flag parsing, relay auth, output |
| `src/tmux_compat.rs` | `tmux_compat.go` | `__tmux-compat` dispatcher, format strings, selectors, compat store |
| `src/agent_launch.rs` | `agent_launch.go` | `claude-teams` / `omo` / `omx` / `omc` shims and environment |
| `src/daemon.rs`, `src/main.rs` | `main.go` (`run`) | Argument parsing, busybox `argv[0]` dispatch, `serve` flag validation |
| `tests/` | `*_test.go` | Ports of the Go test suite (223 tests) |
| `parity/run_parity.py` | – | Differential harness that drives both binaries |

## Design notes

- The core is synchronous: std threads, `std::sync::Mutex`, `flume` channels,
  and drop-to-close signals stand in for goroutines, channels, and contexts.
  Only the WebSocket server uses tokio + axum, and RPC handling there runs on
  `spawn_blocking` so the hub stays lock-simple.
- Lock order is `pty_write_mu → hub`; per-session state is only reachable
  through the hub guard (`session.state(&hub_guard)`), which removes a class
  of lock-ordering bugs the Go code manages by convention.
- JSON output goes through `util::go_json` so field order, `<`/`>`/`&`
  escaping, and integral floats match Go's `encoding/json`.
- I/O error text is rendered Go-style (`dial unix …: connect: no such file or
  directory`) so relay and daemon error messages stay identical.

## Build and test

```bash
cd daemon/remote-rs
cargo build --release          # target/release/cmuxd-remote
cargo test                     # 223 tests: unit + cli + rpc_stdio + persistent + ws
cargo clippy --all-targets -- -D warnings
```

Or run everything CI runs, including the parity harness:

```bash
scripts/run-remote-daemon-rs-checks.sh
```

The version string defaults to `dev`; set `CMUXD_REMOTE_VERSION` at build time
to embed a release version, matching the Go `-ldflags -X main.version` flow.

## Parity harness

```bash
python3 daemon/remote-rs/parity/run_parity.py [--go-bin PATH] [--rust-bin PATH] [--only SUBSTR]
```

The harness builds both binaries (or takes prebuilt ones), then runs 112
scenarios through each: process invocations and flag validation, busybox
`argv[0]` dispatch, stdio JSON-RPC scripts (framing edge cases, `session.*`,
`proxy.*` against a local upstream, `pty.*` with a live shell, oversized
frames), the persistent daemon lifecycle (`serve --stdio --persistent`,
reconnect, `daemon.shutdown`, `--persistent-stop`), and the `cmux` CLI relay
against a mock cmux socket (recorded requests, stdout, stderr, exit codes).
Outputs are normalized (random tokens, timestamps, stream chunking, JSON
parser prose) and diffed. Without a Go toolchain it reports `SKIP`; pass
`--require-go` to make that a failure, as CI does.

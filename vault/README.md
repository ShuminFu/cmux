# cmux-vault

`cmux-vault` discovers local coding-agent session transcripts and syncs them to
cmux Vault cloud storage. Round 1 supports Claude Code, Codex, and pi.

## Install

```bash
cargo build --release
# binary: target/release/cmux-vault
```

Requires Rust 1.88 or newer and a C compiler (the bundled zstd is built from
source).

## Commands

```bash
cmux-vault login
cmux-vault scan
cmux-vault sync
cmux-vault resume <session-id>
cmux-vault status
cmux-vault logout
```

`login` starts a device-code flow, prints a verification URL and user code, and
stores Stack Auth tokens in `~/.config/cmux-vault/auth.json` with mode `0600`.
`sync` uploads changed transcripts directly to S3-compatible object storage via
presigned URLs. `resume` restores a missing transcript from cloud storage and
prints the command the agent expects.

Useful flags:

```bash
cmux-vault --json scan
cmux-vault sync --agent codex --dry-run
cmux-vault sync --limit 25
cmux-vault resume --agent claude <session-id>
cmux-vault resume --force <session-id>
```

Flags accept both `--flag` and `-flag` spellings, and `--flag=value` as well as
`--flag value`. Flags must precede positional arguments.

## Environment

- `CMUX_VAULT_API_BASE`: web API base URL. Defaults to `https://cmux.com`.
- `CMUX_VAULT_CONFIG_DIR`: override the auth token directory.
- `CMUX_VAULT_STATE_DIR`: override the sync state directory.
- `CLAUDE_CONFIG_DIR`: override Claude Code config discovery.
- `CODEX_HOME`: override Codex discovery.

Default local state lives in `~/.local/state/cmux-vault/state.json`.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Unit tests sit next to each module. `tests/cli.rs` drives the built binary end
to end against an in-process mock of the Vault API and presigned blob storage,
covering every command, exit code, and failure path.

The crate is a port of the original Go implementation. The on-disk state
format, token file, HTTP contract, command surface, exit codes, and output text
are unchanged, so existing sync state and scripts keep working. Two deliberate
differences: `version` prints the crate version rather than `dev`, and
`--json scan` prints an empty `sessions` array rather than `null` when nothing
is found.

`scripts/parity-check.py` runs the Go reference build and this binary over the
same fixtures and mock servers and diffs every observable output; its docstring
explains how to build the reference from git history.

#!/usr/bin/env python3
"""Differential check of the Rust `cmux-vault` against the original Go
implementation.

Both binaries run over identical fixtures (every discovery rule, several time
zones, unclean env overrides, symlinked roots) and against identical mock Vault
API + blob servers (uploads, commits, presigned PUT/GET, retries, 4xx/5xx,
storage rejection, device-code login). Exit codes, stdout, stderr, state files,
token files, request bodies/headers, uploaded blobs, and restored transcripts
are compared.

Build the Go reference from the last commit that carried it:

    git worktree add /tmp/cmux-vault-go af987b6
    (cd /tmp/cmux-vault-go/vault && go build -o /tmp/cmux-vault-go-bin ./cmd/cmux-vault)
    cargo build --release
    python3 scripts/parity-check.py /tmp/cmux-vault-go-bin target/release/cmux-vault

Requires the `zstandard` Python package or a `zstd` CLI on PATH.

Known, intentional deviations (normalized by the harness):
  * `version` prints the crate version instead of the Go build's "dev".
  * `--json scan` prints `"sessions": []` for no sessions instead of `null`.
  * Compressed sizes differ between zstd encoders (plaintext is compared).
  * Transport-error wording differs between HTTP stacks (message prefixes are compared).
"""

from __future__ import annotations

import http.server
import json
import os
import re
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path

try:
    import zstandard  # type: ignore
except Exception:  # pragma: no cover - optional
    zstandard = None

GO, RS = sys.argv[1], sys.argv[2]
UUID_A = "11111111-1111-4111-8111-111111111111"
UUID_B = "22222222-2222-4222-8222-222222222222"
UUID_C = "33333333-3333-4333-8333-333333333333"
UUID_D = "44444444-4444-4444-8444-444444444444"
UUID_E = "55555555-5555-4555-8555-555555555555"
UUID_F = "66666666-6666-4666-8666-666666666666"
CODEX_REL = f"sessions/2026/07/04/rollout-2026-07-04T00-00-00-{UUID_A}.jsonl"

failures: list[str] = []
checks = 0


def zstd_decompress(data: bytes) -> bytes:
    if zstandard is not None:
        return zstandard.ZstdDecompressor().decompressobj().decompress(data)
    return subprocess.run(["zstd", "-d", "-c"], input=data, check=True, capture_output=True).stdout


def zstd_compress(data: bytes) -> bytes:
    if zstandard is not None:
        return zstandard.ZstdCompressor().compress(data)
    return subprocess.run(["zstd", "-c"], input=data, check=True, capture_output=True).stdout


# --------------------------------------------------------------------------
# Mock Vault API + blob store (same contract as the Go/Rust test servers)
# --------------------------------------------------------------------------


@dataclass
class MockState:
    blobs: dict[str, bytes] = field(default_factory=dict)
    committed: dict[str, str] = field(default_factory=dict)
    requests: list[dict] = field(default_factory=list)
    sessions: list[dict] = field(default_factory=list)
    objects: dict[str, bytes] = field(default_factory=dict)
    fail_uploads: int = 0
    api_override: tuple[int, str] | None = None
    reject_puts: bool = False
    drop_puts: bool = False


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: "Mock"

    def log_message(self, *args):  # silence
        pass

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _send(self, status: int, data: bytes, ctype: str = "application/json"):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, status: int, obj) -> None:
        self._send(status, json.dumps(obj).encode())

    def _handle(self, method: str):
        st = self.server.state
        body = self._body()
        path, _, query = self.path.partition("?")
        with self.server.lock:
            st.requests.append({"method": method, "url": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
            if path.startswith("/api/") and st.api_override is not None:
                status, text = st.api_override
                return self._send(status, text.encode(), "text/plain")
            if method == "PUT" and path == "/put":
                key = query[len("key="):] if query.startswith("key=") else ""
                if not key:
                    return self._send(400, b"bad blob request", "text/plain")
                if st.reject_puts:
                    return self._send(403, b"denied", "text/plain")
                if not st.drop_puts:
                    st.blobs[key] = body
                return self._send(200, b"", "text/plain")
            if method == "POST" and path == "/api/vault/uploads":
                if st.fail_uploads > 0:
                    st.fail_uploads -= 1
                    return self._send(500, b"boom", "text/plain")
                items = json.loads(body)["items"]
                out = []
                for item in items:
                    key = f"{item['agent']}/{item['relPath']}"
                    if st.committed.get(key) == item["sha256"]:
                        out.append({"agent": item["agent"], "agentSessionId": item["agentSessionId"], "relPath": item["relPath"], "status": "unchanged"})
                    else:
                        out.append({"agent": item["agent"], "agentSessionId": item["agentSessionId"], "relPath": item["relPath"], "status": "upload", "objectKey": key, "putUrl": f"{self.server.base}/put?key={key}"})
                return self._json(200, {"items": out})
            if method == "POST" and path == "/api/vault/sessions/commit":
                items = json.loads(body)["items"]
                out = []
                for item in items:
                    key = f"{item['agent']}/{item['relPath']}"
                    if key not in st.blobs:
                        out.append({"agent": item["agent"], "agentSessionId": item["agentSessionId"], "relPath": item["relPath"], "status": "error", "error": "object_missing"})
                        continue
                    st.committed[key] = item["sha256"]
                    out.append({"agent": item["agent"], "agentSessionId": item["agentSessionId"], "relPath": item["relPath"], "status": "committed", "sessionId": "session-row"})
                return self._json(200, {"items": out})
            if method == "GET" and path == "/api/vault/sessions":
                return self._json(200, {"sessions": st.sessions})
            if method == "GET" and path.startswith("/api/vault/sessions/"):
                sid = path[len("/api/vault/sessions/"):]
                for s in st.sessions:
                    if s["id"] == sid:
                        detail = dict(s)
                        detail["downloadUrl"] = f"{self.server.base}/object/{sid}"
                        detail["snapshots"] = []
                        return self._json(200, detail)
                return self._send(404, b"not found", "text/plain")
            if method == "GET" and path.startswith("/object/"):
                sid = path[len("/object/"):]
                if sid in st.objects:
                    return self._send(200, st.objects[sid], "application/zstd")
                return self._send(404, b"missing object", "text/plain")
            return self._send(404, b"not found", "text/plain")

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")


class Mock(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.state = MockState()
        self.lock = threading.RLock()
        self.base = f"http://127.0.0.1:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def requests_to(self, prefix: str) -> list[dict]:
        return [r for r in self.state.requests if r["url"].startswith(prefix)]


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def write(path: Path, content: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    with open(path, mode) as fh:
        fh.write(content)
    return path


def make_fixture(root: Path) -> None:
    """Agent directories exercising every discovery rule."""
    claude = root / ".claude" / "projects"
    write(claude / "-Users-me-work-cmux" / f"{UUID_A}.jsonl", '{"type":"message","cwd":"/Users/me/work/cmux"}\n')
    write(claude / "-Users-me-work-cmux" / "not-a-session.jsonl", "{}\n")
    write(claude / "-Users-me-work-cmux" / f"{UUID_B}.txt", "{}\n")
    write(claude / "-Users-me-work-cmux" / f"{UUID_B}.JSONL", "{}\n")
    write(claude / "nested" / "deeper" / f"{UUID_B}.jsonl", 'not json\n\n{"a":{"b":[{"cwd":"  "},{"cwd":"/nested"}]}}\n')
    write(claude / "munged-only" / f"{UUID_C.upper()}.jsonl", "")
    write(claude / "crlf" / f"{UUID_D}.jsonl", '\r\n{"cwd":"/crlf"}\r\n')
    write(claude / "late" / f"{UUID_E}.jsonl", ('{"x":1}\n' * 128) + '{"cwd":"/too-late"}\n')
    write(claude / "long" / f"{UUID_F}.jsonl", '{"pad":"' + ("x" * (1024 * 1024)) + '"}\n{"cwd":"/after-long"}\n')
    os.symlink(root / "missing-target.jsonl", claude / "nested" / f"{UUID_C}.jsonl")
    write(root / "elsewhere" / "secret.jsonl", '{"cwd":"/secret"}\n')
    os.symlink(root / "elsewhere" / "secret.jsonl", claude / "nested" / f"{UUID_D}.jsonl")
    os.symlink(root / "elsewhere", claude / "linked-dir")

    codex = root / ".codex"
    write(codex / CODEX_REL, f'{{"type":"session_meta","payload":{{"id":"{UUID_A}","cwd":"/repo"}}}}\n{{"message":"hello"}}\n')
    write(codex / "sessions" / "2026" / "07" / "05" / f"rollout-2026-07-05T00-00-00-{UUID_B.upper()}.jsonl", '{"type":"session_meta","payload":{"id":"not-a-uuid","cwd":" /meta/cwd "}}\n')
    write(codex / "archived_sessions" / "2026" / "07" / "03" / f"rollout-2026-07-03T00-00-00-{UUID_C}.jsonl", '{"type":"other","payload":{}}\n{"nested":{"deep":{"cwd":"/from/body"}}}\n')
    write(codex / "sessions" / "2026" / "07" / "04" / f"junk-{UUID_D}.jsonl", "{}\n")
    write(codex / "sessions" / "2026" / "07" / "06" / f"rollout-x-{UUID_E}.jsonl", "")
    write(codex / "sessions" / "2026" / "07" / "07" / f"rollout-x-{UUID_F}.jsonl", "null\n{\"cwd\":\"/after-null\"}\n")

    pi = root / ".pi" / "agent" / "sessions"
    write(pi / "-Users-me-work-cmux" / f"2026-07-04T00-00-00_{UUID_D}.jsonl", '{"cwd":"/Users/me/work/cmux"}\n')
    write(pi / "-Users-me-work-cmux" / "junk.jsonl", "{}\n")
    write(pi / "-munged-dir" / f"x_{UUID_A}.jsonl", "not json\n")
    write(pi / "-munged-dir" / f"y_{UUID_B.upper()}.JSONL", '{"cwd":"/pi-upper"}\n')


def make_simple_fixture(root: Path) -> None:
    """Unique session ids per agent, so resume flows are unambiguous."""
    write(root / ".codex" / CODEX_REL, f'{{"type":"session_meta","payload":{{"id":"{UUID_A}","cwd":"/repo"}}}}\n{{"message":"hello"}}\n')
    write(root / ".codex" / "archived_sessions" / "2026" / "07" / "03" / f"rollout-2026-07-03T00-00-00-{UUID_C}.jsonl", '{"type":"other","payload":{}}\n')
    write(root / ".claude" / "projects" / "-Users-me-work" / f"{UUID_B}.jsonl", '{"type":"user","cwd":"/Users/me/work"}\n')
    write(root / ".claude" / "projects" / "-Users-me-work" / "notes.jsonl", "{}\n")
    write(root / ".pi" / "agent" / "sessions" / "-Users-me-work" / f"2026-07-04T00-00-00_{UUID_D}.jsonl", "{}\n")
    write(root / "elsewhere" / "secret.jsonl", '{"cwd":"/secret"}\n')
    os.symlink(root / "elsewhere" / "secret.jsonl", root / ".claude" / "projects" / "-Users-me-work" / f"{UUID_F}.jsonl")


def copy_fixture(src: Path, dst: Path) -> None:
    shutil.copytree(src, dst, symlinks=True, copy_function=shutil.copy2)


# --------------------------------------------------------------------------
# Running + comparing
# --------------------------------------------------------------------------


@dataclass
class Run:
    code: int
    stdout: str
    stderr: str


def run_bin(binary: str, home: Path, args: list[str], api_base: str = "", tz: str = "UTC", extra_env: dict | None = None, cwd: Path | None = None) -> Run:
    env = {
        "HOME": str(home),
        "TZ": tz,
        "PATH": "/usr/bin:/bin",
        "CMUX_VAULT_CONFIG_DIR": str(home / "cfg"),
        "CMUX_VAULT_STATE_DIR": str(home / "state"),
        "TMPDIR": str(home / "tmp"),
    }
    if api_base:
        env["CMUX_VAULT_API_BASE"] = api_base
    if extra_env:
        env.update(extra_env)
    p = subprocess.run([binary, *args], env=env, capture_output=True, text=True, cwd=cwd)
    return Run(p.returncode, p.stdout, p.stderr)


HOME_RE = re.compile(r"/tmp/vault-parity-[^/]+/[a-z]+home-(go|rs)")


def norm_home(text: str) -> str:
    return HOME_RE.sub("$HOME", text)


def norm_sizes(text: str) -> str:
    text = norm_home(text)
    text = re.sub(r"-> \d+ bytes\)", "-> N bytes)", text)
    text = re.sub(r"compressedBytes=\d+", "compressedBytes=N", text)
    return text


def norm_json(text: str, allow: set[str]):
    obj = json.loads(text)
    if "sessions" in allow and obj.get("sessions") is None:
        obj["sessions"] = []
    if "version" in allow and "version" in obj:
        obj["version"] = "X"
    if "compressedBytesUploaded" in obj:
        obj["compressedBytesUploaded"] = "N"
    return obj


def check(name: str, cond: bool, detail: str = "") -> None:
    global checks
    checks += 1
    if not cond:
        failures.append(f"{name}: {detail}")
        print(f"  FAIL {name}: {detail}")


def compare(name: str, go: Run, rs: Run, *, json_out: bool = False, allow: set[str] = frozenset(), sort_stdout: bool = False, stderr_mode: str = "exact", stdout_mode: str = "exact") -> None:
    check(f"{name}/exit", go.code == rs.code, f"go={go.code} rs={rs.code}\n  go.stderr={go.stderr!r}\n  rs.stderr={rs.stderr!r}")
    if json_out and not go.stdout.strip() and not rs.stdout.strip():
        check(f"{name}/stdout-empty", True)
    elif json_out:
        try:
            a, b = norm_json(norm_home(go.stdout), allow), norm_json(norm_home(rs.stdout), allow)
            check(f"{name}/stdout-json", a == b, f"\n  go={json.dumps(a)[:600]}\n  rs={json.dumps(b)[:600]}")
        except Exception as e:  # noqa: BLE001
            check(f"{name}/stdout-json", False, f"parse error {e}: go={go.stdout!r} rs={rs.stdout!r}")
    elif stdout_mode == "exact":
        a, b = norm_sizes(go.stdout), norm_sizes(rs.stdout)
        if sort_stdout:
            a, b = "\n".join(sorted(a.splitlines())), "\n".join(sorted(b.splitlines()))
        check(f"{name}/stdout", a == b, f"\n  go={a!r}\n  rs={b!r}")
    elif stdout_mode == "prefix-lines":
        # Compare each line up to the first ": " (error text may differ).
        a = sorted(l.split(": ")[0] for l in norm_home(go.stdout).splitlines())
        b = sorted(l.split(": ")[0] for l in norm_home(rs.stdout).splitlines())
        check(f"{name}/stdout-prefix", a == b, f"\n  go={go.stdout!r}\n  rs={rs.stdout!r}")
    if stderr_mode == "exact":
        check(f"{name}/stderr", norm_home(go.stderr) == norm_home(rs.stderr), f"\n  go={go.stderr!r}\n  rs={rs.stderr!r}")
    elif stderr_mode == "prefix":
        a = go.stderr.split(":")[0]
        b = rs.stderr.split(":")[0]
        check(f"{name}/stderr-prefix", a == b and bool(a), f"\n  go={go.stderr!r}\n  rs={rs.stderr!r}")
    elif stderr_mode == "first-line":
        check(f"{name}/stderr-first-line", go.stderr.splitlines()[:1] == rs.stderr.splitlines()[:1], f"\n  go={go.stderr!r}\n  rs={rs.stderr!r}")


def both(name: str, home: Path, args: list[str], **kw) -> tuple[Run, Run]:
    cmp_kw = {k: kw.pop(k) for k in list(kw) if k in {"json_out", "allow", "sort_stdout", "stderr_mode", "stdout_mode"}}
    go = run_bin(GO, home, args, **kw)
    rs = run_bin(RS, home, args, **kw)
    compare(name, go, rs, **cmp_kw)
    return go, rs


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------


def scenario_cli_surface(home: Path) -> None:
    print("== cli surface")
    both("version", home, ["version"], stdout_mode="skip", stderr_mode="exact")  # expected deviation: dev vs semver
    both("version-json", home, ["--json", "version"], json_out=True, allow={"version"})
    both("noargs", home, [])
    both("unknown", home, ["bogus"])
    both("help", home, ["help"])
    both("help-flag", home, ["--help"])
    both("h-flag", home, ["-h"])
    both("dashdash-help", home, ["--", "--help"])
    both("dashdash-scan", home, ["--", "scan", "--agent", "pi"])
    both("scan-bogus", home, ["scan", "--bogus"])
    both("scan-help", home, ["scan", "-h"])
    both("api-base-missing", home, ["--api-base"])
    both("json-bad-bool", home, ["scan", "--json=maybe"])
    both("limit-bad-int", home, ["sync", "--dry-run", "--limit", "abc"])
    both("limit-hex", home, ["sync", "--dry-run", "--limit=0x1"])
    both("limit-octal", home, ["sync", "--dry-run", "--limit=010"])
    both("bad-syntax", home, ["---x"])
    both("resume-noid", home, ["resume"])
    both("resume-two-ids", home, ["resume", "a", "b"])
    both("status-extra-arg", home, ["status", "extra"])
    both("scan-agent-missing-value", home, ["scan", "--agent"])
    both("api-base-eq", home, ["--api-base=http://127.0.0.1:9/", "status"])


def scenario_scan(home: Path) -> None:
    print("== scan")
    for tz in ["UTC", "Asia/Tokyo", "America/New_York", "Europe/London", "Asia/Kolkata"]:
        both(f"scan-text-{tz}", home, ["scan"], tz=tz)
        both(f"scan-json-{tz}", home, ["--json", "scan"], tz=tz, json_out=True)
    both("scan-json-flag-after", home, ["scan", "-json"], json_out=True)
    both("scan-json-eq-true", home, ["scan", "--json=true"], json_out=True)
    both("scan-json-eq-false", home, ["--json", "scan", "--json=false"])
    for agent in ["claude", "codex", "pi", " PI ", "Codex", "nope", ""]:
        both(f"scan-agent-{agent.strip() or 'empty'}", home, ["scan", "--agent", agent])
        both(f"scan-agent-json-{agent.strip() or 'empty'}", home, ["--json", "scan", f"--agent={agent}"], json_out=True, allow={"sessions"})
    # Unclean env overrides exercise Clean/Rel parity.
    unclean = {"CLAUDE_CONFIG_DIR": f"{home}//.claude/", "CODEX_HOME": f"{home}/./.codex//"}
    both("scan-unclean-env", home, ["scan"], extra_env=unclean)
    both("scan-unclean-env-json", home, ["--json", "scan"], extra_env=unclean, json_out=True)
    # Overrides pointing at missing dirs.
    missing = {"CLAUDE_CONFIG_DIR": f"{home}/nope", "CODEX_HOME": f"{home}/nope2"}
    both("scan-missing-env", home, ["scan"], extra_env=missing)
    both("scan-missing-env-json", home, ["--json", "scan"], extra_env=missing, json_out=True, allow={"sessions"})
    # Whitespace-only override falls back to the default.
    both("scan-blank-env", home, ["scan", "--agent", "codex"], extra_env={"CODEX_HOME": "   "})
    # Empty HOME.
    both("scan-no-home", home, ["scan"], extra_env={"HOME": ""}, stderr_mode="exact")


def scenario_symlinked_roots(base: Path) -> None:
    print("== symlinked roots")
    home = base / "linkhome"
    shared = base / "shared"
    write(shared / "claude-projects" / "-repo" / f"{UUID_A}.jsonl", '{"cwd":"/repo"}\n')
    (home / "claude-config").mkdir(parents=True)
    os.symlink(shared / "claude-projects", home / "claude-config" / "projects")
    write(shared / "codex-sessions" / "2026" / "04" / "05" / f"rollout-2026-04-05T20-01-13-{UUID_B}.jsonl", f'{{"type":"session_meta","payload":{{"id":"{UUID_B}","cwd":"/repo"}}}}\n')
    (home / ".codex").mkdir(parents=True)
    os.symlink(shared / "codex-sessions", home / ".codex" / "sessions")
    write(shared / "pi-sessions" / "-repo" / f"2026-07-02T07-56-15-262Z_{UUID_D}.jsonl", '{"cwd":"/repo"}\n')
    (home / ".pi" / "agent").mkdir(parents=True)
    os.symlink(shared / "pi-sessions", home / ".pi" / "agent" / "sessions")
    env = {"CLAUDE_CONFIG_DIR": str(home / "claude-config")}
    both("linked-scan", home, ["scan"], extra_env=env)
    both("linked-scan-json", home, ["--json", "scan"], extra_env=env, json_out=True)
    both("linked-dry-run", home, ["sync", "--dry-run"], extra_env=env)


def scenario_status(home: Path) -> None:
    print("== status")
    both("status-fresh", home, ["status"])
    both("status-fresh-json", home, ["--json", "status"], json_out=True)
    write(home / "state" / "state.json", '{"entries":{"codex\\u0000a":{"sizeBytes":1},"pi\\u0000b":{}}}')
    both("status-tracked", home, ["status"])
    both("status-tracked-json", home, ["status", "--json"], json_out=True)
    write(home / "state" / "state.json", "")
    both("status-empty-state", home, ["status"])
    write(home / "state" / "state.json", '{"entries":null}')
    both("status-null-entries", home, ["status"])
    write(home / "state" / "state.json", "{broken")
    both("status-broken-state", home, ["status"], stderr_mode="prefix")
    (home / "state" / "state.json").unlink()
    write(home / "cfg" / "auth.json", '{"accessToken":"a","refreshToken":"r"}')
    both("status-logged-in", home, ["status"])
    write(home / "cfg" / "auth.json", '{"accessToken":"a","refreshToken":null}')
    both("status-null-token", home, ["status"])
    write(home / "cfg" / "auth.json", '{"accessToken":"a"}')
    both("status-half-token", home, ["status"])
    write(home / "cfg" / "auth.json", "garbage")
    both("status-bad-auth", home, ["status"], stderr_mode="prefix")
    (home / "cfg" / "auth.json").unlink()
    both("logout-none", home, ["logout"])
    both("logout-none-json", home, ["--json", "logout"], json_out=True)
    write(home / "cfg" / "auth.json", '{"accessToken":"a","refreshToken":"r"}')
    both("logout", home, ["logout"])
    check("logout/removed", not (home / "cfg" / "auth.json").exists(), "auth.json still present")


def scenario_dry_run(home: Path) -> None:
    print("== sync dry run / not logged in")
    both("sync-nologin", home, ["sync"])
    both("sync-nologin-json", home, ["--json", "sync"])
    both("dry-run", home, ["sync", "--dry-run"])
    both("dry-run-json", home, ["sync", "--dry-run", "--json"], json_out=True)
    both("dry-run-limit", home, ["sync", "--dry-run", "--limit", "2"])
    both("dry-run-limit-neg", home, ["sync", "--dry-run", "--limit", "-1"])
    both("dry-run-agent", home, ["sync", "--dry-run", "--agent", "codex"])
    both("dry-run-agent-bad", home, ["sync", "--dry-run", "--agent", "zzz"])
    both("dry-run-agent-bad-json", home, ["--json", "sync", "--dry-run", "--agent", "zzz"], json_out=True)
    check("dry-run/no-state", not (home / "state").exists(), "dry run wrote state")


def login(home: Path) -> None:
    write(home / "cfg" / "auth.json", '{"accessToken":"access-token","refreshToken":"refresh-token"}')


def scenario_sync(fixture: Path, base: Path) -> None:
    print("== sync (mock server)")
    homes = {}
    for label in ("go", "rs"):
        homes[label] = base / f"synchome-{label}"
        copy_fixture(fixture, homes[label])
        login(homes[label])
    binaries = {"go": GO, "rs": RS}
    servers = {"go": Mock(), "rs": Mock()}

    def run_both(name: str, args: list[str], **kw):
        runs = {}
        for label in ("go", "rs"):
            runs[label] = run_bin(binaries[label], homes[label], args, api_base=servers[label].base)
        compare(name, runs["go"], runs["rs"], **kw)
        return runs

    def states():
        out = {}
        for label in ("go", "rs"):
            p = homes[label] / "state" / "state.json"
            out[label] = json.loads(p.read_text()) if p.exists() else None
        return out

    run_both("sync-1", ["sync", "--agent", "codex"], sort_stdout=True)
    st = states()
    check("sync-1/state", st["go"] == st["rs"], f"\n  go={st['go']}\n  rs={st['rs']}")
    check("sync-1/blob-keys", set(servers["go"].state.blobs) == set(servers["rs"].state.blobs) and len(servers["go"].state.blobs) == 2, f"go={list(servers['go'].state.blobs)} rs={list(servers['rs'].state.blobs)}")
    for key in servers["go"].state.blobs:
        a = zstd_decompress(servers["go"].state.blobs[key])
        b = zstd_decompress(servers["rs"].state.blobs.get(key, b""))
        check(f"sync-1/blob-{key.split('/')[-1][:20]}", a == b, "decompressed upload mismatch")
    # Request shape parity on the presign call.
    ga = json.loads(servers["go"].requests_to("/api/vault/uploads")[0]["body"])
    ra = json.loads(servers["rs"].requests_to("/api/vault/uploads")[0]["body"])
    for item in ga["items"] + ra["items"]:
        item.pop("compressedSizeBytes", None)
    check("sync-1/presign-body", sorted(ga["items"], key=lambda i: i["relPath"]) == sorted(ra["items"], key=lambda i: i["relPath"]), f"\n  go={ga}\n  rs={ra}")
    gh = servers["go"].requests_to("/api/vault/uploads")[0]["headers"]
    rh = servers["rs"].requests_to("/api/vault/uploads")[0]["headers"]
    for h in ("authorization", "x-stack-refresh-token", "content-type", "accept"):
        check(f"sync-1/header-{h}", gh.get(h) == rh.get(h), f"go={gh.get(h)!r} rs={rh.get(h)!r}")
    gp = servers["go"].requests_to("/put")[0]["headers"]
    rp = servers["rs"].requests_to("/put")[0]["headers"]
    check("sync-1/put-content-type", gp.get("content-type") == rp.get("content-type") == "application/zstd", f"go={gp} rs={rp}")
    check("sync-1/put-content-length", "content-length" in gp and "content-length" in rp and "transfer-encoding" not in rp, f"go={gp} rs={rp}")

    run_both("sync-2-unchanged", ["sync", "--agent", "codex"], sort_stdout=True)
    run_both("sync-2-all", ["sync"], sort_stdout=True)
    run_both("sync-3-json", ["--json", "sync"], json_out=True)
    st = states()
    check("sync-3/state", st["go"] == st["rs"], f"\n  go={st['go']}\n  rs={st['rs']}")

    # Same content, new mtime -> hashed, recognized as already uploaded.
    for label in ("go", "rs"):
        p = homes[label] / ".codex" / CODEX_REL
        os.utime(p, ns=(1_800_000_000_123_456_789, 1_800_000_000_123_456_789))
    run_both("sync-4-touched", ["sync", "--agent", "codex"], sort_stdout=True)

    # Changed content -> re-upload.
    for label in ("go", "rs"):
        p = homes[label] / ".codex" / CODEX_REL
        with open(p, "a") as fh:
            fh.write('{"message":"updated"}\n')
        os.utime(p, ns=(1_800_000_010_000_000_000, 1_800_000_010_000_000_000))
    run_both("sync-5-changed", ["sync", "--agent", "codex"], sort_stdout=True)
    st = states()
    check("sync-5/state", st["go"] == st["rs"], f"\n  go={st['go']}\n  rs={st['rs']}")

    # Cloud already has the content (state wiped) -> cloud unchanged.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
    run_both("sync-6-cloud-unchanged", ["sync", "--agent", "codex"], sort_stdout=True)
    st = states()
    check("sync-6/state", st["go"] == st["rs"], f"\n  go={st['go']}\n  rs={st['rs']}")

    # 5xx retried once then succeeds.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
        servers[label].state.committed.clear()
        servers[label].state.fail_uploads = 1
        servers[label].state.requests.clear()
    run_both("sync-7-retry-ok", ["sync", "--agent", "codex"], sort_stdout=True)
    for label in ("go", "rs"):
        check(f"sync-7/{label}-attempts", len(servers[label].requests_to("/api/vault/uploads")) == 2, str(len(servers[label].requests_to("/api/vault/uploads"))))

    # 5xx twice -> failure.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
        servers[label].state.committed.clear()
        servers[label].state.fail_uploads = 2
        servers[label].state.requests.clear()
    run_both("sync-8-retry-fail", ["sync", "--agent", "codex"], sort_stdout=True)
    run_both("sync-8b-json", ["--json", "sync", "--agent", "codex"], json_out=True)
    for label in ("go", "rs"):
        check(f"sync-8/{label}-attempts", len(servers[label].requests_to("/api/vault/uploads")) == 3, str(len(servers[label].requests_to("/api/vault/uploads"))))
        servers[label].state.fail_uploads = 0

    # 4xx -> no retry.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
        servers[label].state.committed.clear()
        servers[label].state.api_override = (401, '{"error":"unauthorized"}')
        servers[label].state.requests.clear()
    run_both("sync-9-401", ["sync", "--agent", "codex"], sort_stdout=True)
    for label in ("go", "rs"):
        check(f"sync-9/{label}-attempts", len(servers[label].requests_to("/api/vault/uploads")) == 1, str(len(servers[label].requests_to("/api/vault/uploads"))))
        servers[label].state.api_override = None

    # Storage rejects PUT.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
        servers[label].state.committed.clear()
        servers[label].state.reject_puts = True
    run_both("sync-10-put-403", ["sync", "--agent", "codex"], sort_stdout=True)
    for label in ("go", "rs"):
        servers[label].state.reject_puts = False
        servers[label].state.drop_puts = True
    run_both("sync-11-commit-missing", ["sync", "--agent", "codex"], sort_stdout=True)
    for label in ("go", "rs"):
        servers[label].state.drop_puts = False

    # Connection refused: message text differs by HTTP stack, compare shape.
    for label in ("go", "rs"):
        write(homes[label] / "state" / "state.json", "{}")
    runs = {}
    for label in ("go", "rs"):
        runs[label] = run_bin(binaries[label], homes[label], ["sync", "--agent", "codex"], api_base="http://127.0.0.1:9")
    compare("sync-12-refused", runs["go"], runs["rs"], stdout_mode="prefix-lines")

    # Temp files cleaned up.
    for label in ("go", "rs"):
        tmp = homes[label] / "tmp"
        left = list(tmp.iterdir()) if tmp.exists() else []
        check(f"sync/{label}-tmp-clean", not left, str(left))


def scenario_resume(fixture: Path, base: Path) -> None:
    print("== resume (mock server)")
    homes = {}
    for label in ("go", "rs"):
        homes[label] = base / f"resumehome-{label}"
        copy_fixture(fixture, homes[label])
        login(homes[label])
    binaries = {"go": GO, "rs": RS}
    servers = {"go": Mock(), "rs": Mock()}
    plain = f'{{"type":"session_meta","payload":{{"id":"{UUID_A}","cwd":"/repo"}}}}\n{{"message":"hello"}}\n'.encode()
    for label in ("go", "rs"):
        servers[label].state.sessions.append({"id": "cloud-session", "agent": "codex", "agentSessionId": UUID_A, "relPath": CODEX_REL, "cwd": "/repo", "latestSha256": "", "sizeBytes": 0, "lastUploadedAt": ""})
        servers[label].state.objects["cloud-session"] = zstd_compress(plain)

    def run_both(name: str, args: list[str], **kw):
        runs = {}
        for label in ("go", "rs"):
            runs[label] = run_bin(binaries[label], homes[label], args, api_base=servers[label].base)
        compare(name, runs["go"], runs["rs"], **kw)
        return runs

    run_both("resume-local", ["resume", UUID_A.upper()])
    run_both("resume-local-json", ["--json", "resume", UUID_A], json_out=True)
    run_both("resume-local-agent", ["resume", "--agent", "codex", UUID_A])
    run_both("resume-local-claude", ["resume", UUID_B])
    run_both("resume-local-pi-munged", ["resume", "--agent", "pi", UUID_D])
    run_both("resume-local-pi-nofilter", ["resume", UUID_D])
    run_both("resume-local-archived", ["resume", UUID_C])
    run_both("resume-empty-id", ["resume", "   "])
    for label in ("go", "rs"):
        check(f"resume-local/{label}-no-api", not servers[label].requests_to("/api"), "local hit called the API")
    run_both("resume-symlinked-id-not-local", ["resume", UUID_F])
    for label in ("go", "rs"):
        servers[label].state.requests.clear()
        (homes[label] / ".codex" / CODEX_REL).unlink()
    run_both("resume-cloud", ["resume", "--agent", "codex", UUID_A])
    for label in ("go", "rs"):
        p = homes[label] / ".codex" / CODEX_REL
        check(f"resume-cloud/{label}-bytes", p.exists() and p.read_bytes() == plain, "restored bytes mismatch")
        check(f"resume-cloud/{label}-mode", p.exists() and (p.stat().st_mode & 0o777) == 0o600, oct(p.stat().st_mode) if p.exists() else "missing")
        look = servers[label].requests_to("/api/vault/sessions?")
        check(f"resume-cloud/{label}-lookup", look and look[0]["url"] == f"/api/vault/sessions?agent=codex&agentSessionId={UUID_A}&limit=2", str([r["url"] for r in look]))
        left = [x for x in (homes[label] / ".codex" / "sessions" / "2026" / "07" / "04").iterdir() if x.name.startswith(".restore-")]
        check(f"resume-cloud/{label}-tmp", not left, str(left))
    # Existing undiscovered file: refuse without --force, then overwrite.
    overwrite_rel = "sessions/2026/07/04/not-a-discovered-session.jsonl"
    for label in ("go", "rs"):
        write(homes[label] / ".codex" / overwrite_rel, "existing\n")
        servers[label].state.sessions = [{"id": "cloud-overwrite", "agent": "codex", "agentSessionId": UUID_E, "relPath": overwrite_rel, "cwd": "/repo"}]
        servers[label].state.objects["cloud-overwrite"] = zstd_compress(b"from cloud\n")
    run_both("resume-refuse-overwrite", ["resume", UUID_E])
    run_both("resume-force", ["resume", "--force", UUID_E])
    for label in ("go", "rs"):
        check(f"resume-force/{label}-bytes", (homes[label] / ".codex" / overwrite_rel).read_bytes() == b"from cloud\n", "not overwritten")
    run_both("resume-force-again", ["resume", "-force", UUID_E])
    # Local exists but vault does not know it (with --force).
    for label in ("go", "rs"):
        servers[label].state.sessions = []
    run_both("resume-force-not-in-vault", ["resume", "--force", UUID_A])
    run_both("resume-not-found", ["resume", UUID_F])
    run_both("resume-not-found-json", ["--json", "resume", UUID_F])
    # Ambiguous in the vault.
    for label in ("go", "rs"):
        servers[label].state.sessions = [
            {"id": "one", "agent": "codex", "agentSessionId": UUID_F, "relPath": "x.jsonl"},
            {"id": "two", "agent": "claude", "agentSessionId": UUID_F, "relPath": "x.jsonl"},
        ]
    run_both("resume-ambiguous-cloud", ["resume", UUID_F])
    for label in ("go", "rs"):
        servers[label].state.sessions = [{"id": "g", "agent": "gemini", "agentSessionId": UUID_F, "relPath": "x.jsonl"}]
    run_both("resume-unknown-agent", ["resume", UUID_F])
    for label in ("go", "rs"):
        servers[label].state.sessions = [{"id": "evil", "agent": "codex", "agentSessionId": UUID_F, "relPath": "../../escape.jsonl"}]
        servers[label].state.objects["evil"] = zstd_compress(b"evil\n")
    run_both("resume-traversal", ["resume", UUID_F])
    for label in ("go", "rs"):
        check(f"resume-traversal/{label}-no-escape", not (homes[label] / "escape.jsonl").exists(), "traversal wrote a file")
        servers[label].state.sessions = [{"id": "nodl", "agent": "codex", "agentSessionId": UUID_F, "relPath": "sessions/x.jsonl"}]
    run_both("resume-missing-object", ["resume", UUID_F])
    for label in ("go", "rs"):
        servers[label].state.sessions = [{"id": "nodl2", "agent": "codex", "agentSessionId": UUID_F, "relPath": "sessions/x.jsonl"}]
        servers[label].state.objects["nodl2"] = b"not zstd at all"
    run_both("resume-corrupt-object", ["resume", UUID_F], stderr_mode="prefix")
    for label in ("go", "rs"):
        servers[label].state.api_override = (503, "down")
    run_both("resume-503", ["resume", UUID_F])
    for label in ("go", "rs"):
        servers[label].state.api_override = None
        (homes[label] / "cfg" / "auth.json").unlink()
    run_both("resume-nologin", ["resume", UUID_A])


def scenario_login(base: Path) -> None:
    print("== login (mock device flow)")
    homes = {}
    for label in ("go", "rs"):
        homes[label] = base / f"loginhome-{label}"
        homes[label].mkdir()
    binaries = {"go": GO, "rs": RS}

    class LoginHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n) if n else b""
            self.server.calls.append((self.path, body))
            if self.path == "/api/vault/cli/auth/start":
                data = json.dumps({"deviceCode": "dev-code", "userCode": "ABCD-1234", "verificationUrl": "https://example.test/approve", "expiresInSeconds": 30, "intervalSeconds": 1}).encode()
            elif self.path == "/api/vault/cli/auth/poll":
                self.server.polls += 1
                if self.server.polls < 2:
                    data = json.dumps({"status": "pending"}).encode()
                else:
                    data = json.dumps(self.server.final).encode()
            else:
                data = b"{}"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    def start(final):
        srv = socketserver.ThreadingMixIn.__new__(type("L", (socketserver.ThreadingMixIn, http.server.HTTPServer), {"daemon_threads": True}))
        type(srv).__init__(srv, ("127.0.0.1", 0), LoginHandler)
        srv.calls, srv.polls, srv.final = [], 0, final
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv

    for name, final in [
        ("approved", {"status": "approved", "accessToken": "AT", "refreshToken": "RT"}),
        ("denied", {"status": "denied"}),
        ("weird", {"status": "weird"}),
        ("approved-no-tokens", {"status": "approved"}),
    ]:
        runs = {}
        for label in ("go", "rs"):
            srv = start(final)
            runs[label] = run_bin(binaries[label], homes[label], ["login"], api_base=f"http://127.0.0.1:{srv.server_address[1]}")
            poll_body = [b for p, b in srv.calls if p.endswith("/poll")]
            check(f"login-{name}/{label}-poll-body", poll_body and json.loads(poll_body[0]) == {"deviceCode": "dev-code"}, str(poll_body))
            start_body = [b for p, b in srv.calls if p.endswith("/start")]
            check(f"login-{name}/{label}-start-body", start_body and json.loads(start_body[0]) == {}, str(start_body))
            srv.shutdown()
        compare(f"login-{name}", runs["go"], runs["rs"])
        if name == "approved":
            for label in ("go", "rs"):
                auth = homes[label] / "cfg" / "auth.json"
                check(f"login/{label}-auth-file", auth.exists() and json.loads(auth.read_text()) == {"accessToken": "AT", "refreshToken": "RT"}, "missing or wrong auth.json")
                check(f"login/{label}-auth-mode", auth.exists() and (auth.stat().st_mode & 0o777) == 0o600, oct(auth.stat().st_mode) if auth.exists() else "missing")
                check(f"login/{label}-auth-text", auth.read_text() == '{\n  "accessToken": "AT",\n  "refreshToken": "RT"\n}\n', repr(auth.read_text()))
            runs = {}
            for label in ("go", "rs"):
                srv = start(final)
                runs[label] = run_bin(binaries[label], homes[label], ["--json", "login"], api_base=f"http://127.0.0.1:{srv.server_address[1]}")
                srv.shutdown()
            compare("login-approved-json", runs["go"], runs["rs"], json_out=True)


def main() -> int:
    base = Path(tempfile.mkdtemp(prefix="vault-parity-"))
    fixture = base / "fixture"
    fixture.mkdir()
    make_fixture(fixture)
    print(f"fixture: {fixture}")
    scenario_cli_surface(fixture)
    scenario_scan(fixture)
    scenario_symlinked_roots(base)
    status_home = base / "statushome"
    copy_fixture(fixture, status_home)
    scenario_status(status_home)
    dry_home = base / "dryhome"
    copy_fixture(fixture, dry_home)
    scenario_dry_run(dry_home)
    simple = base / "simple"
    simple.mkdir()
    make_simple_fixture(simple)
    scenario_sync(simple, base)
    scenario_resume(simple, base)
    scenario_login(base)
    print(f"\n{checks} checks, {len(failures)} failures")
    for f in failures:
        print(" -", f.splitlines()[0][:200])
    shutil.rmtree(base, ignore_errors=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

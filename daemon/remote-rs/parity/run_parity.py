#!/usr/bin/env python3
"""Differential parity harness for the Go and Rust `cmuxd-remote` binaries.

Both binaries are driven through identical scenarios (process invocations,
stdio JSON-RPC scripts, persistent-daemon lifecycle, and CLI relay calls
against a mock cmux socket). Their observable behavior (exit codes, stdout,
stderr, RPC events, recorded socket requests) is normalized and diffed.

Usage:
    python3 daemon/remote-rs/parity/run_parity.py [--go-bin PATH] [--rust-bin PATH]
                                                  [--only SUBSTR] [--require-go]
                                                  [--transcripts DIR]

Without --go-bin the Go binary is built with `go build`; without a Go
toolchain the harness reports SKIP and exits 0 (or 3 with --require-go).
Without --rust-bin the Rust binary is built with `cargo build`.
"""

from __future__ import annotations

import argparse
import base64
import difflib
import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable

HERE = pathlib.Path(__file__).resolve()
REPO = HERE.parents[3]
GO_DIR = REPO / "daemon" / "remote"
RS_DIR = REPO / "daemon" / "remote-rs"

SCRUB_KEYS = {"attachment_token", "client_attachment_token", "token", "request_id", "pid"}
STEP_TIMEOUT = 15.0


# --- normalization ---------------------------------------------------------


def scrub(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in SCRUB_KEYS or key.endswith("_at"):
                out[key] = "<scrubbed>" if item not in ("", None) else item
            else:
                out[key] = scrub(item)
        return out
    if isinstance(value, list):
        return [scrub(item) for item in value]
    return value


def parse_line(line: str) -> Any:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"raw": line}


def normalize_rpc_output(lines: list[str]) -> dict[str, Any]:
    """Split stdout lines into ordered responses and events; merge streamed
    data events per stream so chunking differences do not show up as diffs."""
    responses: list[Any] = []
    events: list[Any] = []
    merged: dict[tuple, dict[str, Any]] = {}
    for line in lines:
        obj = parse_line(line)
        if not isinstance(obj, dict):
            responses.append({"raw": line})
            continue
        if "event" in obj:
            event = obj["event"]
            if event in ("pty.data", "proxy.data") and "data_base64" in obj:
                key = (event, obj.get("session_id", ""), obj.get("attachment_id", ""), obj.get("stream_id", ""))
                entry = merged.get(key)
                data = base64.b64decode(obj["data_base64"])
                if entry is None:
                    entry = {k: v for k, v in obj.items() if k != "data_base64"}
                    entry["data"] = ""
                    merged[key] = entry
                    events.append(entry)
                entry["data"] += data.decode("utf-8", "backslashreplace")
            else:
                events.append(obj)
        else:
            responses.append(obj)
    return {"responses": scrub(responses), "events": scrub(events)}


JSON_ERROR_PREFIXES = ("must be valid JSON: ", "invalid JSON params: ")


def normalize_text(text: str, replacements: list[tuple[str, str]]) -> str:
    for old, new in replacements:
        if old:
            text = text.replace(old, new)
    # encoding/json and serde_json describe parse failures differently; the
    # exit code and message prefix are the contract, not the parser's prose.
    lines = []
    for line in text.split("\n"):
        for prefix in JSON_ERROR_PREFIXES:
            idx = line.find(prefix)
            if idx >= 0:
                line = line[: idx + len(prefix)] + "<json-parse-error>"
        lines.append(line)
    return "\n".join(lines)


# --- process helpers -------------------------------------------------------


def base_env(home: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("CMUX")}
    env["HOME"] = home
    env.setdefault("TERM", "xterm-256color")
    env.pop("TMUX", None)
    env.pop("TMUX_PANE", None)
    return env


def run_process(argv: list[str], stdin: bytes = b"", env: dict[str, str] | None = None, timeout: float = 30.0) -> dict[str, Any]:
    try:
        proc = subprocess.run(argv, input=stdin, capture_output=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as err:
        return {"code": "timeout", "stdout": (err.stdout or b"").decode("utf-8", "replace"), "stderr": (err.stderr or b"").decode("utf-8", "replace")}
    return {"code": proc.returncode, "stdout": proc.stdout.decode("utf-8", "replace"), "stderr": proc.stderr.decode("utf-8", "replace")}


class StdioSession:
    """Drives `serve --stdio` interactively so scripts can wait for events."""

    def __init__(self, argv: list[str], env: dict[str, str]):
        self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        self.lines: list[str] = []
        self.cond = threading.Condition()
        self.stderr = bytearray()
        threading.Thread(target=self._pump_stdout, daemon=True).start()
        threading.Thread(target=self._pump_stderr, daemon=True).start()

    def _pump_stdout(self) -> None:
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.decode("utf-8", "replace").rstrip("\n")
            with self.cond:
                self.lines.append(line)
                self.cond.notify_all()
        with self.cond:
            self.cond.notify_all()

    def _pump_stderr(self) -> None:
        assert self.proc.stderr is not None
        for raw in self.proc.stderr:
            self.stderr.extend(raw)

    def send(self, line: str) -> None:
        assert self.proc.stdin is not None
        try:
            self.proc.stdin.write(line.encode("utf-8") + b"\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    def snapshot(self) -> list[Any]:
        with self.cond:
            return [parse_line(line) for line in self.lines]

    def wait_for(self, pred: Callable[[list[Any]], bool], timeout: float = STEP_TIMEOUT) -> bool:
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                parsed = [parse_line(line) for line in self.lines]
                if pred(parsed):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.proc.poll() is not None:
                    parsed = [parse_line(line) for line in self.lines]
                    return pred(parsed)
                self.cond.wait(min(remaining, 0.2))

    def finish(self, timeout: float = 20.0) -> dict[str, Any]:
        assert self.proc.stdin is not None
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            code: Any = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            code = "timeout"
        time.sleep(0.05)
        with self.cond:
            lines = list(self.lines)
        return {"code": code, "lines": lines, "stderr": bytes(self.stderr).decode("utf-8", "replace")}


def has_response(lines: list[Any], rid: Any) -> bool:
    return any(isinstance(l, dict) and "event" not in l and l.get("id") == rid for l in lines)


def has_event(lines: list[Any], name: str, **fields: Any) -> bool:
    for l in lines:
        if isinstance(l, dict) and l.get("event") == name and all(l.get(k) == v for k, v in fields.items()):
            return True
    return False


def merged_pty_data(lines: list[Any], session_id: str) -> str:
    out = ""
    for l in lines:
        if isinstance(l, dict) and l.get("event") == "pty.data" and l.get("session_id") == session_id and "data_base64" in l:
            out += base64.b64decode(l["data_base64"]).decode("utf-8", "replace")
    return out


def response_for(lines: list[Any], rid: Any) -> dict[str, Any]:
    for l in lines:
        if isinstance(l, dict) and "event" not in l and l.get("id") == rid:
            return l
    return {}


def run_stdio_script(binary: str, env: dict[str, str], serve_args: list[str], steps: list[Any]) -> dict[str, Any]:
    session = StdioSession([binary, *serve_args], env)
    waits: list[dict[str, Any]] = []
    for step in steps:
        kind = step[0]
        if kind == "send":
            session.send(step[1])
        elif kind == "send_fn":
            session.send(step[1](session.snapshot()))
        elif kind == "wait":
            ok = session.wait_for(step[1])
            waits.append({"label": step[2], "ok": ok})
        elif kind == "sleep":
            time.sleep(step[1])
        else:
            raise ValueError(f"unknown step {kind}")
    result = session.finish()
    out = normalize_rpc_output(result["lines"])
    out["code"] = result["code"]
    out["waits"] = waits
    out["stderr_nonempty"] = bool(result["stderr"].strip())
    return out


# --- mock cmux socket for CLI relay scenarios --------------------------------


class MockCmuxSocket:
    def __init__(self, path: str):
        self.path = path
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(path)
        self.server.listen(16)
        self.running = True
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self) -> None:
        while self.running:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(5.0)
            buf = b""
            try:
                while b"\n" not in buf:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
            except OSError:
                return
            line = buf.split(b"\n", 1)[0]
            try:
                req = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                conn.sendall(b'{"ok":false,"error":{"code":"parse","message":"bad json"}}\n')
                return
            method = req.get("method")
            params = req.get("params")
            with self.lock:
                self.requests.append({"method": method, "params": params})
            if method == "system.fail":
                resp: dict[str, Any] = {"id": req.get("id"), "ok": False, "error": {"code": "boom", "message": "bad things"}}
            else:
                resp = {"id": req.get("id"), "ok": True, "result": self._result(method, params)}
            conn.sendall((json.dumps(resp, separators=(",", ":")) + "\n").encode("utf-8"))

    @staticmethod
    def _result(method: Any, params: Any) -> Any:
        if method == "system.ping":
            return {}
        if method == "workspace.list":
            return {"workspaces": [{"id": "ws-1", "ref": "workspace:1", "title": "demo", "index": 1}]}
        if method == "workspace.create":
            return {"workspace_id": "ws-1", "surface_id": "surf-1"}
        if method == "surface.read_text":
            return {"text": "line one\nline two\n"}
        if method in ("surface.send_text", "surface.send_key", "notification.create_for_caller"):
            return {}
        return {"method": method, "params": params}

    def take(self) -> list[dict[str, Any]]:
        with self.lock:
            out = list(self.requests)
            self.requests.clear()
        return out

    def close(self) -> None:
        self.running = False
        try:
            self.server.close()
        except OSError:
            pass


# --- scenarios ---------------------------------------------------------------


class Impl:
    def __init__(self, name: str, binary: str, workdir: str):
        self.name = name
        self.binary = binary
        self.workdir = workdir
        self.home = os.path.join(workdir, "home")
        os.makedirs(self.home, exist_ok=True)
        self.bin_dir = os.path.join(workdir, "bin")
        os.makedirs(self.bin_dir, exist_ok=True)
        self.cmux = os.path.join(self.bin_dir, "cmux")
        os.symlink(binary, self.cmux)
        self.env = base_env(self.home)
        self.replacements = [(binary, "<binary>"), (self.cmux, "<cmux>"), (workdir, "<workdir>")]

    def process(self, argv: list[str], stdin: bytes = b"", extra_env: dict[str, str] | None = None, via_cmux: bool = False) -> dict[str, Any]:
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        exe = self.cmux if via_cmux else self.binary
        out = run_process([exe, *argv], stdin=stdin, env=env)
        out["stdout"] = normalize_text(out["stdout"], self.replacements)
        out["stderr"] = normalize_text(out["stderr"], self.replacements)
        return out


PROCESS_CASES: list[tuple[str, list[str], bytes]] = [
    ("proc-version", ["version"], b""),
    ("proc-no-args", [], b""),
    ("proc-unknown-command", ["frobnicate"], b""),
    ("proc-serve-no-transport", ["serve"], b""),
    ("proc-serve-slot-without-persistent", ["serve", "--stdio", "--slot", "slot-without-persistent"], b""),
    ("proc-serve-persistent-without-slot", ["serve", "--stdio", "--persistent"], b""),
    ("proc-serve-persistent-server-without-slot", ["serve", "--persistent-server"], b""),
    ("proc-serve-persistent-server-with-stdio", ["serve", "--persistent-server", "--stdio", "--slot", "x"], b""),
    ("proc-serve-persistent-stop-without-slot", ["serve", "--persistent-stop"], b""),
    ("proc-serve-lease-port-out-of-range", ["serve", "--stdio", "--persistent", "--slot", "x", "--persistent-lease-port", "70000"], b""),
    ("proc-serve-lease-port-negative", ["serve", "--stdio", "--persistent", "--slot", "x", "--persistent-lease-port", "-1"], b""),
    ("proc-serve-ws-without-lease", ["serve", "--ws"], b""),
    ("proc-serve-ws-and-stdio", ["serve", "--ws", "--stdio", "--auth-lease-file", "/tmp/nope.json"], b""),
    ("proc-serve-stdio-empty-stdin", ["serve", "--stdio"], b""),
    ("proc-cli-no-args", ["cli"], b""),
    ("proc-cli-help", ["cli", "--help"], b""),
    ("proc-cli-help-command", ["cli", "help"], b""),
    ("proc-cli-socket-missing-value", ["cli", "--socket"], b""),
    ("proc-cli-unknown-command", ["cli", "--socket", "/dev/null", "does-not-exist"], b""),
    ("proc-cli-no-socket", ["cli", "ping"], b""),
    ("proc-cli-connect-refused", ["cli", "--socket", "/nonexistent/parity.sock", "ping"], b""),
]

CMUX_ARGV0_CASES: list[tuple[str, list[str]]] = [
    ("argv0-no-args", []),
    ("argv0-ping-no-socket", ["ping"]),
    ("argv0-unknown", ["does-not-exist"]),
    ("argv0-help", ["--help"]),
]


def rpc(rid: Any, method: str, params: Any = None) -> str:
    obj: dict[str, Any] = {"id": rid, "method": method}
    if params is not None:
        obj["params"] = params
    return json.dumps(obj, separators=(",", ":"))


def notification(method: str, params: Any) -> str:
    return json.dumps({"method": method, "params": params}, separators=(",", ":"))


BASIC_RPC_STEPS: list[Any] = [
    ("send", rpc(1, "hello")),
    ("send", rpc(2, "ping")),
    ("send", rpc(3, "unknown.method")),
    ("send", "not json at all"),
    ("send", "[]"),
    ("send", '{"id":4}'),
    ("send", '{"id":null,"method":"ping"}'),
    ("send", '{"id":"str-id","method":"ping"}'),
    ("send", '{"id":1.5,"method":"ping"}'),
    ("send", '{"id":5,"method":"ping","params":"notobject"}'),
    ("send", '{"method":"ping"}'),
    ("send", notification("pty.write", {"session_id": "nope", "attachment_id": "a", "client_attachment_token": "t", "data_base64": "aGk="})),
    ("send", notification("pty.write", {})),
    ("send", notification("pty.resize", {"session_id": "nope", "attachment_id": "a", "client_attachment_token": "t", "cols": 1, "rows": 1})),
    ("send", rpc(6, "proxy.open", {"host": "127.0.0.1", "port": 0})),
    ("send", rpc(7, "proxy.open", {"host": "127.0.0.1", "port": "abc"})),
    ("send", rpc(8, "proxy.open", {"port": 80})),
    ("send", rpc(9, "proxy.write", {"stream_id": "x"})),
    ("send", rpc(10, "proxy.write", {"stream_id": "x", "data_base64": "!!"})),
    ("send", rpc(11, "proxy.write", {"stream_id": "x", "data_base64": "aGk="})),
    ("send", rpc(12, "proxy.close", {})),
    ("send", rpc(13, "proxy.close", {"stream_id": "x"})),
    ("send", rpc(14, "session.close", {"session_id": "nope"})),
    ("send", rpc(15, "session.status", {})),
    ("send", rpc(16, "session.attach", {"session_id": "nope", "attachment_id": "a", "cols": 80, "rows": 24})),
    ("send", rpc(17, "pty.attach", {})),
    ("send", rpc(18, "pty.attach", {"session_id": "s", "cols": 0, "rows": 24})),
    ("send", rpc(19, "pty.attach", {"session_id": "s", "cols": 80, "rows": "x"})),
    ("send", rpc(20, "pty.list")),
    ("send", rpc(21, "pty.close", {"session_id": "nope"})),
    ("send", rpc(22, "pty.close", {})),
    ("send", rpc(23, "pty.resize", {"session_id": "nope", "attachment_id": "a", "client_attachment_token": "t", "cols": 1, "rows": 1})),
    ("send", rpc(24, "pty.write", {"session_id": "nope", "attachment_id": "a", "data_base64": "aGk="})),
    ("send", rpc(25, "pty.detach", {"session_id": "nope", "attachment_id": "a", "client_attachment_token": "t"})),
    ("send", rpc(26, "pty.attach", {"session_id": "missing", "cols": 80, "rows": 24, "require_existing": True})),
    ("send", rpc(27, "cli.response", {"request_id": "r", "ok": True, "data_base64": "aGk="})),
    ("send", rpc(28, "daemon.shutdown")),
    ("send", rpc(29, "daemon.auth", {"token": "x"})),
    ("send", rpc(30, "ping")),
    ("wait", lambda lines: has_response(lines, 30), "final ping"),
]

SESSION_RPC_STEPS: list[Any] = [
    ("send", rpc(1, "session.open")),
    ("send", rpc(2, "session.open", {"session_id": "custom"})),
    ("send", rpc(3, "session.attach", {"session_id": "sess-1", "attachment_id": "a1", "cols": 100, "rows": 40})),
    ("send", rpc(4, "session.attach", {"session_id": "sess-1", "attachment_id": "a2", "cols": 80, "rows": 24})),
    ("send", rpc(5, "session.status", {"session_id": "sess-1"})),
    ("send", rpc(6, "session.resize", {"session_id": "sess-1", "attachment_id": "a1", "cols": 120, "rows": 50})),
    ("send", rpc(7, "session.resize", {"session_id": "sess-1", "attachment_id": "zz", "cols": 120, "rows": 50})),
    ("send", rpc(8, "session.resize", {"session_id": "sess-1", "attachment_id": "a1", "cols": 0, "rows": 50})),
    ("send", rpc(9, "session.detach", {"session_id": "sess-1", "attachment_id": "a2"})),
    ("send", rpc(10, "session.detach", {"session_id": "sess-1", "attachment_id": "a2"})),
    ("send", rpc(11, "session.status", {"session_id": "sess-1"})),
    ("send", rpc(12, "session.detach", {"session_id": "sess-1"})),
    ("send", rpc(13, "session.close", {"session_id": "sess-1"})),
    ("send", rpc(14, "session.status", {"session_id": "sess-1"})),
    ("send", rpc(15, "session.status", {"session_id": "custom"})),
    ("send", rpc(16, "session.close", {"session_id": "custom"})),
    ("wait", lambda lines: has_response(lines, 16), "final close"),
]


def pty_steps() -> list[Any]:
    def token_of(lines: list[Any]) -> str:
        return str(response_for(lines, 1).get("result", {}).get("attachment_token", ""))

    return [
        ("send", rpc(1, "pty.attach", {"session_id": "p1", "attachment_id": "att-1", "cols": 80, "rows": 24, "command": "cat"})),
        ("wait", lambda lines: has_response(lines, 1) and has_event(lines, "pty.ready", session_id="p1"), "attach + ready"),
        ("send_fn", lambda lines: notification("pty.write", {"session_id": "p1", "attachment_id": "att-1", "client_attachment_token": token_of(lines), "data_base64": base64.b64encode(b"abc\n").decode()})),
        ("wait", lambda lines: merged_pty_data(lines, "p1").count("abc") >= 2, "cat echo"),
        ("send", rpc(2, "pty.write", {"session_id": "p1", "attachment_id": "att-1", "client_attachment_token": "wrong", "data_base64": "aGk="})),
        ("send", rpc(3, "pty.write", {"session_id": "p1", "attachment_id": "att-1", "data_base64": "aGk="})),
        ("send_fn", lambda lines: rpc(4, "pty.resize", {"session_id": "p1", "attachment_id": "att-1", "client_attachment_token": token_of(lines), "cols": 100, "rows": 30})),
        ("send", rpc(5, "pty.list")),
        ("wait", lambda lines: has_response(lines, 5), "list"),
        ("send", rpc(6, "pty.attach", {"session_id": "p1", "attachment_id": "att-1", "cols": 80, "rows": 24, "require_existing": True})),
        ("wait", lambda lines: has_response(lines, 6), "reattach"),
        ("send_fn", lambda lines: notification("pty.write", {"session_id": "p1", "attachment_id": "att-1", "client_attachment_token": str(response_for(lines, 6).get("result", {}).get("attachment_token", "")), "data_base64": base64.b64encode(b"\x04").decode()})),
        ("wait", lambda lines: has_event(lines, "pty.exit", session_id="p1"), "exit"),
        ("send", rpc(7, "pty.list")),
        ("wait", lambda lines: has_response(lines, 7), "list after exit"),
        ("send", rpc(8, "pty.attach", {"session_id": "p1", "cols": 80, "rows": 24, "require_existing": True})),
        ("wait", lambda lines: has_response(lines, 8), "reattach after exit"),
    ]


def pty_exit_code_steps() -> list[Any]:
    return [
        ("send", rpc(1, "pty.attach", {"session_id": "p2", "cols": 80, "rows": 24, "command": "printf 'hello\\n'; exit 3"})),
        ("wait", lambda lines: has_event(lines, "pty.exit", session_id="p2"), "exit"),
        ("send", rpc(2, "pty.list")),
        ("wait", lambda lines: has_response(lines, 2), "list"),
    ]


def proxy_steps(port: int) -> list[Any]:
    payload = base64.b64encode(b"GET / HTTP/1.0\r\n\r\n").decode()
    return [
        ("send", rpc(1, "proxy.open", {"host": "127.0.0.1", "port": port})),
        ("wait", lambda lines: has_response(lines, 1), "open"),
        ("send_fn", lambda lines: rpc(2, "proxy.write", {"stream_id": str(response_for(lines, 1).get("result", {}).get("stream_id", "")), "data_base64": payload})),
        ("wait", lambda lines: has_response(lines, 2), "write"),
        ("wait", lambda lines: any(isinstance(l, dict) and l.get("event") in ("proxy.closed", "proxy.close", "proxy.eof", "proxy.end") for l in lines) or "OK" in "".join(base64.b64decode(l["data_base64"]).decode("utf-8", "replace") for l in lines if isinstance(l, dict) and l.get("event") == "proxy.data"), "data"),
        ("sleep", 0.3),
        ("send_fn", lambda lines: rpc(3, "proxy.close", {"stream_id": str(response_for(lines, 1).get("result", {}).get("stream_id", ""))})),
        ("wait", lambda lines: has_response(lines, 3), "close"),
        ("send", rpc(4, "proxy.open", {"host": "127.0.0.1", "port": 1})),
        ("wait", lambda lines: has_response(lines, 4), "open refused"),
    ]


def oversized_steps() -> list[Any]:
    big = '{"id":1,"method":"ping","params":{"pad":"' + ("x" * (4 * 1024 * 1024 + 16)) + '"}}'
    return [
        ("send", big),
        ("send", rpc(2, "ping")),
        ("wait", lambda lines: has_response(lines, 2) or any(isinstance(l, dict) and l.get("ok") is False for l in lines), "after oversized"),
        ("sleep", 0.2),
    ]


CLI_CASES: list[tuple[str, list[str], dict[str, str]]] = [
    ("cli-ping", ["ping"], {}),
    ("cli-ping-json", ["--json", "ping"], {}),
    ("cli-list-workspaces", ["list-workspaces"], {}),
    ("cli-list-workspaces-json", ["--json", "list-workspaces"], {}),
    ("cli-notify", ["notify", "--body", "hi"], {}),
    ("cli-notify-env", ["--json", "notify", "--title", "Done", "--body", "Build finished"], {"CMUX_WORKSPACE_ID": "env-ws", "CMUX_SURFACE_ID": "env-sf"}),
    ("cli-new-workspace", ["new-workspace", "--name", "My WS", "--cwd", "/tmp", "--env", "A=1", "--env", "B=2", "--focus", "true", "--layout", '{"splits":[{"direction":"vertical","ratio":0.5}]}'], {}),
    ("cli-new-workspace-command", ["new-workspace", "--command", "claude ."], {}),
    ("cli-new-workspace-groups", ["new-workspace", "--window", "win-1", "--group", "grp-1", "--group-placement", "before", "--group-reference", "ws-ref-1"], {}),
    ("cli-new-workspace-bad-focus", ["new-workspace", "--focus", "maybe"], {}),
    ("cli-new-workspace-bad-env", ["new-workspace", "--env", "NOEQUALS"], {}),
    ("cli-new-workspace-bad-layout", ["new-workspace", "--layout", "not-json"], {}),
    ("cli-new-workspace-positional", ["new-workspace", "unexpected"], {}),
    ("cli-new-workspace-removed-flag", ["new-workspace", "--working-directory", "/x"], {}),
    ("cli-new-workspace-env-file-missing", ["new-workspace", "--env-file", "/nonexistent/vars.env"], {}),
    ("cli-rename-workspace", ["rename-workspace", "--title", "devbox"], {}),
    ("cli-close-workspace", ["--json", "close-workspace", "--workspace", "ws-abc"], {}),
    ("cli-send", ["send", "hello world"], {}),
    ("cli-send-key", ["send-key", "ctrl+c"], {}),
    ("cli-send-text-flag", ["send", "--text", "hello"], {}),
    ("cli-send-missing", ["send"], {}),
    ("cli-close-window", ["close-window", "--window", "win-42"], {}),
    ("cli-new-window", ["new-window"], {}),
    ("cli-new-pane", ["--json", "new-pane", "--workspace", "ws-1", "--type", "browser", "--url", "https://example.com"], {}),
    ("cli-list-panels", ["--json", "list-panels", "--workspace", "ws-1"], {}),
    ("cli-focus-panel", ["--json", "focus-panel", "--workspace", "ws-1", "--panel", "surface-1"], {}),
    ("cli-close-surface-env", ["--json", "close-surface"], {"CMUX_WORKSPACE_ID": "env-ws-id", "CMUX_SURFACE_ID": "env-sf-id"}),
    ("cli-join-pane", ["join-pane", "--pane", "pane-1", "--target-pane", "pane-2"], {}),
    ("cli-swap-pane", ["swap-pane", "--pane", "p1"], {}),
    ("cli-break-pane-missing", ["break-pane"], {}),
    ("cli-next-workspace", ["next-workspace"], {}),
    ("cli-equalize-splits", ["equalize-splits"], {}),
    ("cli-read-screen", ["read-screen"], {}),
    ("cli-read-screen-json", ["--json", "read-screen"], {}),
    ("cli-dismiss-notification", ["dismiss-notification", "--id", "n1"], {}),
    ("cli-browser-open-flag", ["--json", "browser", "open", "--url", "https://example.com"], {}),
    ("cli-browser-open-env", ["--json", "browser", "open", "https://example.com"], {"CMUX_WORKSPACE_ID": "env-ws"}),
    ("cli-browser-get-url", ["--json", "browser", "get-url"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-snapshot", ["--json", "browser", "snapshot", "--selector", "main", "--max-depth", "4"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-wait", ["--json", "browser", "wait", "--timeout-ms", "1500", "--url-contains", "/cloud", "--load-state", "networkidle"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-fill", ["--json", "browser", "fill", "input[name=email]", "dev@example.com"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-select", ["--json", "browser", "select", "select[name=plan]", "free"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-eval", ["--json", "browser", "eval", "document.title"], {"CMUX_SURFACE_ID": "env-sf"}),
    ("cli-browser-click-missing", ["browser", "click"], {}),
    ("cli-browser-unknown", ["browser", "frobnicate"], {}),
    ("cli-browser-none", ["browser"], {}),
    ("cli-ws-group-list", ["--json", "workspace", "group", "list"], {}),
    ("cli-ws-group-list-env", ["--json", "workspace", "group", "list"], {"CMUX_WORKSPACE_ID": "env-ws", "CMUX_SURFACE_ID": "env-sf"}),
    ("cli-ws-group-create", ["--json", "workspace", "group", "create", "--name", "My Group", "--cwd", "/repo/path", "--from", "workspace:1, workspace:2"], {}),
    ("cli-ws-group-add-missing", ["workspace", "group", "add", "--group", "g1"], {}),
    ("cli-ws-group-add", ["--json", "workspace", "group", "add", "--group", "g1", "--workspace", "ws1"], {}),
    ("cli-ws-group-remove-env", ["workspace", "group", "remove"], {"CMUX_WORKSPACE_ID": "env-ws"}),
    ("cli-ws-group-rename", ["--json", "workspace", "group", "rename", "workspace_group:2", "New Name"], {}),
    ("cli-ws-group-new-workspace", ["--json", "workspace", "group", "new-workspace", "workspace_group:3", "--placement", "top"], {}),
    ("cli-ws-group-set-color", ["--json", "workspace", "group", "set-color", "workspace_group:4"], {}),
    ("cli-ws-group-set-color-hex", ["--json", "workspace", "group", "set-color", "workspace_group:4", "#ff0000"], {}),
    ("cli-ws-group-move-missing", ["workspace", "group", "move", "g1"], {}),
    ("cli-ws-group-move-bad-index", ["workspace", "group", "move", "g1", "--to-index", "abc"], {}),
    ("cli-ws-group-move", ["--json", "workspace", "group", "move", "g1", "--to-index", "2"], {}),
    ("cli-ws-group-unknown", ["workspace", "group", "explode"], {}),
    ("cli-ws-group-bare", ["workspace", "group"], {}),
    ("cli-ws-rename-unsupported", ["workspace", "rename"], {}),
    ("cli-ws-group-alias", ["--json", "workspace-group", "collapse", "workspace_group:1"], {}),
    ("cli-rpc", ["rpc", "system.capabilities"], {}),
    ("cli-rpc-json", ["--json", "rpc", "system.capabilities"], {}),
    ("cli-rpc-params", ["rpc", "workspace.create", '{"title":"test"}'], {}),
    ("cli-rpc-bad-params", ["rpc", "workspace.create", "not-json"], {}),
    ("cli-rpc-error", ["rpc", "system.fail"], {}),
    ("cli-rpc-error-json", ["--json", "rpc", "system.fail"], {}),
    ("cli-rpc-no-method", ["rpc"], {}),
    ("cli-unknown", ["does-not-exist"], {}),
    ("cli-unknown-flag", ["ping", "--bogus", "x"], {}),
    ("cli-tmux-version", ["__tmux-compat", "-V"], {}),
    ("cli-tmux-version-lower", ["__tmux-compat", "-v"], {}),
    ("cli-tmux-unsupported", ["__tmux-compat", "copy-mode"], {}),
    ("cli-tmux-noop", ["__tmux-compat", "set-option", "-g", "x", "y"], {}),
    ("cli-tmux-no-args", ["__tmux-compat"], {}),
    ("cli-tmux-display", ["__tmux-compat", "display-message", "-p", "-F", "#{session_name}"], {}),
    ("cli-tmux-list-panes", ["__tmux-compat", "list-panes", "-a"], {}),
    ("cli-tmux-send-keys", ["__tmux-compat", "send-keys", "-t", "pane:1", "echo", "Enter"], {}),
]


def cli_case_result(impl: Impl, mock: MockCmuxSocket, args: list[str], extra_env: dict[str, str]) -> dict[str, Any]:
    mock.take()
    env = {"CMUX_SOCKET_PATH": mock.path}
    env.update(extra_env)
    out = impl.process(args, extra_env=env, via_cmux=True)
    out["requests"] = mock.take()
    out["stdout"] = normalize_text(out["stdout"], [(mock.path, "<socket>")])
    out["stderr"] = normalize_text(out["stderr"], [(mock.path, "<socket>")])
    return out


def persistent_result(impl: Impl, tag: str) -> dict[str, Any]:
    slot = f"parity-{impl.name}-{tag}"
    steps: list[Any] = [
        ("send", rpc(1, "hello")),
        ("send", rpc(2, "ping")),
        ("send", rpc(3, "session.open")),
        ("send", rpc(4, "daemon.shutdown")),
        ("send", rpc(5, "ping")),
        ("wait", lambda lines: has_response(lines, 5), "ping via persistent"),
    ]
    first = run_stdio_script(impl.binary, impl.env, ["serve", "--stdio", "--persistent", "--slot", slot], steps)
    second = run_stdio_script(
        impl.binary,
        impl.env,
        ["serve", "--stdio", "--persistent", "--slot", slot],
        [("send", rpc(1, "session.status", {"session_id": "sess-1"})), ("send", rpc(2, "ping")), ("wait", lambda lines: has_response(lines, 2), "second client")],
    )
    stop = impl.process(["serve", "--persistent-stop", "--slot", slot])
    stop_again = impl.process(["serve", "--persistent-stop", "--slot", slot])
    subprocess.run(["pkill", "-f", slot], capture_output=True)
    return {"first": first, "second": second, "stop": stop, "stop_again": stop_again}


def start_http_upstream() -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(8)
    port = server.getsockname()[1]

    def loop() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                try:
                    conn.settimeout(5.0)
                    conn.recv(65536)
                    conn.sendall(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
                except OSError:
                    pass

    threading.Thread(target=loop, daemon=True).start()
    return port


# --- harness -----------------------------------------------------------------


def build_go_binary(workdir: str) -> str | None:
    if not shutil.which("go"):
        return None
    out = os.path.join(workdir, "cmuxd-remote-go")
    subprocess.run(["go", "build", "-o", out, "./cmd/cmuxd-remote"], cwd=GO_DIR, check=True)
    return out


def build_rust_binary() -> str:
    subprocess.run(["cargo", "build", "--manifest-path", str(RS_DIR / "Cargo.toml")], check=True)
    return str(RS_DIR / "target" / "debug" / "cmuxd-remote")


def diff_json(name: str, left: Any, right: Any) -> str:
    a = json.dumps(left, indent=2, sort_keys=True, ensure_ascii=False).splitlines()
    b = json.dumps(right, indent=2, sort_keys=True, ensure_ascii=False).splitlines()
    return "\n".join(difflib.unified_diff(a, b, fromfile=f"go/{name}", tofile=f"rust/{name}", lineterm=""))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--go-bin", default=os.environ.get("CMUXD_REMOTE_GO_BIN", ""))
    parser.add_argument("--rust-bin", default=os.environ.get("CMUXD_REMOTE_RS_BIN", ""))
    parser.add_argument("--only", default="", help="run only scenarios whose name contains this substring")
    parser.add_argument("--require-go", action="store_true", help="fail instead of skipping when no Go binary is available")
    parser.add_argument("--transcripts", default="", help="directory to dump per-implementation transcripts into")
    parser.add_argument("--skip-persistent", action="store_true")
    args = parser.parse_args(argv)

    workdir = tempfile.mkdtemp(prefix="cmuxd-parity-", dir="/tmp")
    try:
        go_bin = args.go_bin or build_go_binary(workdir)
        if not go_bin:
            print("SKIP: no Go toolchain or --go-bin available; cannot run parity checks")
            return 3 if args.require_go else 0
        rust_bin = args.rust_bin or build_rust_binary()
        go = Impl("go", os.path.abspath(go_bin), os.path.join(workdir, "go"))
        rs = Impl("rust", os.path.abspath(rust_bin), os.path.join(workdir, "rust"))
        impls = [go, rs]

        upstream_port = start_http_upstream()
        scenarios: list[tuple[str, Callable[[Impl], Any]]] = []
        for name, argv_case, stdin in PROCESS_CASES:
            scenarios.append((name, lambda impl, a=argv_case, s=stdin: impl.process(a, stdin=s)))
        for name, argv_case in CMUX_ARGV0_CASES:
            scenarios.append((name, lambda impl, a=argv_case: impl.process(a, via_cmux=True)))
        scenarios.append(("rpc-basic", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], BASIC_RPC_STEPS)))
        scenarios.append(("rpc-session", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], SESSION_RPC_STEPS)))
        scenarios.append(("rpc-proxy", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], proxy_steps(upstream_port))))
        scenarios.append(("rpc-pty", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], pty_steps())))
        scenarios.append(("rpc-pty-exit-code", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], pty_exit_code_steps())))
        scenarios.append(("rpc-oversized-frame", lambda impl: run_stdio_script(impl.binary, impl.env, ["serve", "--stdio"], oversized_steps())))
        if not args.skip_persistent:
            tag = hashlib.sha256(workdir.encode()).hexdigest()[:6]
            scenarios.append(("persistent-lifecycle", lambda impl: persistent_result(impl, tag)))

        mocks = {impl.name: MockCmuxSocket(os.path.join(workdir, f"{impl.name}.sock")) for impl in impls}
        for name, cli_args, extra_env in CLI_CASES:
            scenarios.append((name, lambda impl, a=cli_args, e=extra_env: cli_case_result(impl, mocks[impl.name], a, e)))

        failures = 0
        ran = 0
        transcripts: dict[str, dict[str, Any]] = {impl.name: {} for impl in impls}
        for name, runner in scenarios:
            if args.only and args.only not in name:
                continue
            ran += 1
            results = {}
            for impl in impls:
                try:
                    results[impl.name] = runner(impl)
                except Exception as err:  # noqa: BLE001 - report as a diff
                    results[impl.name] = {"harness_error": f"{type(err).__name__}: {err}"}
                transcripts[impl.name][name] = results[impl.name]
            diff = diff_json(name, results["go"], results["rust"])
            if diff:
                failures += 1
                print(f"FAIL {name}")
                print(diff)
            else:
                print(f"PASS {name}")
        for mock in mocks.values():
            mock.close()
        if args.transcripts:
            os.makedirs(args.transcripts, exist_ok=True)
            for impl_name, data in transcripts.items():
                with open(os.path.join(args.transcripts, f"{impl_name}.json"), "w", encoding="utf-8") as handle:
                    json.dump(data, handle, indent=2, sort_keys=True, ensure_ascii=False)
        print(f"\n{ran - failures}/{ran} scenarios match ({failures} differ)")
        return 1 if failures else 0
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

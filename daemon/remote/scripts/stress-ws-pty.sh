#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

duration="${CMUX_PTY_STRESS_DURATION:-12h}"
log_every="${CMUX_PTY_STRESS_LOG_EVERY:-60}"
stress_filter="${CMUX_PTY_STRESS_FILTER:-scrollback_stays_bounded multi_attach_uses_smallest_size_and_reconnect_keeps_process single_use_lease_is_consumed_once_and_shell_runs_over_binary_frames}"

cd "$REMOTE_DIR"

if [[ ! "$log_every" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid CMUX_PTY_STRESS_LOG_EVERY: $log_every" >&2
  exit 2
fi

echo "== unit and integration =="
cargo test --locked --test ws --test stdio

echo "== randomized lease and pty-size robustness =="
cargo test --locked --release --test robustness

echo "== build stress binary =="
stress_bin="$(cargo test --locked --release --test ws --no-run --message-format=json 2>/dev/null \
  | python3 -c 'import json, sys
for line in sys.stdin:
    try:
        obj = json.loads(line)
    except ValueError:
        continue
    if obj.get("reason") == "compiler-artifact" and obj.get("executable") and obj["target"]["name"] == "ws":
        print(obj["executable"])
')"
if [[ -z "$stress_bin" || ! -x "$stress_bin" ]]; then
  echo "could not locate the ws test binary" >&2
  exit 1
fi

deadline_epoch="$(python3 - "$duration" <<'PY'
import re
import sys
import time

raw = sys.argv[1].strip()
match = re.fullmatch(r"(\d+)([smhd]?)", raw)
if not match:
    raise SystemExit(f"invalid duration: {raw}")
value = int(match.group(1))
unit = match.group(2) or "s"
scale = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
print(int(time.time()) + value * scale)
PY
)"

iteration=0
max_rss_kb=0
echo "== stress loop duration=$duration =="
while [[ "$(date +%s)" -lt "$deadline_epoch" ]]; do
  iteration=$((iteration + 1))
  output_file="$(mktemp)"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    set +e
    # shellcheck disable=SC2086
    /usr/bin/time -l "$stress_bin" --test-threads 1 $stress_filter >"$output_file" 2>&1
    status=$?
    set -e
    rss_kb="$(awk '/maximum resident set size/ {print int($1 / 1024)}' "$output_file" | tail -n 1)"
  else
    set +e
    # shellcheck disable=SC2086
    "$stress_bin" --test-threads 1 $stress_filter >"$output_file" 2>&1
    status=$?
    set -e
    rss_kb=""
  fi
  if [[ -n "$rss_kb" && "$rss_kb" -gt "$max_rss_kb" ]]; then
    max_rss_kb="$rss_kb"
  fi
  if [[ "$status" -eq 0 ]]; then
    if [[ "$iteration" -eq 1 || $((iteration % log_every)) -eq 0 ]]; then
      if [[ -n "$rss_kb" ]]; then
        echo "stress iteration $iteration ok rss=${rss_kb}KB max_rss=${max_rss_kb}KB"
      else
        echo "stress iteration $iteration ok"
      fi
    fi
  else
    echo "stress iteration $iteration failed"
    cat "$output_file"
    rm -f "$output_file"
    exit 1
  fi
  rm -f "$output_file"
done

if [[ "$max_rss_kb" -gt 0 ]]; then
  echo "stress complete iterations=$iteration max_rss=${max_rss_kb}KB"
else
  echo "stress complete iterations=$iteration"
fi

//! Randomized robustness checks standing in for the Go fuzz targets
//! (`FuzzTmuxCompatArgParser`, `FuzzTmuxRenderFormatSupportedSubset`,
//! `FuzzTmuxSendKeysTokens`, `FuzzConsumeWebSocketLease`,
//! `FuzzNormalizePTYSize`, `FuzzWebSocketPTYControlFrame`).

mod common;

use std::collections::HashMap;
use std::io::BufReader;

use cmuxd_remote::cli::tmux::{
    parse_tmux_args, split_tmux_cmd, tmux_render_format, tmux_send_keys_text,
};
use cmuxd_remote::pty::{MAX_PTY_DIMENSION, normalize_pty_size, normalize_pty_size_i64};
use cmuxd_remote::rpc::{RpcFrame, RpcRequest, read_rpc_frame};
use cmuxd_remote::ws::PtyControlFrame;
use cmuxd_remote::ws::lease::{WsAuthFrame, consume_websocket_lease};

const BASE_ITERATIONS: usize = 3000;

/// `CMUXD_ROBUSTNESS_ITERATIONS_MULTIPLIER` scales the seeded runs (the
/// tmux-corpus workflow input) without changing the seeds.
fn iterations() -> usize {
    let multiplier: usize = std::env::var("CMUXD_ROBUSTNESS_ITERATIONS_MULTIPLIER")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1);
    BASE_ITERATIONS * multiplier.max(1)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
    fn string(&mut self, alphabet: &[&str], max_len: usize) -> String {
        let len = self.below(max_len + 1);
        (0..len).map(|_| *self.pick(alphabet)).collect()
    }
}

const ARG_ALPHABET: &[&str] = &[
    "-",
    "--",
    "d",
    "P",
    "h",
    "t",
    "c",
    "F",
    " ",
    "x",
    "%",
    "1",
    ".",
    ":",
    "#",
    "{",
    "}",
    "é",
    "\u{1b}",
    "-L",
    "-V",
    "new-session",
    "split-window",
    "send-keys",
    "capture-pane",
    "-S",
    "-2000",
    "--timeout",
];

#[test]
fn tmux_arg_parser_never_panics_and_preserves_positionals() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let value_flags = ["-c", "-F", "-n", "-s", "-t", "-x", "-y", "-S", "-E"];
    let bool_flags = ["-A", "-b", "-d", "-D", "-h", "-J", "-L", "-N", "-p", "-P", "-R", "-U", "-v"];
    for seed in [
        "new-session -d -s build -c /tmp echo ok",
        "split-window -h -P -F #{pane_id}",
        "capture-pane -p -S -2000",
        "-L cmux has-session -t main",
        "-- send-keys -l C-c Enter",
    ] {
        let fields: Vec<String> = seed.split_whitespace().map(str::to_string).collect();
        let _ = split_tmux_cmd(&fields);
        let _ = parse_tmux_args(&fields, &value_flags, &bool_flags);
    }
    for _ in 0..iterations() {
        let count = rng.below(8);
        let fields: Vec<String> =
            (0..count).map(|_| rng.string(ARG_ALPHABET, 3)).filter(|s| !s.is_empty()).collect();
        let _ = split_tmux_cmd(&fields);
        let parsed = parse_tmux_args(&fields, &value_flags, &bool_flags);
        // Every positional came from the input, in order.
        let mut cursor = 0;
        for positional in &parsed.positional {
            let idx = fields[cursor..]
                .iter()
                .position(|f| f == positional)
                .unwrap_or_else(|| panic!("positional {positional:?} not found in {fields:?}"));
            cursor += idx + 1;
        }
        for flag in parsed.flags.keys() {
            assert!(bool_flags.contains(&flag.as_str()), "{flag}");
        }
        for flag in parsed.options.keys() {
            assert!(value_flags.contains(&flag.as_str()), "{flag}");
        }
    }
}

#[test]
fn render_format_never_panics_and_strips_unknown_variables() {
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    let ctx: HashMap<String, String> = [
        ("session_name", "cmux"),
        ("window_id", "@workspace"),
        ("window_index", "1"),
        ("window_name", "main"),
        ("pane_id", "%pane"),
        ("pane_width", "120"),
        ("pane_height", "40"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let alphabet = [
        "#{",
        "}",
        "#",
        "{",
        "session_name",
        "pane_id",
        "unknown",
        ":",
        " ",
        "x",
        "é",
        "#[fg=#ff0000]",
        "\n",
    ];
    for _ in 0..iterations() {
        let format = rng.string(&alphabet, 12);
        let rendered = tmux_render_format(&format, &ctx, "fallback");
        assert!(!rendered.contains("#{unknown}"), "{format:?} -> {rendered:?}");
        if format.is_empty() {
            assert_eq!(rendered, "fallback");
        }
        assert_eq!(rendered, rendered.trim(), "rendered output is trimmed");
    }
}

#[test]
fn send_keys_tokens_never_panic() {
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let alphabet = [
        "Enter",
        "C-c",
        "C-d",
        "Escape",
        "Tab",
        "BSpace",
        "printf",
        "hello",
        "38;2;255;0;0m",
        "é",
        "",
        " ",
        "c-m",
        "kpenter",
        "x",
    ];
    for _ in 0..iterations() {
        let tokens: Vec<String> =
            (0..rng.below(8)).map(|_| rng.pick(&alphabet).to_string()).collect();
        let literal = tmux_send_keys_text(&tokens, true);
        assert_eq!(literal, tokens.join(" "));
        let translated = tmux_send_keys_text(&tokens, false);
        if tokens.iter().any(|t| t == "Enter") {
            assert!(translated.contains('\r'), "{tokens:?} -> {translated:?}");
        }
        assert!(
            !translated.contains("Enter")
                || tokens.iter().any(|t| t != "Enter" && t.contains("Enter")),
            "{tokens:?} -> {translated:?}"
        );
    }
}

#[test]
fn lease_consumption_never_panics_on_garbage() {
    let mut rng = Rng(0x0bad_5eed_0bad_5eed);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lease.json");
    let path_str = path.to_string_lossy().into_owned();
    let alphabet = [
        "{",
        "}",
        "\"version\":1",
        "\"version\":2",
        ",",
        "\"token_sha256\":\"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b\"",
        "\"token_sha256\":\"bad\"",
        "\"expires_at_unix\":4102444800",
        "\"expires_at_unix\":0",
        "\"session_id\":\"sess\"",
        "\"single_use\":true",
        "not-json",
        "\u{0}",
        "[",
    ];
    let mut consumed_valid = 0;
    for _ in 0..iterations() {
        let lease_json = rng.string(&alphabet, 8);
        std::fs::write(&path, &lease_json).unwrap();
        let token = *rng.pick(&["secret", "wrong", ""]);
        let session = *rng.pick(&["sess", "", "other"]);
        let auth = WsAuthFrame {
            kind: "auth".into(),
            token: token.into(),
            session_id: session.into(),
            cols: 80,
            rows: 24,
            ..WsAuthFrame::default()
        };
        if consume_websocket_lease(&path_str, &auth).is_ok() {
            consumed_valid += 1;
            assert_eq!(token, "secret", "only the real token can pass: {lease_json}");
        }
    }
    // The seeded valid lease is reachable by the generator.
    std::fs::write(&path, r#"{"version":1,"token_sha256":"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b","expires_at_unix":4102444800,"session_id":"sess","single_use":false}"#).unwrap();
    let auth = WsAuthFrame {
        kind: "auth".into(),
        token: "secret".into(),
        session_id: "sess".into(),
        ..WsAuthFrame::default()
    };
    consume_websocket_lease(&path_str, &auth).unwrap();
    let _ = consumed_valid;
}

#[test]
fn normalize_pty_size_is_always_positive_and_bounded() {
    let mut rng = Rng(0x5555_aaaa_5555_aaaa);
    for (cols, rows) in [(80usize, 24usize), (0, 0), (1_000_000, 1_000_000), (usize::MAX, 1)] {
        let (c, r) = normalize_pty_size(cols, rows);
        assert!(c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION);
    }
    for _ in 0..iterations() {
        let cols = rng.next() as i64;
        let rows = rng.next() as i64 >> rng.below(64);
        let (c, r) = normalize_pty_size_i64(cols, rows);
        assert!(
            c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION,
            "{cols},{rows} -> {c}x{r}"
        );
    }
}

#[test]
fn control_frames_never_panic() {
    let mut rng = Rng(0x7777_1111_7777_1111);
    let alphabet = [
        "{",
        "}",
        "\"type\":\"resize\"",
        "\"type\":\"close\"",
        "\"type\":\"\\u001b[?2026$p\"",
        "\"cols\":80",
        "\"cols\":-1",
        "\"cols\":1000000",
        "\"cols\":1.5",
        "\"rows\":24",
        ",",
        "[",
        "null",
        "\"",
        "é",
    ];
    for seed in [
        r#"{"type":"resize","cols":80,"rows":24}"#,
        r#"{"type":"resize","cols":1000000,"rows":1000000}"#,
        r#"{"type":"close"}"#,
        r#"{"type":"resize","cols":-1,"rows":24}"#,
    ] {
        let frame: PtyControlFrame = serde_json::from_str(seed).unwrap();
        let (c, r) = normalize_pty_size_i64(frame.cols, frame.rows);
        assert!(c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION);
    }
    for _ in 0..iterations() {
        let input = rng.string(&alphabet, 8);
        if let Ok(frame) = serde_json::from_str::<PtyControlFrame>(&input)
            && frame.kind == "resize"
            && frame.cols > 0
            && frame.rows > 0
        {
            let (c, r) = normalize_pty_size_i64(frame.cols, frame.rows);
            assert!(c > 0 && r > 0 && c <= MAX_PTY_DIMENSION && r <= MAX_PTY_DIMENSION);
        }
    }
}

#[test]
fn rpc_framing_and_request_parsing_never_panic() {
    let mut rng = Rng(0xfeed_face_feed_face);
    let alphabet: Vec<String> = [
        "{",
        "}",
        "\"id\":1",
        "\"id\":null",
        "\"method\":\"ping\"",
        "\"method\":1",
        "\"params\":{}",
        "\"params\":[]",
        ",",
        "\n",
        "\r\n",
        "x",
        "\u{0}",
        "\"",
        "é",
        "\\",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    let alphabet_refs: Vec<&str> = alphabet.iter().map(String::as_str).collect();
    for _ in 0..iterations() {
        let input = rng.string(&alphabet_refs, 24).into_bytes();
        let max = 1 + rng.below(64);
        let mut reader = BufReader::with_capacity(1 + rng.below(16), input.as_slice());
        let mut consumed_lines = 0;
        loop {
            match read_rpc_frame(&mut reader, max).unwrap() {
                RpcFrame::Eof => break,
                RpcFrame::Oversized => {}
                RpcFrame::Line(line) => {
                    assert!(line.len() <= max, "line {} exceeds cap {max}", line.len());
                    consumed_lines += 1;
                    let _ = RpcRequest::parse(&line);
                }
            }
            assert!(consumed_lines <= input.len() + 1);
        }
        let _ = RpcRequest::parse(&input);
    }
}

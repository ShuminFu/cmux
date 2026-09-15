//! Shared helpers for binary-level tests: a scratch home directory, a runner
//! for the `cmux-vault` executable with a scrubbed environment, and an
//! in-process mock of the Vault web API plus presigned blob storage.

#![allow(dead_code, clippy::too_many_lines)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use cmux_vault::api::{
    CommitResponse, CommitResult, Session, SessionDetail, SessionsResponse, UploadItem,
    UploadResult, UploadsResponse,
};
use tiny_http::{Header, Method, Response, Server};

pub const UUID_A: &str = "11111111-1111-4111-8111-111111111111";
pub const UUID_B: &str = "22222222-2222-4222-8222-222222222222";
pub const UUID_C: &str = "33333333-3333-4333-8333-333333333333";

pub struct Home {
    pub dir: tempfile::TempDir,
}

impl Home {
    pub fn new() -> Self {
        Self { dir: tempfile::tempdir().unwrap() }
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    pub fn join(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.dir.path().join(rel)
    }

    pub fn write(&self, rel: &str, content: impl AsRef<[u8]>) -> PathBuf {
        let path = self.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
        path
    }

    pub fn read(&self, rel: &str) -> Vec<u8> {
        std::fs::read(self.join(rel)).unwrap()
    }

    pub fn login(&self) {
        self.write(
            "cfg/auth.json",
            r#"{"accessToken":"access-token","refreshToken":"refresh-token"}"#,
        );
    }

    pub fn state_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.read("state/state.json")).unwrap()
    }
}

pub struct Output {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("stdout is not JSON ({e}): {:?}", self.stdout))
    }
}

/// Run the built `cmux-vault` binary against `home` with every agent
/// directory and the vault config/state directories pointed inside it.
pub fn run(home: &Home, api_base: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cmux-vault"));
    cmd.env_clear()
        .env("HOME", home.path())
        .env("TZ", "UTC")
        .env("CMUX_VAULT_CONFIG_DIR", home.join("cfg"))
        .env("CMUX_VAULT_STATE_DIR", home.join("state"))
        .env("TMPDIR", home.join("tmp"))
        .args(args);
    if !api_base.is_empty() {
        cmd.env("CMUX_VAULT_API_BASE", api_base);
    }
    let output = cmd.output().expect("run cmux-vault");
    Output {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Default)]
pub struct MockState {
    pub blobs: BTreeMap<String, Vec<u8>>,
    pub committed: BTreeMap<String, String>,
    pub requests: Vec<Recorded>,
    /// Sessions returned by `GET /api/vault/sessions` (the resume lookup).
    pub sessions: Vec<Session>,
    /// Compressed objects served by `GET /object/<id>`.
    pub objects: BTreeMap<String, Vec<u8>>,
    /// Fail this many `/api/vault/uploads` calls with HTTP 500 first.
    pub fail_uploads: usize,
    /// Respond to every API call with this status and body instead.
    pub api_override: Option<(u16, String)>,
    /// Reject presigned PUTs with HTTP 403.
    pub reject_puts: bool,
    /// Accept presigned PUTs but never store them (so commit reports `object_missing`).
    pub drop_puts: bool,
}

pub struct MockServer {
    pub base: String,
    pub state: Arc<Mutex<MockState>>,
}

impl MockServer {
    pub fn start() -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let base = format!("http://{}", server.server_addr().to_ip().unwrap());
        let state = Arc::new(Mutex::new(MockState::default()));
        let thread_state = Arc::clone(&state);
        let thread_base = base.clone();
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                handle(request, &thread_state, &thread_base);
            }
        });
        Self { base, state }
    }

    pub fn state(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().unwrap()
    }

    pub fn requests_to(&self, path_prefix: &str) -> Vec<Recorded> {
        self.state().requests.iter().filter(|r| r.url.starts_with(path_prefix)).cloned().collect()
    }
}

fn json_response(status: u16, body: &impl serde::Serialize) -> Response<std::io::Cursor<Vec<u8>>> {
    let data = serde_json::to_vec(body).unwrap();
    Response::from_data(data)
        .with_status_code(status)
        .with_header(Header::from_bytes("Content-Type", "application/json").unwrap())
}

fn handle(mut request: tiny_http::Request, state: &Arc<Mutex<MockState>>, base: &str) {
    let mut body = Vec::new();
    request.as_reader().read_to_end(&mut body).unwrap();
    let headers: HashMap<String, String> = request
        .headers()
        .iter()
        .map(|h| (h.field.as_str().to_string().to_ascii_lowercase(), h.value.as_str().to_string()))
        .collect();
    let method = request.method().to_string();
    let url = request.url().to_string();
    let mut st = state.lock().unwrap();
    st.requests.push(Recorded { method, url: url.clone(), headers, body: body.clone() });
    let (path, query) = match url.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (url.clone(), String::new()),
    };

    if path.starts_with("/api/")
        && let Some((status, text)) = st.api_override.clone()
    {
        drop(st);
        let _ = request.respond(Response::from_string(text).with_status_code(status));
        return;
    }

    match (request.method(), path.as_str()) {
        (Method::Put, "/put") => {
            let key = query.strip_prefix("key=").unwrap_or("").to_string();
            if key.is_empty() {
                drop(st);
                let _ = request
                    .respond(Response::from_string("bad blob request").with_status_code(400));
                return;
            }
            if st.reject_puts {
                drop(st);
                let _ = request.respond(Response::from_string("denied").with_status_code(403));
                return;
            }
            if !st.drop_puts {
                st.blobs.insert(key, body);
            }
            drop(st);
            let _ = request.respond(Response::from_string("").with_status_code(200));
        }
        (Method::Post, "/api/vault/uploads") => {
            if st.fail_uploads > 0 {
                st.fail_uploads -= 1;
                drop(st);
                let _ = request.respond(Response::from_string("boom").with_status_code(500));
                return;
            }
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let items: Vec<UploadItem> = serde_json::from_value(parsed["items"].clone()).unwrap();
            let mut out = Vec::new();
            for item in items {
                let key = format!("{}/{}", item.agent, item.rel_path);
                let unchanged = st.committed.get(&key) == Some(&item.sha256);
                out.push(UploadResult {
                    agent: item.agent,
                    agent_session_id: item.agent_session_id,
                    rel_path: item.rel_path,
                    status: if unchanged { "unchanged".into() } else { "upload".into() },
                    object_key: if unchanged { String::new() } else { key.clone() },
                    put_url: if unchanged {
                        String::new()
                    } else {
                        format!("{base}/put?key={key}")
                    },
                    error: String::new(),
                });
            }
            drop(st);
            let _ = request.respond(json_response(200, &UploadsResponse { items: out }));
        }
        (Method::Post, "/api/vault/sessions/commit") => {
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let items: Vec<UploadItem> = serde_json::from_value(parsed["items"].clone()).unwrap();
            let mut out = Vec::new();
            for item in items {
                let key = format!("{}/{}", item.agent, item.rel_path);
                if !st.blobs.contains_key(&key) {
                    out.push(CommitResult {
                        agent: item.agent,
                        agent_session_id: item.agent_session_id,
                        rel_path: item.rel_path,
                        status: "error".into(),
                        error: "object_missing".into(),
                        session_id: String::new(),
                    });
                    continue;
                }
                st.committed.insert(key, item.sha256);
                out.push(CommitResult {
                    agent: item.agent,
                    agent_session_id: item.agent_session_id,
                    rel_path: item.rel_path,
                    status: "committed".into(),
                    error: String::new(),
                    session_id: "session-row".into(),
                });
            }
            drop(st);
            let _ = request.respond(json_response(200, &CommitResponse { items: out }));
        }
        (Method::Get, "/api/vault/sessions") => {
            let sessions = st.sessions.clone();
            drop(st);
            let _ = request.respond(json_response(
                200,
                &SessionsResponse { sessions, next_cursor: String::new() },
            ));
        }
        (Method::Get, p) if p.starts_with("/api/vault/sessions/") => {
            let id = p.trim_start_matches("/api/vault/sessions/");
            let found = st.sessions.iter().find(|s| s.id == id).cloned();
            drop(st);
            match found {
                Some(mut session) => {
                    session.download_url = format!("{base}/object/{id}");
                    let _ = request.respond(json_response(
                        200,
                        &SessionDetail { session, snapshots: Vec::new() },
                    ));
                }
                None => {
                    let _ =
                        request.respond(Response::from_string("not found").with_status_code(404));
                }
            }
        }
        (Method::Get, p) if p.starts_with("/object/") => {
            let id = p.trim_start_matches("/object/");
            let data = st.objects.get(id).cloned();
            drop(st);
            match data {
                Some(data) => {
                    let _ = request.respond(Response::from_data(data).with_status_code(200));
                }
                None => {
                    let _ = request
                        .respond(Response::from_string("missing object").with_status_code(404));
                }
            }
        }
        _ => {
            drop(st);
            let _ = request.respond(Response::from_string("not found").with_status_code(404));
        }
    }
}

pub fn compress(data: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(data, 3).unwrap()
}

pub fn decompress(data: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(data).unwrap()
}

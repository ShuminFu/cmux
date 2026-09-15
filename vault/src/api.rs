//! HTTP client for the cmux Vault web API and the presigned object-storage
//! transfers it hands out.

use std::fmt::{self, Write as _};
use std::io::Read;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use ureq::tls::{RootCerts, TlsConfig};
use ureq::{Agent, SendBody};

use crate::authstore::Tokens;
use crate::util::nullable;

pub const DEFAULT_BASE_URL: &str = "https://cmux.com";

const JSON_TIMEOUT: Duration = Duration::from_secs(30);
// Blob transfers can be large, so allow far more than the JSON timeout, but
// still bound the request so a stalled S3 PUT/GET cannot hang the CLI forever.
const BLOB_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const MAX_JSON_BODY: u64 = 2 * 1024 * 1024;
const MAX_ERROR_BODY: u64 = 4096;
const RETRY_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub enum ApiError {
    /// Non-2xx response from the web API.
    Status {
        status: u16,
        body: String,
    },
    NotLoggedIn,
    /// Transport, decode, or storage failure; already formatted for display.
    Message(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status { status, body } => {
                write!(f, "api request failed: status {status}: {}", body.trim())
            }
            Self::NotLoggedIn => write!(f, "not logged in; run cmux-vault login"),
            Self::Message(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<ureq::Error> for ApiError {
    fn from(err: ureq::Error) -> Self {
        Self::Message(err.to_string())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthStartResponse {
    #[serde(rename = "deviceCode", deserialize_with = "nullable")]
    pub device_code: String,
    #[serde(rename = "userCode", deserialize_with = "nullable")]
    pub user_code: String,
    #[serde(rename = "verificationUrl", deserialize_with = "nullable")]
    pub verification_url: String,
    #[serde(rename = "expiresInSeconds", deserialize_with = "nullable")]
    pub expires_in_seconds: i64,
    #[serde(rename = "intervalSeconds", deserialize_with = "nullable")]
    pub interval_seconds: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthPollResponse {
    #[serde(deserialize_with = "nullable")]
    pub status: String,
    #[serde(
        rename = "accessToken",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub access_token: String,
    #[serde(
        rename = "refreshToken",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub refresh_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UploadItem {
    #[serde(deserialize_with = "nullable")]
    pub agent: String,
    #[serde(rename = "agentSessionId", deserialize_with = "nullable")]
    pub agent_session_id: String,
    #[serde(rename = "relPath", deserialize_with = "nullable")]
    pub rel_path: String,
    #[serde(deserialize_with = "nullable", skip_serializing_if = "String::is_empty")]
    pub cwd: String,
    #[serde(deserialize_with = "nullable")]
    pub sha256: String,
    #[serde(rename = "sizeBytes", deserialize_with = "nullable")]
    pub size_bytes: i64,
    #[serde(rename = "compressedSizeBytes", deserialize_with = "nullable")]
    pub compressed_size_bytes: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UploadResult {
    #[serde(deserialize_with = "nullable")]
    pub agent: String,
    #[serde(rename = "agentSessionId", deserialize_with = "nullable")]
    pub agent_session_id: String,
    #[serde(rename = "relPath", deserialize_with = "nullable")]
    pub rel_path: String,
    #[serde(deserialize_with = "nullable")]
    pub status: String,
    #[serde(
        rename = "objectKey",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub object_key: String,
    #[serde(
        rename = "putUrl",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub put_url: String,
    #[serde(deserialize_with = "nullable", skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UploadsResponse {
    #[serde(deserialize_with = "nullable")]
    pub items: Vec<UploadResult>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CommitResult {
    #[serde(deserialize_with = "nullable")]
    pub agent: String,
    #[serde(rename = "agentSessionId", deserialize_with = "nullable")]
    pub agent_session_id: String,
    #[serde(rename = "relPath", deserialize_with = "nullable")]
    pub rel_path: String,
    #[serde(deserialize_with = "nullable")]
    pub status: String,
    #[serde(deserialize_with = "nullable", skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(
        rename = "sessionId",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub session_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CommitResponse {
    #[serde(deserialize_with = "nullable")]
    pub items: Vec<CommitResult>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Session {
    #[serde(deserialize_with = "nullable")]
    pub id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: String,
    #[serde(rename = "agentSessionId", deserialize_with = "nullable")]
    pub agent_session_id: String,
    #[serde(rename = "relPath", deserialize_with = "nullable")]
    pub rel_path: String,
    #[serde(deserialize_with = "nullable", skip_serializing_if = "String::is_empty")]
    pub cwd: String,
    #[serde(rename = "latestSha256", deserialize_with = "nullable")]
    pub latest_sha256: String,
    #[serde(rename = "sizeBytes", deserialize_with = "nullable")]
    pub size_bytes: i64,
    #[serde(rename = "lastUploadedAt", deserialize_with = "nullable")]
    pub last_uploaded_at: String,
    #[serde(
        rename = "downloadUrl",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub download_url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionsResponse {
    #[serde(deserialize_with = "nullable")]
    pub sessions: Vec<Session>,
    #[serde(
        rename = "nextCursor",
        deserialize_with = "nullable",
        skip_serializing_if = "String::is_empty"
    )]
    pub next_cursor: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Snapshot {
    #[serde(deserialize_with = "nullable")]
    pub sha256: String,
    #[serde(rename = "sizeBytes", deserialize_with = "nullable")]
    pub size_bytes: i64,
    #[serde(rename = "compressedSizeBytes", deserialize_with = "nullable")]
    pub compressed_size_bytes: i64,
    #[serde(rename = "uploadedAt", deserialize_with = "nullable")]
    pub uploaded_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionDetail {
    #[serde(flatten)]
    pub session: Session,
    #[serde(deserialize_with = "nullable")]
    pub snapshots: Vec<Snapshot>,
}

pub struct Client {
    pub base_url: String,
    pub tokens: Option<Tokens>,
    agent: Agent,
    blob_agent: Agent,
}

impl Client {
    #[must_use]
    pub fn new(base_url: &str, tokens: Option<Tokens>) -> Self {
        let base_url = base_url.trim().trim_end_matches('/');
        let base_url =
            if base_url.is_empty() { DEFAULT_BASE_URL.to_string() } else { base_url.to_string() };
        Self {
            base_url,
            tokens,
            agent: build_agent(JSON_TIMEOUT),
            blob_agent: build_agent(BLOB_TIMEOUT),
        }
    }

    pub fn start_auth(&self) -> Result<AuthStartResponse, ApiError> {
        self.do_json("POST", "/api/vault/cli/auth/start", Some(&serde_json::json!({})), false)
    }

    pub fn poll_auth(&self, device_code: &str) -> Result<AuthPollResponse, ApiError> {
        self.do_json(
            "POST",
            "/api/vault/cli/auth/poll",
            Some(&serde_json::json!({ "deviceCode": device_code })),
            false,
        )
    }

    pub fn request_uploads(&self, items: &[UploadItem]) -> Result<UploadsResponse, ApiError> {
        self.do_json(
            "POST",
            "/api/vault/uploads",
            Some(&serde_json::json!({ "items": items })),
            true,
        )
    }

    pub fn commit_sessions(&self, items: &[UploadItem]) -> Result<CommitResponse, ApiError> {
        self.do_json(
            "POST",
            "/api/vault/sessions/commit",
            Some(&serde_json::json!({ "items": items })),
            true,
        )
    }

    /// Look up a session by agent session id. `Ok(None)` when the vault has
    /// no such session; an error when the id is ambiguous across agents.
    pub fn find_session(
        &self,
        agent: &str,
        agent_session_id: &str,
    ) -> Result<Option<Session>, ApiError> {
        // Resume lookup uses the sessions collection route filtered by
        // agent+agentSessionId; callers then use get_session(id) to fetch the
        // presigned download URL for the latest snapshot.
        let mut query = String::new();
        if !agent.trim().is_empty() {
            query.push_str("agent=");
            query.push_str(&query_escape(agent));
            query.push('&');
        }
        query.push_str("agentSessionId=");
        query.push_str(&query_escape(agent_session_id));
        // Ask for two rows so an id shared by multiple agents is detected
        // instead of restoring an arbitrary one.
        query.push_str("&limit=2");
        let out: SessionsResponse =
            self.do_json("GET", &format!("/api/vault/sessions?{query}"), None, true)?;
        if out.sessions.is_empty() {
            return Ok(None);
        }
        if out.sessions.len() > 1 {
            return Err(ApiError::Message(format!(
                "session {agent_session_id} exists for multiple agents in cmux vault; pass --agent to disambiguate"
            )));
        }
        Ok(out.sessions.into_iter().next())
    }

    pub fn get_session(&self, id: &str) -> Result<SessionDetail, ApiError> {
        self.do_json("GET", &format!("/api/vault/sessions/{}", path_escape(id)), None, true)
    }

    /// Upload exactly `size` bytes from `body` to a presigned URL.
    pub fn put_object(
        &self,
        put_url: &str,
        body: &mut dyn Read,
        size: i64,
    ) -> Result<(), ApiError> {
        let mut resp = self
            .blob_agent
            .put(put_url)
            .header("Content-Type", "application/zstd")
            .header("Content-Length", size.to_string())
            .send(SendBody::from_reader(body))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let text = read_limited(resp.body_mut().as_reader(), MAX_ERROR_BODY);
            return Err(ApiError::Message(format!(
                "storage PUT failed: status {status}: {}",
                text.trim()
            )));
        }
        Ok(())
    }

    /// Stream an object from a presigned URL.
    pub fn download(&self, download_url: &str) -> Result<Box<dyn Read + Send + Sync>, ApiError> {
        let mut resp = self.blob_agent.get(download_url).call()?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let text = read_limited(resp.body_mut().as_reader(), MAX_ERROR_BODY);
            return Err(ApiError::Message(format!(
                "storage GET failed: status {status}: {}",
                text.trim()
            )));
        }
        Ok(Box::new(resp.into_body().into_reader()))
    }

    fn do_json<T>(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
        auth: bool,
    ) -> Result<T, ApiError>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        let payload = match body {
            Some(value) => {
                Some(serde_json::to_vec(value).map_err(|e| ApiError::Message(e.to_string()))?)
            }
            None => None,
        };
        let mut last_err = None;
        for attempt in 0..2 {
            if attempt > 0 {
                std::thread::sleep(RETRY_DELAY);
            }
            match self.do_json_once(method, path, payload.as_deref(), auth) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if let ApiError::Status { status, .. } = &err
                        && *status < 500
                    {
                        return Err(err);
                    }
                    last_err = Some(err);
                }
            }
        }
        Err(last_err.expect("at least one attempt"))
    }

    fn do_json_once<T>(
        &self,
        method: &str,
        path: &str,
        payload: Option<&[u8]>,
        auth: bool,
    ) -> Result<T, ApiError>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        let endpoint = format!("{}{path}", self.base_url);
        let mut headers: Vec<(&str, String)> = vec![("Accept", "application/json".to_string())];
        if payload.is_some() {
            headers.push(("Content-Type", "application/json".to_string()));
        }
        if auth {
            let tokens = match &self.tokens {
                Some(t) if !t.access_token.is_empty() && !t.refresh_token.is_empty() => t,
                _ => return Err(ApiError::NotLoggedIn),
            };
            headers.push(("Authorization", format!("Bearer {}", tokens.access_token)));
            headers.push(("X-Stack-Refresh-Token", tokens.refresh_token.clone()));
        }
        let mut resp = if let Some(payload) = payload {
            let mut req = match method {
                "POST" => self.agent.post(&endpoint),
                "PUT" => self.agent.put(&endpoint),
                other => return Err(ApiError::Message(format!("unsupported method {other}"))),
            };
            for (k, v) in &headers {
                req = req.header(*k, v);
            }
            req.send(payload)?
        } else {
            let mut req = match method {
                "GET" => self.agent.get(&endpoint),
                "DELETE" => self.agent.delete(&endpoint),
                other => return Err(ApiError::Message(format!("unsupported method {other}"))),
            };
            for (k, v) in &headers {
                req = req.header(*k, v);
            }
            req.call()?
        };
        let status = resp.status().as_u16();
        let mut data = Vec::new();
        resp.body_mut()
            .as_reader()
            .take(MAX_JSON_BODY)
            .read_to_end(&mut data)
            .map_err(|e| ApiError::Message(e.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(ApiError::Status {
                status,
                body: String::from_utf8_lossy(&data).into_owned(),
            });
        }
        if data.is_empty() {
            return Ok(T::default());
        }
        serde_json::from_slice(&data).map_err(|e| ApiError::Message(e.to_string()))
    }
}

fn build_agent(timeout: Duration) -> Agent {
    Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(timeout))
        .user_agent(concat!("cmux-vault/", env!("CARGO_PKG_VERSION")))
        .tls_config(TlsConfig::builder().root_certs(RootCerts::PlatformVerifier).build())
        .build()
        .new_agent()
}

fn read_limited(reader: impl Read, limit: u64) -> String {
    let mut data = Vec::new();
    let _ = reader.take(limit).read_to_end(&mut data);
    String::from_utf8_lossy(&data).into_owned()
}

fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
}

/// Port of Go's `url.QueryEscape`.
#[must_use]
pub fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else if b == b' ' {
            out.push('+');
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// Port of Go's `url.PathEscape` (path-segment mode).
#[must_use]
pub fn path_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if is_unreserved(b) || matches!(b, b'$' | b'&' | b'+' | b':' | b'=' | b'@') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_match_go() {
        assert_eq!(query_escape("a b/c?d=e&f"), "a+b%2Fc%3Fd%3De%26f");
        assert_eq!(query_escape("uuid-1234_.~"), "uuid-1234_.~");
        assert_eq!(query_escape("é"), "%C3%A9");
        assert_eq!(path_escape("a b/c;d,e?f"), "a%20b%2Fc%3Bd%2Ce%3Ff");
        assert_eq!(path_escape("id$&+:=@"), "id$&+:=@");
        assert_eq!(path_escape("x!'()*"), "x%21%27%28%29%2A");
    }

    #[test]
    fn base_url_normalization() {
        assert_eq!(Client::new("  https://x.test///  ", None).base_url, "https://x.test");
        assert_eq!(Client::new("", None).base_url, DEFAULT_BASE_URL);
        assert_eq!(Client::new("/", None).base_url, DEFAULT_BASE_URL);
    }

    #[test]
    fn wire_structs_tolerate_null_and_missing_fields() {
        let detail: SessionDetail = serde_json::from_str(
            r#"{"id":"s","agent":"codex","agentSessionId":"x","relPath":"r","cwd":null,"downloadUrl":null,"snapshots":null,"unknown":1}"#,
        )
        .unwrap();
        assert_eq!(detail.session.id, "s");
        assert_eq!(detail.session.cwd, "");
        assert!(detail.snapshots.is_empty());
        let resp: UploadsResponse =
            serde_json::from_str(r#"{"items":[{"status":"upload","putUrl":null}]}"#).unwrap();
        assert_eq!(resp.items[0].put_url, "");
        let item = UploadItem { agent: "a".into(), ..UploadItem::default() };
        let json = serde_json::to_string(&item).unwrap();
        assert!(!json.contains("cwd"), "{json}");
        assert!(
            json.starts_with(
                r#"{"agent":"a","agentSessionId":"","relPath":"","sha256":"","sizeBytes":0"#
            ),
            "{json}"
        );
    }

    #[test]
    fn unauthenticated_client_refuses_authed_calls_without_network() {
        let client = Client::new("http://127.0.0.1:9", None);
        let err = client.request_uploads(&[]).unwrap_err();
        assert!(matches!(err, ApiError::NotLoggedIn));
        assert_eq!(err.to_string(), "not logged in; run cmux-vault login");
        let client = Client::new(
            "http://127.0.0.1:9",
            Some(Tokens { access_token: "a".into(), refresh_token: String::new() }),
        );
        assert!(matches!(client.get_session("x").unwrap_err(), ApiError::NotLoggedIn));
    }
}

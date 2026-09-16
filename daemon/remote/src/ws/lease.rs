//! Lease files that gate the WebSocket transports, and the HTTPS admin
//! endpoint that installs them.

use std::io;
use std::path::Path;
use std::sync::Mutex;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64, STANDARD_NO_PAD as BASE64_NO_PAD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::http::Request;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsLease {
    #[serde(default)]
    pub version: i64,
    #[serde(default)]
    pub token_sha256: String,
    #[serde(default)]
    pub expires_at_unix: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default)]
    pub single_use: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsLeaseInstallRequest {
    #[serde(default)]
    pub pty_lease: Option<WsLease>,
    #[serde(default)]
    pub rpc_lease: Option<WsLease>,
    #[serde(default)]
    pub rpc_client: Option<WsRpcClientPayload>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsRpcClientPayload {
    #[serde(default)]
    pub token: String,
    #[serde(default, rename = "sessionId")]
    pub session_id: String,
    #[serde(default, rename = "expiresAtUnix")]
    pub expires_at_unix: i64,
}

/// The first text frame on `/terminal` and `/rpc`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsAuthFrame {
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub attachment_id: String,
    #[serde(default)]
    pub cols: i64,
    #[serde(default)]
    pub rows: i64,
    #[serde(skip)]
    pub session_id_explicit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    Missing,
    Expired,
    Forbidden,
}

#[derive(Debug)]
pub enum ConsumeError {
    Lease(LeaseError),
    Io(io::Error),
}

static LEASE_MU: Mutex<()> = Mutex::new(());

/// Validate the auth token against the lease at `path`, removing single-use
/// leases on success.
pub fn consume_websocket_lease(path: &str, auth: &WsAuthFrame) -> Result<(), ConsumeError> {
    let _guard = LEASE_MU.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(ConsumeError::Lease(LeaseError::Missing));
        }
        Err(e) => return Err(ConsumeError::Io(e)),
    };
    let Ok(lease) = serde_json::from_slice::<WsLease>(&data) else {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    };
    if lease.version != 1 {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    }
    if lease.expires_at_unix <= unix_now() {
        return Err(ConsumeError::Lease(LeaseError::Expired));
    }
    if !lease.session_id.is_empty() && lease.session_id != auth.session_id {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    }
    let Ok(expected) = hex::decode(lease.token_sha256.trim()) else {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    };
    if expected.len() != 32 {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    }
    let actual = Sha256::digest(auth.token.as_bytes());
    if !bool::from(expected.as_slice().ct_eq(actual.as_slice())) {
        return Err(ConsumeError::Lease(LeaseError::Forbidden));
    }
    if lease.single_use {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(ConsumeError::Io(e)),
        }
    }
    Ok(())
}

pub fn unix_now() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
    .unwrap_or(i64::MAX)
}

pub fn decode_admin_ed25519_public_key(raw: &str) -> Result<VerifyingKey, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("missing ed25519 public key".to_string());
    }
    let decoded = BASE64
        .decode(trimmed)
        .or_else(|_| BASE64_NO_PAD.decode(trimmed))
        .map_err(|_| "invalid ed25519 public key".to_string())?;
    let bytes: [u8; 32] =
        decoded.try_into().map_err(|_| "invalid ed25519 public key".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| "invalid ed25519 public key".to_string())
}

#[must_use]
pub fn verify_admin_lease_install_auth(
    request: &Request,
    body: &[u8],
    expected_hash: Option<&[u8]>,
    public_key: Option<&VerifyingKey>,
) -> bool {
    const BEARER_PREFIX: &str = "Bearer ";
    let auth = request.header("Authorization").unwrap_or("");
    if let Some(expected) = expected_hash
        && expected.len() == 32
        && let Some(token) = auth.strip_prefix(BEARER_PREFIX)
    {
        let actual = Sha256::digest(token.as_bytes());
        if bool::from(expected.ct_eq(actual.as_slice())) {
            return true;
        }
    }
    if let Some(key) = public_key {
        let raw = request.header("X-Cmux-Admin-Signature-Ed25519").unwrap_or("").trim();
        let decoded = BASE64.decode(raw).or_else(|_| BASE64_NO_PAD.decode(raw));
        if let Ok(signature) = decoded
            && let Ok(bytes) = <[u8; 64]>::try_from(signature.as_slice())
        {
            let signature = Signature::from_bytes(&bytes);
            if key.verify(body, &signature).is_ok() {
                return true;
            }
        }
    }
    false
}

pub fn write_lease_file(path: &str, lease: &WsLease) -> io::Result<()> {
    if path.trim().is_empty() {
        return Err(io::Error::other("lease path is empty"));
    }
    write_json_file(path, lease)
}

pub fn write_json_file<T: Serialize>(path: &str, value: &T) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let path = Path::new(path);
    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    }
    let mut data = serde_json::to_vec(value).map_err(io::Error::other)?;
    data.push(b'\n');
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    use std::io::Write as _;
    file.write_all(&data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_lease(dir: &Path, lease: &WsLease) -> String {
        let path = dir.join("lease.json").to_string_lossy().into_owned();
        write_lease_file(&path, lease).unwrap();
        path
    }

    #[test]
    fn lease_serializes_like_go() {
        let lease = WsLease {
            version: 1,
            token_sha256: "ab".into(),
            expires_at_unix: 5,
            session_id: String::new(),
            single_use: true,
        };
        assert_eq!(
            serde_json::to_string(&lease).unwrap(),
            r#"{"version":1,"token_sha256":"ab","expires_at_unix":5,"single_use":true}"#
        );
        let payload =
            WsRpcClientPayload { token: "t".into(), session_id: "s".into(), expires_at_unix: 9 };
        assert_eq!(
            serde_json::to_string(&payload).unwrap(),
            r#"{"token":"t","sessionId":"s","expiresAtUnix":9}"#
        );
    }

    #[test]
    fn consume_validates_and_removes_single_use() {
        let dir = tempfile::tempdir().unwrap();
        let token = "secret";
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        let lease = WsLease {
            version: 1,
            token_sha256: hash,
            expires_at_unix: unix_now() + 60,
            session_id: "s1".into(),
            single_use: true,
        };
        let path = write_lease(dir.path(), &lease);
        let mut auth = WsAuthFrame {
            token: token.into(),
            session_id: "other".into(),
            ..WsAuthFrame::default()
        };
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Forbidden))
        ));
        auth.session_id = "s1".into();
        auth.token = "wrong".into();
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Forbidden))
        ));
        auth.token = token.into();
        consume_websocket_lease(&path, &auth).unwrap();
        assert!(!Path::new(&path).exists(), "single-use lease must be removed");
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Missing))
        ));

        let expired =
            WsLease { expires_at_unix: unix_now() - 1, single_use: false, ..lease.clone() };
        let path = write_lease(dir.path(), &expired);
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Expired))
        ));
        let bad_version = WsLease { version: 2, expires_at_unix: unix_now() + 60, ..lease.clone() };
        let path = write_lease(dir.path(), &bad_version);
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Forbidden))
        ));
        std::fs::write(&path, b"not json").unwrap();
        assert!(matches!(
            consume_websocket_lease(&path, &auth),
            Err(ConsumeError::Lease(LeaseError::Forbidden))
        ));
        let reusable = WsLease { single_use: false, session_id: String::new(), ..lease };
        let path = write_lease(dir.path(), &reusable);
        consume_websocket_lease(&path, &auth).unwrap();
        consume_websocket_lease(&path, &auth).unwrap();
        assert!(Path::new(&path).exists());
    }

    #[test]
    fn admin_auth_accepts_bearer_hash_or_signature() {
        use ed25519_dalek::{Signer, SigningKey};
        let token = "admin-token";
        let hash = Sha256::digest(token.as_bytes());
        let body = b"{\"pty_lease\":null}";
        let mut request = Request {
            headers: vec![("Authorization".into(), format!("Bearer {token}"))],
            ..Request::default()
        };
        assert!(verify_admin_lease_install_auth(&request, body, Some(hash.as_slice()), None));
        request.headers[0].1 = "Bearer nope".into();
        assert!(!verify_admin_lease_install_auth(&request, body, Some(hash.as_slice()), None));

        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let public = signing.verifying_key();
        let encoded = BASE64.encode(public.to_bytes());
        let decoded = decode_admin_ed25519_public_key(&encoded).unwrap();
        assert_eq!(decoded, public);
        assert!(decode_admin_ed25519_public_key(&BASE64_NO_PAD.encode(public.to_bytes())).is_ok());
        assert!(decode_admin_ed25519_public_key("short").is_err());
        assert!(decode_admin_ed25519_public_key("").is_err());
        let signature = signing.sign(body);
        request.headers =
            vec![("X-Cmux-Admin-Signature-Ed25519".into(), BASE64.encode(signature.to_bytes()))];
        assert!(verify_admin_lease_install_auth(&request, body, None, Some(&public)));
        assert!(!verify_admin_lease_install_auth(&request, b"tampered", None, Some(&public)));
        assert!(!verify_admin_lease_install_auth(&request, body, Some(hash.as_slice()), None));
    }
}

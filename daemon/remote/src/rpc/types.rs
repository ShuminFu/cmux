//! JSON-RPC message types with the exact field names and omission rules of
//! the Go structs, plus the tolerant parameter accessors.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The frame was not a well-formed request object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRequest;

impl std::fmt::Display for InvalidRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid JSON request")
    }
}

impl std::error::Error for InvalidRequest {}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RpcRequest {
    /// The request id, `None` when absent or JSON `null`.
    pub id: Option<Value>,
    /// Whether an `id` key was present at all (even `null`).
    pub has_id: bool,
    pub method: String,
    pub params: Map<String, Value>,
}

impl RpcRequest {
    #[must_use]
    pub fn new(id: impl Into<Value>, method: &str, params: Map<String, Value>) -> Self {
        let id = id.into();
        Self {
            has_id: true,
            id: if id.is_null() { None } else { Some(id) },
            method: method.to_string(),
            params,
        }
    }

    /// Decode one request line with `encoding/json` semantics for the Go
    /// `rpcRequest` type: the top level must be an object, `method` must be
    /// a string (or absent), and `params` must be an object (or absent/null).
    pub fn parse(data: &[u8]) -> Result<Self, InvalidRequest> {
        let value: Value = serde_json::from_slice(data).map_err(|_| InvalidRequest)?;
        let Value::Object(mut obj) = value else { return Err(InvalidRequest) };
        let (has_id, id) = match obj.remove("id") {
            None => (false, None),
            Some(Value::Null) => (true, None),
            Some(other) => (true, Some(normalize_id(other))),
        };
        let method = match obj.remove("method") {
            None | Some(Value::Null) => String::new(),
            Some(Value::String(s)) => s,
            Some(_) => return Err(InvalidRequest),
        };
        let params = match obj.remove("params") {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(map)) => map,
            Some(_) => return Err(InvalidRequest),
        };
        Ok(Self { id, has_id, method, params })
    }
}

impl Serialize for RpcRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("rpcRequest", 3)?;
        state.serialize_field("id", &self.id.clone().unwrap_or(Value::Null))?;
        state.serialize_field("method", &self.method)?;
        state.serialize_field("params", &self.params)?;
        state.end()
    }
}

/// Go decodes every JSON number into `float64`, so `2.0` and `2` both
/// re-encode as `2`. Mirror that for ids we echo back.
fn normalize_id(value: Value) -> Value {
    match value {
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && n.is_f64()
                && f.fract() == 0.0
                && f.abs() < 9_007_199_254_740_992.0
            {
                #[allow(clippy::cast_possible_truncation)]
                return Value::from(f as i64);
            }
            Value::Number(n)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(normalize_id).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, normalize_id(v))).collect())
        }
        other => other,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(default)]
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    #[must_use]
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self { id, ok: true, result: Some(result), error: None }
    }

    #[must_use]
    pub fn failure(id: Option<Value>, code: &str, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(RpcError { code: code.to_string(), message: message.into() }),
        }
    }

    #[must_use]
    pub fn error_code(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.code.as_str())
    }

    #[must_use]
    pub fn error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.message.as_str())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcEvent {
    #[serde(default)]
    pub event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data_base64: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub seq: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl RpcEvent {
    #[must_use]
    pub fn named(event: &str) -> Self {
        Self { event: event.to_string(), ..Self::default() }
    }
}

/// `params[key]` when it is a JSON string.
#[must_use]
pub fn get_string_param(params: &Map<String, Value>, key: &str) -> Option<String> {
    match params.get(key) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// `params[key]` when it is an integral JSON number.
#[must_use]
pub fn get_int_param(params: &Map<String, Value>, key: &str) -> Option<i64> {
    match params.get(key) {
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                return Some(i);
            }
            if let Some(u) = n.as_u64() {
                return i64::try_from(u).ok();
            }
            let f = n.as_f64()?;
            if f.fract() != 0.0 || !f.is_finite() {
                return None;
            }
            #[allow(clippy::cast_possible_truncation)]
            Some(f as i64)
        }
        _ => None,
    }
}

/// `params[key]` when it is a JSON boolean.
#[must_use]
pub fn get_bool_param(params: &Map<String, Value>, key: &str) -> Option<bool> {
    match params.get(key) {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parsing_follows_go_semantics() {
        let req = RpcRequest::parse(br#"{"id":1,"method":"ping","params":{}}"#).unwrap();
        assert_eq!(req.id, Some(Value::from(1)));
        assert!(req.has_id);
        assert_eq!(req.method, "ping");
        let req = RpcRequest::parse(br#"{"method":"ping"}"#).unwrap();
        assert!(!req.has_id);
        assert_eq!(req.id, None);
        let req = RpcRequest::parse(br#"{"id":null,"method":"ping","params":null}"#).unwrap();
        assert!(req.has_id);
        assert_eq!(req.id, None);
        let req = RpcRequest::parse(br#"{"id":"abc","method":"x"}"#).unwrap();
        assert_eq!(req.id, Some(Value::from("abc")));
        let req = RpcRequest::parse(br#"{"id":2.0,"method":"x"}"#).unwrap();
        assert_eq!(serde_json::to_string(&req.id).unwrap(), "2");
        assert!(RpcRequest::parse(b"[1]").is_err());
        assert!(RpcRequest::parse(br#"{"method":1}"#).is_err());
        assert!(RpcRequest::parse(br#"{"method":"x","params":[1]}"#).is_err());
        assert!(RpcRequest::parse(b"{").is_err());
        assert_eq!(RpcRequest::parse(b"{}").unwrap().method, "");
    }

    #[test]
    fn response_and_event_omit_empty_fields_like_go() {
        let resp = RpcResponse::success(None, serde_json::json!({"pong": true}));
        assert_eq!(serde_json::to_string(&resp).unwrap(), r#"{"ok":true,"result":{"pong":true}}"#);
        let resp = RpcResponse::failure(Some(Value::from(7)), "not_found", "nope");
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"id":7,"ok":false,"error":{"code":"not_found","message":"nope"}}"#
        );
        let event =
            RpcEvent { event: "pty.exit".into(), session_id: "s".into(), ..RpcEvent::default() };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"event":"pty.exit","session_id":"s"}"#
        );
        let event = RpcEvent { event: "pty.input_ack".into(), seq: 3, ..RpcEvent::default() };
        assert_eq!(serde_json::to_string(&event).unwrap(), r#"{"event":"pty.input_ack","seq":3}"#);
        let req = RpcRequest::new("auth", "daemon.auth", Map::new());
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"id":"auth","method":"daemon.auth","params":{}}"#
        );
    }

    #[test]
    fn int_param_rejects_fractional_floats() {
        let params: Map<String, Value> =
            serde_json::from_str(r#"{"port":80.9,"timeout_ms":100.0,"n":5,"s":"5","b":true}"#)
                .unwrap();
        assert_eq!(get_int_param(&params, "port"), None);
        assert_eq!(get_int_param(&params, "timeout_ms"), Some(100));
        assert_eq!(get_int_param(&params, "n"), Some(5));
        assert_eq!(get_int_param(&params, "s"), None);
        assert_eq!(get_string_param(&params, "s"), Some("5".into()));
        assert_eq!(get_string_param(&params, "n"), None);
        assert_eq!(get_bool_param(&params, "b"), Some(true));
        assert_eq!(get_bool_param(&params, "n"), None);
    }
}

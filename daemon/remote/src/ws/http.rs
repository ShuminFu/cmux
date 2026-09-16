//! Just enough HTTP/1.1 to route the daemon's four endpoints and complete a
//! WebSocket upgrade.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;

use super::frame::{WsConn, WsRole, accept_key};

const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Default)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }

    fn header_has_token(&self, name: &str, token: &str) -> bool {
        self.headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .any(|(_, v)| v.split(',').any(|part| part.trim().eq_ignore_ascii_case(token)))
    }
}

/// Read one request (headers plus a `Content-Length` body capped at
/// `max_body`). Returns `Ok(None)` when the client hung up before sending
/// anything.
pub fn read_request(
    reader: &mut BufReader<TcpStream>,
    max_body: usize,
) -> io::Result<Option<Request>> {
    let mut raw = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if raw.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete request headers"));
        }
        let take = available.len().min(MAX_HEADER_BYTES - raw.len().min(MAX_HEADER_BYTES));
        if take == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "request headers too large"));
        }
        raw.extend_from_slice(&available[..take]);
        reader.consume(take);
        if let Some(end) = find_header_end(&raw) {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut parsed = httparse::Request::new(&mut headers);
            let status = parsed
                .parse(&raw[..end])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let httparse::Status::Complete(consumed) = status else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed request"));
            };
            let mut request = Request {
                method: parsed.method.unwrap_or("").to_string(),
                path: parsed.path.unwrap_or("").to_string(),
                headers: parsed
                    .headers
                    .iter()
                    .map(|h| (h.name.to_string(), String::from_utf8_lossy(h.value).into_owned()))
                    .collect(),
                body: Vec::new(),
            };
            let content_length: usize =
                request.header("content-length").and_then(|v| v.trim().parse().ok()).unwrap_or(0);
            let want = content_length.min(max_body);
            let mut body = raw[consumed..].to_vec();
            if body.len() < want {
                let mut rest = vec![0u8; want - body.len()];
                reader.read_exact(&mut rest)?;
                body.extend_from_slice(&rest);
            }
            body.truncate(want);
            request.body = body;
            return Ok(Some(request));
        }
        if raw.len() >= MAX_HEADER_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "request headers too large"));
        }
    }
}

fn find_header_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        101 => "Switching Protocols",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        426 => "Upgrade Required",
        500 => "Internal Server Error",
        _ => "",
    }
}

pub fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!("HTTP/1.1 {status} {}\r\n", reason_phrase(status));
    head.push_str(&format!("Content-Type: {content_type}\r\n"));
    if content_type.starts_with("text/plain") {
        head.push_str("X-Content-Type-Options: nosniff\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

/// `http.Error`: plain-text message with a trailing newline.
pub fn write_error(stream: &mut TcpStream, status: u16, message: &str) -> io::Result<()> {
    write_response(stream, status, "text/plain; charset=utf-8", format!("{message}\n").as_bytes())
}

/// Complete a server-side WebSocket handshake, or answer with an error like
/// `websocket.Accept` does when the request is not an upgrade.
pub fn accept_websocket(
    mut stream: TcpStream,
    request: &Request,
    read_limit: usize,
) -> io::Result<WsConn> {
    let fail = |stream: &mut TcpStream, status: u16, message: &str| -> io::Result<WsConn> {
        let _ = write_error(stream, status, message);
        Err(io::Error::new(io::ErrorKind::InvalidData, message.to_string()))
    };
    if request.method != "GET" {
        return fail(
            &mut stream,
            405,
            "WebSocket protocol violation: handshake request method is not GET",
        );
    }
    if !request.header_has_token("Connection", "Upgrade") {
        let value = request.header("Connection").unwrap_or("");
        let message = format!(
            "WebSocket protocol violation: Connection header {} does not contain Upgrade",
            crate::util::quote(value)
        );
        return fail(&mut stream, 426, &message);
    }
    if !request.header_has_token("Upgrade", "websocket") {
        let value = request.header("Upgrade").unwrap_or("");
        let message = format!(
            "WebSocket protocol violation: Upgrade header {} does not contain websocket",
            crate::util::quote(value)
        );
        return fail(&mut stream, 426, &message);
    }
    if request.header("Sec-WebSocket-Version").map(str::trim) != Some("13") {
        return fail(&mut stream, 400, "unsupported WebSocket version");
    }
    let Some(key) = request.header("Sec-WebSocket-Key") else {
        return fail(&mut stream, 400, "WebSocket protocol violation: missing Sec-WebSocket-Key");
    };
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key)
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    WsConn::new(stream, WsRole::Server, read_limit)
}

/// Client-side handshake against `addr` for `path` (tests and the CLI).
pub fn connect_websocket(addr: &str, path: &str, read_limit: usize) -> io::Result<WsConn> {
    let mut stream = TcpStream::connect(addr)?;
    let mut key_bytes = [0u8; 16];
    getrandom::fill(&mut key_bytes).map_err(|e| io::Error::other(e.to_string()))?;
    let key = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(key_bytes)
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut raw = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "handshake failed: connection closed",
            ));
        }
        let take = available.len();
        raw.extend_from_slice(available);
        reader.consume(take);
        if let Some(end) = find_header_end(&raw) {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut response = httparse::Response::new(&mut headers);
            response
                .parse(&raw[..end])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            if response.code != Some(101) {
                let body = String::from_utf8_lossy(&raw[end..]).trim().to_string();
                return Err(io::Error::other(format!(
                    "handshake failed: status {} {}",
                    response.code.unwrap_or(0),
                    body
                )));
            }
            let expected = accept_key(&key);
            let got = response
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("Sec-WebSocket-Accept"))
                .map(|h| String::from_utf8_lossy(h.value).trim().to_string());
            if got.as_deref() != Some(expected.as_str()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handshake failed: bad accept key",
                ));
            }
            if raw.len() > end {
                // Bytes after the handshake belong to the WebSocket stream.
                // The server never sends before we do, so this cannot happen
                // in practice; treat it as a protocol error.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected data after handshake",
                ));
            }
            drop(reader);
            return WsConn::new(stream, WsRole::Client, read_limit);
        }
        if raw.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "handshake response too large"));
        }
    }
}

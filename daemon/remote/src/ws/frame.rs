//! Minimal RFC 6455 framing over a `TcpStream`, with independent reader and
//! writer halves so a PTY pump can read while output is being written.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use sha1::{Digest, Sha1};

use crate::pty::AttachmentConn;

pub const STATUS_NORMAL_CLOSURE: u16 = 1000;
pub const STATUS_UNSUPPORTED_DATA: u16 = 1003;
pub const STATUS_POLICY_VIOLATION: u16 = 1008;
pub const STATUS_MESSAGE_TOO_BIG: u16 = 1009;
pub const STATUS_INTERNAL_ERROR: u16 = 1011;

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsRole {
    Server,
    Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(Vec<u8>),
    Binary(Vec<u8>),
    /// A close frame from the peer; the reply has already been sent.
    Close {
        code: Option<u16>,
        reason: String,
    },
}

/// Compute the `Sec-WebSocket-Accept` value for a handshake key.
#[must_use]
pub fn accept_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.trim().as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    BASE64.encode(hasher.finalize())
}

pub struct WsConn {
    reader: Mutex<BufReader<TcpStream>>,
    writer: Mutex<TcpStream>,
    control: TcpStream,
    role: WsRole,
    read_limit: usize,
    close_sent: AtomicBool,
    closed: AtomicBool,
}

impl WsConn {
    pub fn new(stream: TcpStream, role: WsRole, read_limit: usize) -> io::Result<Self> {
        let reader = stream.try_clone()?;
        let control = stream.try_clone()?;
        Ok(Self {
            reader: Mutex::new(BufReader::with_capacity(64 * 1024, reader)),
            writer: Mutex::new(stream),
            control,
            role,
            read_limit,
            close_sent: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.control.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.control.set_write_timeout(timeout)
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Read the next data message, transparently answering pings and
    /// reassembling fragmented messages.
    pub fn read(&self) -> io::Result<WsMessage> {
        let mut reader = self.reader.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut assembled: Vec<u8> = Vec::new();
        let mut assembled_op: Option<u8> = None;
        loop {
            let (fin, opcode, payload) = self.read_frame(&mut reader)?;
            match opcode {
                OP_PING => {
                    self.write_frame(OP_PONG, &payload)?;
                }
                OP_PONG => {}
                OP_CLOSE => {
                    let (code, reason) = parse_close_payload(&payload);
                    let reply_code = code.unwrap_or(STATUS_NORMAL_CLOSURE);
                    let _ = self.send_close_frame(reply_code, "");
                    self.closed.store(true, Ordering::SeqCst);
                    return Ok(WsMessage::Close { code, reason });
                }
                OP_TEXT | OP_BINARY => {
                    if assembled_op.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unexpected new data frame during fragmented message",
                        ));
                    }
                    if fin {
                        return Ok(if opcode == OP_TEXT {
                            WsMessage::Text(payload)
                        } else {
                            WsMessage::Binary(payload)
                        });
                    }
                    assembled_op = Some(opcode);
                    assembled = payload;
                    if assembled.len() > self.read_limit {
                        return self.too_big();
                    }
                }
                OP_CONTINUATION => {
                    let Some(op) = assembled_op else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "continuation frame without start",
                        ));
                    };
                    assembled.extend_from_slice(&payload);
                    if assembled.len() > self.read_limit {
                        return self.too_big();
                    }
                    if fin {
                        let data = std::mem::take(&mut assembled);
                        return Ok(if op == OP_TEXT {
                            WsMessage::Text(data)
                        } else {
                            WsMessage::Binary(data)
                        });
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown opcode {opcode}"),
                    ));
                }
            }
        }
    }

    fn too_big(&self) -> io::Result<WsMessage> {
        let _ = self.send_close_frame(STATUS_MESSAGE_TOO_BIG, "read limit exceeded");
        self.close_now();
        Err(io::Error::new(io::ErrorKind::InvalidData, "message exceeds read limit"))
    }

    fn read_frame(&self, reader: &mut BufReader<TcpStream>) -> io::Result<(bool, u8, Vec<u8>)> {
        let mut header = [0u8; 2];
        read_exact_or_eof(reader, &mut header)?;
        let fin = header[0] & 0x80 != 0;
        let opcode = header[0] & 0x0F;
        let masked = header[1] & 0x80 != 0;
        let mut len = u64::from(header[1] & 0x7F);
        if len == 126 {
            let mut ext = [0u8; 2];
            reader.read_exact(&mut ext)?;
            len = u64::from(u16::from_be_bytes(ext));
        } else if len == 127 {
            let mut ext = [0u8; 8];
            reader.read_exact(&mut ext)?;
            len = u64::from_be_bytes(ext);
        }
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too large"))?;
        if len > self.read_limit {
            let _ = self.send_close_frame(STATUS_MESSAGE_TOO_BIG, "read limit exceeded");
            self.close_now();
            return Err(io::Error::new(io::ErrorKind::InvalidData, "frame exceeds read limit"));
        }
        let mut mask = [0u8; 4];
        if masked {
            reader.read_exact(&mut mask)?;
        }
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        if masked {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
        }
        Ok((fin, opcode, payload))
    }

    pub fn write_text(&self, payload: &[u8]) -> io::Result<()> {
        self.write_frame(OP_TEXT, payload)
    }

    pub fn write_binary(&self, payload: &[u8]) -> io::Result<()> {
        self.write_frame(OP_BINARY, payload)
    }

    fn write_frame(&self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x80 | opcode);
        let mask_bit = if self.role == WsRole::Client { 0x80 } else { 0 };
        let len = payload.len();
        if len < 126 {
            #[allow(clippy::cast_possible_truncation)]
            frame.push(mask_bit | len as u8);
        } else if len <= usize::from(u16::MAX) {
            frame.push(mask_bit | 126);
            #[allow(clippy::cast_possible_truncation)]
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(mask_bit | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        if self.role == WsRole::Client {
            let mut key = [0u8; 4];
            getrandom::fill(&mut key).map_err(|e| io::Error::other(e.to_string()))?;
            frame.extend_from_slice(&key);
            frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        } else {
            frame.extend_from_slice(payload);
        }
        let mut writer = self.writer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        writer.write_all(&frame)?;
        writer.flush()
    }

    fn send_close_frame(&self, code: u16, reason: &str) -> io::Result<()> {
        if self.close_sent.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(2 + reason.len());
        payload.extend_from_slice(&code.to_be_bytes());
        payload.extend_from_slice(reason.as_bytes());
        self.write_frame(OP_CLOSE, &payload)
    }

    /// Send a close frame with `code` and `reason`, give the peer a moment
    /// to acknowledge, then tear the socket down.
    pub fn close(&self, code: u16, reason: &str) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.send_close_frame(code, reason);
        let _ = self.control.set_read_timeout(Some(Duration::from_secs(1)));
        if let Ok(mut reader) = self.reader.try_lock() {
            for _ in 0..16 {
                match self.read_frame(&mut reader) {
                    Ok((_, OP_CLOSE, _)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
        let _ = self.control.shutdown(Shutdown::Both);
    }

    /// Abort the connection without a close handshake.
    pub fn close_now(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.control.shutdown(Shutdown::Both);
    }
}

impl AttachmentConn for WsConn {
    fn close_now(&self) {
        WsConn::close_now(self);
    }
}

fn read_exact_or_eof<R: BufRead>(reader: &mut R, buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..])?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
        }
        filled += n;
    }
    Ok(())
}

fn parse_close_payload(payload: &[u8]) -> (Option<u16>, String) {
    if payload.len() < 2 {
        return (None, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (Some(code), String::from_utf8_lossy(&payload[2..]).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn accept_key_matches_rfc_example() {
        assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn frames_roundtrip_between_client_and_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let stream = TcpStream::connect(addr).unwrap();
            let conn = WsConn::new(stream, WsRole::Client, 1 << 20).unwrap();
            conn.write_text(b"hello").unwrap();
            conn.write_binary(&vec![7u8; 70_000]).unwrap();
            conn.write_frame(OP_PING, b"p").unwrap();
            let reply = conn.read().unwrap();
            assert_eq!(reply, WsMessage::Text(b"world".to_vec()));
            conn.close(STATUS_NORMAL_CLOSURE, "done");
        });
        let (stream, _) = listener.accept().unwrap();
        let conn = WsConn::new(stream, WsRole::Server, 1 << 20).unwrap();
        assert_eq!(conn.read().unwrap(), WsMessage::Text(b"hello".to_vec()));
        assert_eq!(conn.read().unwrap(), WsMessage::Binary(vec![7u8; 70_000]));
        conn.write_text(b"world").unwrap();
        match conn.read().unwrap() {
            WsMessage::Close { code, reason } => {
                assert_eq!(code, Some(STATUS_NORMAL_CLOSURE));
                assert_eq!(reason, "done");
            }
            other => panic!("unexpected {other:?}"),
        }
        client.join().unwrap();
    }

    #[test]
    fn oversized_frame_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let stream = TcpStream::connect(addr).unwrap();
            let conn = WsConn::new(stream, WsRole::Client, 1 << 20).unwrap();
            let _ = conn.write_binary(&vec![0u8; 2048]);
            matches!(conn.read(), Ok(WsMessage::Close { code: Some(STATUS_MESSAGE_TOO_BIG), .. }))
        });
        let (stream, _) = listener.accept().unwrap();
        let conn = WsConn::new(stream, WsRole::Server, 1024).unwrap();
        assert!(conn.read().is_err());
        assert!(client.join().unwrap());
    }
}

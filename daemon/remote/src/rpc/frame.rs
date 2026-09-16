//! Line framing with a hard size cap, and the serialized frame writer.

use std::io::{self, BufRead, BufWriter, Write};
use std::sync::Mutex;

use serde::Serialize;

use super::types::{RpcEvent, RpcResponse};

pub const MAX_RPC_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum RpcFrame {
    /// A complete line, including its trailing newline when one was present.
    Line(Vec<u8>),
    /// The line exceeded `max_bytes`; the rest of it has been discarded.
    Oversized,
    /// Clean end of input with no partial line pending.
    Eof,
}

/// Read one newline-terminated frame. A frame larger than `max_bytes` is
/// discarded through its newline and reported as [`RpcFrame::Oversized`] so
/// the connection keeps serving. A final unterminated line is still returned.
pub fn read_rpc_frame<R: BufRead>(reader: &mut R, max_bytes: usize) -> io::Result<RpcFrame> {
    let mut frame: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(if frame.is_empty() { RpcFrame::Eof } else { RpcFrame::Line(frame) });
        }
        match available.iter().position(|b| *b == b'\n') {
            Some(idx) => {
                let chunk_len = idx + 1;
                if frame.len() + chunk_len > max_bytes {
                    reader.consume(chunk_len);
                    return Ok(RpcFrame::Oversized);
                }
                frame.extend_from_slice(&available[..chunk_len]);
                reader.consume(chunk_len);
                return Ok(RpcFrame::Line(frame));
            }
            None => {
                let chunk_len = available.len();
                if frame.len() + chunk_len > max_bytes {
                    reader.consume(chunk_len);
                    discard_until_newline(reader)?;
                    return Ok(RpcFrame::Oversized);
                }
                frame.extend_from_slice(available);
                reader.consume(chunk_len);
            }
        }
    }
}

fn discard_until_newline<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }
        match available.iter().position(|b| *b == b'\n') {
            Some(idx) => {
                reader.consume(idx + 1);
                return Ok(());
            }
            None => {
                let len = available.len();
                reader.consume(len);
            }
        }
    }
}

/// Strip the frame terminator (`\n`, and a preceding `\r`).
#[must_use]
pub fn trim_frame(mut line: Vec<u8>) -> Vec<u8> {
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    line
}

pub trait FrameWriter: Send + Sync {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()>;
    fn write_event(&self, event: &RpcEvent) -> io::Result<()>;
}

/// Serializes each frame as one JSON line and flushes it immediately, under a
/// lock so concurrent pumps never interleave bytes.
pub struct StdioFrameWriter {
    inner: Mutex<BufWriter<Box<dyn Write + Send>>>,
    closed: std::sync::atomic::AtomicBool,
}

impl StdioFrameWriter {
    pub fn new(writer: impl Write + Send + 'static) -> Self {
        Self {
            inner: Mutex::new(BufWriter::new(Box::new(writer))),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).flush()
    }

    /// Stop accepting frames: once the client has hung up, background pumps
    /// must not race a final event onto a transport nobody reads.
    pub fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn write_json_frame(&self, payload: &impl Serialize) -> io::Result<()> {
        let data = serde_json::to_vec(payload).map_err(io::Error::other)?;
        let mut writer = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "frame writer closed"));
        }
        writer.write_all(&data)?;
        writer.write_all(b"\n")?;
        writer.flush()
    }
}

impl FrameWriter for StdioFrameWriter {
    fn write_response(&self, resp: &RpcResponse) -> io::Result<()> {
        self.write_json_frame(resp)
    }

    fn write_event(&self, event: &RpcEvent) -> io::Result<()> {
        self.write_json_frame(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    fn frames(input: &[u8], max: usize) -> Vec<RpcFrame> {
        let mut reader = BufReader::with_capacity(16, input);
        let mut out = Vec::new();
        loop {
            let frame = read_rpc_frame(&mut reader, max).unwrap();
            let done = frame == RpcFrame::Eof;
            out.push(frame);
            if done {
                break;
            }
        }
        out
    }

    #[test]
    fn splits_lines_and_keeps_partial_tail() {
        assert_eq!(
            frames(b"one\ntwo\r\nthree", 64),
            vec![
                RpcFrame::Line(b"one\n".to_vec()),
                RpcFrame::Line(b"two\r\n".to_vec()),
                RpcFrame::Line(b"three".to_vec()),
                RpcFrame::Eof
            ]
        );
        assert_eq!(frames(b"", 64), vec![RpcFrame::Eof]);
        assert_eq!(trim_frame(b"x\r\n".to_vec()), b"x");
        assert_eq!(trim_frame(b"x".to_vec()), b"x");
    }

    #[test]
    fn oversized_line_is_discarded_and_serving_continues() {
        let big = vec![b'a'; 100];
        let mut input = big.clone();
        input.extend_from_slice(b"\nsmall\n");
        assert_eq!(
            frames(&input, 32),
            vec![RpcFrame::Oversized, RpcFrame::Line(b"small\n".to_vec()), RpcFrame::Eof]
        );
        // Oversized line that hits EOF before a newline.
        assert_eq!(frames(&big, 32), vec![RpcFrame::Oversized, RpcFrame::Eof]);
        // Exactly at the cap is fine; one more byte is not.
        let exact = vec![b'a'; 31];
        let mut input = exact;
        input.push(b'\n');
        assert_eq!(frames(&input, 32), vec![RpcFrame::Line(input.clone()), RpcFrame::Eof]);
        let mut input = vec![b'a'; 32];
        input.push(b'\n');
        assert_eq!(frames(&input, 32), vec![RpcFrame::Oversized, RpcFrame::Eof]);
    }
}

//! Deterministic artifact evidence: content hashes and byte counts for the
//! specification that was rendered and the HTML that was written.

use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct FileReceipt {
    pub sha256: String,
    pub bytes: usize,
}

impl FileReceipt {
    pub fn of(bytes: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(bytes);
        let digest = h.finalize();
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        Self {
            sha256: hex,
            bytes: bytes.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FileReceipt;

    #[test]
    fn sha256_matches_the_known_digest_of_abc() {
        let r = FileReceipt::of(b"abc");
        assert_eq!(
            r.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(r.bytes, 3);
    }
}

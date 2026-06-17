use sha2::{Digest, Sha256};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Lowercase hex encoding of the SHA-256 digest of `bytes`.
///
/// Used for content-addressed object versions and segment checksums. The output
/// is byte-identical to `format!("{:02x}", ..)` over each digest byte, but
/// avoids a per-byte allocation.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

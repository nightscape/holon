use sha2::{Digest, Sha256};
use std::path::Path;

/// Compute SHA256 hash of file contents.
pub fn hash_file(path: &Path) -> std::io::Result<String> {
    let contents = std::fs::read(path)?;
    Ok(hash_bytes(&contents))
}

/// Compute SHA256 hash of a byte slice.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

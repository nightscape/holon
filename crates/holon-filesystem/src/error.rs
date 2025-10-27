//! Error types for filesystem operations

use std::fmt;

#[derive(Debug)]
pub enum FilesystemError {
    NotFound(String),
    Io(std::io::Error),
    InvalidPath(String),
}

impl fmt::Display for FilesystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilesystemError::NotFound(path) => write!(f, "Path not found: {}", path),
            FilesystemError::Io(err) => write!(f, "IO error: {}", err),
            FilesystemError::InvalidPath(path) => write!(f, "Invalid path: {}", path),
        }
    }
}

impl std::error::Error for FilesystemError {}

impl From<std::io::Error> for FilesystemError {
    fn from(err: std::io::Error) -> Self {
        FilesystemError::Io(err)
    }
}

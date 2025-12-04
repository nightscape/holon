//! Filesystem utilities for Holon
//!
//! This crate provides filesystem operations and utilities used by other Holon crates.

pub mod directory;
pub mod error;

pub use directory::{ChangesWithMetadata, DirectoryChangeProvider, DirectoryDataSource};
pub use directory::{Directory, ROOT_ID};
pub use error::FilesystemError;

use std::path::Path;

/// Filesystem utilities
pub struct Filesystem;

impl Filesystem {
    /// Check if a path exists
    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().exists()
    }

    /// Check if a path is a directory
    pub fn is_dir<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_dir()
    }

    /// Check if a path is a file
    pub fn is_file<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filesystem_exists() {
        // Test with a path that should exist (current directory)
        assert!(Filesystem::exists("."));
    }
}

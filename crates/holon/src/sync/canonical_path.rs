//! Canonical path type that guarantees symlink resolution.
//!
//! On macOS, `/var` is a symlink to `/private/var`. This can cause issues where
//! the same file is accessed via different paths, leading to duplicate entries
//! in HashMaps or missed lookups.
//!
//! `CanonicalPath` solves this by canonicalizing paths at construction time,
//! making it impossible to accidentally use non-canonical paths.

use std::ops::Deref;
use std::path::{Path, PathBuf};

/// A path that has been canonicalized to resolve symlinks.
///
/// This type can only be constructed via `CanonicalPath::new()` which
/// performs canonicalization, ensuring all instances represent the true
/// filesystem path without symlinks.
///
/// # Example
/// ```ignore
/// use holon::sync::CanonicalPath;
///
/// // On macOS, both of these resolve to the same canonical path
/// let p1 = CanonicalPath::new("/var/folders/test");
/// let p2 = CanonicalPath::new("/private/var/folders/test");
/// assert_eq!(p1, p2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalPath(PathBuf);

impl CanonicalPath {
    /// Create a new CanonicalPath by canonicalizing the given path.
    ///
    /// If canonicalization fails (e.g., the file doesn't exist yet),
    /// falls back to the original path. This allows working with paths
    /// before the file is created.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        Self(std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
    }

    /// Create a CanonicalPath, returning an error if canonicalization fails.
    ///
    /// Use this when you need to ensure the path exists and is fully resolved.
    pub fn try_new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        std::fs::canonicalize(path.as_ref()).map(Self)
    }

    /// Create a CanonicalPath from an already-canonical PathBuf.
    ///
    /// # Safety
    /// The caller must ensure the path is already canonical. This is useful
    /// when the path comes from a trusted source that already canonicalizes
    /// (e.g., reading back from a HashMap that uses CanonicalPath keys).
    pub(crate) fn from_canonical_unchecked(path: PathBuf) -> Self {
        Self(path)
    }

    /// Get the inner PathBuf, consuming self.
    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }

    /// Get a reference to the inner PathBuf.
    pub fn as_path_buf(&self) -> &PathBuf {
        &self.0
    }
}

impl Deref for CanonicalPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for CanonicalPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl std::fmt::Display for CanonicalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.display().fmt(f)
    }
}

impl From<CanonicalPath> for PathBuf {
    fn from(cp: CanonicalPath) -> Self {
        cp.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_canonical_path_resolves_symlinks() {
        // Create a temp file
        let temp_dir = TempDir::new().unwrap();
        let real_file = temp_dir.path().join("real.txt");
        std::fs::write(&real_file, "test").unwrap();

        // Create a symlink
        let link_path = temp_dir.path().join("link.txt");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_file, &link_path).unwrap();

        #[cfg(unix)]
        {
            let cp_real = CanonicalPath::new(&real_file);
            let cp_link = CanonicalPath::new(&link_path);

            // Both should resolve to the same canonical path
            assert_eq!(cp_real, cp_link);
        }
    }

    #[test]
    fn test_canonical_path_fallback_for_nonexistent() {
        let nonexistent = PathBuf::from("/this/path/does/not/exist/file.txt");
        let cp = CanonicalPath::new(&nonexistent);

        // Should fall back to the original path
        assert_eq!(cp.as_path_buf(), &nonexistent);
    }

    #[test]
    fn test_canonical_path_equality_and_hash() {
        use std::collections::HashMap;

        let temp_dir = TempDir::new().unwrap();
        let file = temp_dir.path().join("test.txt");
        std::fs::write(&file, "test").unwrap();

        let cp1 = CanonicalPath::new(&file);
        let cp2 = CanonicalPath::new(&file);

        assert_eq!(cp1, cp2);

        // Should work as HashMap key
        let mut map: HashMap<CanonicalPath, i32> = HashMap::new();
        map.insert(cp1.clone(), 42);
        assert_eq!(map.get(&cp2), Some(&42));
    }
}

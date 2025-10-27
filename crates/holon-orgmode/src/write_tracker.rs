//! Write tracker to prevent sync loops.
//!
//! When we write to an org file, we don't want the file watcher to trigger
//! a re-sync back. This tracks recent writes so they can be ignored.
//!
//! Also tracks when OrgAdapter is processing external changes, so OrgFileWriter
//! can skip rendering during that time (prevents the sync loop bug where
//! OrgFileWriter re-renders with stale state before OrgAdapter finishes).
//!
//! Uses `CanonicalPath` to ensure symlinks are resolved (e.g., /var -> /private/var on macOS).

use crate::file_utils::hash_file;
use holon::sync::CanonicalPath;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

/// Tracks the content hash of a file when we wrote it.
/// This allows us to detect if an external process modified the file.
#[derive(Debug, Clone)]
struct WriteInfo {
    hash: String,
    time: Instant,
}

/// Tracks recent writes to files to prevent sync loops.
#[derive(Debug)]
pub struct WriteTracker {
    writes: HashMap<CanonicalPath, WriteInfo>,
    window: Duration,
    /// Tracks files that OrgAdapter is currently processing external changes for.
    /// OrgFileWriter should not render these files until processing is complete.
    external_processing: HashMap<CanonicalPath, Instant>,
    /// Window for external processing (longer than write window to allow for block creation)
    external_window: Duration,
}

impl Default for WriteTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteTracker {
    pub fn new() -> Self {
        Self {
            writes: HashMap::new(),
            // Write window must be longer than FileWatcher detection time (up to 1000ms on macOS)
            // plus OrgFileWriter debounce (500ms). Using 2000ms for safety margin.
            window: Duration::from_millis(2000),
            external_processing: HashMap::new(),
            // External processing window must be longer than write window + event propagation.
            // This prevents OrgFileWriter from rendering while OrgAdapter is processing.
            external_window: Duration::from_millis(3000),
        }
    }

    /// Record a write with a pre-computed hash of the content we're about to write.
    /// Call this BEFORE writing so the file watcher can't race between write and mark.
    pub fn mark_write_with_hash(&mut self, path: &Path, hash: String) {
        let canonical = CanonicalPath::new(path);
        eprintln!(
            "[WriteTracker] Marking write: {:?} -> {} (hash={}...)",
            path,
            canonical,
            &hash[..8.min(hash.len())]
        );
        self.writes.insert(
            canonical,
            WriteInfo {
                hash,
                time: Instant::now(),
            },
        );
    }

    /// Record that we wrote to a file by reading back its content hash.
    /// Prefer `mark_write_with_hash` when you know the content ahead of time.
    pub fn mark_write(&mut self, path: &Path) {
        let canonical = CanonicalPath::new(path);
        if let Ok(hash) = hash_file(path) {
            self.mark_write_with_hash(path, hash);
        } else {
            eprintln!(
                "[WriteTracker] Marking write: {:?} -> {} (failed to hash)",
                path, canonical
            );
        }
    }

    /// Check if a file change is from our own write.
    /// Returns true only if:
    /// 1. We wrote to this file recently (within write window), AND
    /// 2. The current file content hash matches what we wrote
    pub fn is_our_write(&self, path: &Path) -> bool {
        let canonical = CanonicalPath::new(path);
        let result = if let Some(write_info) = self.writes.get(&canonical) {
            let elapsed = write_info.time.elapsed();
            if elapsed >= self.window {
                eprintln!(
                    "[WriteTracker] Checking {:?} -> {}: write expired ({:?} >= {:?})",
                    path, canonical, elapsed, self.window
                );
                return false;
            }

            // Check if current file content matches what we wrote
            let current_hash = hash_file(path).ok();
            let hash_matches = current_hash.as_ref() == Some(&write_info.hash);
            eprintln!(
                "[WriteTracker] Checking {:?} -> {}: elapsed={:?}, hash_matches={}",
                path, canonical, elapsed, hash_matches
            );
            hash_matches
        } else {
            eprintln!(
                "[WriteTracker] Checking {:?} -> {}: not found in writes",
                path, canonical
            );
            false
        };
        result
    }

    /// Clean up old entries.
    pub fn cleanup(&mut self) {
        let now = Instant::now();
        self.writes
            .retain(|_, info| now.duration_since(info.time) < self.window);
        self.external_processing
            .retain(|_, time| now.duration_since(*time) < self.external_window);
    }

    /// Mark that OrgAdapter is starting to process external changes for a file.
    /// OrgFileWriter should not render this file until the window expires.
    pub fn mark_external_processing(&mut self, path: &Path) {
        let canonical = CanonicalPath::new(path);
        eprintln!(
            "[WriteTracker] Marking external processing: {:?} -> {}",
            path, canonical
        );
        self.external_processing.insert(canonical, Instant::now());
    }

    /// Check if OrgFileWriter should skip rendering for this file.
    /// Returns true if OrgAdapter is currently processing external changes for this file.
    pub fn should_skip_render(&self, path: &Path) -> bool {
        let canonical = CanonicalPath::new(path);
        if let Some(start_time) = self.external_processing.get(&canonical) {
            let elapsed = start_time.elapsed();
            let should_skip = elapsed < self.external_window;
            eprintln!(
                "[WriteTracker] should_skip_render {:?} -> {}: elapsed={:?}, skip={}",
                path, canonical, elapsed, should_skip
            );
            should_skip
        } else {
            false
        }
    }

    /// Clear the external processing flag for a file.
    /// Called when OrgAdapter finishes processing.
    pub fn clear_external_processing(&mut self, path: &Path) {
        let canonical = CanonicalPath::new(path);
        eprintln!(
            "[WriteTracker] Clearing external processing: {:?} -> {}",
            path, canonical
        );
        self.external_processing.remove(&canonical);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_is_our_write_matches_hash() {
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("hello world");
        let hash = hash_file(file.path()).unwrap();

        tracker.mark_write_with_hash(file.path(), hash);
        assert!(tracker.is_our_write(file.path()));
    }

    #[test]
    fn test_is_our_write_rejects_modified_file() {
        let mut tracker = WriteTracker::new();
        let mut file = write_temp_file("original content");
        let hash = hash_file(file.path()).unwrap();

        tracker.mark_write_with_hash(file.path(), hash);

        // Externally modify the file
        file.write_all(b" modified").unwrap();
        file.flush().unwrap();

        assert!(!tracker.is_our_write(file.path()));
    }

    #[test]
    fn test_is_our_write_unknown_path() {
        let tracker = WriteTracker::new();
        let file = write_temp_file("anything");
        assert!(!tracker.is_our_write(file.path()));
    }

    #[test]
    fn test_mark_write_reads_current_hash() {
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("content for mark_write");

        tracker.mark_write(file.path());
        assert!(tracker.is_our_write(file.path()));
    }

    #[test]
    fn test_external_processing_blocks_render() {
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("org content");

        assert!(!tracker.should_skip_render(file.path()));

        tracker.mark_external_processing(file.path());
        assert!(tracker.should_skip_render(file.path()));
    }

    #[test]
    fn test_clear_external_processing_unblocks_render() {
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("org content");

        tracker.mark_external_processing(file.path());
        assert!(tracker.should_skip_render(file.path()));

        tracker.clear_external_processing(file.path());
        assert!(!tracker.should_skip_render(file.path()));
    }

    #[test]
    fn test_cleanup_removes_expired_writes() {
        let mut tracker = WriteTracker {
            writes: HashMap::new(),
            window: Duration::from_millis(0), // Expire immediately
            external_processing: HashMap::new(),
            external_window: Duration::from_millis(0),
        };

        let file = write_temp_file("content");
        tracker.mark_write(file.path());

        std::thread::sleep(Duration::from_millis(1));
        tracker.cleanup();

        assert!(!tracker.is_our_write(file.path()));
    }

    #[test]
    fn test_write_then_external_processing_prevents_loop() {
        // Simulates the full loop prevention scenario:
        // 1. OrgFileWriter marks its write hash
        // 2. FileWatcher fires → OrgAdapter checks is_our_write → true → skips
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("* Headline\n");
        let hash = hash_file(file.path()).unwrap();

        // OrgFileWriter writes and marks hash
        tracker.mark_write_with_hash(file.path(), hash);

        // FileWatcher fires → OrgAdapter checks
        assert!(
            tracker.is_our_write(file.path()),
            "OrgAdapter must skip re-processing our own write"
        );
    }

    #[test]
    fn test_external_edit_triggers_reprocessing() {
        // Simulates: external edit should NOT be suppressed
        let mut tracker = WriteTracker::new();
        let mut file = write_temp_file("* Original\n");
        let hash = hash_file(file.path()).unwrap();

        tracker.mark_write_with_hash(file.path(), hash);

        // External editor modifies the file
        file.write_all(b"* Changed externally\n").unwrap();
        file.flush().unwrap();

        assert!(
            !tracker.is_our_write(file.path()),
            "External edits must trigger reprocessing"
        );
    }

    #[test]
    fn test_orgfilewriter_skips_during_orgadapter_processing() {
        // Simulates: OrgAdapter marks external processing → OrgFileWriter must wait
        let mut tracker = WriteTracker::new();
        let file = write_temp_file("* Content\n");

        // OrgAdapter starts processing this file from external change
        tracker.mark_external_processing(file.path());

        // OrgFileWriter debounce fires → should skip
        assert!(
            tracker.should_skip_render(file.path()),
            "OrgFileWriter must skip while OrgAdapter processes"
        );

        // OrgAdapter finishes
        tracker.clear_external_processing(file.path());

        // OrgFileWriter can now render
        assert!(
            !tracker.should_skip_render(file.path()),
            "OrgFileWriter should render after OrgAdapter finishes"
        );
    }
}

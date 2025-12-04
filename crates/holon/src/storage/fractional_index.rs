use anyhow::{Context, Result};
use loro_fractional_index::FractionalIndex;

/// Maximum length for sort_key before triggering rebalancing
pub const MAX_SORT_KEY_LENGTH: usize = 32;

/// Generate a fractional index between two optional keys
///
/// # Arguments
/// * `prev_key` - The sort_key of the predecessor block (None if inserting at beginning)
/// * `next_key` - The sort_key of the successor block (None if inserting at end)
///
/// # Returns
/// A new sort_key that sorts between prev_key and next_key
pub fn gen_key_between(prev_key: Option<&str>, next_key: Option<&str>) -> Result<String> {
    let prev_index = prev_key
        .map(FractionalIndex::from_hex_string)
        .map(|idx| idx);

    let next_index = next_key
        .map(FractionalIndex::from_hex_string)
        .map(|idx| idx);

    let new_index = FractionalIndex::new(prev_index.as_ref(), next_index.as_ref())
        .context("Failed to generate fractional index between given keys")?;

    Ok(new_index.to_string())
}

/// Generate N evenly-spaced fractional index keys
///
/// Used for rebalancing siblings to create uniform spacing.
///
/// # Arguments
/// * `count` - Total number of keys to generate
///
/// # Returns
/// A vector of evenly-spaced sort_keys
pub fn gen_n_keys(count: usize) -> Result<Vec<String>> {
    let indices = FractionalIndex::generate_n_evenly(None, None, count)
        .context("Failed to generate evenly-spaced fractional indices")?;

    Ok(indices.into_iter().map(|idx| idx.to_string()).collect())
}

/// Generate a fractional index that comes after the given key
///
/// This function generates a key that sorts immediately after the given key.
/// Conceptually uses the same approach as `FractionalIndex::new_after()` (which
/// is pub(crate) and not directly accessible), but achieves the same result
/// by using `gen_key_between` with no upper bound.
///
/// # Arguments
/// * `prev_key` - The sort_key to come after (hex string format)
///
/// # Returns
/// A new sort_key that sorts after prev_key
pub fn gen_key_after(prev_key: &str) -> Result<String> {
    // Generate a key after prev_key with no upper bound
    // This is equivalent to new_after(prev_key_bytes)
    gen_key_between(Some(prev_key), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_key_between_empty() {
        let key = gen_key_between(None, None).unwrap();
        assert!(!key.is_empty());
    }

    #[test]
    fn test_gen_key_between_at_beginning() {
        let next = gen_key_between(None, None).unwrap();
        let new_key = gen_key_between(None, Some(&next)).unwrap();

        assert!(new_key < next);
    }

    #[test]
    fn test_gen_key_between_at_end() {
        let prev = gen_key_between(None, None).unwrap();
        let new_key = gen_key_between(Some(&prev), None).unwrap();

        assert!(new_key > prev);
    }

    #[test]
    fn test_gen_key_between_middle() {
        let first = gen_key_between(None, None).unwrap();
        let third = gen_key_between(Some(&first), None).unwrap();
        let second = gen_key_between(Some(&first), Some(&third)).unwrap();

        assert!(first < second);
        assert!(second < third);
    }

    #[test]
    fn test_gen_key_ordering() {
        let mut keys = Vec::new();
        let mut prev: Option<String> = None;

        // Generate 10 keys in sequence
        for _ in 0..10 {
            let key = gen_key_between(prev.as_deref(), None).unwrap();
            keys.push(key.clone());
            prev = Some(key);
        }

        // Verify they sort correctly
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn test_gen_n_keys() {
        let keys = gen_n_keys(5).unwrap();
        assert_eq!(keys.len(), 5);

        // Verify ordering
        for i in 0..keys.len() - 1 {
            assert!(keys[i] < keys[i + 1]);
        }
    }

    #[test]
    fn test_gen_n_keys_empty() {
        let keys = gen_n_keys(0).unwrap();
        assert_eq!(keys.len(), 0);
    }

    #[test]
    fn test_gen_n_keys_single() {
        let keys = gen_n_keys(1).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn test_deep_nesting() {
        // Test that keys remain reasonable even with deep nesting
        let mut keys = vec![gen_key_between(None, None).unwrap()];

        // Create a series of increasingly nested insertions
        for _ in 0..50 {
            let last = keys.last().unwrap();
            let new_key = gen_key_between(Some(last), None).unwrap();
            keys.push(new_key);
        }

        // Verify all keys are in order
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);

        // Check that keys remain reasonably sized (less than max length)
        let max_len = keys.iter().map(|k| k.len()).max().unwrap();
        println!("Max key length after 50 insertions: {}", max_len);
        assert!(max_len < MAX_SORT_KEY_LENGTH);
    }

    #[test]
    fn test_rebalancing() {
        // Test that rebalancing generates evenly-spaced keys
        let count = 10;
        let keys = gen_n_keys(count).unwrap();

        assert_eq!(keys.len(), count);

        // Verify all keys are in order
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);

        // Verify all keys are unique
        let mut unique = keys.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), keys.len());

        // Verify keys remain within reasonable length
        let max_len = keys.iter().map(|k| k.len()).max().unwrap();
        println!("Max key length for {} keys: {}", count, max_len);
        assert!(max_len < MAX_SORT_KEY_LENGTH);
    }

    #[test]
    fn test_gen_key_after() {
        // Test that gen_key_after generates a key that sorts after the input
        let prev_key = gen_key_between(None, None).unwrap();
        let after_key = gen_key_after(&prev_key).unwrap();

        assert!(after_key > prev_key);

        // Test with multiple sequential calls
        let mut keys = vec![gen_key_between(None, None).unwrap()];
        for _ in 0..5 {
            let last = keys.last().unwrap();
            let next = gen_key_after(last).unwrap();
            keys.push(next);
        }

        // Verify all keys are in order
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}

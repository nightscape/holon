//! Link parsing for org-mode content
//!
//! Extracts `[[target][text]]` style links from org-mode content.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;

/// Represents a link found in org-mode content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// Target URI (e.g., "local://uuid")
    pub target: String,
    /// Display text
    pub text: String,
    /// Start position in content (byte offset)
    pub start: usize,
    /// End position in content (byte offset)
    pub end: usize,
}

/// Regex pattern for matching org-mode links: `[[target][text]]`
static LINK_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"\[\[([^\]]+)\]\[([^\]]+)\]\]").unwrap());

/// Extract all `[[target][text]]` links from org-mode content.
///
/// # Arguments
/// * `content` - The org-mode content to search
///
/// # Returns
/// Vector of links found, in order of appearance
pub fn extract_links(content: &str) -> Vec<Link> {
    let mut links = Vec::new();

    for mat in LINK_REGEX.find_iter(content) {
        let captures = LINK_REGEX.captures(mat.as_str()).unwrap();

        let target = captures
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let text = captures
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();

        links.push(Link {
            target,
            text,
            start: mat.start(),
            end: mat.end(),
        });
    }

    links
}

/// Extract unique link targets from content.
///
/// Returns a set of all unique target URIs found in links.
pub fn extract_link_targets(content: &str) -> HashSet<String> {
    extract_links(content)
        .iter()
        .map(|link| link.target.clone())
        .collect()
}

/// Replace links in content with plain text (keeping the display text).
///
/// Useful for rendering or processing content without link markup.
pub fn strip_links(content: &str) -> String {
    let links = extract_links(content);
    let mut result = content.to_string();

    // Replace in reverse order to maintain correct positions
    for link in links.iter().rev() {
        result.replace_range(link.start..link.end, &link.text);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_link() {
        let content = "This is a [[local://uuid-123][link to block]] in text.";
        let links = extract_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "local://uuid-123");
        assert_eq!(links[0].text, "link to block");
    }

    #[test]
    fn test_extract_multiple_links() {
        let content = "First [[local://1][one]] and second [[local://2][two]].";
        let links = extract_links(content);

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].target, "local://1");
        assert_eq!(links[1].target, "local://2");
    }

    #[test]
    fn test_extract_link_targets() {
        let content = "[[local://a][A]] and [[local://b][B]] and [[local://a][A again]].";
        let targets = extract_link_targets(content);

        assert_eq!(targets.len(), 2);
        assert!(targets.contains("local://a"));
        assert!(targets.contains("local://b"));
    }

    #[test]
    fn test_strip_links() {
        let content = "See [[local://123][this block]] for details.";
        let stripped = strip_links(content);

        assert_eq!(stripped, "See this block for details.");
    }
}

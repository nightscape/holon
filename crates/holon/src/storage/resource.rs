//! Resource types for dependency tracking in the operation scheduler.
//!
//! Resources represent named entities that operations can provide or require.
//! The scheduler uses these to build a dependency graph and execute operations
//! in topological order.

use std::fmt;

/// A resource that can be provided or required by a database operation.
///
/// Resources enable dependency tracking between operations. For example,
/// a CREATE TABLE operation provides `Schema("users")`, and an INSERT
/// operation requires `Schema("users")`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    /// A database schema object (table, view, index).
    ///
    /// Examples:
    /// - `Schema("blocks")` - The blocks table
    /// - `Schema("watch_view_abc123")` - A materialized view
    Schema(String),

    /// An abstract capability or signal.
    ///
    /// Used for coordination that isn't tied to a specific schema object.
    ///
    /// Examples:
    /// - `Capability("ddl_complete")` - All startup DDL is done
    /// - `Capability("module_todoist_ready")` - Todoist module initialized
    Capability(String),
}

impl Resource {
    /// Create a Schema resource for a table or view.
    pub fn schema(name: impl Into<String>) -> Self {
        Self::Schema(name.into())
    }

    /// Create a Capability resource for an abstract signal.
    pub fn capability(name: impl Into<String>) -> Self {
        Self::Capability(name.into())
    }

    /// Check if this is a schema resource.
    pub fn is_schema(&self) -> bool {
        matches!(self, Self::Schema(_))
    }

    /// Check if this is a capability resource.
    pub fn is_capability(&self) -> bool {
        matches!(self, Self::Capability(_))
    }

    /// Get the name of this resource.
    pub fn name(&self) -> &str {
        match self {
            Self::Schema(name) | Self::Capability(name) => name,
        }
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(name) => write!(f, "Schema({})", name),
            Self::Capability(name) => write!(f, "Capability({})", name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_resource() {
        let r = Resource::schema("blocks");
        assert!(r.is_schema());
        assert!(!r.is_capability());
        assert_eq!(r.name(), "blocks");
        assert_eq!(format!("{}", r), "Schema(blocks)");
    }

    #[test]
    fn test_capability_resource() {
        let r = Resource::capability("ddl_complete");
        assert!(r.is_capability());
        assert!(!r.is_schema());
        assert_eq!(r.name(), "ddl_complete");
        assert_eq!(format!("{}", r), "Capability(ddl_complete)");
    }

    #[test]
    fn test_equality() {
        assert_eq!(Resource::schema("x"), Resource::schema("x"));
        assert_ne!(Resource::schema("x"), Resource::schema("y"));
        assert_ne!(Resource::schema("x"), Resource::capability("x"));
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Resource::schema("a"));
        set.insert(Resource::schema("b"));
        set.insert(Resource::capability("a"));

        assert!(set.contains(&Resource::schema("a")));
        assert!(set.contains(&Resource::capability("a")));
        assert!(!set.contains(&Resource::schema("c")));
    }
}

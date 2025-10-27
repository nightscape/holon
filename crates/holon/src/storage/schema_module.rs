//! Schema module system for declarative database object lifecycle management.
//!
//! This module provides a trait-based system for managing database schema objects
//! (tables, views, materialized views) with automatic dependency ordering.
//!
//! ## Problem
//!
//! Database objects have dependencies: materialized views depend on tables,
//! views depend on other views, etc. Creating them in the wrong order causes
//! failures. Previously, ordering was managed manually in `di/mod.rs` with
//! scattered `mark_available()` calls.
//!
//! ## Solution
//!
//! Each schema component implements `SchemaModule`, declaring:
//! - `provides()`: Resources this module creates (tables, views)
//! - `requires()`: Resources this module depends on
//! - `ensure_schema()`: The actual DDL execution
//!
//! The `SchemaRegistry` collects all modules, builds a dependency DAG,
//! and executes them in topological order. After each module completes,
//! its provided resources are automatically registered with the `DbHandle`.
//!
//! ## Example
//!
//! ```rust,ignore
//! struct BlockHierarchySchema;
//!
//! impl SchemaModule for BlockHierarchySchema {
//!     fn name(&self) -> &str { "block_hierarchy" }
//!
//!     fn provides(&self) -> Vec<Resource> {
//!         vec![Resource::schema("blocks_with_paths")]
//!     }
//!
//!     fn requires(&self) -> Vec<Resource> {
//!         vec![Resource::schema("blocks")]
//!     }
//!
//!     async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
//!         backend.execute_ddl(BLOCK_HIERARCHY_SQL).await
//!     }
//! }
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::resource::Resource;
use super::turso::{DbHandle, TursoBackend};
use super::types::Result;

/// A module that manages a set of database schema objects.
///
/// Implement this trait for each logical group of database objects that
/// should be created together. The registry will call modules in dependency
/// order based on their `provides()` and `requires()` declarations.
#[async_trait]
pub trait SchemaModule: Send + Sync {
    /// Unique name for this module (used in logging and error messages).
    fn name(&self) -> &str;

    /// Resources this module creates (tables, views, materialized views).
    ///
    /// These resources will be automatically registered with the `DbHandle`
    /// after `ensure_schema()` completes successfully.
    fn provides(&self) -> Vec<Resource>;

    /// Resources this module depends on.
    ///
    /// The registry ensures all required resources are available before
    /// calling `ensure_schema()`.
    fn requires(&self) -> Vec<Resource>;

    /// Execute DDL to create/update the schema objects.
    ///
    /// This method should be idempotent (safe to call multiple times).
    /// Use `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, etc.
    ///
    /// The backend is provided with write access for DDL operations.
    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()>;

    /// Optional post-schema initialization (e.g., inserting default data).
    ///
    /// Called after `ensure_schema()` succeeds but before resources are
    /// marked as available. Override if you need to insert seed data.
    async fn initialize_data(&self, _backend: &TursoBackend) -> Result<()> {
        Ok(())
    }
}

/// Error type for schema registry operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaRegistryError {
    #[error("Dependency cycle detected: {0}")]
    CycleDetected(String),

    #[error(
        "Missing dependency: module '{module}' requires '{resource}' but no module provides it"
    )]
    MissingDependency { module: String, resource: String },

    #[error("Schema initialization failed for module '{module}': {error}")]
    InitializationFailed { module: String, error: String },
}

/// Registry that collects schema modules and initializes them in dependency order.
///
/// ## Usage
///
/// ```rust,ignore
/// let mut registry = SchemaRegistry::new();
/// registry.register(Arc::new(CoreSchemaModule));
/// registry.register(Arc::new(BlockHierarchySchemaModule));
/// registry.register(Arc::new(NavigationSchemaModule));
///
/// // Initialize all schemas in dependency order
/// registry.initialize_all(backend, &db_handle, vec![]).await?;
/// ```
pub struct SchemaRegistry {
    modules: Vec<Arc<dyn SchemaModule>>,
}

impl SchemaRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            modules: Vec::new(),
        }
    }

    /// Register a schema module.
    ///
    /// Modules can be registered in any order; the registry will determine
    /// the correct initialization order based on dependencies.
    pub fn register(&mut self, module: Arc<dyn SchemaModule>) {
        tracing::debug!(
            "[SchemaRegistry] Registering module '{}' (provides: {:?}, requires: {:?})",
            module.name(),
            module
                .provides()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>(),
            module
                .requires()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>()
        );
        self.modules.push(module);
    }

    /// Get the number of registered modules.
    pub fn len(&self) -> usize {
        self.modules.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    /// Compute topological order for module initialization.
    ///
    /// Returns modules in an order where each module's dependencies are
    /// satisfied by modules earlier in the list.
    fn topological_sort(
        &self,
    ) -> std::result::Result<Vec<Arc<dyn SchemaModule>>, SchemaRegistryError> {
        // Build resource -> module index mapping
        let mut provider_map: HashMap<Resource, usize> = HashMap::new();
        for (idx, module) in self.modules.iter().enumerate() {
            for resource in module.provides() {
                provider_map.insert(resource, idx);
            }
        }

        // Build adjacency list (module -> modules it depends on)
        let mut in_degree: Vec<usize> = vec![0; self.modules.len()];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); self.modules.len()];

        for (idx, module) in self.modules.iter().enumerate() {
            for required in module.requires() {
                if let Some(&provider_idx) = provider_map.get(&required) {
                    if provider_idx != idx {
                        // idx depends on provider_idx
                        dependents[provider_idx].push(idx);
                        in_degree[idx] += 1;
                    }
                }
                // Note: We don't error on missing dependencies here because
                // some resources might be provided externally (e.g., already existing tables)
            }
        }

        // Kahn's algorithm for topological sort
        let mut queue: VecDeque<usize> = VecDeque::new();
        for (idx, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                queue.push_back(idx);
            }
        }

        let mut result: Vec<Arc<dyn SchemaModule>> = Vec::new();
        let mut visited = 0;

        while let Some(idx) = queue.pop_front() {
            result.push(self.modules[idx].clone());
            visited += 1;

            for &dependent_idx in &dependents[idx] {
                in_degree[dependent_idx] -= 1;
                if in_degree[dependent_idx] == 0 {
                    queue.push_back(dependent_idx);
                }
            }
        }

        if visited != self.modules.len() {
            // Cycle detected - find modules involved
            let cycle_modules: Vec<String> = self
                .modules
                .iter()
                .enumerate()
                .filter(|(idx, _)| in_degree[*idx] > 0)
                .map(|(_, m)| m.name().to_string())
                .collect();
            return Err(SchemaRegistryError::CycleDetected(
                cycle_modules.join(" -> "),
            ));
        }

        Ok(result)
    }

    /// Initialize all registered schema modules in dependency order.
    ///
    /// For each module:
    /// 1. Verify all required resources are available
    /// 2. Call `ensure_schema()` to create database objects
    /// 3. Call `initialize_data()` for any seed data
    /// 4. Register provided resources with the DbHandle
    ///
    /// # Arguments
    /// * `backend` - Database backend for executing DDL
    /// * `db_handle` - Database handle for resource registration
    /// * `pre_available` - Resources that are already available (e.g., from external sources)
    pub async fn initialize_all(
        &self,
        backend: Arc<RwLock<TursoBackend>>,
        db_handle: &DbHandle,
        pre_available: Vec<Resource>,
    ) -> std::result::Result<(), SchemaRegistryError> {
        tracing::info!(
            "[SchemaRegistry] Initializing {} schema modules",
            self.modules.len()
        );

        // Track available resources
        let mut available: HashSet<Resource> = pre_available.into_iter().collect();

        // Get modules in topological order
        let ordered_modules = self.topological_sort()?;

        for module in ordered_modules {
            let module_name = module.name();
            tracing::info!("[SchemaRegistry] Initializing module '{}'", module_name);

            // Verify dependencies are satisfied
            for required in module.requires() {
                if !available.contains(&required) {
                    // Check if DbHandle already has this resource
                    let exists = db_handle.resource_exists(&required).await.unwrap_or(false);

                    if !exists {
                        return Err(SchemaRegistryError::MissingDependency {
                            module: module_name.to_string(),
                            resource: required.name().to_string(),
                        });
                    }
                    // Resource exists in DbHandle, add to our tracking
                    available.insert(required);
                }
            }

            // Execute schema DDL
            {
                let backend_guard = backend.read().await;
                module.ensure_schema(&backend_guard).await.map_err(|e| {
                    SchemaRegistryError::InitializationFailed {
                        module: module_name.to_string(),
                        error: e.to_string(),
                    }
                })?;

                // Initialize any seed data
                module.initialize_data(&backend_guard).await.map_err(|e| {
                    SchemaRegistryError::InitializationFailed {
                        module: module_name.to_string(),
                        error: format!("data initialization failed: {}", e),
                    }
                })?;
            }

            // Register provided resources
            let provides = module.provides();
            if !provides.is_empty() {
                tracing::debug!(
                    "[SchemaRegistry] Module '{}' completed, marking {} resources as available",
                    module_name,
                    provides.len()
                );

                db_handle
                    .mark_available(provides.clone())
                    .await
                    .map_err(|e| SchemaRegistryError::InitializationFailed {
                        module: module_name.to_string(),
                        error: format!("failed to mark resources available: {}", e),
                    })?;

                // Update local tracking
                for resource in provides {
                    available.insert(resource);
                }
            }

            tracing::info!(
                "[SchemaRegistry] Module '{}' initialized successfully",
                module_name
            );
        }

        tracing::info!(
            "[SchemaRegistry] All {} modules initialized successfully",
            self.modules.len()
        );
        Ok(())
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test module that just tracks calls
    struct TestModule {
        name: String,
        provides: Vec<Resource>,
        requires: Vec<Resource>,
    }

    impl TestModule {
        fn new(name: &str, provides: Vec<&str>, requires: Vec<&str>) -> Self {
            Self {
                name: name.to_string(),
                provides: provides.into_iter().map(Resource::schema).collect(),
                requires: requires.into_iter().map(Resource::schema).collect(),
            }
        }
    }

    #[async_trait]
    impl SchemaModule for TestModule {
        fn name(&self) -> &str {
            &self.name
        }

        fn provides(&self) -> Vec<Resource> {
            self.provides.clone()
        }

        fn requires(&self) -> Vec<Resource> {
            self.requires.clone()
        }

        async fn ensure_schema(&self, _backend: &TursoBackend) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_topological_sort_linear() {
        let mut registry = SchemaRegistry::new();

        // C depends on B, B depends on A
        registry.register(Arc::new(TestModule::new("module_c", vec!["c"], vec!["b"])));
        registry.register(Arc::new(TestModule::new("module_a", vec!["a"], vec![])));
        registry.register(Arc::new(TestModule::new("module_b", vec!["b"], vec!["a"])));

        let sorted = registry.topological_sort().unwrap();
        let names: Vec<&str> = sorted.iter().map(|m| m.name()).collect();

        // A must come before B, B must come before C
        let pos_a = names.iter().position(|&n| n == "module_a").unwrap();
        let pos_b = names.iter().position(|&n| n == "module_b").unwrap();
        let pos_c = names.iter().position(|&n| n == "module_c").unwrap();

        assert!(pos_a < pos_b, "A must come before B");
        assert!(pos_b < pos_c, "B must come before C");
    }

    #[test]
    fn test_topological_sort_diamond() {
        let mut registry = SchemaRegistry::new();

        // Diamond: D depends on B and C, both depend on A
        registry.register(Arc::new(TestModule::new(
            "module_d",
            vec!["d"],
            vec!["b", "c"],
        )));
        registry.register(Arc::new(TestModule::new("module_b", vec!["b"], vec!["a"])));
        registry.register(Arc::new(TestModule::new("module_c", vec!["c"], vec!["a"])));
        registry.register(Arc::new(TestModule::new("module_a", vec!["a"], vec![])));

        let sorted = registry.topological_sort().unwrap();
        let names: Vec<&str> = sorted.iter().map(|m| m.name()).collect();

        let pos_a = names.iter().position(|&n| n == "module_a").unwrap();
        let pos_b = names.iter().position(|&n| n == "module_b").unwrap();
        let pos_c = names.iter().position(|&n| n == "module_c").unwrap();
        let pos_d = names.iter().position(|&n| n == "module_d").unwrap();

        assert!(pos_a < pos_b, "A must come before B");
        assert!(pos_a < pos_c, "A must come before C");
        assert!(pos_b < pos_d, "B must come before D");
        assert!(pos_c < pos_d, "C must come before D");
    }

    #[test]
    fn test_topological_sort_cycle_detection() {
        let mut registry = SchemaRegistry::new();

        // Cycle: A -> B -> C -> A
        registry.register(Arc::new(TestModule::new("module_a", vec!["a"], vec!["c"])));
        registry.register(Arc::new(TestModule::new("module_b", vec!["b"], vec!["a"])));
        registry.register(Arc::new(TestModule::new("module_c", vec!["c"], vec!["b"])));

        let result = registry.topological_sort();
        assert!(matches!(result, Err(SchemaRegistryError::CycleDetected(_))));
    }

    #[test]
    fn test_topological_sort_independent() {
        let mut registry = SchemaRegistry::new();

        // Independent modules with no dependencies
        registry.register(Arc::new(TestModule::new("module_a", vec!["a"], vec![])));
        registry.register(Arc::new(TestModule::new("module_b", vec!["b"], vec![])));
        registry.register(Arc::new(TestModule::new("module_c", vec!["c"], vec![])));

        let sorted = registry.topological_sort().unwrap();
        assert_eq!(sorted.len(), 3);
    }

    #[test]
    fn test_multiple_provides() {
        let mut registry = SchemaRegistry::new();

        // Module that provides multiple resources
        registry.register(Arc::new(TestModule::new(
            "core",
            vec!["blocks", "documents", "directories"],
            vec![],
        )));
        registry.register(Arc::new(TestModule::new(
            "hierarchy",
            vec!["blocks_with_paths"],
            vec!["blocks"],
        )));

        let sorted = registry.topological_sort().unwrap();
        let names: Vec<&str> = sorted.iter().map(|m| m.name()).collect();

        let pos_core = names.iter().position(|&n| n == "core").unwrap();
        let pos_hierarchy = names.iter().position(|&n| n == "hierarchy").unwrap();

        assert!(pos_core < pos_hierarchy);
    }
}

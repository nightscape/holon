//! Todoist integration for holon
//!
//! This crate provides Todoist-specific implementations:
//!
//! ## Stream-Based DataSource Implementation
//! - `client` - TodoistClient (HTTP client)
//! - `provider` - TodoistProvider (underlying API provider)
//! - `todoist_sync_provider` - Stream-based TodoistSyncProvider with builder pattern
//! - `datasource` - TodoistTaskDataSource and TodoistProjectDataSource for DataSource trait
//! - `todoist_datasource` - Stream-based TodoistTaskDataSource
//! - `fake` - TodoistTaskFake for optimistic updates
//! - `models` - API models
//! - `converters` - Type converters

pub mod client;
pub mod converters;
pub mod datasource;
pub mod di;
#[cfg(not(target_arch = "wasm32"))]
pub mod fake;
pub mod models;
pub mod queries;
pub mod todoist_datasource;
pub mod todoist_sync_provider;

// OperationProvider wrappers for generic testing
#[cfg(not(target_arch = "wasm32"))]
pub mod fake_wrapper;
pub mod provider_wrapper;

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod integration_test;

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod pbt_test;

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod stream_integration_test;

#[cfg(test)]
mod operations_demo;

pub use client::TodoistClient;
pub use converters::*;
pub use di::{TodoistConfig, TodoistModule};
#[cfg(not(target_arch = "wasm32"))]
pub use fake::*;
#[cfg(not(target_arch = "wasm32"))]
pub use fake_wrapper::TodoistFakeOperationProvider;
pub use models::*;
pub use provider_wrapper::TodoistOperationProvider;
pub use todoist_sync_provider::TodoistSyncProvider;

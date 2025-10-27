//! Todoist integration for holon
//!
//! This crate provides Todoist-specific implementations:
//!
//! ## Stream-Based DataSource Implementation
//! - `api_client` - TodoistApiClient trait for abstraction over real/fake clients
//! - `client` - TodoistClient (HTTP client)
//! - `provider` - TodoistProvider (underlying API provider)
//! - `todoist_sync_provider` - Stream-based TodoistSyncProvider with builder pattern
//! - `datasource` - TodoistTaskDataSource and TodoistProjectDataSource for DataSource trait
//! - `todoist_datasource` - Stream-based TodoistTaskDataSource
//! - `fake` - TodoistTaskFake for optimistic updates
//! - `fake_client` - TodoistFakeClient for testing (in-memory API client)
//! - `models` - API models
//! - `converters` - Type converters

pub mod api_client;
pub mod client;
pub mod converters;
pub mod datasource;
pub mod di;
mod dispatch_helper;
#[cfg(not(target_arch = "wasm32"))]
pub mod fake;
#[cfg(not(target_arch = "wasm32"))]
pub mod fake_client;
pub mod models;
pub mod queries;
pub mod todoist_datasource;
pub mod todoist_event_adapter;
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
#[cfg(feature = "integration-tests")]
mod inverse_operation_test;

#[cfg(test)]
mod operations_demo;

pub use api_client::TodoistApiClient;
pub use client::TodoistClient;
pub use converters::*;
pub use di::{TodoistConfig, TodoistModule, TodoistServiceCollectionExt};
#[cfg(not(target_arch = "wasm32"))]
pub use fake::*;
#[cfg(not(target_arch = "wasm32"))]
pub use fake_client::TodoistFakeClient;
#[cfg(not(target_arch = "wasm32"))]
pub use fake_wrapper::TodoistFakeOperationProvider;
pub use models::*;
pub use provider_wrapper::TodoistOperationProvider;
pub use todoist_sync_provider::TodoistSyncProvider;

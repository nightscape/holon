//! Navigation module for backend-driven navigation state
//!
//! This module provides navigation operations that work through Turso's IVM (Incremental View Maintenance).
//! Navigation state is stored in database tables, and queries JOIN against `current_focus` view
//! to automatically update UI when navigation changes.
//!
//! ## Key concepts:
//! - `navigation_history`: Stores navigation history for back/forward
//! - `navigation_cursor`: Points to current position in history per region
//! - `current_focus`: View that JOINs cursor to history for easy querying
//!
//! ## Operations:
//! - `focus(region, block_id)`: Navigate to view a block and its children
//! - `go_back(region)`: Navigate to previous view in history
//! - `go_forward(region)`: Navigate to next view in history
//! - `go_home(region)`: Return to root view (clear focus)
//!
//! ## Schema Management
//!
//! Navigation schema is managed by `NavigationSchemaModule` via the `SchemaRegistry`.
//! See `storage/schema_modules.rs` for the schema definition.

mod provider;

pub use provider::NavigationProvider;

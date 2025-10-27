//! Command Log trait and types for persistent undo/redo
//!
//! The CommandLog stores user intent (commands) with their inverse operations
//! for undo/redo functionality.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::storage::types::Result;
use crate::sync::event_bus::CommandId;
use holon_api::Operation;

/// Command entry in the log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEntry {
    /// Command ID (ULID)
    pub id: CommandId,
    /// The operation that was executed
    pub operation: Operation,
    /// Inverse operation (None if irreversible)
    pub inverse: Option<Operation>,
    /// Human-readable display name
    pub display_name: String,
    /// Entity type affected
    pub entity_type: String,
    /// Entity ID affected
    pub entity_id: String,
    /// Target system (None for internal operations)
    pub target_system: Option<String>,
    /// Command status
    pub status: CommandStatus,
    /// Sync status
    pub sync_status: SyncStatus,
    /// Timestamps
    pub created_at: i64,
    pub executed_at: Option<i64>,
    pub synced_at: Option<i64>,
    pub undone_at: Option<i64>,
    /// Error details (if status = Failed)
    pub error_details: Option<String>,
    /// Undo chain
    pub undone_by_command_id: Option<CommandId>,
    pub undoes_command_id: Option<CommandId>,
}

/// Command status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandStatus {
    Pending,
    Executed,
    Undone,
    Failed,
}

impl CommandStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CommandStatus::Pending => "pending",
            CommandStatus::Executed => "executed",
            CommandStatus::Undone => "undone",
            CommandStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(CommandStatus::Pending),
            "executed" => Some(CommandStatus::Executed),
            "undone" => Some(CommandStatus::Undone),
            "failed" => Some(CommandStatus::Failed),
            _ => None,
        }
    }
}

/// Sync status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    Local,
    PendingSync,
    Synced,
    SyncFailed,
}

impl SyncStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncStatus::Local => "local",
            SyncStatus::PendingSync => "pending_sync",
            SyncStatus::Synced => "synced",
            SyncStatus::SyncFailed => "sync_failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "local" => Some(SyncStatus::Local),
            "pending_sync" => Some(SyncStatus::PendingSync),
            "synced" => Some(SyncStatus::Synced),
            "sync_failed" => Some(SyncStatus::SyncFailed),
            _ => None,
        }
    }
}

/// Command Log trait for persistent undo/redo
#[async_trait]
pub trait CommandLog: Send + Sync {
    /// Record a command with its inverse (if reversible)
    ///
    /// Returns the command ID.
    async fn record(
        &self,
        command: Operation,
        inverse: Option<Operation>,
        display_name: impl Into<String> + Send,
        entity_type: impl Into<String> + Send,
        entity_id: impl Into<String> + Send,
        target_system: Option<String>,
    ) -> Result<CommandId>;

    /// Update command status to 'executed' after successful execution
    async fn mark_executed(&self, command_id: &CommandId) -> Result<()>;

    /// Update command status to 'undone' (does NOT execute the undo - caller does that)
    ///
    /// Also records which command undid this one (for redo tracking).
    async fn mark_undone(&self, command_id: &CommandId, undone_by: &CommandId) -> Result<()>;

    /// Update command status back to 'executed' (does NOT execute redo - caller does that)
    ///
    /// Used when redoing an undone command.
    async fn mark_redone(&self, command_id: &CommandId) -> Result<()>;

    /// Get recent executed commands with inverses (for undo UI)
    ///
    /// Returns commands ordered by created_at DESC, limited to `limit`.
    async fn get_undo_stack(&self, limit: usize) -> Result<Vec<CommandEntry>>;

    /// Get recently undone commands (for redo UI)
    ///
    /// Returns commands ordered by undone_at DESC, limited to `limit`.
    async fn get_redo_stack(&self, limit: usize) -> Result<Vec<CommandEntry>>;

    /// Get command by ID
    async fn get_command(&self, command_id: &CommandId) -> Result<Option<CommandEntry>>;

    /// Update sync status
    async fn update_sync_status(
        &self,
        command_id: &CommandId,
        sync_status: SyncStatus,
    ) -> Result<()>;

    /// Mark command as failed
    async fn mark_failed(&self, command_id: &CommandId, error_details: String) -> Result<()>;
}

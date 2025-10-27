//! OperationWrapper - Decorator for automatic change propagation after operations
//!
//! This module provides a wrapper around OperationProvider that handles:
//! - Sync to external systems via SyncableProvider after operation execution
//! - Future: Cache updates via FieldDelta propagation

use async_trait::async_trait;
use std::sync::Arc;

use crate::core::datasource::{
    OperationProvider, OperationResult, Result, StreamPosition, SyncableProvider,
    generate_sync_operation,
};
use crate::storage::types::StorageEntity;
use holon_api::OperationDescriptor;

/// Wrapper that adds automatic sync after operation execution.
///
/// This decorator wraps an OperationProvider and automatically calls
/// sync_changes() on the SyncableProvider after each operation completes.
pub struct OperationWrapper<P, S> {
    inner: Arc<P>,
    sync_provider: Option<Arc<S>>,
}

impl<P, S> OperationWrapper<P, S> {
    /// Create a new OperationWrapper with an inner provider and optional sync provider.
    pub fn new(inner: Arc<P>, sync_provider: Option<Arc<S>>) -> Self {
        Self {
            inner,
            sync_provider,
        }
    }

    /// Create a wrapper without sync (passthrough mode)
    pub fn without_sync(inner: Arc<P>) -> Self {
        Self {
            inner,
            sync_provider: None,
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P, S> OperationProvider for OperationWrapper<P, S>
where
    P: OperationProvider + Send + Sync,
    S: SyncableProvider + Send + Sync,
{
    fn operations(&self) -> Vec<OperationDescriptor> {
        let mut ops = self.inner.operations();

        // Add sync operation if sync_provider is present
        if let Some(ref sync_provider) = self.sync_provider {
            ops.push(generate_sync_operation(sync_provider.provider_name()));
        }

        ops
    }

    fn find_operations(
        &self,
        entity_name: &str,
        available_args: &[String],
    ) -> Vec<OperationDescriptor> {
        self.inner.find_operations(entity_name, available_args)
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<OperationResult> {
        // Handle sync operation specially - delegates to sync_provider
        if op_name == "sync" {
            if let Some(ref sync_provider) = self.sync_provider {
                tracing::info!(
                    "[OperationWrapper] Executing sync operation for provider '{}'",
                    sync_provider.provider_name()
                );
                sync_provider.sync(StreamPosition::Beginning).await?;
                return Ok(OperationResult::irreversible(Vec::new()));
            } else {
                return Err("No sync provider configured".into());
            }
        }

        // 1. Execute operation on inner provider
        let result = self
            .inner
            .execute_operation(entity_name, op_name, params)
            .await?;

        // 2. Sync to external systems (if sync provider is available)
        // Extract FieldDeltas from the operation result and pass to sync_changes
        if let Some(ref sync_provider) = self.sync_provider {
            if let Err(e) = sync_provider.sync_changes(&result.changes).await {
                tracing::warn!(
                    "[OperationWrapper] Post-operation sync failed for {}.{}: {}",
                    entity_name,
                    op_name,
                    e
                );
            }
        }

        // 3. Return operation result (contains both changes and undo action)
        Ok(result)
    }

    fn get_last_created_id(&self) -> Option<String> {
        self.inner.get_last_created_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::datasource::{FieldDelta, StreamPosition, SyncableProvider};
    use std::collections::HashMap;

    // Mock OperationProvider for testing
    struct MockProvider;

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl OperationProvider for MockProvider {
        fn operations(&self) -> Vec<OperationDescriptor> {
            vec![]
        }

        fn find_operations(&self, _: &str, _: &[String]) -> Vec<OperationDescriptor> {
            vec![]
        }

        async fn execute_operation(
            &self,
            _entity_name: &str,
            _op_name: &str,
            _params: StorageEntity,
        ) -> Result<OperationResult> {
            Ok(OperationResult::irreversible(Vec::new()))
        }
    }

    // Mock SyncableProvider for testing
    struct MockSyncProvider;

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl SyncableProvider for MockSyncProvider {
        fn provider_name(&self) -> &str {
            "mock"
        }

        async fn sync(
            &self,
            _position: crate::core::datasource::StreamPosition,
        ) -> Result<crate::core::datasource::StreamPosition> {
            Ok(crate::core::datasource::StreamPosition::Beginning)
        }

        async fn sync_changes(&self, _changes: &[FieldDelta]) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_wrapper_passthrough() {
        let provider = Arc::new(MockProvider);
        let wrapper: OperationWrapper<MockProvider, MockSyncProvider> =
            OperationWrapper::without_sync(provider);

        let result = wrapper
            .execute_operation("test", "test_op", HashMap::new())
            .await;

        assert!(result.is_ok());
    }
}

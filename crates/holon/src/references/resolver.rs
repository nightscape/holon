use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::references::{BlockReference, ViewConfig};
use crate::storage::{Result, StorageEntity, StorageError};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResolvedBlock {
    Internal {
        block_id: String,
        content: String,
    },
    External {
        system: String,
        entity_type: String,
        entity: StorageEntity,
        related: Vec<StorageEntity>,
    },
}

#[async_trait]
pub trait ReferenceResolver: Send + Sync {
    async fn resolve(&self, reference: &BlockReference) -> Result<ResolvedBlock>;

    async fn resolve_internal(&self, block_id: &str) -> Result<ResolvedBlock>;

    async fn resolve_external(
        &self,
        system: &str,
        entity_type: &str,
        entity_id: &str,
        view: &Option<ViewConfig>,
    ) -> Result<ResolvedBlock>;
}

pub struct DefaultReferenceResolver {
    external_resolvers: std::collections::HashMap<String, Arc<dyn ExternalSystemResolver>>,
}

impl DefaultReferenceResolver {
    pub fn new() -> Self {
        Self {
            external_resolvers: std::collections::HashMap::new(),
        }
    }

    pub fn register_external_resolver(
        &mut self,
        system: String,
        resolver: Arc<dyn ExternalSystemResolver>,
    ) {
        self.external_resolvers.insert(system, resolver);
    }
}

#[async_trait]
impl ReferenceResolver for DefaultReferenceResolver {
    async fn resolve(&self, reference: &BlockReference) -> Result<ResolvedBlock> {
        match reference {
            BlockReference::Internal { block_id } => self.resolve_internal(block_id).await,
            BlockReference::External {
                system,
                entity_type,
                entity_id,
                view,
            } => {
                self.resolve_external(system, entity_type, entity_id, view)
                    .await
            }
        }
    }

    async fn resolve_internal(&self, block_id: &str) -> Result<ResolvedBlock> {
        Ok(ResolvedBlock::Internal {
            block_id: block_id.to_string(),
            content: "TODO: Implement Loro block retrieval".to_string(),
        })
    }

    async fn resolve_external(
        &self,
        system: &str,
        entity_type: &str,
        entity_id: &str,
        view: &Option<ViewConfig>,
    ) -> Result<ResolvedBlock> {
        let resolver = self
            .external_resolvers
            .get(system)
            .ok_or_else(|| StorageError::BackendError(format!("Unknown system: {}", system)))?;

        resolver.resolve(entity_type, entity_id, view).await
    }
}

impl Default for DefaultReferenceResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait ExternalSystemResolver: Send + Sync {
    async fn resolve(
        &self,
        entity_type: &str,
        entity_id: &str,
        view: &Option<ViewConfig>,
    ) -> Result<ResolvedBlock>;
}

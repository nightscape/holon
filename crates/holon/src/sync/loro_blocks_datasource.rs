//! DataSource that reads blocks from Loro and emits changes.

use async_trait::async_trait;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::api::types::Traversal;
use crate::api::{CoreOperations, LoroBackend};
use crate::core::datasource::{CrudOperations, DataSource, OperationResult, Result};
use crate::sync::LoroDocumentStore;
use holon_api::ApiError;
use holon_api::Value;
use holon_api::block::Block;
use holon_api::streaming::{Change, ChangeNotifications, ChangeOrigin, StreamPosition};

pub struct LoroBlocksDataSource {
    doc_store: Arc<RwLock<LoroDocumentStore>>,
    change_tx: tokio::sync::broadcast::Sender<Vec<Change<Block>>>,
}

impl LoroBlocksDataSource {
    pub fn new(doc_store: Arc<RwLock<LoroDocumentStore>>) -> Self {
        let (change_tx, _) = tokio::sync::broadcast::channel(100);
        Self {
            doc_store,
            change_tx,
        }
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Vec<Change<Block>>> {
        self.change_tx.subscribe()
    }

    /// Detect and broadcast changes immediately (synchronous version).
    ///
    /// This reads the current state from Loro and broadcasts any changes
    /// compared to the given previous state. Returns the new state.
    ///
    /// Use this for immediate change detection without waiting for polling.
    pub async fn detect_and_broadcast_changes(
        &self,
        last_state: &HashMap<String, Block>,
    ) -> HashMap<String, Block> {
        let store = self.doc_store.read().await;
        let mut current_state: HashMap<String, Block> = HashMap::new();

        for (_path, collab_doc) in store.iter().await {
            let backend = LoroBackend::from_document(collab_doc);

            if let Ok(blocks) = backend.get_all_blocks(Traversal::ALL_BUT_ROOT).await {
                for block in blocks {
                    current_state.insert(block.id.clone(), block);
                }
            }
        }
        drop(store);

        // Compute diff
        let mut changes = Vec::new();

        // Created and Updated
        for (id, block) in &current_state {
            match last_state.get(id) {
                None => {
                    changes.push(Change::Created {
                        data: block.clone(),
                        origin: ChangeOrigin::Local {
                            operation_id: None,
                            trace_id: None,
                        },
                    });
                }
                Some(old) if old.content != block.content || old.properties != block.properties => {
                    changes.push(Change::Updated {
                        id: id.clone(),
                        data: block.clone(),
                        origin: ChangeOrigin::Local {
                            operation_id: None,
                            trace_id: None,
                        },
                    });
                }
                _ => {}
            }
        }

        // Deleted
        for id in last_state.keys() {
            if !current_state.contains_key(id) {
                changes.push(Change::Deleted {
                    id: id.clone(),
                    origin: ChangeOrigin::Local {
                        operation_id: None,
                        trace_id: None,
                    },
                });
            }
        }

        if !changes.is_empty() {
            let _ = self.change_tx.send(changes);
        }

        current_state
    }

    /// Start polling for changes. Call once at startup.
    pub async fn start_polling(self: Arc<Self>) {
        let mut last_state: HashMap<String, Block> = HashMap::new();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                last_state = self.detect_and_broadcast_changes(&last_state).await;
            }
        });
    }
}

#[async_trait]
impl DataSource<Block> for LoroBlocksDataSource {
    async fn get_all(&self) -> Result<Vec<Block>> {
        let store = self.doc_store.read().await;
        let mut all = Vec::new();

        for (_path, collab_doc) in store.iter().await {
            let backend = LoroBackend::from_document(collab_doc);

            if let Ok(blocks) = backend.get_all_blocks(Traversal::ALL_BUT_ROOT).await {
                all.extend(blocks);
            }
        }

        Ok(all)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Block>> {
        let store = self.doc_store.read().await;

        for (_path, collab_doc) in store.iter().await {
            let backend = LoroBackend::from_document(collab_doc);

            if let Ok(block) = backend.get_block(id).await {
                return Ok(Some(block));
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl CrudOperations<Block> for LoroBlocksDataSource {
    async fn set_field(&self, _id: &str, _field: &str, _value: Value) -> Result<OperationResult> {
        Err("Use LoroBlockOperations for mutations".into())
    }

    async fn create(&self, _fields: HashMap<String, Value>) -> Result<(String, OperationResult)> {
        Err("Use LoroBlockOperations for mutations".into())
    }

    async fn delete(&self, _id: &str) -> Result<OperationResult> {
        Err("Use LoroBlockOperations for mutations".into())
    }
}

#[async_trait]
impl ChangeNotifications<Block> for LoroBlocksDataSource {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<
        Box<
            dyn tokio_stream::Stream<Item = std::result::Result<Vec<Change<Block>>, ApiError>>
                + Send,
        >,
    > {
        let rx = self.change_tx.subscribe();

        Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx)
                .filter_map(|result| result.ok().map(Ok)),
        )
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        // Return empty version for polling-based datasource
        Ok(Vec::new())
    }
}

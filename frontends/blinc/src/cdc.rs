use holon_api::streaming::Change;
use holon_frontend::RowChangeStream;
use tokio_stream::StreamExt;

use crate::state::AppState;

/// Listens to a CDC RowChangeStream and applies changes to AppState.
///
/// This runs as a spawned tokio task. Each batch of changes is applied
/// to the shared state, which the render loop polls via the dirty flag.
pub async fn cdc_listener(mut stream: RowChangeStream, state: AppState) {
    while let Some(batch) = stream.next().await {
        for row_change in &batch.inner.items {
            match &row_change.change {
                Change::Created { data, .. } => {
                    state.insert_row(data.clone());
                }
                Change::Updated { id, data, .. } => {
                    state.update_row(id, data.clone());
                }
                Change::Deleted { id, .. } => {
                    state.remove_row(id);
                }
                Change::FieldsChanged {
                    entity_id, fields, ..
                } => {
                    let patches: Vec<_> = fields
                        .iter()
                        .map(|(name, _old, new)| (name.clone(), new.clone()))
                        .collect();
                    state.patch_row(entity_id, &patches);
                }
            }
        }
    }
    tracing::info!("CDC stream ended");
}

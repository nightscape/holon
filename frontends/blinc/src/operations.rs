use std::collections::HashMap;
use std::sync::Arc;

use holon_api::Value;
use holon_frontend::FrontendSession;

/// Dispatch an operation to the backend asynchronously.
///
/// Uses a runtime handle to spawn since this is called from the Blinc render
/// thread which doesn't have a tokio context.
pub fn dispatch_operation(
    handle: &tokio::runtime::Handle,
    session: &Arc<FrontendSession>,
    entity_name: String,
    op_name: String,
    params: HashMap<String, Value>,
) {
    let session = Arc::clone(session);
    handle.spawn(async move {
        if let Err(e) = session
            .execute_operation(&entity_name, &op_name, params)
            .await
        {
            tracing::error!("Operation {entity_name}.{op_name} failed: {e}");
        }
    });
}

#[allow(dead_code)]
pub fn dispatch_undo(handle: &tokio::runtime::Handle, session: &Arc<FrontendSession>) {
    let session = Arc::clone(session);
    handle.spawn(async move {
        if let Err(e) = session.undo().await {
            tracing::error!("Undo failed: {e}");
        }
    });
}

#[allow(dead_code)]
pub fn dispatch_redo(handle: &tokio::runtime::Handle, session: &Arc<FrontendSession>) {
    let session = Arc::clone(session);
    handle.spawn(async move {
        if let Err(e) = session.redo().await {
            tracing::error!("Redo failed: {e}");
        }
    });
}

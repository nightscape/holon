//! Document entity - re-exports from holon-api.

// Re-export everything from holon-api::document
// Note: DocumentPathResolver is intentionally not re-exported (flutter_rust_bridge:ignore)
pub use holon_api::document::{
    DOCUMENT_URI_SCHEME, Document, document_uri, is_document_uri, parse_document_uri,
};

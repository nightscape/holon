//! Event Subscriber trait with origin filtering template method pattern
//!
//! The EventSubscriber trait provides a template method pattern for filtering
//! events by origin to prevent sync loops. Subscribers automatically skip events
//! from their own origin.

use async_trait::async_trait;

use crate::storage::types::Result;
use crate::sync::event_bus::Event;

/// Event Subscriber trait with origin filtering
///
/// Provides a template method pattern where `handle_event()` filters by origin
/// and delegates to `process_event()` for actual processing. This prevents
/// sync loops by automatically skipping events from the subscriber's own origin.
#[async_trait]
pub trait EventSubscriber: Send + Sync {
    /// Return the origin string for this subscriber (e.g., "loro", "org")
    fn origin(&self) -> &str;

    /// Template method: filters by origin, then delegates to process_event
    ///
    /// Automatically skips events from this subscriber's own origin to prevent
    /// sync loops. Subclasses should implement `process_event()` instead of
    /// overriding this method.
    async fn handle_event(&self, event: &Event) -> Result<()> {
        // Skip events from our own origin to prevent sync loops
        if event.origin.as_str() == self.origin() {
            return Ok(());
        }
        self.process_event(event).await
    }

    /// Process an event (implement this in concrete subscribers)
    ///
    /// This method is called by `handle_event()` after origin filtering.
    /// Implement this to handle events from other origins.
    async fn process_event(&self, event: &Event) -> Result<()>;
}

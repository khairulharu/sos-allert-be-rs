//! Topic handlers.
//!
//! The Go worker parses the entity events and logs them (`// TODO process
//! event`); only `sos.dispatched` does real work. That split is preserved: the
//! entity consumers exist so a deploy does not lag offsets, and the SOS
//! consumer owns the asynchronous fan-out.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::domain::Alert;
use crate::error::{AppError, AppResult};
use crate::model::events::{AddressEvent, ContactEvent, SosEvent, UserEvent};
use crate::usecase::sos::SosNotifier;

use super::consumer::{InboundMessage, MessageHandler};

/// `sos.dispatched` work is bounded so a hung push/SMS gateway cannot pin the
/// partition or block shutdown.
pub const SOS_CONSUME_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub enum EntityKind {
    User,
    Contact,
    Address,
}

pub struct EntityEventConsumer {
    kind: EntityKind,
}

impl EntityEventConsumer {
    pub fn new(kind: EntityKind) -> Self {
        Self { kind }
    }
}

#[async_trait]
impl MessageHandler for EntityEventConsumer {
    async fn handle(&self, message: &InboundMessage) -> AppResult<()> {
        let parsed = match self.kind {
            EntityKind::User => serde_json::from_slice::<UserEvent>(&message.payload).map(|e| e.id),
            EntityKind::Contact => {
                serde_json::from_slice::<ContactEvent>(&message.payload).map(|e| e.id)
            }
            EntityKind::Address => {
                serde_json::from_slice::<AddressEvent>(&message.payload).map(|e| e.id)
            }
        }
        .map_err(|e| AppError::BadRequest(format!("invalid event payload: {e}")))?;

        tracing::info!(
            kind = ?self.kind,
            id = %parsed,
            partition = message.partition,
            offset = message.offset,
            "received entity event"
        );
        Ok(())
    }
}

/// `sos.dispatched`: fan the notification out in the worker.
pub struct SosDispatchConsumer {
    notifier: Arc<dyn SosNotifier>,
    timeout: Duration,
}

impl SosDispatchConsumer {
    pub fn new(notifier: Arc<dyn SosNotifier>) -> Self {
        Self {
            notifier,
            timeout: SOS_CONSUME_TIMEOUT,
        }
    }
}

impl SosDispatchConsumer {
    /// Escape hatch for tests that must not wait 10 s.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl MessageHandler for SosDispatchConsumer {
    async fn handle(&self, message: &InboundMessage) -> AppResult<()> {
        let event: SosEvent = serde_json::from_slice(&message.payload)
            .map_err(|e| AppError::BadRequest(format!("invalid SOS event: {e}")))?;

        let alert = Alert {
            id: event.alert_id.clone(),
            sender_id: event.sender_id.clone(),
            longitude: event.longitude,
            latitude: event.latitude,
            triggered_at: event.triggered_at,
            status: event.status.clone(),
            emergency_type: event.emergency_type.clone(),
            severity: (event.severity != 0.0).then_some(event.severity),
            summary: (!event.summary.is_empty()).then(|| event.summary.clone()),
            accuracy_meters: None,
            battery_percentage: None,
            cancellation_reason: None,
            resolved_at: None,
            cancelled_at: None,
            dispatch_due_at: None,
            dispatched_at: None,
        };

        let cancelled = || false;
        let _ = cancelled;

        let work = async {
            if event.push_only {
                // Escalation event: only the added responders, and the
                // emergency-contact SMS must not run twice.
                self.notifier
                    .notify_recipients(&event.recipients, &alert)
                    .await
            } else {
                self.notifier.notify(&event.recipients, &alert).await
            }
        };

        match tokio::time::timeout(self.timeout, work).await {
            Ok(result) => result,
            Err(_) => Err(AppError::Unavailable("SOS dispatch timed out".to_string())),
        }
    }
}

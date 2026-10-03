//! Ticket-local conversations with independent immutable artifacts and durable coordination.
mod application;
mod attachments;
mod client;
mod commands;
mod definition;
mod http_activity;
mod model;
mod notification;
mod service;
mod sql;
mod wire;

pub use application::{
    ATTACHMENTS, Attachments, DEADLINES, Deadlines, NOTIFICATIONS, Notifications, SupportDesk,
    TICKETS, Tickets, compile,
};
pub use attachments::{AttachmentClient, AttachmentPhase, AttachmentPlan, RetainedIdentity};
pub use client::{CoordinationClient, TicketClient};
pub use commands::{ChangeTicket, EscalateTicket, GetTicket, ListMessages};
pub use definition::ScheduleDeadline;
pub use http_activity::receiver_token;
pub use model::*;
pub use notification::ScheduleNotification;
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, spawn_coordination};

/// Preserves typed native, storage, I/O, and external transport errors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) fn ticket_target(
    scope: &cellule_runtime::CellTarget,
    key: &TicketKey,
) -> cellule_runtime::Result<cellule_runtime::CellTarget> {
    cellule_runtime::CellTarget::new(
        scope.tenant(),
        scope.application(),
        TICKETS,
        &sql::partition(key)?,
    )
}

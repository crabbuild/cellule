//! Durable Queue ingress, permanent device sequence bindings, and independent summary shards.
mod application;
mod audit;
mod client;
mod consumer;
mod device;
mod model;
mod service;
mod sql;
mod summary;
mod wire;
pub use application::{
    AUDITS, Audit, DEVICES, Devices, INGRESS, Ingress, SUMMARIES, Summaries, TelemetryIngest,
    compile,
};
pub use audit::{CompleteBatch, GetBatch};
pub use client::{
    AuditClient, DeviceClient, Producer, ProgressError, ProjectionProgress, ProjectionState,
    SummaryClient,
};
pub use consumer::{ConsumerOptions, ConsumerProgress, spawn_consumers};
pub use device::{GetDevice, RecordEvent, RegisterDevice};
pub use model::*;
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, spawn_delivery};
pub use summary::{GetSummary, ListBuckets, PublishSummary};
/// Owned application error retaining its source and unresolved native evidence.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

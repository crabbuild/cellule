//! Application-owned authentication and tenant authorization before Cellule dispatch.
//! No public service route accepts a raw target, namespace, SQL, or arbitrary KV key.
mod application;
mod auth;
mod client;
mod commands;
mod http;
mod model;

pub use application::{Preferences, Projects, WorkspaceApplication, compile};
pub use auth::{Credentials, MemberPage, Principal, Role, Tenant};
pub use client::{Workspace, WorkspaceClient};
pub use commands::{ChangeProject, GetProject};
pub use http::router;
pub use model::{
    Identity, Mutation, Preference, PreferenceChange, PreferenceKey, Project, ProjectChange,
    ProjectInput, ProjectKey, ProjectOutcome, ReceiptData, Resource, Version,
};

/// Stable installation identity; principals supply tenant identity independently.
pub const APPLICATION: cellule_runtime::ApplicationId =
    cellule_runtime::ApplicationId::from_bytes([0xa4; 16]);
/// Domain, admission, and invocation errors retain their original source.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid or missing opaque credential.
    #[error("authentication required")]
    Unauthorized,
    /// Principal does not authorize the tenant, role, or retained subject.
    #[error("access denied")]
    Forbidden,
    /// JSON ingress exceeds the fixed 4-KiB body limit.
    #[error("body too large")]
    BodyTooLarge,
    /// Bounded domain input is invalid.
    #[error("invalid request: {0}")]
    Invalid(&'static str),
    /// Project key is outside this installation's configured admission set.
    #[error("resource not found")]
    NotFound,
    /// Original operating-system failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Original JSON format failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Original wire codec failure.
    #[error(transparent)]
    Codec(#[from] cellule_runtime::codec::CodecError),
    /// Original native capability or routing failure.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
    /// Original lifecycle or provider failure.
    #[error(transparent)]
    Support(#[from] cellule_cookbook_support::Error),
    /// Original project command failure, including rejection or pending evidence.
    #[error(transparent)]
    ProjectWrite(#[from] cellule_runtime::InvocationError<ProjectOutcome>),
    /// Original project query failure.
    #[error(transparent)]
    ProjectRead(#[from] cellule_runtime::InvocationError<Option<Project>>),
    /// Original native KV command failure.
    #[error(transparent)]
    PreferenceWrite(
        #[from] cellule_runtime::InvocationError<cellule_runtime::primitives::kv::KvAtomicOutcome>,
    ),
    /// Original native KV page failure.
    #[error(transparent)]
    PreferenceRead(
        #[from] cellule_runtime::InvocationError<cellule_runtime::primitives::kv::KvPage>,
    ),
    /// Original retained-outcome resolution failure.
    #[error(transparent)]
    Resolution(#[from] cellule_runtime::InvocationError<Vec<u8>>),
}

//! Project aggregates, immutable native Blob attachments, and a revisioned tenant dashboard.
//! The embedding owns authentication, explicit source rosters, providers, admission, and lifecycle.
mod application;
mod attachments;
mod client;
mod dashboard;
mod model;
mod projects;
mod service;
mod sql;
mod wire;
pub use application::{
    ATTACHMENTS, Attachments, DASHBOARD, Dashboard, PROJECTS, ProjectTracker, Projects, compile,
};
pub use attachments::{AttachmentClient, AttachmentPhase, AttachmentPlan, RetainedIdentity};
pub use client::{
    DashboardClient, ProgressError, ProjectClient, ProjectionProgress, ProjectionState,
};
pub use dashboard::{ListDashboard, LookupProject, ProjectDashboard};
pub use model::{
    Assignee, AttachmentDescriptor, AttachmentId, AttachmentLink, AttachmentObject,
    AttachmentPublication, ChangeOutcome, DashboardPage, DashboardPageRequest, Decision, Issue,
    IssueFields, IssueId, IssueStatus, MAX_ATTACHMENT_BYTES, MAX_DASHBOARD_PROJECTS,
    MAX_ISSUE_ATTACHMENTS, MAX_ISSUES, MAX_PROJECT_ATTACHMENTS, ProjectChange, ProjectKey,
    ProjectMutation, ProjectState, ProjectSummary, ProjectVersion, ProjectionOutcome,
};
pub use projects::{ChangeProject, GetProject, LinkAttachment};
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, spawn_delivery};
/// Application facade errors preserve native, provider, filesystem, and codec sources.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

use axum::{
    extract::{FromRequestParts, State},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use cellule_app::ApplicationHandle;
use cellule_axum::{CellEndpoint, CommandEndpoint, HttpError, utoipa};
use cellule_runtime::{CellTarget, PreparedCommand, TenantId};
use std::sync::Arc;

use crate::{
    ServiceState,
    application::{ORDERS, OrdersApp, SetTotal},
    journal::Journal,
};

pub const ALPHA: TenantId = TenantId::from_bytes([1; 16]);
pub const BETA: TenantId = TenantId::from_bytes([2; 16]);

struct Principal {
    tenant: TenantId,
    can_write: bool,
}

// Loopback tutorial credentials map to server-owned tenant/permission records.
// Replace this function with your existing session/JWT verifier and ACL lookup.
fn authenticate(headers: &HeaderMap) -> Result<Principal, StatusCode> {
    let values = headers.get_all("authorization");
    if values.iter().count() != 1 {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match values.iter().next().and_then(|value| value.to_str().ok()) {
        Some("Bearer alpha-writer") => Ok(Principal {
            tenant: ALPHA,
            can_write: true,
        }),
        Some("Bearer beta-reader") => Ok(Principal {
            tenant: BETA,
            can_write: false,
        }),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

pub struct Authorized<const WRITE: bool> {
    app: ApplicationHandle<OrdersApp>,
    target: CellTarget,
    journal: Journal,
}

pub type WriteOrders = Authorized<true>;
pub type ReadOrders = Authorized<false>;

impl<const WRITE: bool> Authorized<WRITE> {
    pub fn journal(&self) -> &Journal {
        &self.journal
    }
}

impl<const WRITE: bool> FromRequestParts<Arc<ServiceState>> for Authorized<WRITE> {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServiceState>,
    ) -> Result<Self, Self::Rejection> {
        let principal = authenticate(&parts.headers).map_err(IntoResponse::into_response)?;
        if WRITE && !principal.can_write {
            return Err(StatusCode::FORBIDDEN.into_response());
        }
        if !state.node.is_ready() {
            return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
        // Client headers/body cannot replace the authenticated tenant or target.
        let app = state.node.scope(principal.tenant);
        let target = app
            .target_for_scope(ORDERS, b"orders")
            .map_err(|error| HttpError::from(error).into_response())?;
        Ok(Self {
            app,
            target,
            journal: state.journal.clone(),
        })
    }
}

impl<const WRITE: bool> CellEndpoint<OrdersApp> for Authorized<WRITE> {
    fn application(&self) -> &ApplicationHandle<OrdersApp> {
        &self.app
    }
    fn target(&self) -> &CellTarget {
        &self.target
    }
}

impl CommandEndpoint<OrdersApp, SetTotal> for WriteOrders {
    async fn retain(&self, prepared: &PreparedCommand<SetTotal>) -> Result<(), HttpError> {
        self.journal.retain(prepared).await
    }
}

#[cellule_axum::utoipa::path(
    get, path = "/ready", operation_id = "readiness",
    responses((status = 204, description = "Node and lease are ready"),
              (status = 503, description = "Node is unavailable"))
)]
pub async fn ready(State(state): State<Arc<ServiceState>>) -> StatusCode {
    if state.node.is_ready() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

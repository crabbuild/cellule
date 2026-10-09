use std::{convert::Infallible, ops::Deref};

use crate::HttpError;
use axum::{
    extract::{FromRef, FromRequestParts},
    http::request::Parts,
};
use cellule_app::{ApplicationHandle, CellApplication};

/// An existing tenant/application-scoped handle extracted from Axum state.
///
/// Use the handle itself as router state, or implement
/// [`FromRef<S>`] for `ApplicationHandle<A>` in a larger application state.
/// Extraction clones the capability and leaves the request body untouched.
/// It does not authenticate a caller or choose a tenant: the application must
/// authorize access to the scope installed in state before invoking it.
pub struct Cellule<A>(pub ApplicationHandle<A>);

impl<A> Clone for Cellule<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A> Deref for Cellule<A> {
    type Target = ApplicationHandle<A>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S, A> FromRequestParts<S> for Cellule<A>
where
    S: Send + Sync,
    A: CellApplication,
    ApplicationHandle<A>: FromRef<S>,
{
    type Rejection = Infallible;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(ApplicationHandle::from_ref(state)))
    }
}

/// A request-scoped capability installed by application authorization middleware.
///
/// Reads only `Extension<ApplicationHandle<A>>`; it never falls back to router
/// state or trusts tenant headers. Missing middleware is a server configuration
/// error. The middleware must authenticate the caller and authorize the scope
/// before installing the handle.
pub struct RequestCellule<A>(pub ApplicationHandle<A>);

impl<A> Clone for RequestCellule<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A> Deref for RequestCellule<A> {
    type Target = ApplicationHandle<A>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S, A> FromRequestParts<S> for RequestCellule<A>
where
    S: Send + Sync,
    A: CellApplication + 'static,
{
    type Rejection = HttpError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum::Extension(handle) =
            axum::Extension::<ApplicationHandle<A>>::from_request_parts(parts, state)
                .await
                .map_err(HttpError::internal)?;
        Ok(Self(handle))
    }
}

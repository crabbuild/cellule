use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    marker::PhantomData,
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{FromRequest, FromRequestParts, Request},
    routing::post,
};
use cellule_app::{ApplicationHandle, CellApplication, CompiledApplication};
use cellule_runtime::{
    CellTarget, Command, Digest, Error, NamespaceId, PreparedCommand, Query, Registry,
};
use serde::{Serialize, de::DeserializeOwned};
use utoipa::{
    IntoParams, IntoResponses, PartialSchema, ToSchema,
    openapi::{
        Components, Content, Info, OpenApi, RefOr, Required, Schema,
        path::{HttpMethod, OperationBuilder, Parameter, ParameterBuilder, ParameterIn},
        request_body::RequestBodyBuilder,
        response::ResponseBuilder,
    },
};
use utoipa_axum::router::OpenApiRouter;

use crate::{
    CellJson, ErrorDto, HttpError, MinimumReceipt, MutationBody, MutationJson, ReceiptDto,
    mutation::json_error,
};

/// Application-authorized target for one generated handler.
///
/// Implement this on your `FromRequestParts` extractor. Authenticate and
/// authorize the operation and target before returning it; body inputs cannot
/// change this selection. No default tenant or permission policy is supplied.
pub trait CellEndpoint<A: CellApplication>: Send + Sync + 'static {
    /// The authorized tenant/application capability.
    fn application(&self) -> &ApplicationHandle<A>;
    /// The authorized Cell within that capability.
    fn target(&self) -> &CellTarget;
}

/// Required application-owned custody before a generated command dispatches.
pub trait CommandEndpoint<A: CellApplication, C: Command>: CellEndpoint<A> {
    /// Atomically retain `prepared.snapshot().to_bytes()` and its exact
    /// `input_bytes()`, bound to this authorized scope. Return only after the
    /// evidence is durable. Retention failure prevents dispatch.
    ///
    /// Retention must be idempotent for identical evidence and refuse conflicting
    /// bytes. Supervise recovery across cancellation/restart: resolve uncertain
    /// attempts before replay; never mint a new identity or re-encode input.
    fn retain(
        &self,
        prepared: &PreparedCommand<C>,
    ) -> impl Future<Output = Result<(), HttpError>> + Send;
}

/// One POST route declaration shared by its handler and OpenAPI operation.
///
/// Queries use a JSON body without mutation identity. Commands use
/// [`MutationBody`]. Register each captured path segment with
/// [`Self::path_parameter`]; wildcards and embedded captures are refused.
pub struct EndpointSpec {
    path: String,
    operation_id: String,
    description: Option<String>,
    parameters: BTreeMap<String, Parameter>,
    schemas: Vec<(String, RefOr<Schema>)>,
}

impl EndpointSpec {
    /// Describes the application-chosen path and unique SDK operation name.
    pub fn new(path: impl Into<String>, operation_id: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            operation_id: operation_id.into(),
            description: None,
            parameters: BTreeMap::new(),
            schemas: Vec::new(),
        }
    }
    /// Adds an application-facing explanation of the operation.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    /// Documents a required path capture using its application-selected type.
    pub fn path_parameter<T: ToSchema>(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.parameters.insert(
            name.clone(),
            ParameterBuilder::new()
                .name(name)
                .parameter_in(ParameterIn::Path)
                .required(Required::True)
                .schema(Some(T::schema()))
                .build(),
        );
        self.schemas.push((T::name().into_owned(), T::schema()));
        T::schemas(&mut self.schemas);
        self
    }
}

/// Registers routine typed operations and their OpenAPI descriptions together.
///
/// Registration validates the compiled namespace/module/id/codec contract.
/// Every request verifies the authorized handle matches the documented artifact.
/// The runtime remains the sole command publication and receipt authority.
/// Use manual handlers for domain DTO mapping or custom success statuses.
pub struct CellApi<A, S = ()> {
    registry: Arc<Registry>,
    namespaces: Vec<NamespaceId>,
    artifact: Digest,
    router: Router<S>,
    document: OpenApi,
    paths: BTreeMap<String, String>,
    ids: BTreeSet<String>,
    marker: PhantomData<fn() -> A>,
}

impl<A: CellApplication + 'static, S: Clone + Send + Sync + 'static> CellApi<A, S> {
    /// Starts an API for an immutable compiled application artifact.
    pub fn new(application: &CompiledApplication) -> Result<Self, Error> {
        if application.name() != A::NAME {
            return Err(Error::Registry(
                "HTTP application type differs from compiled application",
            ));
        }
        Ok(Self {
            registry: application.registry(),
            namespaces: application
                .cell_types()
                .iter()
                .map(|cell| cell.namespace())
                .collect(),
            artifact: application.descriptor_digest(),
            router: Router::new(),
            document: OpenApi::new(
                Info::new(A::NAME, env!("CARGO_PKG_VERSION")),
                utoipa::openapi::Paths::new(),
            ),
            paths: BTreeMap::new(),
            ids: BTreeSet::new(),
            marker: PhantomData,
        })
    }

    /// Adds a command using an authorized extractor and mandatory custody hook.
    pub fn command<C, X>(
        mut self,
        namespace: NamespaceId,
        spec: EndpointSpec,
    ) -> Result<Self, Error>
    where
        C: Command,
        C::Input: DeserializeOwned + ToSchema,
        C::Output: Serialize + ToSchema + Sync,
        X: CommandEndpoint<A, C> + FromRequestParts<S>,
        X::Rejection: Send,
    {
        self.validate_namespace(namespace)?;
        self.registry.command_contract::<C>(namespace)?;
        let path = spec.path.clone();
        self.register::<MutationBody<C::Input>, C::Output>(spec, false)?;
        let artifact = self.artifact;
        self.router = self.router.route(
            &path,
            post(
                move |context: X, input: MutationJson<C::Input>| async move {
                    validate_context::<A, X>(&context, namespace, artifact)?;
                    let prepared = context
                        .application()
                        .prepare_command::<C>(context.target(), input.identity, input.input)
                        .await?;
                    context.retain(&prepared).await?;
                    let committed = prepared.execute().await?;
                    Ok::<_, HttpError>(CellJson::from(committed))
                },
            ),
        );
        Ok(self)
    }

    /// Adds an owner/replica-policy query accepting JSON input and a full minimum receipt.
    pub fn query<Q, X>(mut self, namespace: NamespaceId, spec: EndpointSpec) -> Result<Self, Error>
    where
        Q: Query,
        Q::Input: DeserializeOwned + ToSchema + Sync,
        Q::Output: Serialize + ToSchema + Sync,
        X: CellEndpoint<A> + FromRequestParts<S>,
        X::Rejection: Send,
    {
        self.validate_namespace(namespace)?;
        self.registry.query_contract::<Q>(namespace)?;
        let path = spec.path.clone();
        self.register::<Q::Input, Q::Output>(spec, true)?;
        let artifact = self.artifact;
        self.router = self.router.route(
            &path,
            post(
                move |context: X, minimum: MinimumReceipt, input: QueryJson<Q::Input>| async move {
                    validate_context::<A, X>(&context, namespace, artifact)?;
                    let observed = context
                        .application()
                        .query::<Q>(context.target(), minimum.0, input.0)
                        .await?;
                    Ok::<_, HttpError>(CellJson::from(observed))
                },
            ),
        );
        Ok(self)
    }

    /// Returns a composable router; merging/nesting also carries its document.
    ///
    /// Add your authentication error responses and security schemes to the
    /// returned document. The application decides where to serve `/openapi.json`.
    pub fn into_router(self) -> OpenApiRouter<S> {
        OpenApiRouter::with_openapi(self.document).merge(self.router.into())
    }

    fn validate_namespace(&self, namespace: NamespaceId) -> Result<(), Error> {
        if !self.namespaces.contains(&namespace) {
            return Err(Error::Registry(
                "HTTP namespace is not declared by the application",
            ));
        }
        Ok(())
    }

    fn register<I: ToSchema, O: ToSchema>(
        &mut self,
        spec: EndpointSpec,
        query: bool,
    ) -> Result<(), Error> {
        let (shape, captures) = validate_path(&spec.path)?;
        if spec.operation_id.is_empty()
            || !spec
                .operation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(Error::Registry(
                "HTTP operation ID must contain letters, digits or underscores",
            ));
        }
        if self.ids.contains(&spec.operation_id) || self.paths.contains_key(&shape) {
            return Err(Error::Registry(
                "duplicate HTTP operation ID or route shape",
            ));
        }
        if captures != spec.parameters.keys().cloned().collect() {
            return Err(Error::Registry(
                "HTTP path captures require exactly matching parameter schemas",
            ));
        }
        let mut operation = OperationBuilder::new().operation_id(Some(&spec.operation_id)).description(spec.description)
            .parameters(Some(spec.parameters.into_values()))
            .request_body(Some(RequestBodyBuilder::new().required(Some(Required::True)).content("application/json", Content::new(Some(I::schema()))).build()))
            .response("200", ResponseBuilder::new().description("Published command result or observed query result, with the complete receipt.").content("application/json", Content::new(Some(CellJson::<O>::schema()))).build());
        for (status, response) in HttpError::responses() {
            operation = operation.response(status, response);
        }
        if query {
            for parameter in MinimumReceipt::into_params(|| None) {
                operation = operation.parameter(parameter);
            }
        }
        self.document.paths.add_path_operation(
            &spec.path,
            vec![HttpMethod::Post],
            operation.build(),
        );
        let mut schemas = spec.schemas;
        for (name, schema) in [
            (I::name().into_owned(), I::schema()),
            (CellJson::<O>::name().into_owned(), CellJson::<O>::schema()),
            (ErrorDto::name().into_owned(), ErrorDto::schema()),
            (ReceiptDto::name().into_owned(), ReceiptDto::schema()),
        ] {
            schemas.push((name, schema));
        }
        I::schemas(&mut schemas);
        CellJson::<O>::schemas(&mut schemas);
        ErrorDto::schemas(&mut schemas);
        let components = self.document.components.get_or_insert_with(Components::new);
        for (name, schema) in schemas {
            if components
                .schemas
                .get(&name)
                .is_some_and(|existing| existing != &schema)
            {
                return Err(Error::Registry("conflicting HTTP schema names"));
            }
            components.schemas.insert(name, schema);
        }
        self.paths.insert(shape, spec.path);
        self.ids.insert(spec.operation_id);
        Ok(())
    }
}

fn validate_context<A: CellApplication, X: CellEndpoint<A>>(
    context: &X,
    namespace: NamespaceId,
    artifact: Digest,
) -> Result<(), HttpError> {
    if context.target().namespace() != namespace
        || context.application().compiled().descriptor_digest() != artifact
    {
        return Err(HttpError::internal(Error::Registry(
            "authorized HTTP target differs from registered artifact or namespace",
        )));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(String, BTreeSet<String>), Error> {
    if !path.starts_with('/') {
        return Err(Error::Registry("HTTP path must start with /"));
    }
    let mut shape = String::new();
    let mut captures = BTreeSet::new();
    for segment in path[1..].split('/') {
        shape.push('/');
        if let Some(name) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || !captures.insert(name.to_owned())
            {
                return Err(Error::Registry(
                    "HTTP path capture must be a unique identifier",
                ));
            }
            shape.push_str("{}");
        } else {
            if !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
            {
                return Err(Error::Registry("HTTP path contains unsupported syntax"));
            }
            shape.push_str(segment);
        }
    }
    Ok((shape, captures))
}

struct QueryJson<T>(T);
impl<S: Send + Sync, T: DeserializeOwned + Send> FromRequest<S> for QueryJson<T> {
    type Rejection = HttpError;
    async fn from_request(request: Request, state: &S) -> Result<Self, HttpError> {
        let Json(input) = Json::<T>::from_request(request, state)
            .await
            .map_err(json_error)?;
        Ok(Self(input))
    }
}

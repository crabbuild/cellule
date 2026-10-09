use std::{borrow::Cow, collections::BTreeMap};

use utoipa::{
    IntoResponses, PartialSchema, ToSchema,
    openapi::{
        Content, RefOr, Schema,
        response::{Response, ResponseBuilder},
        schema::ObjectBuilder,
    },
};

use crate::{
    CellJson, ErrorDto, HttpError, MINIMUM_RECEIPT_HEADER, MinimumReceipt, MutationBody,
    MutationIdentityDto, MutationJson, ReceiptDto,
};

// Escape punctuation reversibly: two instantiations must not replace each
// other's envelope schema when a document includes several output types.
fn envelope_name<T>(prefix: &str) -> Cow<'static, str> {
    let mut name = prefix.to_owned();
    for byte in std::any::type_name::<T>().bytes() {
        if byte.is_ascii_alphanumeric() {
            name.push(char::from(byte));
        } else {
            name.push('_');
            name.push_str(&crate::response::hex(&[byte]));
        }
    }
    Cow::Owned(name)
}

fn collect<T: ToSchema>(schemas: &mut Vec<(String, RefOr<Schema>)>) {
    schemas.push((T::name().into_owned(), T::schema()));
    T::schemas(schemas);
}

impl<T: ToSchema> utoipa::__dev::ComposeSchema for CellJson<T> {
    fn compose(schemas: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        ObjectBuilder::new()
            .property(
                "output",
                schemas.into_iter().next().unwrap_or_else(T::schema),
            )
            .required("output")
            .property("receipt", ReceiptDto::schema())
            .required("receipt")
            .into()
    }
}

impl<T: ToSchema> ToSchema for CellJson<T> {
    fn name() -> Cow<'static, str> {
        envelope_name::<T>("CellJson_")
    }
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        collect::<T>(schemas);
        collect::<ReceiptDto>(schemas);
    }
}

impl<T: ToSchema> utoipa::__dev::ComposeSchema for MutationBody<T> {
    fn compose(schemas: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        ObjectBuilder::new()
            .property("identity", MutationIdentityDto::schema())
            .required("identity")
            .property(
                "input",
                schemas.into_iter().next().unwrap_or_else(T::schema),
            )
            .required("input")
            .additional_properties(Some(
                utoipa::openapi::schema::AdditionalProperties::FreeForm(false),
            ))
            .into()
    }
}

impl<T: ToSchema> ToSchema for MutationBody<T> {
    fn name() -> Cow<'static, str> {
        envelope_name::<T>("MutationBody_")
    }
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        collect::<T>(schemas);
        collect::<MutationIdentityDto>(schemas);
    }
}

impl<T: ToSchema> ToSchema for MutationJson<T> {
    fn name() -> Cow<'static, str> {
        MutationBody::<T>::name()
    }
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        MutationBody::<T>::schemas(schemas);
    }
}

impl<T: ToSchema> utoipa::__dev::ComposeSchema for MutationJson<T> {
    fn compose(schemas: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        <MutationBody<T> as utoipa::__dev::ComposeSchema>::compose(schemas)
    }
}

impl utoipa::IntoParams for MinimumReceipt {
    fn into_params(
        _: impl Fn() -> Option<utoipa::openapi::path::ParameterIn>,
    ) -> Vec<utoipa::openapi::path::Parameter> {
        use utoipa::openapi::{
            Required,
            path::{ParameterBuilder, ParameterIn},
        };
        vec![ParameterBuilder::new().name(MINIMUM_RECEIPT_HEADER).parameter_in(ParameterIn::Header)
            .required(Required::False).description(Some("Optional JSON ReceiptDto; at most 256 bytes. Cell and incarnation must match this query target."))
            .schema(Some(String::schema())).build()]
    }
}

impl IntoResponses for HttpError {
    fn responses() -> BTreeMap<String, RefOr<Response>> {
        [
            ("400", "invalid_request: invalid identity, command, receipt header, or malformed JSON."),
            ("409", "request_conflict, or command_rejected with the durable receipt. A rejected command already has an outcome."),
            ("413", "body_too_large: request exceeds the application's Axum body limit."),
            ("415", "unsupported_media_type: JSON content type required."),
            ("422", "invalid_request: JSON does not match the typed input."),
            ("500", "internal_error, or invalid_published_result with a receipt: recover the original output before retrying."),
            ("503", "unavailable, or outcome_unknown: resolve the retained original mutation before retrying. Status alone does not establish absence."),
            ("504", "deadline_exceeded before submission."),
        ].into_iter().map(|(status, description)| (
            status.to_owned(),
            ResponseBuilder::new().description(description)
                .content("application/json", Content::new(Some(ErrorDto::schema())))
                .header("Cache-Control", utoipa::openapi::Header::new(utoipa::openapi::schema::Object::with_type(utoipa::openapi::schema::Type::String)))
                .build().into(),
        )).collect()
    }
}

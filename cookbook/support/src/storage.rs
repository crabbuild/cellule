use std::sync::Arc;

use cellule_store::Store;
use object_store::aws::AmazonS3Builder;

use crate::{Error, Result};

/// Constructs the cookbook's local S3 provider with explicit development credentials.
///
/// This intentionally accepts loopback HTTP only. Production credentials and
/// endpoints belong to a separately configured embedding application.
pub fn local_s3_store(endpoint: &str, bucket: &str) -> Result<Store> {
    let endpoint_url =
        url::Url::parse(endpoint).map_err(|_| Error::Configuration("invalid local storage URL"))?;
    if endpoint_url.scheme() != "http"
        || !matches!(
            endpoint_url.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        )
        || !endpoint_url.username().is_empty()
        || endpoint_url.password().is_some()
        || endpoint_url.path() != "/"
        || endpoint_url.query().is_some()
        || endpoint_url.fragment().is_some()
        || bucket.is_empty()
    {
        return Err(Error::Configuration(
            "local provider requires a loopback HTTP endpoint and nonempty bucket",
        ));
    }
    let provider = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_access_key_id("cellule-cookbook")
        .with_secret_access_key("cellule-cookbook-local-only")
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Store::new(Arc::new(provider)))
}

//! Configuration checks for the exercised local provider boundary.
#![allow(clippy::unwrap_used)]

use crate::local_s3_store;

#[test]
fn local_provider_rejects_remote_or_credentialed_urls() {
    for endpoint in [
        "http://localhost:19000@external.example/",
        "http://external.example:19000/",
        "https://localhost:19000/",
        "http://127.0.0.1:19000/path",
        "http://127.0.0.1:19000/?query=1",
        "http://user:password@localhost:19000/",
    ] {
        assert!(local_s3_store(endpoint, "private").is_err());
    }
    assert!(local_s3_store("http://127.0.0.1:19000", "").is_err());
    assert!(local_s3_store("http://127.0.0.1:19000", "private").is_ok());
}

//! Public HTTP and typed-client isolation, command recovery, and admission evidence.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use cellule_cookbook_support::{LocalNode, NodeConfig};
use cellule_cookbook_tenant_workspace::*;
use cellule_runtime::{
    Resolution,
    codec::{BoundedEncoder, WireValue},
};
use cellule_store::Store;
use http_body_util::BodyExt as _;
use object_store::memory::InMemory;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};
use tower::ServiceExt as _;

struct Fixture {
    root: tempfile::TempDir,
    node: Arc<LocalNode>,
    workspace: Arc<Workspace>,
    tokens: Value,
}
async fn node(store: Store, root: &Path) -> Arc<LocalNode> {
    Arc::new(
        LocalNode::start(
            compile().unwrap(),
            store,
            NodeConfig {
                state_directory: root.join("state"),
                storage_prefix: object_store::path::Path::from("test-tenant-workspace"),
                application_id: APPLICATION,
            },
        )
        .await
        .unwrap(),
    )
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("credentials.json");
        Credentials::initialize(&path).unwrap();
        let tokens = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let node = node(Store::new(Arc::new(InMemory::new())), root.path()).await;
        let workspace = Arc::new(Workspace::new(
            node.clone(),
            Arc::new(Credentials::load(&path).unwrap()),
        ));
        Self {
            root,
            node,
            workspace,
            tokens,
        }
    }
    fn token(&self, subject: &str) -> &str {
        self.tokens["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["subject"] == subject)
            .unwrap()["token"]
            .as_str()
            .unwrap()
    }
    fn client(&self, subject: &str) -> WorkspaceClient {
        let p = self.workspace.authenticate(self.token(subject)).unwrap();
        let tenant = p.tenant().slug();
        self.workspace.client(p, tenant).unwrap()
    }
    fn router(&self) -> Router {
        router(self.workspace.clone())
    }
    async fn close(self) {
        self.node.shutdown().await.unwrap();
    }
}
fn project_change(subject: &str, revision: Option<i64>, title: &str) -> Mutation<ProjectChange> {
    Mutation {
        tenant: if subject.starts_with("acme-") {
            Tenant::Acme
        } else {
            Tenant::Globex
        },
        resource: Resource::Project {
            key: "launch".into(),
        },
        version: 1,
        subject: subject.into(),
        identity: Identity::new().unwrap(),
        change: ProjectChange {
            expected_revision: revision,
            title: title.into(),
            description: "Durable project document".into(),
        },
    }
}
fn preference_change(
    subject: &str,
    value: &str,
    expected: Option<Version>,
) -> Mutation<PreferenceChange> {
    Mutation {
        tenant: if subject.starts_with("acme-") {
            Tenant::Acme
        } else {
            Tenant::Globex
        },
        resource: Resource::Preferences,
        version: 1,
        subject: subject.into(),
        identity: Identity::new().unwrap(),
        change: PreferenceChange {
            key: PreferenceKey::Theme,
            expected,
            value: value.into(),
        },
    }
}
async fn request(
    app: Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
    headers: &[(&str, String)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    for (key, value) in headers {
        builder = builder.header(*key, value);
    }
    let response = app
        .oneshot(
            builder
                .body(if body.is_null() {
                    Body::empty()
                } else {
                    Body::from(serde_json::to_vec(&body).unwrap())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let output = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, output)
}
#[tokio::test]
async fn identical_project_keys_and_preference_names_are_independent_tenant_cells() {
    let f = Fixture::new().await;
    let project = ProjectKey::parse("launch").unwrap();
    let a = f.client("acme-editor");
    let b = f.client("globex-editor");
    let first = a
        .prepare_project(
            &project,
            &project_change("acme-editor", None, "Acme secret"),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let second = b
        .prepare_project(
            &project,
            &project_change("globex-editor", None, "Globex secret"),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_ne!(first.receipt.cell, second.receipt.cell);
    assert_eq!(
        a.project(&project, Some(first.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .title,
        "Acme secret"
    );
    assert_eq!(
        b.project(&project, Some(second.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .title,
        "Globex secret"
    );
    let acme = f.client("acme-admin");
    let globex = f.client("globex-admin");
    let acme_write = acme
        .prepare_preference(&preference_change("acme-admin", "dark", None))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let globex_write = globex
        .prepare_preference(&preference_change("globex-admin", "light", None))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_ne!(acme_write.receipt.cell, globex_write.receipt.cell);
    assert_ne!(first.receipt.cell, acme_write.receipt.cell);
    assert_eq!(
        acme.preferences(Some(acme_write.receipt))
            .await
            .unwrap()
            .output[0]
            .value,
        "dark"
    );
    assert_eq!(
        globex
            .preferences(Some(globex_write.receipt))
            .await
            .unwrap()
            .output[0]
            .value,
        "light"
    );
    f.close().await;
}
#[tokio::test]
async fn every_tenant_route_rejects_cross_tenant_before_native_dispatch_or_json_extraction() {
    let f = Fixture::new().await;
    // Deliberately use malformed bodies: tenant authorization must precede JSON processing.
    let routes = [
        ("GET", "projects/launch"),
        ("PUT", "projects/launch"),
        ("POST", "projects/launch/resolve"),
        ("GET", "preferences"),
        ("PUT", "preferences"),
        ("POST", "preferences/resolve"),
        ("GET", "admin/members"),
    ];
    for subject in [
        "acme-admin",
        "acme-editor",
        "acme-viewer",
        "globex-admin",
        "globex-editor",
        "globex-viewer",
    ] {
        let other = if subject.starts_with("acme-") {
            "globex"
        } else {
            "acme"
        };
        for (method, suffix) in routes {
            let before = f.workspace.dispatch_count();
            let (status, body) = request(
                f.router(),
                method,
                &format!("/v1/tenants/{other}/{suffix}"),
                Some(f.token(subject)),
                json!({"target":{"tenant":"globex"}}),
                &[],
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {suffix} {subject}");
            assert_eq!(body, json!({"error":"forbidden"}));
            assert_eq!(f.workspace.dispatch_count(), before);
        }
    }
    assert!(
        std::fs::read_dir(f.root.path().join("state"))
            .unwrap()
            .next()
            .is_none(),
        "denied ingress must not provision cells"
    );
    f.close().await;
}
#[tokio::test]
async fn forged_targets_subjects_receipts_queries_and_privileges_do_not_dispatch() {
    let f = Fixture::new().await;
    let project = ProjectKey::parse("launch").unwrap();
    let other = f
        .client("globex-editor")
        .prepare_project(
            &project,
            &project_change("globex-editor", None, "Foreign secret"),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let before = f.workspace.dispatch_count();
    let foreign_receipt = serde_json::to_string(&ReceiptData::from(other.receipt)).unwrap();
    let mut forged = serde_json::to_value(project_change("acme-editor", None, "Own")).unwrap();
    forged["target"] = json!({"tenant":"globex"});
    let cases = [
        (
            "GET",
            "/v1/tenants/acme/projects/launch",
            "acme-editor",
            Value::Null,
            vec![("x-cellule-receipt", foreign_receipt)],
        ),
        (
            "GET",
            "/v1/tenants/acme/projects/launch?tenant=globex",
            "acme-editor",
            Value::Null,
            vec![],
        ),
        (
            "PUT",
            "/v1/tenants/acme/projects/launch",
            "acme-editor",
            forged,
            vec![],
        ),
        (
            "PUT",
            "/v1/tenants/acme/projects/launch",
            "acme-editor",
            serde_json::to_value(project_change("globex-editor", None, "Forged subject")).unwrap(),
            vec![],
        ),
        (
            "PUT",
            "/v1/tenants/acme/projects/launch",
            "acme-viewer",
            json!({}),
            vec![],
        ),
        (
            "POST",
            "/v1/tenants/acme/projects/launch/resolve",
            "acme-viewer",
            json!({}),
            vec![],
        ),
        (
            "PUT",
            "/v1/tenants/acme/preferences",
            "acme-editor",
            json!({}),
            vec![],
        ),
        (
            "POST",
            "/v1/tenants/acme/preferences/resolve",
            "acme-editor",
            json!({}),
            vec![],
        ),
        (
            "GET",
            "/v1/tenants/acme/admin/members",
            "acme-editor",
            Value::Null,
            vec![],
        ),
    ];
    for (method, path, subject, body, headers) in cases {
        let (status, output) = request(
            f.router(),
            method,
            path,
            Some(f.token(subject)),
            body,
            &headers,
        )
        .await;
        assert!(
            matches!(status, StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN),
            "{method} {path}: {status} {output}"
        );
        assert!(!output.to_string().contains("Foreign secret"));
        assert_eq!(before, f.workspace.dispatch_count());
    }
    for hint in [
        "x-tenant-id",
        "x-application-id",
        "x-namespace-id",
        "x-cellule-target",
        "x-cellule-partition",
    ] {
        let (status, _) = request(
            f.router(),
            "GET",
            "/v1/tenants/acme/projects/launch",
            Some(f.token("acme-admin")),
            Value::Null,
            &[(hint, "globex".into())],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(before, f.workspace.dispatch_count());
    }
    f.close().await;
}
#[tokio::test]
async fn project_replay_resolution_and_concurrent_revisions_preserve_one_durable_edit() {
    let f = Fixture::new().await;
    let client = f.client("acme-editor");
    let project = ProjectKey::parse("launch").unwrap();
    let first = project_change("acme-editor", None, "Initial");
    assert!(matches!(
        client.resolve_project(&project, &first).await.unwrap(),
        Resolution::Absent
    ));
    let sent = client
        .prepare_project(&project, &first)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        sent,
        client
            .prepare_project(&project, &first)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
    );
    let left = client
        .prepare_project(&project, &project_change("acme-editor", Some(1), "Left"))
        .await
        .unwrap();
    let right = client
        .prepare_project(&project, &project_change("acme-editor", Some(1), "Right"))
        .await
        .unwrap();
    let (left, right) = tokio::join!(left.execute(), right.execute());
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let value = client
        .project(&project, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(value.revision, 2);
    let conflict = project_change("acme-editor", Some(1), "Stale");
    let first_rejection = client
        .prepare_project(&project, &conflict)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap_err();
    let repeated = client
        .prepare_project(&project, &conflict)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap_err();
    match (first_rejection, repeated) {
        (
            cellule_runtime::InvocationError::Rejected(a),
            cellule_runtime::InvocationError::Rejected(b),
        ) => assert_eq!(a, b),
        _ => panic!("expected durable rejections"),
    }
    assert!(
        matches!(client.resolve_project(&project,&first).await.unwrap(),Resolution::Committed(value)if value.commit_sequence()==sent.receipt.commit_sequence)
    );
    f.close().await;
}
#[tokio::test]
async fn admin_member_pages_and_viewer_reads_are_bounded_and_credential_free() {
    let f = Fixture::new().await;
    let mut after = None;
    let mut subjects = Vec::new();
    loop {
        let path = format!(
            "/v1/tenants/acme/admin/members?limit=1{}",
            after
                .as_ref()
                .map(|v| format!("&after={v}"))
                .unwrap_or_default()
        );
        let (status, page) = request(
            f.router(),
            "GET",
            &path,
            Some(f.token("acme-admin")),
            Value::Null,
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["members"].as_array().unwrap().len(), 1);
        assert!(!page.to_string().contains("token"));
        assert_eq!(page["members"][0]["tenant"], "acme");
        subjects.push(page["members"][0]["subject"].as_str().unwrap().to_owned());
        after = page["next"].as_str().map(str::to_owned);
        if after.is_none() {
            break;
        }
    }
    assert_eq!(subjects, vec!["acme-admin", "acme-editor", "acme-viewer"]);
    let (status, _) = request(
        f.router(),
        "GET",
        "/v1/tenants/acme/admin/members?limit=11",
        Some(f.token("acme-admin")),
        Value::Null,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for path in [
        "/v1/tenants/acme/projects/launch",
        "/v1/tenants/acme/preferences",
    ] {
        let (status, _) = request(
            f.router(),
            "GET",
            path,
            Some(f.token("acme-viewer")),
            Value::Null,
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    f.close().await;
}
#[tokio::test]
async fn cold_restore_preserves_both_tenants_and_original_project_and_preference_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("credentials.json");
    Credentials::initialize(&path).unwrap();
    let credentials = Arc::new(Credentials::load(&path).unwrap());
    let tokens: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let token = tokens["members"][0]["token"].as_str().unwrap();
    let principal = credentials.authenticate(token).unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first_node = node(store.clone(), root.path()).await;
    let workspace = Workspace::new(first_node.clone(), credentials.clone());
    let client = workspace.client(principal.clone(), "acme").unwrap();
    let project = ProjectKey::parse("launch").unwrap();
    let project_record = project_change("acme-admin", None, "Survives restore");
    let preference_record = preference_change("acme-admin", "dark", None);
    let sent = client
        .prepare_project(&project, &project_record)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let preference = client
        .prepare_preference(&preference_record)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    first_node.shutdown().await.unwrap();
    drop(client);
    drop(workspace);
    drop(first_node);
    assert!(
        std::fs::read_dir(root.path().join("state"))
            .unwrap()
            .next()
            .is_none()
    );
    let second_node = node(store, root.path()).await;
    let workspace = Workspace::new(second_node.clone(), credentials);
    let client = workspace.client(principal, "acme").unwrap();
    assert_eq!(
        client
            .project(&project, Some(sent.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .title,
        "Survives restore"
    );
    assert_eq!(
        client
            .preferences(Some(preference.receipt))
            .await
            .unwrap()
            .output[0]
            .value,
        "dark"
    );
    assert!(
        matches!(client.resolve_project(&project,&project_record).await.unwrap(),Resolution::Committed(value)if value.commit_sequence()==sent.receipt.commit_sequence)
    );
    assert!(
        matches!(client.resolve_preference(&preference_record).await.unwrap(),Resolution::Committed(value)if value.commit_sequence()==preference.receipt.commit_sequence)
    );
    second_node.shutdown().await.unwrap();
}
#[test]
fn credentials_are_private_unique_canonical_and_never_overwritten() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("credentials.json");
    Credentials::initialize(&path).unwrap();
    let original = std::fs::read(&path).unwrap();
    assert!(Credentials::initialize(&path).is_err());
    assert_eq!(original, std::fs::read(&path).unwrap());
    let credentials = Credentials::load(&path).unwrap();
    assert!(credentials.authenticate(&"0".repeat(64)).is_err());
    assert!(credentials.authenticate("acme-admin").is_err());
    let mut document: Value = serde_json::from_slice(&original).unwrap();
    document["members"][1]["token"] = document["members"][0]["token"].clone();
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(Credentials::load(&path).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(&path, original).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Credentials::load(&path).is_err());
    }
}
#[test]
fn canonical_keys_identity_bounds_and_project_outcome_wire_fixture_are_stable() {
    assert!(ProjectKey::parse("Launch").is_err());
    assert!(ProjectKey::parse("other").is_err());
    let mut identity = Identity::new().unwrap();
    identity.expires_at_ms = identity.issued_at_ms + 300001;
    let value = serde_json::to_value(Mutation {
        tenant: Tenant::Acme,
        resource: Resource::Project {
            key: "launch".into(),
        },
        version: 1,
        subject: "acme-admin".into(),
        identity,
        change: ProjectChange {
            expected_revision: None,
            title: "x".into(),
            description: "".into(),
        },
    })
    .unwrap();
    assert!(
        serde_json::from_value::<Mutation<ProjectChange>>(json!({"tenant":"globex","version":1}))
            .is_err()
    );
    assert_eq!(value["version"], 1);
    let mut encoder = BoundedEncoder::new(16).unwrap();
    ProjectOutcome::Conflict.encode(&mut encoder).unwrap();
    assert_eq!(encoder.finish(), vec![2]);
}

#[tokio::test]
async fn authentication_body_limits_and_frozen_resource_binding_fail_before_dispatch() {
    let f = Fixture::new().await;
    let path = "/v1/tenants/acme/projects/launch";
    for token in [
        None,
        Some("acme-admin"),
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
    ] {
        let (status, body) = request(f.router(), "GET", path, token, Value::Null, &[]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, json!({"error":"unauthorized"}));
    }
    let mut malformed = serde_json::to_value(project_change("acme-admin", None, "x")).unwrap();
    malformed["identity"]["expires_at_ms"] =
        json!(malformed["identity"]["issued_at_ms"].as_i64().unwrap() + 300001);
    let mut wrong_resource = serde_json::to_value(project_change("acme-admin", None, "x")).unwrap();
    wrong_resource["resource"]["key"] = json!("support");
    let mut wrong_tenant = serde_json::to_value(project_change("acme-admin", None, "x")).unwrap();
    wrong_tenant["tenant"] = json!("globex");
    for body in [malformed, wrong_resource, wrong_tenant] {
        let (status, _) = request(
            f.router(),
            "PUT",
            path,
            Some(f.token("acme-admin")),
            body,
            &[],
        )
        .await;
        assert!(matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN
        ));
    }
    let (status, _) = request(
        f.router(),
        "PUT",
        path,
        Some(f.token("acme-admin")),
        json!({"padding":"x".repeat(5000)}),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(f.workspace.dispatch_count(), 0);
    let request = Request::builder()
        .uri(path)
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", f.token("acme-admin")),
        )
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", f.token("globex-admin")),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        f.router().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(f.workspace.dispatch_count(), 0);
    f.close().await;
}
#[tokio::test]
async fn concurrent_admin_preference_edits_have_one_winner_and_durable_conflict_resolution() {
    let f = Fixture::new().await;
    let client = f.client("acme-admin");
    let first = preference_change("acme-admin", "dark", None);
    let initial = client
        .prepare_preference(&first)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let version = client
        .preferences(Some(initial.receipt))
        .await
        .unwrap()
        .output[0]
        .version;
    let left = preference_change("acme-admin", "light", Some(version));
    let right = preference_change("acme-admin", "dark", Some(version));
    let l = client.prepare_preference(&left).await.unwrap();
    let r = client.prepare_preference(&right).await.unwrap();
    let (l, r) = tokio::join!(l.execute(), r.execute());
    assert_eq!(usize::from(l.is_ok()) + usize::from(r.is_ok()), 1);
    let rejected = if l.is_err() { left } else { right };
    let first = client
        .prepare_preference(&rejected)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap_err();
    let repeated = client
        .prepare_preference(&rejected)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap_err();
    match (first, repeated) {
        (
            cellule_runtime::InvocationError::Rejected(a),
            cellule_runtime::InvocationError::Rejected(b),
        ) => {
            assert_eq!(a, b);
            assert!(
                matches!(client.resolve_preference(&rejected).await.unwrap(),Resolution::Committed(value)if value.commit_sequence()==a.receipt.commit_sequence)
            );
        }
        _ => panic!("expected native durable conditional rejection"),
    }
    f.close().await;
}

#[tokio::test]
async fn closing_admission_rejects_new_http_work_and_finishes_an_accepted_slow_publication() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use std::time::Duration;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("credentials.json");
    Credentials::initialize(&path).unwrap();
    let document: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let token = document["members"][0]["token"].as_str().unwrap().to_owned();
    let provider = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let node = node(Store::new(provider.clone()), root.path()).await;
    let workspace = Arc::new(Workspace::new(
        node.clone(),
        Arc::new(Credentials::load(&path).unwrap()),
    ));
    let principal = workspace.authenticate(&token).unwrap();
    let client = workspace.client(principal, "acme").unwrap();
    let key = ProjectKey::parse("launch").unwrap();
    client.project(&key, None).await.unwrap();
    provider.config_mut(|config| config.wait_put_per_call = Duration::from_millis(300));
    let before = workspace.dispatch_count();
    let app = router(workspace.clone());
    let retained =
        serde_json::to_value(project_change("acme-admin", None, "Accepted before drain")).unwrap();
    let request_token = token.clone();
    let accepted = tokio::spawn(async move {
        request(
            app,
            "PUT",
            "/v1/tenants/acme/projects/launch",
            Some(&request_token),
            retained,
            &[],
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while workspace.dispatch_count() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !accepted.is_finished(),
        "slow publication must still be in flight at admission closure"
    );
    workspace.stop_admission();
    let (status, body) = request(
        router(workspace.clone()),
        "GET",
        "/v1/tenants/acme/projects/launch",
        Some(&token),
        Value::Null,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"error":"not_ready"}));
    let (status, result) = tokio::time::timeout(Duration::from_secs(10), accepted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    provider.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
    let receipt: ReceiptData = serde_json::from_value(result["receipt"].clone()).unwrap();
    assert_eq!(
        client
            .project(&key, Some(receipt.native().unwrap()))
            .await
            .unwrap()
            .output
            .unwrap()
            .title,
        "Accepted before drain"
    );
    node.shutdown().await.unwrap();
    assert!(
        std::fs::read_dir(root.path().join("state"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn expired_resolution_preserves_authorization_and_never_claims_absence_or_dispatches() {
    let f = Fixture::new().await;
    let now = cellule_cookbook_support::now_ms().unwrap();
    let mut project = project_change("acme-admin", None, "Original frozen bytes");
    let mut preference = preference_change("acme-admin", "dark", None);
    for identity in [&mut project.identity, &mut preference.identity] {
        identity.issued_at_ms = now - 2000;
        identity.expires_at_ms = now - 1000;
    }
    for (route, body) in [
        ("projects/launch", serde_json::to_value(&project).unwrap()),
        ("preferences", serde_json::to_value(&preference).unwrap()),
    ] {
        let (status, resolution) = request(
            f.router(),
            "POST",
            &format!("/v1/tenants/acme/{route}/resolve"),
            Some(f.token("acme-admin")),
            body.clone(),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            resolution,
            json!({"resolution":"expired","absence_proven":false})
        );
        let (status, _) = request(
            f.router(),
            "PUT",
            &format!("/v1/tenants/acme/{route}"),
            Some(f.token("acme-admin")),
            body.clone(),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, denied) = request(
            f.router(),
            "POST",
            &format!("/v1/tenants/globex/{route}/resolve"),
            Some(f.token("acme-admin")),
            body,
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(denied, json!({"error":"forbidden"}));
    }
    let mut future = project_change("acme-admin", None, "Invalid future identity");
    future.identity.issued_at_ms = now + 3600000;
    future.identity.expires_at_ms = future.identity.issued_at_ms + 300000;
    let (status, _) = request(
        f.router(),
        "PUT",
        "/v1/tenants/acme/projects/launch",
        Some(f.token("acme-admin")),
        serde_json::to_value(future).unwrap(),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(f.workspace.dispatch_count(), 0);
    f.close().await;
}

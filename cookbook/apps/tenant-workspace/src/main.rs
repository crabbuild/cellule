//! Loopback HTTP ingress, private credentials, retained requests, and owned service drain.
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, shutdown_signal};
use cellule_cookbook_tenant_workspace::{APPLICATION, Credentials, Workspace, compile, router};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const HELP: &str = "Cellule tenant workspace\n\n  init CREDENTIAL_FILE\n  prepare CREDENTIAL_FILE SUBJECT RESOURCE INPUT_JSON MUTATION_FILE\n  serve STATE_DIRECTORY CREDENTIAL_FILE [LOOPBACK_ADDRESS]\n  demo STATE_DIRECTORY\n\nThe server requires private random bearer credentials; init refuses overwrite.\nDefault listener: 127.0.0.1:18080. Authentication and tenant policy belong to this application.\nRetain a version-one mutation with its subject, identity and exact change before sending it.";
async fn start(state: PathBuf) -> Result<Arc<LocalNode>> {
    let endpoint = std::env::var("CELLULE_COOKBOOK_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
    Ok(Arc::new(
        LocalNode::start(
            compile()?,
            local_s3_store(&endpoint, "cellule-cookbook")?,
            NodeConfig {
                state_directory: state,
                storage_prefix: object_store::path::Path::from("cookbook/tenant-workspace"),
                application_id: APPLICATION,
            },
        )
        .await?,
    ))
}
async fn serve_listener(
    node: Arc<LocalNode>,
    workspace: Arc<Workspace>,
    listener: tokio::net::TcpListener,
    cancel: CancellationToken,
    announce: bool,
) -> Result<()> {
    if announce {
        println!(
            "{}",
            serde_json::json!({"event":"ready","address":listener.local_addr()?})
        );
        use std::io::Write as _;
        std::io::stdout().flush()?;
    }
    let readiness = node.clone();
    let stopped = cancel.clone();
    // The readiness guard and listener are owned by this awaited future. No
    // listener or accepted request is detached from the explicit drain path.
    let guard = async move {
        loop {
            tokio::select! {
                () = stopped.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
            if !readiness.is_ready() {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    "workspace readiness closed".into(),
                );
            }
        }
    };
    let server = axum::serve(listener, router(workspace.clone()))
        .with_graceful_shutdown(cancel.clone().cancelled_owned());
    tokio::pin!(guard);
    let mut server = Box::pin(std::future::IntoFuture::into_future(server));
    let result = tokio::select! {
        result = &mut server => { workspace.stop_admission(); return result.map_err(Into::into); },
        signal = shutdown_signal() => signal.map_err(Into::into),
        result = &mut guard => result,
    };
    workspace.stop_admission();
    cancel.cancel();
    let drained = tokio::time::timeout(Duration::from_secs(20), server).await;
    match drained {
        Ok(Ok(())) => result,
        Ok(Err(error)) => {
            if let Err(original) = result {
                tracing::error!(%original, "listener failed before drain");
            }
            Err(error.into())
        }
        Err(error) => {
            if let Err(original) = result {
                tracing::error!(%original, "listener failed before drain timeout");
            }
            Err(error.into())
        }
    }
}
fn read_json(path: &Path) -> Result<serde_json::Value> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err("input file exceeds 32 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn token_for(document: &serde_json::Value, subject: &str) -> Result<String> {
    let members = document["members"]
        .as_array()
        .ok_or("credential members are missing")?;
    members
        .iter()
        .find(|value| value["subject"].as_str() == Some(subject))
        .and_then(|value| value["token"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| "subject is absent from credential file".into())
}
fn retain(path: &Path, value: &serde_json::Value) -> Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
fn prepare(
    credentials: &Path,
    subject: &str,
    resource: &str,
    input: &Path,
    output: &Path,
) -> Result<()> {
    use cellule_cookbook_tenant_workspace::{
        Identity, Mutation, PreferenceChange, ProjectChange, ProjectKey, Resource,
    };
    let registry = Credentials::load(credentials)?;
    let principal = registry.authenticate(&token_for(&read_json(credentials)?, subject)?)?;
    let change = read_json(input)?;
    let identity = Identity::new()?;
    let value = if resource == "preferences" {
        serde_json::to_value(Mutation {
            version: 1,
            tenant: principal.tenant(),
            resource: Resource::Preferences,
            subject: principal.subject().into(),
            identity,
            change: serde_json::from_value::<PreferenceChange>(change)?,
        })?
    } else if let Some(key) = resource.strip_prefix("project:") {
        ProjectKey::parse(key)?;
        serde_json::to_value(Mutation {
            version: 1,
            tenant: principal.tenant(),
            resource: Resource::Project { key: key.into() },
            subject: principal.subject().into(),
            identity,
            change: serde_json::from_value::<ProjectChange>(change)?,
        })?
    } else {
        return Err("resource must be project:launch, project:support, or preferences".into());
    };
    retain(output, &value)?;
    println!(
        "{}",
        serde_json::json!({"prepared":output,"request_id":value["identity"]["request_id"]})
    );
    Ok(())
}
async fn answer(
    client: &reqwest::Client,
    address: &str,
    method: reqwest::Method,
    token: &str,
    route: &str,
    body: Option<&serde_json::Value>,
    expected: u16,
) -> Result<serde_json::Value> {
    let mut request = client
        .request(method, format!("http://{address}{route}"))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    let value: serde_json::Value = response.json().await?;
    if status != expected {
        return Err(format!("demo expected HTTP {expected}, received {status}: {value}").into());
    }
    Ok(value)
}
async fn demo_journey(
    workspace: Arc<Workspace>,
    address: String,
    credentials: &Path,
    retained_directory: &Path,
) -> Result<()> {
    use cellule_cookbook_tenant_workspace::{
        Identity, Mutation, PreferenceChange, PreferenceKey, ProjectChange, Resource,
    };
    use serde_json::json;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let document = read_json(credentials)?;
    let run = uuid::Uuid::now_v7().to_string();
    let mut receipts = Vec::new();
    for tenant in ["acme", "globex"] {
        let subject = format!("{tenant}-admin");
        let token = token_for(&document, &subject)?;
        let principal = workspace.authenticate(&token)?;
        let route = format!("/v1/tenants/{tenant}/projects/launch");
        let prior = answer(
            &client,
            &address,
            reqwest::Method::GET,
            &token,
            &route,
            None,
            200,
        )
        .await?;
        let change = serde_json::to_value(Mutation {
            version: 1,
            tenant: principal.tenant(),
            resource: Resource::Project {
                key: "launch".into(),
            },
            subject: subject.clone(),
            identity: Identity::new()?,
            change: ProjectChange {
                expected_revision: prior["project"]["revision"].as_i64(),
                title: format!("{tenant} launch {run}"),
                description: format!("Private {tenant} project"),
            },
        })?;
        retain(
            &retained_directory.join(format!("{run}-{tenant}-project.json")),
            &change,
        )?;
        let result = answer(
            &client,
            &address,
            reqwest::Method::PUT,
            &token,
            &route,
            Some(&change),
            200,
        )
        .await?;
        if answer(
            &client,
            &address,
            reqwest::Method::PUT,
            &token,
            &route,
            Some(&change),
            200,
        )
        .await?
            != result
        {
            return Err("project replay changed its outcome".into());
        }
        let resolved = answer(
            &client,
            &address,
            reqwest::Method::POST,
            &token,
            &format!("{route}/resolve"),
            Some(&change),
            200,
        )
        .await?;
        if resolved["commit_sequence"] != result["receipt"]["commit_sequence"] {
            return Err("resolution changed the original project sequence".into());
        }
        receipts.push(result["receipt"]["cell"].clone());
        let preference_route = format!("/v1/tenants/{tenant}/preferences");
        let current = answer(
            &client,
            &address,
            reqwest::Method::GET,
            &token,
            &preference_route,
            None,
            200,
        )
        .await?;
        let previous = current["preferences"]
            .as_array()
            .and_then(|values| values.iter().find(|value| value["key"] == "theme"));
        let change = serde_json::to_value(Mutation {
            version: 1,
            tenant: principal.tenant(),
            resource: Resource::Preferences,
            subject,
            identity: Identity::new()?,
            change: PreferenceChange {
                key: PreferenceKey::Theme,
                expected: previous
                    .map(|value| serde_json::from_value(value["version"].clone()))
                    .transpose()?,
                value: if tenant == "acme" { "dark" } else { "light" }.into(),
            },
        })?;
        retain(
            &retained_directory.join(format!("{run}-{tenant}-preference.json")),
            &change,
        )?;
        let sent = answer(
            &client,
            &address,
            reqwest::Method::PUT,
            &token,
            &preference_route,
            Some(&change),
            200,
        )
        .await?;
        if answer(
            &client,
            &address,
            reqwest::Method::PUT,
            &token,
            &preference_route,
            Some(&change),
            200,
        )
        .await?
            != sent
        {
            return Err("preference replay changed its outcome".into());
        }
    }
    if receipts[0] == receipts[1] {
        return Err("tenant project identities collided".into());
    }
    let acme = token_for(&document, "acme-admin")?;
    let before = workspace.dispatch_count();
    for (method, suffix) in [
        (reqwest::Method::GET, "projects/launch"),
        (reqwest::Method::PUT, "projects/launch"),
        (reqwest::Method::POST, "projects/launch/resolve"),
        (reqwest::Method::GET, "preferences"),
        (reqwest::Method::PUT, "preferences"),
        (reqwest::Method::POST, "preferences/resolve"),
        (reqwest::Method::GET, "admin/members"),
    ] {
        answer(
            &client,
            &address,
            method,
            &acme,
            &format!("/v1/tenants/globex/{suffix}"),
            Some(&json!({"target":"globex"})),
            403,
        )
        .await?;
    }
    if workspace.dispatch_count() != before {
        return Err("denied tenant request reached native dispatch".into());
    }
    let members = answer(
        &client,
        &address,
        reqwest::Method::GET,
        &acme,
        "/v1/tenants/acme/admin/members?limit=1",
        None,
        200,
    )
    .await?;
    if members["members"]
        .as_array()
        .is_none_or(|values| values.len() != 1)
        || members["members"][0]["tenant"] != "acme"
    {
        return Err("admin membership scope or bound failed".into());
    }
    println!(
        "{}",
        json!({"scenario":"passed","run":run,"retained":retained_directory,"project_cells":receipts,"checks":["real-loopback-http","opaque-bearer-verification","identical-project-tenant-isolation","tenant-kv-preferences","retained-command-replay","original-outcome-resolution","every-route-cross-tenant-denial","zero-dispatch-on-denial","bounded-tenant-admin","http-and-node-drain"]})
    );
    Ok(())
}
async fn demo(node: Arc<LocalNode>, state: &Path) -> Result<()> {
    let state_name = state
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("demo state directory needs a UTF-8 basename")?;
    // Append to the actual state basename so even a directory named
    // tenant-workspace-demo keeps credentials and retained input in a sibling.
    let configuration = state.with_file_name(format!("{state_name}-demo-inputs"));
    std::fs::create_dir_all(&configuration)?;
    let credentials = configuration.join("credentials.json");
    if !credentials.exists() {
        Credentials::initialize(&credentials)?;
    }
    let workspace = Arc::new(Workspace::new(
        node.clone(),
        Arc::new(Credentials::load(&credentials)?),
    ));
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?.to_string();
    let cancel = CancellationToken::new();
    let server = serve_listener(node, workspace.clone(), listener, cancel.clone(), false);
    tokio::pin!(server);
    let journey = demo_journey(workspace, address, &credentials, &configuration);
    let result = tokio::select! {
        result = journey => result,
        result = &mut server => return match result { Ok(()) => Err("demo listener stopped before journey finished".into()), Err(error) => Err(error) },
    };
    cancel.cancel();
    let drained = tokio::time::timeout(Duration::from_secs(25), &mut server).await;
    match drained {
        Ok(Ok(())) => result,
        Ok(Err(error)) => {
            if let Err(original) = result {
                tracing::error!(%original, "demo failed before HTTP drain");
            }
            Err(error)
        }
        Err(error) => {
            if let Err(original) = result {
                tracing::error!(%original, "demo failed before HTTP drain timeout");
            }
            Err(error.into())
        }
    }
}
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()?;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || matches!(args[0].as_str(), "help" | "--help") {
        println!("{HELP}");
        return Ok(());
    }
    if let [operation, path] = args.as_slice()
        && operation == "init"
    {
        Credentials::initialize(Path::new(path))?;
        println!("{}", serde_json::json!({"credentials":path,"created":true}));
        return Ok(());
    }
    if let [operation, credentials, subject, resource, input, output] = args.as_slice()
        && operation == "prepare"
    {
        return prepare(
            Path::new(credentials),
            subject,
            resource,
            Path::new(input),
            Path::new(output),
        );
    }
    match args.as_slice() {
        [operation, state, credentials, rest @ ..] if operation == "serve" && rest.len() <= 1 => {
            let credentials = Arc::new(Credentials::load(Path::new(credentials))?);
            let address = rest
                .first()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18080));
            if !address.ip().is_loopback() {
                return Err("the local credential profile requires a loopback listener".into());
            }
            let node = start(state.into()).await?;
            let result = async {
                let listener = tokio::net::TcpListener::bind(address).await?;
                let workspace = Arc::new(Workspace::new(node.clone(), credentials));
                serve_listener(
                    node.clone(),
                    workspace,
                    listener,
                    CancellationToken::new(),
                    true,
                )
                .await
            }
            .await;
            let drain = node.shutdown().await;
            if let Err(error) = drain {
                if let Err(original) = result {
                    tracing::error!(%original,"ingress failed before drain");
                }
                return Err(error.into());
            }
            result
        }
        [operation, state] if operation == "demo" => {
            let node = start(state.into()).await?;
            let result = demo(node.clone(), Path::new(state)).await;
            let drain = node.shutdown().await;
            if let Err(error) = drain {
                if let Err(original) = result {
                    tracing::error!(%original, "demo failed before node drain");
                }
                return Err(error.into());
            }
            result
        }
        _ => Err(HELP.into()),
    }
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("tenant-workspace: {error}");
        let mut source = error.source();
        while let Some(error) = source {
            eprintln!("caused by: {error}");
            source = error.source();
        }
        std::process::exit(1);
    }
}

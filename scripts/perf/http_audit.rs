//! Comparison fixture: validate every acknowledged row and its exact stored retry.
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
#[derive(Deserialize)]
struct Config {
    address: String,
    observations: String,
    cold: bool,
}
#[derive(Deserialize, Serialize, PartialEq)]
struct Order {
    id: i64,
    total_cents: i64,
    value: String,
}
#[derive(Deserialize)]
struct Receipt {
    cell: String,
    incarnation: String,
    commit_sequence: u64,
}
#[derive(Deserialize)]
struct Observation {
    output: Order,
    receipt: Receipt,
    #[serde(default)]
    request: serde_json::Value,
}
#[derive(Default, Serialize)]
struct Counts {
    checked: u64,
    retries_checked: u64,
    errors: u64,
    changed_incarnations: u64,
    first_errors: Vec<String>,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("configuration path required")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let observations: Vec<Observation> =
        serde_json::from_slice(&std::fs::read(&config.observations)?)?;
    if observations.iter().any(|observation| !observation.request.is_object()) {
        return Err("audit requires the original request for every acknowledgement".into());
    }
    let observations = Arc::new(observations);
    let config = Arc::new(config);
    let client = reqwest::Client::builder()
        .http1_only()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..128 {
        let (observations, config, client) = (observations.clone(), config.clone(), client.clone());
        tasks.spawn(async move {
            let mut counts = Counts::default();
            for expected in observations.iter().skip(index).step_by(128) {
                let result = async {
                    let actual: Observation = client
                        .get(format!(
                            "http://{}/orders/{}",
                            config.address, expected.output.id
                        ))
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    if actual.output != expected.output
                        || actual.receipt.cell != expected.receipt.cell
                        || actual.receipt.commit_sequence < expected.receipt.commit_sequence
                        || (!config.cold
                            && actual.receipt.incarnation != expected.receipt.incarnation)
                    {
                        return Err(format!(
                            "row {} differs from acknowledged output or receipt",
                            expected.output.id
                        )
                        .into());
                    }
                    let retry: serde_json::Value = client
                        .post(format!("http://{}/orders", config.address))
                        .json(&expected.request)
                        .send().await?.error_for_status()?.json().await?;
                    if retry["output"] != serde_json::to_value(&expected.output)?
                        || retry["receipt"]["cell"].as_str() != Some(expected.receipt.cell.as_str())
                        || retry["receipt"]["commit_sequence"].as_u64() != Some(expected.receipt.commit_sequence)
                        || (!config.cold && retry["receipt"]["incarnation"].as_str()
                            != Some(expected.receipt.incarnation.as_str()))
                    {
                        return Err(format!("row {} stored retry differs", expected.output.id).into());
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        actual.receipt.incarnation != expected.receipt.incarnation,
                    )
                }
                .await;
                counts.checked += 1;
                match result {
                    Ok(changed) => {
                        counts.changed_incarnations += u64::from(changed);
                        counts.retries_checked += 1;
                    }
                    Err(error) => {
                        counts.errors += 1;
                        if counts.first_errors.len() < 4 {
                            counts.first_errors.push(error.to_string());
                        }
                    }
                }
            }
            counts
        });
    }
    let mut total = Counts::default();
    while let Some(result) = tasks.join_next().await {
        let counts = result?;
        total.checked += counts.checked;
        total.retries_checked += counts.retries_checked;
        total.errors += counts.errors;
        total.changed_incarnations += counts.changed_incarnations;
        total.first_errors.extend(counts.first_errors);
    }
    println!("{}", serde_json::to_string(&total)?);
    if total.errors > 0 {
        return Err("recovery audit failed".into());
    }
    Ok(())
}

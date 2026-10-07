//! Comparison fixture: validate every acknowledged row and its exact stored retry.
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, Read},
    sync::Arc,
    time::Duration,
};
type AuditError = Box<dyn std::error::Error + Send + Sync>;
const MAX_OBSERVATION_BYTES: u64 = 64 * 1024;
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

fn read_observation(reader: &mut impl BufRead) -> Result<Option<Observation>, AuditError> {
    let mut bytes = Vec::new();
    let read = Read::take(reader, MAX_OBSERVATION_BYTES + 1).read_until(b'\n', &mut bytes)?;
    if read == 0 {
        return Ok(None);
    }
    if read as u64 > MAX_OBSERVATION_BYTES {
        return Err("audit observation exceeds the fixture record bound".into());
    }
    let observation: Observation = serde_json::from_slice(&bytes)?;
    if !observation.request.is_object() {
        return Err("audit requires the original request for every acknowledgement".into());
    }
    Ok(Some(observation))
}

impl Counts {
    fn merge(&mut self, counts: Self) {
        self.checked += counts.checked;
        self.retries_checked += counts.retries_checked;
        self.errors += counts.errors;
        self.changed_incarnations += counts.changed_incarnations;
        self.first_errors.extend(
            counts
                .first_errors
                .into_iter()
                .take(4_usize.saturating_sub(self.first_errors.len())),
        );
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("configuration path required")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let config = Arc::new(config);
    let (sender, mut observations) = tokio::sync::mpsc::channel(256);
    let source = config.observations.clone();
    let reading = tokio::task::spawn_blocking(move || {
        let mut reader = std::io::BufReader::new(std::fs::File::open(source)?);
        while let Some(observation) = read_observation(&mut reader)? {
            sender
                .blocking_send(observation)
                .map_err(|_| "audit consumer stopped")?;
        }
        Ok::<_, AuditError>(())
    });
    let client = reqwest::Client::builder()
        .http1_only()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut tasks = tokio::task::JoinSet::new();
    let mut total = Counts::default();
    while let Some(expected) = observations.recv().await {
        if tasks.len() >= 128
            && let Some(counts) = tasks.join_next().await
        {
            total.merge(counts?);
        }
        let (config, client) = (config.clone(), client.clone());
        tasks.spawn(async move {
            let mut counts = Counts::default();
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
                    || (!config.cold && actual.receipt.incarnation != expected.receipt.incarnation)
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
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if retry["output"] != serde_json::to_value(&expected.output)?
                    || retry["receipt"]["cell"].as_str() != Some(expected.receipt.cell.as_str())
                    || retry["receipt"]["commit_sequence"].as_u64()
                        != Some(expected.receipt.commit_sequence)
                    || (!config.cold
                        && retry["receipt"]["incarnation"].as_str()
                            != Some(expected.receipt.incarnation.as_str()))
                {
                    return Err(format!("row {} stored retry differs", expected.output.id).into());
                }
                Ok::<_, AuditError>(actual.receipt.incarnation != expected.receipt.incarnation)
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
            counts
        });
    }
    while let Some(result) = tasks.join_next().await {
        total.merge(result?);
    }
    // The producer may fail after earlier records were checked. Such a partial
    // audit must fail even when all dispatched GET/retry pairs succeeded.
    reading.await??;
    if total.checked == 0 {
        return Err("empty acknowledgement audit".into());
    }
    println!("{}", serde_json::to_string(&total)?);
    if total.errors > 0 {
        return Err("recovery audit failed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROW: &[u8] = br#"{"output":{"id":1,"total_cents":2,"value":"p"},"receipt":{"cell":"c","incarnation":"i","commit_sequence":1},"request":{"id":1}}"#;

    #[test]
    fn stream_reads_each_record_and_requires_original_requests() {
        let bytes = [ROW, b"\n", ROW].concat();
        let mut reader = bytes.as_slice();
        assert!(read_observation(&mut reader).unwrap().is_some());
        assert!(read_observation(&mut reader).unwrap().is_some());
        assert!(read_observation(&mut reader).unwrap().is_none());
        let missing = String::from_utf8(ROW.to_vec())
            .unwrap()
            .replace(r#""request":{"id":1}"#, r#""request":null"#);
        assert!(read_observation(&mut missing.as_bytes()).is_err());
    }

    #[test]
    fn malformed_or_oversize_record_fails_after_a_valid_prefix() {
        for tail in [
            b"{broken".to_vec(),
            vec![b'x'; MAX_OBSERVATION_BYTES as usize + 1],
        ] {
            let bytes = [ROW, b"\n", &tail].concat();
            let mut reader = bytes.as_slice();
            assert!(read_observation(&mut reader).unwrap().is_some());
            assert!(read_observation(&mut reader).is_err());
        }
    }
}

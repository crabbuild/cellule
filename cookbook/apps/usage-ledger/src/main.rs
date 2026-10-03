//! Runnable synthetic Usage Ledger demonstration and cold inspection CLI.
mod demo;

use cellule_cookbook_usage_ledger::*;
use std::{path::Path, process::ExitCode};

const HELP: &str = "Cellule usage ledger\n\n  demo STATE\n  inspect STATE TENANT PERIOD_HEX\n\nThe demo retains its exact period roster and event identities in STATE.\nThe embedding owns authorization, tenant selection, rates, and storage credentials.";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();
    match run(std::env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            eprintln!("usage-ledger: {source}");
            ExitCode::FAILURE
        }
    }
}

async fn run(arguments: Vec<String>) -> Result<(), BoxError> {
    match arguments.as_slice() {
        [operation, state] if operation == "demo" => demo::run(Path::new(state)).await,
        [operation, state, tenant, period] if operation == "inspect" => {
            let id = parse_period(period)?;
            let service = demo::start_service(Path::new(state), tenant).await?;
            let period = PeriodClient::new(service.handle.clone(), id)?;
            service.node.open_cell(period.target(), &Periods).await?;
            let observed = period.get(None).await?;
            let output = emit(&serde_json::json!({
                "period": observed.output,
                "receipt": receipt(observed.receipt)
            }));
            let drained = service.node.shutdown().await;
            output?;
            drained?;
            Ok(())
        }
        [operation] if operation == "help" || operation == "--help" || operation == "-h" => {
            println!("{HELP}");
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}

fn emit(value: &impl serde::Serialize) -> Result<(), BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 1 << 20 {
        return Err("usage-ledger output exceeds one MiB".into());
    }
    use std::io::Write as _;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

fn receipt(value: cellule_runtime::Receipt) -> serde_json::Value {
    serde_json::json!({
        "cell": format!("{:?}", value.cell),
        "incarnation": format!("{:?}", value.incarnation),
        "commit_sequence": value.commit_sequence
    })
}

fn parse_period(value: &str) -> Result<[u8; 16], BoxError> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("period identity requires 32 lowercase hexadecimal digits".into());
    }
    let mut result = [0_u8; 16];
    for (index, output) in result.iter_mut().enumerate() {
        let start = index * 2;
        *output = u8::from_str_radix(&value[start..start + 2], 16)?;
    }
    if result == [0; 16] {
        return Err("zero usage-ledger period identity".into());
    }
    Ok(result)
}

//! Persistent local CLI and HTTP embedding; reusable domain code lives in the library.
mod ingress;
use std::process::ExitCode;
#[tokio::main]
async fn main() -> ExitCode {
    match ingress::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(source) => {
            ingress::print_error(source.as_ref());
            ExitCode::FAILURE
        }
    }
}

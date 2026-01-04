#![forbid(unsafe_code)]

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    gwtui::cli::main().await
}

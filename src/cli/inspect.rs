#![forbid(unsafe_code)]

use crate::core::{git::Git, worktree::WorktreeManager};
use clap::Parser;
use serde::Serialize;
use std::path::Path;
use std::process::{Command, ExitCode};

#[derive(Debug, Parser)]
pub struct DoctorArgs {
    /// Emit a machine-readable diagnostic snapshot
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct Diagnostic {
    version: &'static str,
    git_version: String,
    config_path: String,
    repository: Option<String>,
    worktrees: usize,
    missing_worktrees: usize,
}

pub async fn doctor(args: DoctorArgs) -> anyhow::Result<ExitCode> {
    let cfg = super::load_cfg().await?;
    let out = Command::new("git").arg("--version").output()?;
    anyhow::ensure!(out.status.success(), "git --version failed");
    let git = Git::from_cwd().ok();
    let worktrees = match &git {
        Some(git) => WorktreeManager::new(git.clone(), cfg).list()?,
        None => Vec::new(),
    };
    let report = Diagnostic {
        version: env!("CARGO_PKG_VERSION"),
        git_version: String::from_utf8(out.stdout)?.trim().to_owned(),
        config_path: crate::config::default_paths()?
            .config_file
            .display()
            .to_string(),
        repository: git
            .as_ref()
            .map(|git| git.repo_root().display().to_string()),
        worktrees: worktrees.len(),
        missing_worktrees: worktrees
            .iter()
            .filter(|wt| !Path::new(&wt.path).exists())
            .count(),
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "gwtui {}\n{}\nConfig: {}",
            report.version, report.git_version, report.config_path
        );
        println!(
            "Repository: {}",
            report
                .repository
                .as_deref()
                .unwrap_or("not inside a repository")
        );
        println!(
            "Worktrees: {} ({} missing)",
            report.worktrees, report.missing_worktrees
        );
    }
    Ok(ExitCode::SUCCESS)
}

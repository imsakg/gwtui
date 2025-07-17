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
    locked_worktrees: usize,
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
        locked_worktrees: match &git {
            Some(git) => git
                .list_worktrees_porcelain()?
                .split('\0')
                .filter(|field| *field == "locked" || field.starts_with("locked "))
                .count(),
            None => 0,
        },
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
            "Worktrees: {} ({} missing, {} locked)",
            report.worktrees, report.missing_worktrees, report.locked_worktrees
        );
    }
    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Parser)]
pub struct BranchArgs {
    /// Include remote-tracking branches (does not fetch)
    #[arg(short = 'a', long)]
    pub all: bool,
    #[arg(long)]
    pub json: bool,
    /// Case-insensitive branch substring
    #[arg(long)]
    pub filter: Option<String>,
}

pub fn branches(args: BranchArgs) -> anyhow::Result<ExitCode> {
    let mut branches = Git::from_cwd()?.list_branches(args.all)?;
    if let Some(pattern) = args.filter {
        let pattern = pattern.to_lowercase();
        branches.retain(|branch| branch.name.to_lowercase().contains(&pattern));
    }
    branches.sort_by(|a, b| a.name.cmp(&b.name));
    if args.json {
        let rows: Vec<_> = branches
            .iter()
            .map(|branch| {
                serde_json::json!({
                    "name": branch.name,
                    "is_current": branch.is_current,
                    "is_remote": branch.is_remote,
                    "commit": branch.last_commit.hash,
                    "subject": branch.last_commit.message,
                    "author": branch.last_commit.author,
                    "date": branch.last_commit.date_iso,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        let mut table = crate::output::table::Table::new(["BRANCH", "CURRENT", "REMOTE", "COMMIT"]);
        for branch in branches {
            table.row([
                branch.name,
                branch.is_current.to_string(),
                branch.is_remote.to_string(),
                branch.last_commit.hash,
            ]);
        }
        table.print()?;
    }
    Ok(ExitCode::SUCCESS)
}

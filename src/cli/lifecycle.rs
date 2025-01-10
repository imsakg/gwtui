#![forbid(unsafe_code)]

use crate::core::{git::Git, worktree::WorktreeManager};
use clap::Parser;
use std::process::ExitCode;

#[derive(Debug, Parser)]
pub struct LockArgs {
    /// Exact branch/path or unambiguous substring
    pub pattern: String,
    /// Explain why this worktree must not be pruned
    #[arg(long)]
    pub reason: Option<String>,
}

pub async fn lock(args: LockArgs) -> anyhow::Result<ExitCode> {
    let manager = WorktreeManager::new(Git::from_cwd()?, super::load_cfg().await?);
    let worktree = manager.resolve(&args.pattern)?;
    manager.lock(std::path::Path::new(&worktree.path), args.reason.as_deref())?;
    println!("Locked {}", worktree.path);
    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Parser)]
pub struct SelectorArgs {
    /// Exact branch/path or unambiguous substring
    pub pattern: String,
}

pub async fn unlock(args: SelectorArgs) -> anyhow::Result<ExitCode> {
    let manager = WorktreeManager::new(Git::from_cwd()?, super::load_cfg().await?);
    let worktree = manager.resolve(&args.pattern)?;
    manager.unlock(std::path::Path::new(&worktree.path))?;
    println!("Unlocked {}", worktree.path);
    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Parser)]
pub struct MoveArgs {
    pub pattern: String,
    pub destination: std::path::PathBuf,
}

pub async fn move_worktree(args: MoveArgs) -> anyhow::Result<ExitCode> {
    let manager = WorktreeManager::new(Git::from_cwd()?, super::load_cfg().await?);
    let worktree = manager.resolve(&args.pattern)?;
    manager.move_worktree(std::path::Path::new(&worktree.path), &args.destination)?;
    println!("Moved {} to {}", worktree.path, args.destination.display());
    Ok(ExitCode::SUCCESS)
}

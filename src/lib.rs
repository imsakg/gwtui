//! Git remains the source of truth; gwtui delegates mutations to Git itself.
//!
//! Repository-scoped callers can use the same lifecycle API as the CLI:
//! ```no_run
//! use gwtui::{config::Config, core::{git::Git, worktree::WorktreeManager}};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let git = Git::from_cwd()?;
//! let manager = WorktreeManager::new(git, Config::default());
//! for worktree in manager.list()? {
//!     println!("{} {}", worktree.branch, worktree.path);
//! }
//! let main = manager.resolve("main")?;
//! assert!(main.is_main);
//! # Ok(())
//! # }
//! ```
//!
//! Explicit API paths are literal filesystem paths. Naming templates and
//! environment expansion apply only when gwtui generates a destination.
//! Removal refuses the main checkout; Git still enforces dirty/locked guards.

#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc)]

pub mod cli;
pub mod config;
pub mod core;
pub mod error;
pub mod mux;
pub mod output;
pub mod task;
pub mod tui;

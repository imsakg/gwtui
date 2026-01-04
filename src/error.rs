#![forbid(unsafe_code)]

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GwtuiError {
    #[error("not inside a git repository")]
    NotInGitRepo,

    #[error("git is required but was not found in PATH")]
    GitNotFound,

    #[error("zellij is required but was not found in PATH")]
    ZellijNotFound,

    #[error("config error: {0}")]
    Config(String),

    #[error("invalid config key '{0}'")]
    InvalidConfigKey(String),

    #[error("invalid config value for '{key}': {msg}")]
    InvalidConfigValue { key: String, msg: String },

    #[error("worktree not found matching pattern: {0}")]
    WorktreeNotFound(String),

    #[error("multiple worktrees match pattern: {0}")]
    AmbiguousWorktree(String),

    #[error("operation cancelled")]
    Cancelled,

    #[error("io error at {path}: {source}")]
    IoPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Other(String),
}

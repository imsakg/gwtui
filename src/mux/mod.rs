#![forbid(unsafe_code)]

pub mod zellij;

use std::path::Path;

use crate::error::GwtuiError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    pub name: String,
}

pub trait Mux {
    fn ensure_session_background(&self, name: &str) -> Result<(), GwtuiError>;
    fn list_sessions(&self) -> Result<Vec<SessionInfo>, GwtuiError>;
    fn attach(&self, name: &str) -> Result<(), GwtuiError>;
    fn kill(&self, name: &str) -> Result<(), GwtuiError>;
    fn kill_all(&self) -> Result<(), GwtuiError>;

    fn run(
        &self,
        session: &str,
        cwd: &Path,
        pane_name: Option<&str>,
        close_on_exit: bool,
        shell: &str,
        cmd: &str,
    ) -> Result<(), GwtuiError>;

    fn require_session_for_run(&self) -> bool;
}

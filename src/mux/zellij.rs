#![forbid(unsafe_code)]

use std::path::Path;
use std::process::Command;

use crate::error::GwtuiError;
use crate::mux::{Mux, SessionInfo};

#[derive(Debug, Clone)]
pub struct ZellijMux {
    pub zellij_command: String,
    pub require_session_for_run: bool,
}

impl ZellijMux {
    #[must_use]
    pub fn new(zellij_command: String, require_session_for_run: bool) -> Self {
        Self {
            zellij_command,
            require_session_for_run,
        }
    }

    fn ensure_available(&self) -> Result<(), GwtuiError> {
        match Command::new(&self.zellij_command)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(GwtuiError::ZellijNotFound),
            Err(e) => Err(GwtuiError::Other(format!("failed to run zellij: {e}"))),
        }
    }

    fn has_session(&self, name: &str) -> Result<bool, GwtuiError> {
        Ok(self.list_sessions()?.iter().any(|s| s.name == name))
    }
}

impl Mux for ZellijMux {
    fn ensure_session_background(&self, name: &str) -> Result<(), GwtuiError> {
        self.ensure_available()?;
        let status = Command::new(&self.zellij_command)
            .args(["attach", "--create-background", name])
            .status()
            .map_err(|e| GwtuiError::Other(format!("failed to run zellij attach: {e}")))?;
        if status.success() {
            Ok(())
        } else {
            Err(GwtuiError::Other(format!(
                "zellij attach --create-background failed with exit code {code}",
                code = status.code().unwrap_or(1)
            )))
        }
    }

    fn list_sessions(&self) -> Result<Vec<SessionInfo>, GwtuiError> {
        self.ensure_available()?;
        let out = Command::new(&self.zellij_command)
            .args(["list-sessions", "--short", "--no-formatting"])
            .output()
            .map_err(|e| GwtuiError::Other(format!("failed to run zellij: {e}")))?;

        // When no sessions exist, zellij exits non-zero. Treat as empty list.
        if !out.status.success() {
            return Ok(Vec::new());
        }

        let stdout = String::from_utf8_lossy(&out.stdout);
        let mut sessions = Vec::new();
        for line in stdout.lines() {
            let name = line.trim();
            if name.is_empty() {
                continue;
            }
            // zellij list-sessions prints like: "name [Created ...]" in some versions.
            // Keep first whitespace-delimited token as the session name.
            let token = name.split_whitespace().next().unwrap_or(name).to_owned();
            sessions.push(SessionInfo { name: token });
        }
        Ok(sessions)
    }

    fn attach(&self, name: &str) -> Result<(), GwtuiError> {
        self.ensure_available()?;
        let status = Command::new(&self.zellij_command)
            .args(["attach", name])
            .status()
            .map_err(|e| GwtuiError::Other(format!("failed to run zellij attach: {e}")))?;
        if status.success() {
            Ok(())
        } else {
            Err(GwtuiError::Other(format!(
                "zellij attach failed with exit code {code}",
                code = status.code().unwrap_or(1)
            )))
        }
    }

    fn kill(&self, name: &str) -> Result<(), GwtuiError> {
        self.ensure_available()?;
        let status = Command::new(&self.zellij_command)
            .args(["kill-session", name])
            .status()
            .map_err(|e| GwtuiError::Other(format!("failed to run zellij kill-sessions: {e}")))?;
        if status.success() {
            Ok(())
        } else {
            Err(GwtuiError::Other(format!(
                "zellij kill-session failed with exit code {code}",
                code = status.code().unwrap_or(1)
            )))
        }
    }

    fn kill_all(&self) -> Result<(), GwtuiError> {
        self.ensure_available()?;
        let status = Command::new(&self.zellij_command)
            .args(["kill-all-sessions"])
            .status()
            .map_err(|e| {
                GwtuiError::Other(format!("failed to run zellij kill-all-sessions: {e}"))
            })?;
        if status.success() {
            Ok(())
        } else {
            Err(GwtuiError::Other(format!(
                "zellij kill-all-sessions failed with exit code {code}",
                code = status.code().unwrap_or(1)
            )))
        }
    }

    fn run(
        &self,
        session: &str,
        cwd: &Path,
        pane_name: Option<&str>,
        close_on_exit: bool,
        shell: &str,
        cmd: &str,
    ) -> Result<(), GwtuiError> {
        self.ensure_available()?;

        if !self.has_session(session)? {
            if self.require_session_for_run {
                return Err(GwtuiError::Other(format!(
                    "zellij session '{session}' does not exist (create it with 'zellij attach --create-background {session}')"
                )));
            }
            self.ensure_session_background(session)?;
        }

        let mut command = Command::new(&self.zellij_command);
        command.args(["--session", session, "run"]);
        if close_on_exit {
            command.arg("--close-on-exit");
        }
        command.args(["--cwd", &cwd.to_string_lossy()]);
        if let Some(name) = pane_name {
            command.args(["-n", name]);
        }
        command.args(["--", shell, "-lc", cmd]);

        let status = command
            .status()
            .map_err(|e| GwtuiError::Other(format!("failed to run zellij run: {e}")))?;

        if status.success() {
            Ok(())
        } else {
            Err(GwtuiError::Other(format!(
                "zellij run failed with exit code {code}",
                code = status.code().unwrap_or(1)
            )))
        }
    }

    fn require_session_for_run(&self) -> bool {
        self.require_session_for_run
    }
}

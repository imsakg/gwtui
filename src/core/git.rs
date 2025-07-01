#![forbid(unsafe_code)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use crate::error::GwtuiError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitInfo {
    pub hash: String,
    pub message: String,
    pub author: String,
    pub date_iso: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Branch {
    pub name: String,
    pub is_current: bool,
    pub is_remote: bool,
    pub last_commit: CommitInfo,
}

#[derive(Debug, Clone)]
pub struct Git {
    repo_root: PathBuf,
}

impl Git {
    pub fn from_cwd() -> Result<Self, GwtuiError> {
        let cwd = std::env::current_dir()
            .map_err(|e| GwtuiError::Other(format!("failed to get cwd: {e}")))?;
        let repo_root = find_repo_root(&cwd).ok_or(GwtuiError::NotInGitRepo)?;
        Ok(Self { repo_root })
    }

    pub fn from_dir(dir: &Path) -> Result<Self, GwtuiError> {
        let repo_root = find_repo_root(dir).ok_or(GwtuiError::NotInGitRepo)?;
        Ok(Self { repo_root })
    }

    #[must_use]
    pub fn new(repo_root: PathBuf) -> Self {
        Self { repo_root }
    }

    #[must_use]
    pub fn repo_root(&self) -> &Path {
        &self.repo_root
    }

    pub fn get_repository_url(&self) -> Result<String, GwtuiError> {
        let out = self.run(&["remote", "get-url", "origin"])?;
        Ok(out.trim().to_owned())
    }

    pub fn list_worktrees_porcelain(&self) -> Result<String, GwtuiError> {
        let out = self.run_raw(&["worktree", "list", "--porcelain", "-z"])?;
        if !out.status.success() {
            return Err(GwtuiError::Other(format!(
                "git worktree list: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        decode_worktree_porcelain(out.stdout)
    }

    pub fn add_worktree(
        &self,
        path: &Path,
        branch: &str,
        create_branch: bool,
    ) -> Result<(), GwtuiError> {
        self.validate_branch(branch)?;
        if create_branch {
            self.run_os(&[
                "worktree".as_ref(),
                "add".as_ref(),
                "-b".as_ref(),
                branch.as_ref(),
                "--".as_ref(),
                path.as_os_str(),
            ])?;
        } else {
            self.run_os(&[
                "worktree".as_ref(),
                "add".as_ref(),
                "--".as_ref(),
                path.as_os_str(),
                branch.as_ref(),
            ])?;
        }
        Ok(())
    }

    pub fn add_worktree_from_base(
        &self,
        path: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), GwtuiError> {
        self.validate_branch(branch)?;
        let mut args: Vec<&OsStr> = vec![
            "worktree".as_ref(),
            "add".as_ref(),
            "-b".as_ref(),
            branch.as_ref(),
            "--".as_ref(),
            path.as_os_str(),
        ];
        let base = base
            .map(|revision| self.resolve_commit(revision))
            .transpose()?;
        if let Some(base) = &base {
            args.push(base.as_ref());
        }
        self.run_os(&args)?;
        Ok(())
    }

    pub fn add_detached_worktree(
        &self,
        path: &Path,
        revision: Option<&str>,
    ) -> Result<(), GwtuiError> {
        let revision = self.resolve_commit(revision.unwrap_or("HEAD"))?;
        self.run_os(&[
            "worktree".as_ref(),
            "add".as_ref(),
            "--detach".as_ref(),
            "--".as_ref(),
            path.as_os_str(),
            revision.as_ref(),
        ])?;
        Ok(())
    }

    fn resolve_commit(&self, revision: &str) -> Result<String, GwtuiError> {
        if revision.is_empty() || revision.starts_with('-') || revision.contains('\0') {
            return Err(GwtuiError::Other(format!("invalid revision: {revision:?}")));
        }
        self.run(&[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{revision}^{{commit}}"),
        ])
        .map(|out| out.trim().to_owned())
        .map_err(|error| GwtuiError::Other(format!("invalid revision {revision:?}: {error}")))
    }

    pub fn remove_worktree(&self, path: &Path, force: bool) -> Result<(), GwtuiError> {
        let mut args: Vec<&OsStr> = vec!["worktree".as_ref(), "remove".as_ref()];
        if force {
            args.push("--force".as_ref());
        }
        args.extend(["--".as_ref(), path.as_os_str()]);
        self.run_os(&args)?;
        Ok(())
    }

    pub fn lock_worktree(&self, path: &Path, reason: Option<&str>) -> Result<(), GwtuiError> {
        let mut args: Vec<&OsStr> = vec!["worktree".as_ref(), "lock".as_ref()];
        if let Some(reason) = reason {
            args.extend([OsStr::new("--reason"), OsStr::new(reason)]);
        }
        args.extend(["--".as_ref(), path.as_os_str()]);
        self.run_os(&args)?;
        Ok(())
    }

    pub fn unlock_worktree(&self, path: &Path) -> Result<(), GwtuiError> {
        self.run_os(&[
            "worktree".as_ref(),
            "unlock".as_ref(),
            "--".as_ref(),
            path.as_os_str(),
        ])?;
        Ok(())
    }

    pub fn move_worktree(&self, source: &Path, destination: &Path) -> Result<(), GwtuiError> {
        self.run_os(&[
            OsStr::new("worktree"),
            OsStr::new("move"),
            OsStr::new("--"),
            source.as_os_str(),
            destination.as_os_str(),
        ])?;
        Ok(())
    }

    pub fn repair_worktrees(&self, paths: &[PathBuf]) -> Result<(), GwtuiError> {
        let mut args = vec![
            OsStr::new("worktree"),
            OsStr::new("repair"),
            OsStr::new("--"),
        ];
        args.extend(paths.iter().map(|path| path.as_os_str()));
        self.run_os(&args)?;
        Ok(())
    }

    pub fn prune_worktrees(&self) -> Result<(), GwtuiError> {
        self.prune_worktrees_with_options(false, None)?;
        Ok(())
    }

    pub fn prune_worktrees_with_options(
        &self,
        dry_run: bool,
        expire: Option<&str>,
    ) -> Result<String, GwtuiError> {
        let mut args = vec!["worktree", "prune", "--verbose"];
        if dry_run {
            args.push("--dry-run");
        }
        let expiry;
        if let Some(expire) = expire {
            expiry = format!("--expire={expire}");
            args.push(&expiry);
        }
        let args: Vec<&OsStr> = args.into_iter().map(OsStr::new).collect();
        let out = self.run_os_output(&args)?;
        Ok(format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ))
    }

    pub fn delete_branch(&self, branch: &str, force: bool) -> Result<(), GwtuiError> {
        self.validate_branch(branch)?;
        if force {
            let _ = self.run(&["branch", "-D", "--", branch])?;
        } else {
            let _ = self.run(&["branch", "-d", "--", branch])?;
        }
        Ok(())
    }

    fn validate_branch(&self, branch: &str) -> Result<(), GwtuiError> {
        if branch.is_empty() || branch.starts_with('-') || branch == "HEAD" || branch.contains('\0')
        {
            return Err(GwtuiError::Other(format!(
                "invalid branch name: {branch:?}"
            )));
        }
        self.run(&["check-ref-format", &format!("refs/heads/{branch}")])
            .map(|_| ())
            .map_err(|_| GwtuiError::Other(format!("invalid branch name: {branch:?}")))
    }

    pub fn list_branches(&self, include_remote: bool) -> Result<Vec<Branch>, GwtuiError> {
        let mut args = vec![
            "for-each-ref",
            "--format=%(refname)%00%(HEAD)%00%(committerdate:iso)%00%(objectname)%00%(subject)%00%(authorname)%00%(symref)",
            "refs/heads/",
        ];
        if include_remote {
            args.push("refs/remotes/");
        }
        let out = self.run(&args)?;
        let mut branches = Vec::new();
        for line in out.lines() {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('\0').collect();
            if parts.len() != 7 || !parts[6].is_empty() {
                continue;
            }
            let (name, is_remote) = if let Some(name) = parts[0].strip_prefix("refs/remotes/") {
                (name.to_owned(), true)
            } else if let Some(name) = parts[0].strip_prefix("refs/heads/") {
                (name.to_owned(), false)
            } else {
                continue;
            };
            let is_current = parts[1] == "*";
            let date_iso = parts[2].to_owned();
            let hash = parts[3].to_owned();
            let message = parts[4].to_owned();
            let author = parts[5].to_owned();

            branches.push(Branch {
                name,
                is_current,
                is_remote,
                last_commit: CommitInfo {
                    hash,
                    message,
                    author,
                    date_iso,
                },
            });
        }
        Ok(branches)
    }

    pub fn run(&self, args: &[&str]) -> Result<String, GwtuiError> {
        let out = self.run_raw(args)?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&out.stderr);
            Err(GwtuiError::Other(format!(
                "git {}: {}",
                args.join(" "),
                stderr.trim()
            )))
        }
    }

    fn run_os(&self, args: &[&OsStr]) -> Result<String, GwtuiError> {
        let out = self.run_os_output(args)?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn run_os_output(&self, args: &[&OsStr]) -> Result<Output, GwtuiError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&self.repo_root)
            .output()
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => GwtuiError::GitNotFound,
                _ => GwtuiError::Other(format!("failed to run git: {e}")),
            })?;
        if out.status.success() {
            Ok(out)
        } else {
            Err(GwtuiError::Other(format!(
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    pub fn run_raw(&self, args: &[&str]) -> Result<Output, GwtuiError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&self.repo_root)
            .output()
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => GwtuiError::GitNotFound,
                _ => GwtuiError::Other(format!("failed to run git: {e}")),
            })?;
        Ok(out)
    }

    pub fn run_in_dir(&self, dir: &Path, args: &[&str]) -> Result<String, GwtuiError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => GwtuiError::GitNotFound,
                _ => GwtuiError::Other(format!("failed to run git: {e}")),
            })?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&out.stderr);
            Err(GwtuiError::Other(format!(
                "git {}: {}",
                args.join(" "),
                stderr.trim()
            )))
        }
    }

    pub fn get_recent_commits(
        &self,
        dir: &Path,
        limit: usize,
    ) -> Result<Vec<CommitInfo>, GwtuiError> {
        let out = self.run_in_dir(
            dir,
            &[
                "log",
                &format!("--max-count={limit}"),
                "--pretty=format:%H%x00%s%x00%an%x00%ai",
                "--",
            ],
        )?;
        let mut commits = Vec::new();
        for line in out.lines() {
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('\0').collect();
            if parts.len() != 4 {
                continue;
            }
            commits.push(CommitInfo {
                hash: parts[0].to_owned(),
                message: parts[1].to_owned(),
                author: parts[2].to_owned(),
                date_iso: parts[3].to_owned(),
            });
        }
        Ok(commits)
    }
}

fn decode_worktree_porcelain(bytes: Vec<u8>) -> Result<String, GwtuiError> {
    String::from_utf8(bytes).map_err(|_| {
        GwtuiError::Other(
            "worktree paths are not valid UTF-8; use git directly for this repository".to_owned(),
        )
    })
}

#[cfg(test)]
mod metadata_encoding_tests {
    use super::*;

    #[test]
    fn worktree_metadata_rejects_invalid_utf8() {
        let error = decode_worktree_porcelain(b"worktree /invalid-\xff\0\0".to_vec()).unwrap_err();
        assert!(error.to_string().contains("UTF-8"));
    }
}

fn find_repo_root(start: &Path) -> Option<PathBuf> {
    let out = Command::new("git")
        .arg("-C")
        .arg(start)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    let out = if out.status.success() {
        out
    } else {
        let bare = Command::new("git")
            .arg("-C")
            .arg(start)
            .args(["rev-parse", "--is-bare-repository"])
            .output()
            .ok()?;
        if !bare.status.success() || bare.stdout != b"true\n" {
            return None;
        }
        let out = Command::new("git")
            .arg("-C")
            .arg(start)
            .args(["rev-parse", "--absolute-git-dir"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        out
    };
    let text = String::from_utf8(out.stdout).ok()?;
    // Git appends exactly one record newline. Trimming all whitespace would
    // change valid directory names with leading/trailing spaces or newlines.
    let text = text.strip_suffix('\n').unwrap_or(&text);
    #[cfg(windows)]
    let text = text.strip_suffix('\r').unwrap_or(text);
    Some(PathBuf::from(text))
}

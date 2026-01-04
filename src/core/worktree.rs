#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::config as config_util;
use crate::config::Config;
use crate::core::git::Git;
use crate::core::naming::{RepoId, parse_origin_url, render_template, sanitize_all};
use crate::error::GwtuiError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Worktree {
    pub path: String,
    pub branch: String,
    pub commit_hash: String,
    pub is_main: bool,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct WorktreeManager {
    git: Git,
    cfg: Config,
}

impl WorktreeManager {
    #[must_use]
    pub fn new(git: Git, cfg: Config) -> Self {
        Self { git, cfg }
    }

    pub fn list(&self) -> Result<Vec<Worktree>, GwtuiError> {
        let out = self.git.list_worktrees_porcelain()?;
        let tmp_entries = parse_worktree_porcelain(&out);

        let current_top = self.git.repo_root().to_path_buf();

        let mut worktrees = Vec::new();
        for entry in tmp_entries {
            let path = entry.path;
            let mut branch = entry.branch;
            let head = entry.head;
            let path_buf = PathBuf::from(&path);
            if branch.is_empty()
                && let Ok(out) = self
                    .git
                    .run_in_dir(&path_buf, &["rev-parse", "--abbrev-ref", "HEAD"])
            {
                out.trim().clone_into(&mut branch);
            }

            let created_at = std::fs::metadata(&path_buf)
                .and_then(|m| m.modified())
                .ok()
                .and_then(system_time_to_rfc3339)
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_owned());

            worktrees.push(Worktree {
                path: path.clone(),
                branch,
                commit_hash: head,
                is_main: path_buf == current_top,
                created_at,
            });
        }

        Ok(worktrees)
    }

    pub fn validate_worktree_path(&self, path: &Path) -> Result<(), GwtuiError> {
        if !path.exists() {
            return Ok(());
        }
        let meta = std::fs::metadata(path).map_err(|e| GwtuiError::IoPath {
            path: path.to_path_buf(),
            source: e,
        })?;
        if meta.is_dir() {
            let mut it = std::fs::read_dir(path).map_err(|e| GwtuiError::IoPath {
                path: path.to_path_buf(),
                source: e,
            })?;
            if it.next().is_some() {
                return Err(GwtuiError::Other(format!(
                    "directory is not empty: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        Err(GwtuiError::Other(format!(
            "path exists and is not a directory: {}",
            path.display()
        )))
    }

    pub fn add(
        &self,
        branch: &str,
        custom_path: Option<&Path>,
        create_branch: bool,
    ) -> Result<(), GwtuiError> {
        let raw_path = if let Some(p) = custom_path {
            p.to_string_lossy().to_string()
        } else {
            self.generate_worktree_path(branch)
        };

        let expanded =
            config_util::expand_path(&raw_path).map_err(|e| GwtuiError::Other(e.to_string()))?;

        if custom_path.is_some() && !create_branch {
            // match gwq behavior: validate only when user supplies path and not forcing
        }

        if self.cfg.worktree.auto_mkdir
            && let Some(parent) = expanded.parent()
        {
            std::fs::create_dir_all(parent).map_err(|e| GwtuiError::IoPath {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        self.git.add_worktree(&expanded, branch, create_branch)?;
        Ok(())
    }

    pub fn add_from_base(
        &self,
        branch: &str,
        base_branch: Option<&str>,
        custom_path: Option<&Path>,
    ) -> Result<PathBuf, GwtuiError> {
        let raw_path = if let Some(p) = custom_path {
            p.to_string_lossy().to_string()
        } else {
            self.generate_worktree_path(branch)
        };

        let expanded =
            config_util::expand_path(&raw_path).map_err(|e| GwtuiError::Other(e.to_string()))?;

        if self.cfg.worktree.auto_mkdir
            && let Some(parent) = expanded.parent()
        {
            std::fs::create_dir_all(parent).map_err(|e| GwtuiError::IoPath {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        self.git
            .add_worktree_from_base(&expanded, branch, base_branch)?;
        Ok(expanded)
    }

    pub fn remove(&self, path: &Path, force: bool) -> Result<(), GwtuiError> {
        self.git.remove_worktree(path, force)
    }

    pub fn remove_with_branch(
        &self,
        path: &Path,
        branch: &str,
        force_worktree: bool,
        delete_branch: bool,
        force_branch: bool,
    ) -> Result<(), GwtuiError> {
        self.git.remove_worktree(path, force_worktree)?;
        if delete_branch && !branch.is_empty() {
            self.git.delete_branch(branch, force_branch)?;
        }
        Ok(())
    }

    pub fn prune(&self) -> Result<(), GwtuiError> {
        self.git.prune_worktrees()
    }

    pub fn get_matching_worktrees(&self, pattern: &str) -> Result<Vec<Worktree>, GwtuiError> {
        let worktrees = self.list()?;
        let p = pattern.to_lowercase();
        let matches = worktrees
            .into_iter()
            .filter(|wt| {
                wt.branch.to_lowercase().contains(&p) || wt.path.to_lowercase().contains(&p)
            })
            .collect();
        Ok(matches)
    }

    #[must_use]
    pub fn resolve_path_from_worktree(&self, wt: &Worktree) -> PathBuf {
        PathBuf::from(&wt.path)
    }

    fn generate_worktree_path(&self, branch: &str) -> String {
        let repo_id = match self
            .git
            .get_repository_url()
            .ok()
            .and_then(|url| parse_origin_url(&url))
        {
            Some(id) => id,
            None => RepoId {
                host: "local".to_owned(),
                owner: "local".to_owned(),
                repo: self
                    .git
                    .repo_root()
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("repo")
                    .to_owned(),
            },
        };

        let sanitized_branch = sanitize_all(branch.to_owned(), &self.cfg.worktree.sanitize);
        let rel = render_template(
            &self.cfg.worktree.naming_template,
            &repo_id,
            &sanitized_branch,
        );
        PathBuf::from(&self.cfg.worktree.base_dir)
            .join(rel)
            .to_string_lossy()
            .to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PorcelainEntry {
    path: String,
    branch: String,
    head: String,
}

fn parse_worktree_porcelain(out: &str) -> Vec<PorcelainEntry> {
    let mut entries: Vec<PorcelainEntry> = Vec::new();

    let mut cur_path: Option<String> = None;
    let mut cur_branch = String::new();
    let mut cur_head = String::new();

    for line in out.lines() {
        let line = line.trim_end();
        if let Some(path) = line.strip_prefix("worktree ") {
            if let Some(p) = cur_path.take() {
                entries.push(PorcelainEntry {
                    path: p,
                    branch: cur_branch.clone(),
                    head: cur_head.clone(),
                });
                cur_branch.clear();
                cur_head.clear();
            }
            cur_path = Some(path.to_owned());
        } else if let Some(branch) = line.strip_prefix("branch ") {
            branch
                .trim()
                .trim_start_matches("refs/heads/")
                .clone_into(&mut cur_branch);
        } else if let Some(head) = line.strip_prefix("HEAD ") {
            head.trim().clone_into(&mut cur_head);
        }
    }
    if let Some(p) = cur_path.take() {
        entries.push(PorcelainEntry {
            path: p,
            branch: cur_branch,
            head: cur_head,
        });
    }

    entries
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn parses_worktree_list_porcelain() {
        let out = r#"worktree /repo
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /repo/.worktrees/feature
HEAD 2222222222222222222222222222222222222222
branch refs/heads/feature/test

worktree /repo/.worktrees/detached
HEAD 3333333333333333333333333333333333333333
detached
"#;

        let entries = parse_worktree_porcelain(out);
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            PorcelainEntry {
                path: "/repo".to_owned(),
                branch: "main".to_owned(),
                head: "1111111111111111111111111111111111111111".to_owned(),
            }
        );
        assert_eq!(entries[1].branch, "feature/test");
        assert_eq!(entries[2].branch, "");
    }
}

fn system_time_to_rfc3339(t: std::time::SystemTime) -> Option<String> {
    let dt = OffsetDateTime::from(t);
    dt.format(&Rfc3339).ok()
}

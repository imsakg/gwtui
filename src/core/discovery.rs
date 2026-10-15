#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config;
use crate::core::git::Git;
use crate::core::naming::parse_origin_url;
use crate::error::GwtuiError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GlobalWorktreeEntry {
    pub repository_url: String,
    pub repository: Option<String>,
    pub branch: String,
    pub path: String,
    pub commit_hash: String,
    pub is_main: bool,
}

pub fn discover_global_worktrees(
    base_dir: &str,
    scan_depth: usize,
) -> Result<Vec<GlobalWorktreeEntry>, GwtuiError> {
    if base_dir.trim().is_empty() {
        return Err(GwtuiError::Config(
            "worktree.base_dir must not be empty".to_owned(),
        ));
    }

    let base = config::expand_path(base_dir).map_err(|e| GwtuiError::Other(e.to_string()))?;
    if !base.exists() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut visited = BTreeSet::new();
    walk_dirs(&base, scan_depth, &mut visited, &mut |dir| {
        if let Some(entry) = extract_worktree_info(dir) {
            out.push(entry);
        }
    });
    Ok(out)
}

#[must_use]
pub fn filter_global_worktrees<'a>(
    entries: &'a [GlobalWorktreeEntry],
    pattern: &str,
) -> Vec<&'a GlobalWorktreeEntry> {
    let p = pattern.to_lowercase();
    entries
        .iter()
        .filter(|e| {
            let branch = e.branch.to_lowercase();
            let path = e.path.to_lowercase();
            let repo = e.repository.clone().unwrap_or_default().to_lowercase();
            branch.contains(&p)
                || path.contains(&p)
                || repo.contains(&p)
                || format!("{repo}:{branch}").contains(&p)
        })
        .collect()
}

fn extract_worktree_info(worktree_path: &Path) -> Option<GlobalWorktreeEntry> {
    let gitfile = worktree_path.join(".git");
    let raw = std::fs::read_to_string(&gitfile).ok()?;
    let s = raw.trim();
    if !s.starts_with("gitdir: ") {
        return None;
    }

    let git = Git::new(worktree_path.to_path_buf());
    let repository_url = git.get_repository_url().ok()?;
    let repository = parse_origin_url(&repository_url).map(|id| id.repo);
    let branch = git
        .run(&["rev-parse", "--abbrev-ref", "HEAD"])
        .ok()
        .map_or_else(|| "HEAD".to_owned(), |s| s.trim().to_owned());
    let commit_hash = git
        .run(&["rev-parse", "HEAD"])
        .ok()
        .map(|s| s.trim().to_owned())
        .unwrap_or_default();

    Some(GlobalWorktreeEntry {
        repository_url,
        repository,
        branch,
        path: worktree_path.to_string_lossy().to_string(),
        commit_hash,
        is_main: false,
    })
}

fn walk_dirs(
    base: &Path,
    max_depth: usize,
    visited: &mut BTreeSet<PathBuf>,
    on_dir: &mut dyn FnMut(&Path),
) {
    let mut stack: Vec<(PathBuf, usize)> = vec![(base.to_path_buf(), 0)];
    while let Some((dir, depth)) = stack.pop() {
        if depth > max_depth {
            continue;
        }
        if !visited.insert(dir.clone()) {
            continue;
        }

        on_dir(&dir);

        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if should_skip_dir(&path) {
                    continue;
                }
                stack.push((path, depth + 1));
            }
        }
    }
}

fn should_skip_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return false;
    };
    if name == ".git" {
        return true;
    }
    if name == "node_modules" || name == "target" || name == ".idea" || name == ".vscode" {
        return true;
    }
    // Skip hidden dirs except the base.
    if name.starts_with('.') {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_global_worktrees_by_repo_branch_path_and_repo_branch_combo() {
        let entries = vec![
            GlobalWorktreeEntry {
                repository_url: "https://example.com/a/repo.git".to_owned(),
                repository: Some("repo".to_owned()),
                branch: "main".to_owned(),
                path: "/tmp/repo/main".to_owned(),
                commit_hash: "abc".to_owned(),
                is_main: false,
            },
            GlobalWorktreeEntry {
                repository_url: "https://example.com/a/other.git".to_owned(),
                repository: Some("other".to_owned()),
                branch: "feature/x".to_owned(),
                path: "/tmp/other/feature-x".to_owned(),
                commit_hash: "def".to_owned(),
                is_main: false,
            },
        ];

        assert_eq!(filter_global_worktrees(&entries, "repo").len(), 1);
        assert_eq!(filter_global_worktrees(&entries, "feature/x").len(), 1);
        assert_eq!(filter_global_worktrees(&entries, "/tmp/other").len(), 1);
        assert_eq!(filter_global_worktrees(&entries, "repo:main").len(), 1);
        assert_eq!(filter_global_worktrees(&entries, "REPO:MAIN").len(), 1);
    }
}

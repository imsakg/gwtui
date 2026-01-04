#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config;
use crate::core::git::Git;
use crate::core::worktree::Worktree;
use crate::error::GwtuiError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeState {
    Clean,
    Modified,
    Staged,
    Conflict,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GitStatus {
    pub modified: u32,
    pub added: u32,
    pub deleted: u32,
    pub untracked: u32,
    pub staged: u32,
    pub ahead: u32,
    pub behind: u32,
    pub conflicts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorktreeStatus {
    pub path: String,
    pub branch: String,
    pub repository: String,
    pub status: WorktreeState,
    pub git_status: GitStatus,
    pub last_activity: String,
    pub is_current: bool,
}

#[derive(Debug, Clone)]
pub struct StatusCollectorOptions {
    pub include_process: bool,
    pub fetch_remote: bool,
    pub stale_threshold: Duration,
    pub base_dir: String,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct StatusCollector {
    opts: StatusCollectorOptions,
}

impl StatusCollector {
    #[must_use]
    pub fn new(opts: StatusCollectorOptions) -> Self {
        Self { opts }
    }

    pub async fn collect_all(
        &self,
        worktrees: &[Worktree],
    ) -> Result<Vec<WorktreeStatus>, GwtuiError> {
        let current_path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

        let sem = std::sync::Arc::new(Semaphore::new(self.opts.concurrency.max(1)));
        let mut handles = Vec::with_capacity(worktrees.len());

        for wt in worktrees {
            let wt = wt.clone();
            let permit = sem.clone().acquire_owned().await.map_err(|_| {
                GwtuiError::Other("failed to acquire status collector semaphore".to_owned())
            })?;
            let opts = self.opts.clone();
            let current_path = current_path.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                collect_one_blocking(&opts, &wt, &current_path)
            }));
        }

        let mut statuses = Vec::new();
        for h in handles {
            match h.await {
                Ok(s) => statuses.push(s),
                Err(e) => return Err(GwtuiError::Other(format!("status task join error: {e}"))),
            }
        }

        Ok(statuses)
    }
}

fn collect_one_blocking(
    opts: &StatusCollectorOptions,
    wt: &Worktree,
    current_path: &Path,
) -> WorktreeStatus {
    let worktree_path = PathBuf::from(&wt.path);
    let git = Git::new(worktree_path.clone());

    let mut status = WorktreeStatus {
        path: wt.path.clone(),
        branch: wt.branch.clone(),
        repository: extract_repository(&wt.path, &opts.base_dir),
        status: WorktreeState::Clean,
        git_status: GitStatus::default(),
        last_activity: "unknown".to_owned(),
        is_current: current_path.starts_with(&worktree_path),
    };

    if let Ok(gs) = collect_git_status(&git, opts.fetch_remote) {
        status.status = determine_worktree_state(&gs);
        status.git_status = gs;
    } else {
        status.status = WorktreeState::Unknown;
    }

    let last = get_last_activity(&git, &worktree_path);
    if last != SystemTime::UNIX_EPOCH {
        status.last_activity = format_rfc3339(last).unwrap_or_else(|| "unknown".to_owned());
        if let Ok(age) = SystemTime::now().duration_since(last)
            && age > opts.stale_threshold
        {
            status.status = WorktreeState::Stale;
        }
    }

    if opts.include_process {
        // Process detection not implemented yet.
    }

    status
}

fn collect_git_status(git: &Git, fetch_remote: bool) -> Result<GitStatus, GwtuiError> {
    let mut status = GitStatus::default();

    let out = git.run(&["status", "--porcelain=v1", "-uno"])?;
    for line in out.lines() {
        if line.len() < 2 {
            continue;
        }
        process_status_line(line, &mut status);
    }

    // Count untracked via ls-files for accuracy.
    if let Ok(untracked) = git.run(&["ls-files", "--others", "--exclude-standard"]) {
        let count = untracked.lines().filter(|l| !l.trim().is_empty()).count();
        status.untracked = u32::try_from(count).unwrap_or(u32::MAX);
    }

    if fetch_remote {
        let _ = fetch_remote_status(git, &mut status);
    }

    Ok(status)
}

fn process_status_line(line: &str, status: &mut GitStatus) {
    let bytes = line.as_bytes();
    let index = bytes[0] as char;
    let worktree = bytes[1] as char;

    if index != ' ' && index != '?' {
        status.staged += 1;
    }

    match worktree {
        'M' => status.modified += 1,
        'A' => status.added += 1,
        'D' => status.deleted += 1,
        '?' => status.untracked += 1,
        'U' => status.conflicts += 1,
        _ => {}
    }
}

fn fetch_remote_status(git: &Git, status: &mut GitStatus) -> Result<(), GwtuiError> {
    let current_branch = git.run(&["rev-parse", "--abbrev-ref", "HEAD"])?;
    let current_branch = current_branch.trim();
    if current_branch.is_empty() {
        return Ok(());
    }

    let upstream = match git.run(&[
        "rev-parse",
        "--abbrev-ref",
        &format!("{current_branch}@{{upstream}}"),
    ]) {
        Ok(s) => s.trim().to_owned(),
        Err(_) => return Ok(()),
    };
    if upstream.is_empty() {
        return Ok(());
    }

    status.ahead = count_rev_list(git, &format!("{upstream}..HEAD")).unwrap_or(0);
    status.behind = count_rev_list(git, &format!("HEAD..{upstream}")).unwrap_or(0);
    Ok(())
}

fn count_rev_list(git: &Git, rev_range: &str) -> Result<u32, GwtuiError> {
    let out = git.run(&["rev-list", "--count", rev_range])?;
    let n: u32 = out
        .trim()
        .parse()
        .map_err(|e| GwtuiError::Other(format!("invalid rev-list count: {e}")))?;
    Ok(n)
}

fn determine_worktree_state(status: &GitStatus) -> WorktreeState {
    if status.conflicts > 0 {
        return WorktreeState::Conflict;
    }
    if status.staged > 0 {
        return WorktreeState::Staged;
    }
    if status.modified > 0 || status.added > 0 || status.deleted > 0 || status.untracked > 0 {
        return WorktreeState::Modified;
    }
    WorktreeState::Clean
}

fn get_last_activity(git: &Git, worktree_path: &Path) -> SystemTime {
    let mut latest = SystemTime::UNIX_EPOCH;
    let mut any = false;

    if let Ok(tracked) = git.run(&["ls-files", "-z"]) {
        for rel in tracked.split('\0') {
            if rel.is_empty() {
                continue;
            }
            let p = worktree_path.join(rel);
            if let Ok(m) = std::fs::metadata(&p)
                && let Ok(t) = m.modified()
                && (!any || t > latest)
            {
                latest = t;
                any = true;
            }
        }

        if let Ok(untracked) = git.run(&["ls-files", "-z", "--others", "--exclude-standard"]) {
            for rel in untracked.split('\0') {
                if rel.is_empty() {
                    continue;
                }
                let p = worktree_path.join(rel);
                if let Ok(m) = std::fs::metadata(&p) {
                    if m.is_dir() {
                        continue;
                    }
                    if let Ok(t) = m.modified()
                        && (!any || t > latest)
                    {
                        latest = t;
                        any = true;
                    }
                }
            }
        }
    } else {
        // Fallback to directory walk.
        latest = get_last_activity_fallback(worktree_path);
        any = latest != SystemTime::UNIX_EPOCH;
    }

    if !any
        && let Ok(m) = std::fs::metadata(worktree_path)
        && let Ok(t) = m.modified()
    {
        return t;
    }

    latest
}

fn get_last_activity_fallback(root: &Path) -> SystemTime {
    let mut latest = SystemTime::UNIX_EPOCH;

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if should_skip_status_dir(name) {
                    continue;
                }
                stack.push(p);
                continue;
            }
            if let Ok(m) = entry.metadata()
                && let Ok(t) = m.modified()
                && t > latest
            {
                latest = t;
            }
        }
    }

    latest
}

fn should_skip_status_dir(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | "node_modules"
            | "vendor"
            | ".next"
            | "dist"
            | "build"
            | "target"
            | ".cache"
            | "coverage"
            | "__pycache__"
            | ".pytest_cache"
            | ".venv"
            | "venv"
            | ".idea"
            | ".vscode"
    ) || (name.starts_with('.') && name != "." && name != "..")
}

fn extract_repository(path: &str, base_dir: &str) -> String {
    if base_dir.trim().is_empty() {
        return PathBuf::from(path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(path)
            .to_owned();
    }

    let base = config::expand_path(base_dir).unwrap_or_else(|_| PathBuf::from(base_dir));
    let base = base.components().as_path().to_path_buf();
    let p = PathBuf::from(path);

    if !p.starts_with(&base) {
        return p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(path)
            .to_owned();
    }

    let Ok(rel) = p.strip_prefix(&base) else {
        return p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(path)
            .to_owned();
    };
    let parts: Vec<_> = rel
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();
    if parts.len() >= 3 {
        return Path::new(parts[0])
            .join(parts[1])
            .join(parts[2])
            .to_string_lossy()
            .to_string();
    }
    rel.to_string_lossy().to_string()
}

fn format_rfc3339(t: SystemTime) -> Option<String> {
    let dt = time::OffsetDateTime::from(t);
    dt.format(&time::format_description::well_known::Rfc3339)
        .ok()
}

#[must_use]
pub fn format_status_for_table(status: WorktreeState) -> &'static str {
    match status {
        WorktreeState::Clean => "up to date",
        WorktreeState::Modified => "changed",
        WorktreeState::Staged => "staged",
        WorktreeState::Conflict => "conflicted",
        WorktreeState::Stale => "inactive",
        WorktreeState::Unknown => "unknown",
    }
}

#[must_use]
pub fn filter_statuses(statuses: Vec<WorktreeStatus>, filter: &str) -> Vec<WorktreeStatus> {
    let f = filter.trim().to_lowercase();
    if f.is_empty() || f == "all" {
        return statuses;
    }
    statuses
        .into_iter()
        .filter(|s| match f.as_str() {
            "modified" | "changed" => s.status == WorktreeState::Modified,
            "clean" | "up to date" => s.status == WorktreeState::Clean,
            "stale" | "inactive" => s.status == WorktreeState::Stale,
            "staged" => s.status == WorktreeState::Staged,
            "conflict" | "conflicted" => s.status == WorktreeState::Conflict,
            _ => {
                let needle = f.as_str();
                s.branch.to_lowercase().contains(needle)
                    || s.repository.to_lowercase().contains(needle)
                    || s.path.to_lowercase().contains(needle)
            }
        })
        .collect()
}

pub fn sort_statuses(statuses: &mut [WorktreeStatus], sort_by: &str) {
    let key = sort_by.to_lowercase();
    match key.as_str() {
        "branch" | "name" => statuses.sort_by(|a, b| a.branch.cmp(&b.branch)),
        "status" => {
            statuses.sort_by(|a, b| status_priority(a.status).cmp(&status_priority(b.status)));
        }
        "modified" | "changes" => statuses.sort_by(|a, b| {
            count_total_changes(b.git_status)
                .cmp(&count_total_changes(a.git_status))
                .then_with(|| a.branch.cmp(&b.branch))
        }),
        "activity" | "time" => statuses.sort_by(|a, b| b.last_activity.cmp(&a.last_activity)),
        "ahead" => statuses.sort_by(|a, b| b.git_status.ahead.cmp(&a.git_status.ahead)),
        "behind" => statuses.sort_by(|a, b| b.git_status.behind.cmp(&a.git_status.behind)),
        _ => {}
    }
}

fn status_priority(status: WorktreeState) -> u32 {
    match status {
        WorktreeState::Conflict => 0,
        WorktreeState::Modified => 1,
        WorktreeState::Staged => 2,
        WorktreeState::Stale => 3,
        WorktreeState::Clean => 4,
        WorktreeState::Unknown => 999,
    }
}

fn count_total_changes(gs: GitStatus) -> u32 {
    gs.modified + gs.added + gs.deleted + gs.untracked + gs.staged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_status_porcelain_v1_lines() {
        let mut gs = GitStatus::default();
        process_status_line(" M foo.txt", &mut gs);
        process_status_line("A  added.txt", &mut gs);
        process_status_line("?? untracked.txt", &mut gs);
        process_status_line("UU conflict.txt", &mut gs);

        assert_eq!(gs.modified, 1);
        assert_eq!(gs.untracked, 1);
        assert_eq!(gs.conflicts, 1);
        // "A  file" counts as staged.
        assert!(gs.staged >= 1);
    }

    #[test]
    fn determines_worktree_state_priority() {
        let mut gs = GitStatus::default();
        assert_eq!(determine_worktree_state(&gs), WorktreeState::Clean);

        gs.modified = 1;
        assert_eq!(determine_worktree_state(&gs), WorktreeState::Modified);

        gs = GitStatus::default();
        gs.staged = 1;
        assert_eq!(determine_worktree_state(&gs), WorktreeState::Staged);

        gs = GitStatus::default();
        gs.conflicts = 1;
        assert_eq!(determine_worktree_state(&gs), WorktreeState::Conflict);
    }

    #[test]
    fn filter_statuses_supports_substring_search() {
        let statuses = vec![
            WorktreeStatus {
                path: "/tmp/repo/feature/foo".to_owned(),
                branch: "feature/foo".to_owned(),
                repository: "repo".to_owned(),
                status: WorktreeState::Clean,
                git_status: GitStatus::default(),
                last_activity: "unknown".to_owned(),
                is_current: false,
            },
            WorktreeStatus {
                path: "/tmp/repo/feature/bar".to_owned(),
                branch: "feature/bar".to_owned(),
                repository: "repo".to_owned(),
                status: WorktreeState::Modified,
                git_status: GitStatus::default(),
                last_activity: "unknown".to_owned(),
                is_current: false,
            },
        ];

        let out = filter_statuses(statuses.clone(), "foo");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].branch, "feature/foo");

        let out = filter_statuses(statuses.clone(), "modified");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].branch, "feature/bar");
    }
}

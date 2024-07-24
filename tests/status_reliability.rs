#![forbid(unsafe_code)]
mod common;

use common::TestRepo;
use gwtui::core::status::{StatusCollector, StatusCollectorOptions, WorktreeState, WorktreeStatus};
use gwtui::core::worktree::Worktree;
use std::time::Duration;

fn worktree(repo: &TestRepo) -> Worktree {
    Worktree {
        path: repo.path.to_string_lossy().into_owned(),
        branch: "main".into(),
        commit_hash: repo.git(&["rev-parse", "HEAD"]),
        is_main: true,
        created_at: String::new(),
    }
}
fn collector(threshold: Duration) -> StatusCollector {
    StatusCollector::new(StatusCollectorOptions {
        include_process: false,
        fetch_remote: false,
        stale_threshold: threshold,
        base_dir: String::new(),
        concurrency: 2,
    })
}
async fn status(repo: &TestRepo, threshold: Duration) -> WorktreeStatus {
    collector(threshold)
        .collect_all(&[worktree(repo)])
        .await
        .unwrap()
        .remove(0)
}

#[tokio::test]
async fn untracked_paths_are_counted_without_filename_line_parsing() {
    let repo = TestRepo::new();
    for name in ["   ", "line\nbreak", "café", "normal"] {
        std::fs::write(repo.path.join(name), "data").unwrap();
    }
    let s = status(&repo, Duration::MAX).await;
    assert_eq!(s.git_status.untracked, 4);
    assert_eq!(s.status, WorktreeState::Modified);
}

#[tokio::test]
async fn stale_age_does_not_hide_dirty_or_conflicted_worktrees() {
    let repo = TestRepo::new();
    assert_eq!(
        status(&repo, Duration::ZERO).await.status,
        WorktreeState::Stale
    );
    std::fs::write(repo.path.join("README.md"), "changed\n").unwrap();
    assert_eq!(
        status(&repo, Duration::ZERO).await.status,
        WorktreeState::Modified
    );
    repo.git(&["add", "README.md"]);
    assert_eq!(
        status(&repo, Duration::ZERO).await.status,
        WorktreeState::Staged
    );
    repo.git(&["commit", "-m", "main change"]);
    repo.git(&["checkout", "-b", "other", "HEAD~1"]);
    std::fs::write(repo.path.join("README.md"), "other\n").unwrap();
    repo.git(&["commit", "-am", "other change"]);
    let merge = std::process::Command::new("git")
        .args(["merge", "main"])
        .current_dir(&repo.path)
        .output()
        .unwrap();
    assert!(!merge.status.success());
    let s = status(&repo, Duration::ZERO).await;
    assert_eq!(s.git_status.conflicts, 1);
    assert_eq!(s.status, WorktreeState::Conflict);
}

#[tokio::test]
async fn mixed_index_worktree_and_untracked_changes_are_reported() {
    let repo = TestRepo::new();
    std::fs::write(repo.path.join("README.md"), "staged\n").unwrap();
    repo.git(&["add", "README.md"]);
    std::fs::write(repo.path.join("README.md"), "unstaged\n").unwrap();
    std::fs::write(repo.path.join("new"), "untracked\n").unwrap();
    let s = status(&repo, Duration::MAX).await;
    assert_eq!(s.status, WorktreeState::Staged);
    assert_eq!(s.git_status.staged, 1);
    assert_eq!(s.git_status.modified, 1);
    assert_eq!(s.git_status.untracked, 1);
    assert_eq!(s.git_status.total_changes(), 3);
}

#[tokio::test]
async fn divergence_is_read_from_local_refs_without_network_fetch() {
    let repo = TestRepo::new();
    repo.git(&["checkout", "-b", "upstream"]);
    repo.git(&["commit", "--allow-empty", "-m", "upstream-only"]);
    repo.git(&["checkout", "main"]);
    repo.git(&["commit", "--allow-empty", "-m", "local-only"]);
    repo.git(&["branch", "--set-upstream-to", "upstream"]);
    let collect = StatusCollector::new(StatusCollectorOptions {
        include_process: false,
        fetch_remote: true,
        stale_threshold: Duration::MAX,
        base_dir: String::new(),
        concurrency: 1,
    });
    let s = collect
        .collect_all(&[worktree(&repo)])
        .await
        .unwrap()
        .remove(0);
    assert_eq!((s.git_status.ahead, s.git_status.behind), (1, 1));
    assert_eq!(s.status, WorktreeState::Clean);
}

#[tokio::test]
async fn collector_keeps_input_order_and_unknown_errors_with_zero_concurrency() {
    let repo = TestRepo::new();
    let good = worktree(&repo);
    let mut missing = good.clone();
    missing.path = repo.child("missing").to_string_lossy().into_owned();
    let mut nongit = good.clone();
    nongit.path = repo.child("not-git").to_string_lossy().into_owned();
    std::fs::create_dir(&nongit.path).unwrap();
    let collect = StatusCollector::new(StatusCollectorOptions {
        include_process: false,
        fetch_remote: false,
        stale_threshold: Duration::ZERO,
        base_dir: String::new(),
        concurrency: 0,
    });
    let input = vec![missing, good, nongit];
    let result = collect.collect_all(&input).await.unwrap();
    assert_eq!(
        result.iter().map(|s| &s.path).collect::<Vec<_>>(),
        input.iter().map(|s| &s.path).collect::<Vec<_>>()
    );
    assert_eq!(result[0].status, WorktreeState::Unknown);
    assert_eq!(result[1].status, WorktreeState::Stale);
    assert_eq!(result[2].status, WorktreeState::Unknown);
    assert!(collect.collect_all(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn unborn_repository_can_report_untracked_changes() {
    let tmp = tempfile::tempdir().unwrap();
    common::git(tmp.path(), &["init", "-b", "new-main"]);
    std::fs::write(tmp.path().join("new"), "new").unwrap();
    let wt = Worktree {
        path: tmp.path().to_string_lossy().into_owned(),
        branch: "new-main".into(),
        commit_hash: String::new(),
        is_main: true,
        created_at: String::new(),
    };
    let s = collector(Duration::MAX)
        .collect_all(&[wt])
        .await
        .unwrap()
        .remove(0);
    assert_eq!(s.status, WorktreeState::Modified);
    assert_eq!(s.git_status.untracked, 1);
}

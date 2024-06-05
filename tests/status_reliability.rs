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

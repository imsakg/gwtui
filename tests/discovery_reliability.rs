#![forbid(unsafe_code)]
mod common;
use common::TestRepo;
use gwtui::core::discovery::{discover_global_worktrees, filter_global_worktrees};

fn scan(path: &std::path::Path, depth: usize) -> Vec<gwtui::core::discovery::GlobalWorktreeEntry> {
    discover_global_worktrees(path.to_str().unwrap(), depth).unwrap()
}

#[test]
fn ordinary_main_repositories_are_discovered() {
    let repo = TestRepo::new();
    repo.git(&[
        "remote",
        "add",
        "origin",
        "https://example.com/team/project.git",
    ]);
    let found = scan(repo.temp.path(), 1);
    assert_eq!(found.len(), 1);
    assert!(found[0].is_main);
    assert_eq!(found[0].repository.as_deref(), Some("project"));
    assert_eq!(found[0].branch, "main");
    assert_eq!(found[0].commit_hash, repo.git(&["rev-parse", "HEAD"]));
}

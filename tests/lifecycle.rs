mod common;

use std::path::Path;

use common::TestRepo;
use gwtui::config::Config;
use gwtui::core::git::Git;
use gwtui::core::worktree::WorktreeManager;

#[test]
fn listing_preserves_whitespace_and_newlines_in_paths() {
    let repo = TestRepo::new();
    let linked = repo.child("line\nbreak and trailing ");
    repo.git(&["worktree", "add", "-b", "topic", linked.to_str().unwrap()]);
    let worktree = manager(&repo.path).list().unwrap().remove(1);
    assert_eq!(worktree.path, linked.to_str().unwrap());
    assert_eq!(worktree.branch, "topic");
}

fn manager(path: &Path) -> WorktreeManager {
    WorktreeManager::new(Git::from_dir(path).unwrap(), Config::default())
}

#[test]
fn main_identity_is_stable_from_linked_worktrees() {
    let repo = TestRepo::new();
    let linked = repo.child("linked");
    repo.git(&["worktree", "add", "-b", "topic", linked.to_str().unwrap()]);
    let worktrees = manager(&linked).list().unwrap();
    assert!(worktrees[0].is_main);
    assert!(!worktrees[1].is_main);
    assert_eq!(worktrees.iter().filter(|wt| wt.is_main).count(), 1);
}

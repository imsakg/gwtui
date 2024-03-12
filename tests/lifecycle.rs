mod common;

use std::path::Path;

use common::TestRepo;
use gwtui::config::Config;
use gwtui::core::git::Git;
use gwtui::core::worktree::WorktreeManager;

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

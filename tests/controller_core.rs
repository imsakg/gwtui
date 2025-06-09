mod common;
use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

fn manager(path: &std::path::Path) -> WorktreeManager {
    WorktreeManager::new(Git::from_dir(path).unwrap(), Config::default())
}

#[test]
fn remove_with_branch_works_when_the_manager_was_created_in_the_linked_checkout() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    manager(&path)
        .remove_with_branch(&path, "feature", false, true, false)
        .unwrap();
    assert!(!path.exists());
    assert_eq!(repo.git(&["branch", "--format=%(refname:short)"]), "main");
}

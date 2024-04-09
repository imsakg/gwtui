mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn detached_add_defaults_to_head_without_creating_a_branch() {
    let repo = TestRepo::new();
    let destination = repo.child("detached");
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    manager.add_detached(&destination, None).unwrap();
    assert_eq!(
        common::git(&destination, &["rev-parse", "HEAD"]),
        repo.git(&["rev-parse", "HEAD"])
    );
    assert_eq!(
        common::git(&destination, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "HEAD"
    );
    assert_eq!(repo.git(&["branch", "--format=%(refname:short)"]), "main");
}

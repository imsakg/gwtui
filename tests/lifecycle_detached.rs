mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn detached_revision_is_verified_before_creating_the_destination() {
    let repo = TestRepo::new();
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    let first = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["commit", "--allow-empty", "-m", "second"]);
    let destination = repo.child("older");
    manager.add_detached(&destination, Some("HEAD^")).unwrap();
    assert_eq!(common::git(&destination, &["rev-parse", "HEAD"]), first);
    for revision in ["", "--orphan", "HEAD:README.md", "does-not-exist"] {
        let target = repo.child("invalid");
        let error = manager.add_detached(&target, Some(revision)).unwrap_err();
        assert!(error.to_string().contains("revision"), "{error}");
        assert!(!target.exists());
    }
}

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

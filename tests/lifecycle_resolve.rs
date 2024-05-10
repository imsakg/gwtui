mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
    error::GwtuiError,
};

#[test]
fn resolve_prefers_exact_branch_or_path_and_rejects_fuzzy_ambiguity() {
    let repo = TestRepo::new();
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    let one = repo.child("feature");
    let two = repo.child("feature-extra");
    manager.add("feature", Some(&one), true).unwrap();
    manager.add("feature-extra", Some(&two), true).unwrap();
    assert_eq!(
        manager.resolve("feature").unwrap().path,
        one.to_str().unwrap()
    );
    assert_eq!(
        manager.resolve(two.to_str().unwrap()).unwrap().branch,
        "feature-extra"
    );
    assert_eq!(manager.resolve("EXTRA").unwrap().branch, "feature-extra");
    assert!(matches!(
        manager.resolve("feat"),
        Err(GwtuiError::AmbiguousWorktree(_))
    ));
    assert!(matches!(
        manager.resolve("absent"),
        Err(GwtuiError::WorktreeNotFound(_))
    ));
}

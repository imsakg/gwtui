mod common;
use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn branch_creation_verifies_base_revision_and_starts_at_its_commit() {
    let repo = TestRepo::new();
    let initial = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["commit", "--allow-empty", "-m", "next"]);
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    let destination = repo.child("based");
    assert_eq!(
        manager
            .add_from_base("based", Some("HEAD^"), Some(&destination))
            .unwrap(),
        destination
    );
    assert_eq!(common::git(&destination, &["rev-parse", "HEAD"]), initial);
    for base in ["", "--orphan", "HEAD:README.md", "absent"] {
        let target = repo.child("invalid-base");
        let error = manager
            .add_from_base("invalid-base", Some(base), Some(&target))
            .unwrap_err();
        assert!(error.to_string().contains("invalid revision"), "{error}");
        assert!(!target.exists());
        assert!(repo.git(&["branch", "--list", "invalid-base"]).is_empty());
    }
}

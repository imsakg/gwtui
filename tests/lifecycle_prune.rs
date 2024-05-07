mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn prune_preview_returns_diagnostics_without_removing_registration() {
    let repo = TestRepo::new();
    let missing = repo.child("missing");
    repo.git(&["worktree", "add", "-b", "stale", missing.to_str().unwrap()]);
    std::fs::remove_dir_all(&missing).unwrap();
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    let preview = manager.prune_with_options(true, Some("now")).unwrap();
    assert!(!preview.trim().is_empty());
    assert_eq!(manager.list().unwrap().len(), 2);
    let applied = manager.prune_with_options(false, Some("now")).unwrap();
    assert!(!applied.trim().is_empty());
    assert_eq!(manager.list().unwrap().len(), 1);
    assert_eq!(repo.git(&["branch", "--list", "stale"]), "stale");
}

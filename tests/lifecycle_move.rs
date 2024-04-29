mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn move_updates_git_registration_and_preserves_checkout() {
    let repo = TestRepo::new();
    let source = repo.child("old");
    let destination = repo.child("new\nlocation");
    repo.git(&["worktree", "add", "-b", "moving", source.to_str().unwrap()]);
    std::fs::write(source.join("untracked"), "keep me").unwrap();
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    manager.move_worktree(&source, &destination).unwrap();
    assert!(!source.exists());
    assert_eq!(
        std::fs::read_to_string(destination.join("untracked")).unwrap(),
        "keep me"
    );
    assert_eq!(
        manager.list().unwrap()[1].path,
        destination.to_str().unwrap()
    );
    assert_eq!(
        common::git(&destination, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "moving"
    );
}

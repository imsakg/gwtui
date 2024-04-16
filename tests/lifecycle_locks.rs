mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn locks_round_trip_optional_reasons_in_git_metadata() {
    let repo = TestRepo::new();
    let target = repo.child("locked");
    repo.git(&["worktree", "add", "-b", "locked", target.to_str().unwrap()]);
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    manager
        .lock(&target, Some("--keep\nportable drive"))
        .unwrap();
    assert!(
        repo.git(&["worktree", "list", "--porcelain", "-z"])
            .contains("locked --keep\nportable drive\0")
    );
    manager.unlock(&target).unwrap();
    assert!(
        !repo
            .git(&["worktree", "list", "--porcelain", "-z"])
            .contains("\0locked")
    );
    manager.lock(&target, None).unwrap();
    manager.unlock(&target).unwrap();
}

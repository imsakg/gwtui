mod common;

use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

#[test]
fn repair_reconnects_a_manually_moved_linked_checkout() {
    let repo = TestRepo::new();
    let source = repo.child("before");
    let destination = repo.child("after");
    repo.git(&[
        "worktree",
        "add",
        "-b",
        "repairing",
        source.to_str().unwrap(),
    ]);
    std::fs::rename(&source, &destination).unwrap();
    let manager = WorktreeManager::new(Git::new(repo.path.clone()), Config::default());
    manager.repair(std::slice::from_ref(&destination)).unwrap();
    assert_eq!(
        manager.list().unwrap()[1].path,
        destination.to_str().unwrap()
    );
    assert_eq!(
        common::git(&destination, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "repairing"
    );
    manager.repair(&[]).unwrap();
}

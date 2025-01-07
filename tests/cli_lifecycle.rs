mod common;
use common::TestRepo;
use std::process::Output;

fn success(out: Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn lock_command_keeps_a_literal_reason() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    success(repo.cli(&["lock", "feature", "--reason", "offline backup | keep"]));
    assert!(
        repo.git(&["worktree", "list", "--porcelain"])
            .contains("locked offline backup | keep")
    );
}

#[test]
fn unlock_command_releases_protection() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    repo.git(&["worktree", "lock", path.to_str().unwrap()]);
    success(repo.cli(&["unlock", "feature"]));
    assert!(
        !repo
            .git(&["worktree", "list", "--porcelain"])
            .contains("locked")
    );
}

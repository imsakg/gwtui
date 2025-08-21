mod common;
use common::TestRepo;

fn checkout(repo: &TestRepo) -> std::path::PathBuf {
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    path
}

#[test]
fn removal_dry_run_preserves_checkout_and_branch() {
    let repo = TestRepo::new();
    let path = checkout(&repo);
    let out = repo.cli(&["remove", "feature", "--dry-run", "--delete-branch"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("Would delete branch: feature"));
    assert!(path.exists());
    assert_eq!(
        repo.git(&["branch", "--list", "feature", "--format=%(refname:short)"])
            .trim(),
        "feature"
    );
}

#[test]
fn one_force_flag_does_not_remove_a_locked_worktree() {
    let repo = TestRepo::new();
    let path = checkout(&repo);
    repo.git(&[
        "worktree",
        "lock",
        "--reason",
        "keep",
        path.to_str().unwrap(),
    ]);
    let out = repo.cli(&["remove", "feature", "--force"]);
    assert!(!out.status.success());
    assert!(path.join(".git").is_file());
}

#[test]
fn main_checkout_is_never_a_removal_target() {
    let repo = TestRepo::new();
    let _ = checkout(&repo);
    let out = repo.cli(&["remove", "main", "--force"]);
    assert!(!out.status.success());
    assert!(repo.path.join(".git").is_dir());
}

use std::process::Command;

use gwtui::config::Config;
use gwtui::core::git::Git;
use gwtui::core::worktree::WorktreeManager;

#[test]
fn git_worktree_add_and_list_smoke() {
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: git not found");
        return;
    }

    let td = tempfile::tempdir().expect("tempdir");
    let repo = td.path().join("repo");
    std::fs::create_dir_all(&repo).expect("mkdir repo");

    run(&repo, &["init"]);
    run(&repo, &["config", "user.email", "test@example.com"]);
    run(&repo, &["config", "user.name", "Test"]);

    std::fs::write(repo.join("README.md"), "hello\n").expect("write");
    run(&repo, &["add", "."]);
    run(&repo, &["commit", "-m", "init"]);

    let git = Git::from_dir(&repo).expect("git from dir");
    let cfg = Config::default();
    let wm = WorktreeManager::new(git, cfg);

    let worktree_path = td.path().join("wt-feature");
    let _ = wm
        .add_from_base("feature/test", None, Some(&worktree_path))
        .expect("add worktree");

    let listed = wm.list().expect("list");
    assert!(listed.iter().any(|w| w.branch == "feature/test"));
}

fn run(dir: &std::path::Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git command");
    if !out.status.success() {
        panic!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#![forbid(unsafe_code)]
mod common;
use common::TestRepo;
use gwtui::core::discovery::{discover_global_worktrees, filter_global_worktrees};

fn scan(path: &std::path::Path, depth: usize) -> Vec<gwtui::core::discovery::GlobalWorktreeEntry> {
    discover_global_worktrees(path.to_str().unwrap(), depth).unwrap()
}

#[test]
fn ordinary_main_repositories_are_discovered() {
    let repo = TestRepo::new();
    repo.git(&[
        "remote",
        "add",
        "origin",
        "https://example.com/team/project.git",
    ]);
    let found = scan(repo.temp.path(), 1);
    assert_eq!(found.len(), 1);
    assert!(found[0].is_main);
    assert_eq!(found[0].repository.as_deref(), Some("project"));
    assert_eq!(found[0].branch, "main");
    assert_eq!(found[0].commit_hash, repo.git(&["rev-parse", "HEAD"]));
}

#[test]
fn repositories_without_origin_keep_main_and_linked_worktrees() {
    let repo = TestRepo::new();
    let linked = repo.child("linked");
    repo.git(&["worktree", "add", "-b", "feature", linked.to_str().unwrap()]);
    let found = scan(repo.temp.path(), 1);
    assert_eq!(found.len(), 2);
    assert_eq!(found.iter().filter(|e| e.is_main).count(), 1);
    assert!(found.iter().all(|e| e.repository_url.is_empty()));
    assert!(
        found
            .iter()
            .all(|e| e.repository.as_deref() == Some("repo"))
    );
    assert_eq!(filter_global_worktrees(&found, "repo:feature").len(), 1);
}

#[test]
fn invalid_git_markers_are_not_reported_as_worktrees() {
    let repo = TestRepo::new();
    for (name, marker) in [
        ("bad-pointer", "gitdir: /does/not/exist"),
        ("empty-pointer", "gitdir: "),
        ("not-git", "some text"),
    ] {
        let path = repo.path.join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join(".git"), marker).unwrap();
    }
    let invalid_dir = repo.path.join("bad-dir/.git");
    std::fs::create_dir_all(invalid_dir).unwrap();
    let found = scan(&repo.path, 2);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].is_main);
}

#[cfg(unix)]
#[test]
fn descendant_symlinks_neither_escape_scan_root_nor_repeat_directories() {
    let repo = TestRepo::new();
    let scope = repo.child("scope");
    std::fs::create_dir(&scope).unwrap();
    std::os::unix::fs::symlink(&repo.path, scope.join("outside")).unwrap();
    std::os::unix::fs::symlink(&scope, scope.join("loop")).unwrap();
    assert!(scan(&scope, 4).is_empty());
    // Explicitly selected symlink roots remain useful and supported.
    let direct = scan(&scope.join("outside"), 0);
    assert_eq!(direct.len(), 1);
}

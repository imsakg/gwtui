#![forbid(unsafe_code)]

#[test]
fn depth_boundaries_hidden_and_generated_directories_are_respected() {
    let tmp = tempfile::tempdir().unwrap();
    let visible = tmp.path().join("group/repo");
    std::fs::create_dir_all(&visible).unwrap();
    common::git(&visible, &["init", "-b", "main"]);
    for skip in [
        ".hidden",
        "node_modules",
        "target",
        ".idea",
        ".vscode",
        ".git",
    ] {
        let child = tmp.path().join(skip).join("repo");
        std::fs::create_dir_all(&child).unwrap();
        common::git(&child, &["init", "-b", "main"]);
    }
    assert!(scan(tmp.path(), 0).is_empty());
    assert!(scan(tmp.path(), 1).is_empty());
    assert_eq!(scan(tmp.path(), 2).len(), 1);
    assert_eq!(scan(&visible, 0).len(), 1);
    assert_eq!(scan(tmp.path(), usize::MAX).len(), 1);
    assert!(scan(&tmp.path().join("missing"), 2).is_empty());
    let file = tmp.path().join("plain-file");
    std::fs::write(&file, "data").unwrap();
    assert!(scan(&file, 2).is_empty());
    assert!(discover_global_worktrees(" ", 1).is_err());
    let found = scan(tmp.path(), 2);
    assert_eq!(filter_global_worktrees(&found, "").len(), 1);
    assert_eq!(filter_global_worktrees(&found, "REPO:MAIN").len(), 1);
    assert!(filter_global_worktrees(&found, "absent").is_empty());
}

#[test]
fn unborn_and_detached_repositories_keep_distinct_branch_states() {
    let repo = TestRepo::new();
    repo.git(&["checkout", "--detach"]);
    let unborn = repo.child("unborn");
    std::fs::create_dir(&unborn).unwrap();
    common::git(&unborn, &["init", "-b", "new-main"]);
    let found = scan(repo.temp.path(), 1);
    let detached = found
        .iter()
        .find(|e| std::path::Path::new(&e.path) == repo.path)
        .unwrap();
    assert_eq!(detached.branch, "HEAD");
    assert!(!detached.commit_hash.is_empty());
    let fresh = found
        .iter()
        .find(|e| std::path::Path::new(&e.path) == unborn)
        .unwrap();
    assert_eq!(fresh.branch, "new-main");
    assert!(fresh.commit_hash.is_empty());
}

#[test]
fn separate_git_directory_is_still_a_main_checkout() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("checkout");
    let metadata = tmp.path().join("metadata");
    common::git(
        tmp.path(),
        &[
            "init",
            "-b",
            "main",
            "--separate-git-dir",
            metadata.to_str().unwrap(),
            repo.to_str().unwrap(),
        ],
    );
    let found = scan(&repo, 0);
    assert_eq!(found.len(), 1);
    assert!(found[0].is_main);
}

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

#[test]
fn discovery_results_are_stably_sorted_by_path() {
    let repo = TestRepo::new();
    for name in ["alpha", "beta", "gamma", "zeta"] {
        repo.git(&[
            "worktree",
            "add",
            "-b",
            name,
            repo.child(name).to_str().unwrap(),
        ]);
    }
    let found = scan(repo.temp.path(), 1);
    let paths: Vec<_> = found.iter().map(|e| &e.path).collect();
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted);
    assert_eq!(found, scan(repo.temp.path(), 1));
}

#[test]
fn scans_stop_at_linked_worktrees_but_not_ordinary_main_repositories() {
    let repo = TestRepo::new();
    let linked = repo.child("linked");
    repo.git(&["worktree", "add", "-b", "feature", linked.to_str().unwrap()]);
    let main_child = repo.path.join("nested-main");
    std::fs::create_dir(&main_child).unwrap();
    common::git(&main_child, &["init", "-b", "nested"]);
    let linked_child = linked.join("nested-linked");
    std::fs::create_dir(&linked_child).unwrap();
    common::git(&linked_child, &["init", "-b", "excluded"]);
    let found = scan(repo.temp.path(), 4);
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(
        found
            .iter()
            .any(|e| std::path::Path::new(&e.path) == main_child)
    );
    assert!(
        !found
            .iter()
            .any(|e| std::path::Path::new(&e.path) == linked_child)
    );
}

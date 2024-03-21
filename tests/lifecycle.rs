mod common;

use std::path::Path;

use common::TestRepo;
use gwtui::config::Config;
use gwtui::core::git::Git;
use gwtui::core::worktree::WorktreeManager;

#[test]
fn listing_preserves_whitespace_and_newlines_in_paths() {
    let repo = TestRepo::new();
    let linked = repo.child("line\nbreak and trailing ");
    repo.git(&["worktree", "add", "-b", "topic", linked.to_str().unwrap()]);
    let worktree = manager(&repo.path).list().unwrap().remove(1);
    assert_eq!(worktree.path, linked.to_str().unwrap());
    assert_eq!(worktree.branch, "topic");
}

#[test]
fn main_removal_is_guarded_even_through_a_path_alias() {
    let repo = TestRepo::new();
    let error = manager(&repo.path)
        .remove(&repo.path.join("."), true)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot remove the main worktree")
    );
    assert!(repo.path.join(".git").is_dir());
}

#[test]
fn removing_with_the_wrong_branch_does_not_remove_the_worktree() {
    let repo = TestRepo::new();
    let linked = repo.child("topic");
    repo.git(&["worktree", "add", "-b", "topic", linked.to_str().unwrap()]);
    repo.git(&["branch", "unrelated"]);
    let result = manager(&repo.path).remove_with_branch(&linked, "unrelated", false, true, true);
    assert!(result.is_err());
    assert!(linked.exists());
    assert_eq!(repo.git(&["branch", "--list", "unrelated"]), "unrelated");
}

#[cfg(unix)]
#[test]
fn git_path_arguments_preserve_non_utf8_bytes() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let repo = TestRepo::new();
    let path = repo.temp.path().join(OsStr::from_bytes(b"linked-\xff"));
    let git = Git::from_dir(&repo.path).unwrap();
    let result = git.add_worktree(&path, "byte-path", true);
    // macOS filesystems reject invalid UTF-8; never silently create a different path.
    if cfg!(target_os = "macos") {
        assert!(result.is_err());
        assert!(!repo.temp.path().join("linked-�").exists());
    } else {
        result.unwrap();
        assert!(path.join(".git").is_file());
        git.remove_worktree(&path, false).unwrap();
        assert!(!path.exists());
    }
}

fn manager(path: &Path) -> WorktreeManager {
    WorktreeManager::new(Git::from_dir(path).unwrap(), Config::default())
}

#[test]
fn main_identity_is_stable_from_linked_worktrees() {
    let repo = TestRepo::new();
    let linked = repo.child("linked");
    repo.git(&["worktree", "add", "-b", "topic", linked.to_str().unwrap()]);
    let worktrees = manager(&linked).list().unwrap();
    assert!(worktrees[0].is_main);
    assert!(!worktrees[1].is_main);
    assert_eq!(worktrees.iter().filter(|wt| wt.is_main).count(), 1);
}

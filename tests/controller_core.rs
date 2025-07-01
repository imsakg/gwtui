mod common;
use common::TestRepo;
use gwtui::{
    config::Config,
    core::{git::Git, worktree::WorktreeManager},
};

fn manager(path: &std::path::Path) -> WorktreeManager {
    WorktreeManager::new(Git::from_dir(path).unwrap(), Config::default())
}

#[test]
fn remove_with_branch_works_when_the_manager_was_created_in_the_linked_checkout() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    manager(&path)
        .remove_with_branch(&path, "feature", false, true, false)
        .unwrap();
    assert!(!path.exists());
    assert_eq!(repo.git(&["branch", "--format=%(refname:short)"]), "main");
}

#[test]
fn custom_worktree_paths_are_literal_not_environment_templates() {
    let repo = TestRepo::new();
    let path = repo.child("$HOME");
    manager(&repo.path)
        .add("literal", Some(&path), true)
        .unwrap();
    assert!(path.join(".git").is_file());
    let other = repo.child("${HOME}");
    manager(&repo.path)
        .add_from_base("other", Some("HEAD"), Some(&other))
        .unwrap();
    assert!(other.join(".git").is_file());
}

#[test]
fn repository_detection_rejects_forged_git_markers() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join(".git")).unwrap();
    assert!(Git::from_dir(temp.path()).is_err());
    let repo = TestRepo::new();
    std::fs::create_dir(repo.path.join("nested")).unwrap();
    assert_eq!(
        Git::from_dir(&repo.path.join("nested"))
            .unwrap()
            .repo_root(),
        repo.path.canonicalize().unwrap()
    );
}

#[test]
fn repository_detection_preserves_a_trailing_carriage_return_in_unix_names() {
    let repo = TestRepo::new();
    let path = repo.child("repo\r");
    std::fs::rename(&repo.path, &path).unwrap();
    assert_eq!(
        Git::from_dir(&path).unwrap().repo_root(),
        path.canonicalize().unwrap()
    );
}

#[test]
fn bare_repositories_can_manage_linked_review_worktrees() {
    let repo = TestRepo::new();
    let bare = repo.child("bare.git");
    repo.git(&[
        "clone",
        "--bare",
        repo.path.to_str().unwrap(),
        bare.to_str().unwrap(),
    ]);
    let wm = manager(&bare);
    let checkout = repo.child("review");
    wm.add_detached(&checkout, None).unwrap();
    let rows = wm.list().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].is_main);
    assert!(checkout.join(".git").exists());
}

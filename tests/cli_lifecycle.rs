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

#[test]
fn move_command_updates_git_registration() {
    let repo = TestRepo::new();
    let old = repo.child("feature");
    let new = repo.child("moved feature");
    repo.git(&["worktree", "add", "-b", "feature", old.to_str().unwrap()]);
    success(repo.cli(&["move", "feature", new.to_str().unwrap()]));
    assert!(!old.exists());
    assert!(new.join(".git").exists());
    assert!(
        repo.git(&["worktree", "list", "--porcelain"])
            .contains(new.to_str().unwrap())
    );
}

#[test]
fn repair_command_reconnects_an_externally_moved_checkout() {
    let repo = TestRepo::new();
    let old = repo.child("feature");
    let new = repo.child("repaired");
    repo.git(&["worktree", "add", "-b", "feature", old.to_str().unwrap()]);
    std::fs::rename(&old, &new).unwrap();
    success(repo.cli(&["repair", new.to_str().unwrap()]));
    assert!(
        repo.git(&["worktree", "list", "--porcelain"])
            .contains(new.to_str().unwrap())
    );
}

#[test]
fn prune_dry_run_does_not_remove_registration() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    std::fs::remove_dir_all(&path).unwrap();
    success(repo.cli(&["prune", "--dry-run", "--expire", "now"]));
    assert!(
        repo.git(&["worktree", "list", "--porcelain"])
            .contains("feature")
    );
    success(repo.cli(&["prune", "--expire", "now"]));
    assert!(
        !repo
            .git(&["worktree", "list", "--porcelain"])
            .contains("branch refs/heads/feature")
    );
}

#[test]
fn add_can_start_a_branch_from_an_explicit_revision() {
    let repo = TestRepo::new();
    let initial = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["commit", "--allow-empty", "-m", "next"]);
    let path = repo.child("feature");
    success(repo.cli(&[
        "add",
        "-b",
        "feature",
        path.to_str().unwrap(),
        "--base",
        &initial,
    ]));
    assert_eq!(repo.git(&["rev-parse", "feature"]), initial);
}

#[test]
fn add_detached_accepts_an_optional_revision_without_creating_branch() {
    let repo = TestRepo::new();
    let initial = repo.git(&["rev-parse", "HEAD"]);
    repo.git(&["commit", "--allow-empty", "-m", "next"]);
    let path = repo.child("detached");
    success(repo.cli(&[
        "add",
        "--detach",
        path.to_str().unwrap(),
        "--revision",
        &initial,
    ]));
    assert_eq!(common::git(&path, &["rev-parse", "HEAD"]), initial);
    assert_eq!(common::git(&path, &["branch", "--show-current"]), "");
    assert_eq!(repo.git(&["branch", "--format=%(refname:short)"]), "main");
}

#[test]
fn add_print_path_is_suitable_for_command_substitution() {
    let repo = TestRepo::new();
    let path = repo.child("feature with spaces");
    let out = success(repo.cli(&[
        "add",
        "-b",
        "feature",
        path.to_str().unwrap(),
        "--print-path",
    ]));
    assert_eq!(
        out.trim_end(),
        path.canonicalize().unwrap().to_str().unwrap()
    );
}

#[test]
fn list_paths_omits_headers_and_branch_names() {
    let repo = TestRepo::new();
    let out = success(repo.cli(&["list", "--paths"]));
    assert_eq!(
        out.trim_end(),
        repo.path.canonicalize().unwrap().to_str().unwrap()
    );
}

#[test]
fn list_null_preserves_embedded_newlines_in_paths() {
    let repo = TestRepo::new();
    let path = repo.child("line\nbreak");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    let out = success(repo.cli(&["list", "--paths", "--null"]));
    let paths: Vec<_> = out.split('\0').filter(|p| !p.is_empty()).collect();
    assert_eq!(paths.len(), 2);
    assert!(paths.contains(&path.canonicalize().unwrap().to_str().unwrap()));
}

#[test]
fn list_filter_matches_branches_and_paths_case_insensitively() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&[
        "worktree",
        "add",
        "-b",
        "feature/login",
        path.to_str().unwrap(),
    ]);
    let text = success(repo.cli(&["list", "--json", "--filter", "LOGIN"]));
    let rows: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["branch"], "feature/login");
}

#[test]
fn list_sort_orders_json_by_branch_and_rejects_unknown_keys() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "aaa", path.to_str().unwrap()]);
    let text = success(repo.cli(&["list", "--json", "--sort", "branch"]));
    let rows: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(rows[0]["branch"], "aaa");
    assert!(!repo.cli(&["list", "--sort", "typo"]).status.success());
}

#[test]
fn list_linked_excludes_the_main_checkout() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    let text = success(repo.cli(&["list", "--json", "--linked"]));
    let rows: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["is_main"], false);
}

#[test]
fn get_prefers_an_exact_branch_over_a_longer_prefix_match() {
    let repo = TestRepo::new();
    let first = repo.child("first");
    let other = repo.child("second");
    repo.git(&["worktree", "add", "-b", "feature", first.to_str().unwrap()]);
    repo.git(&[
        "worktree",
        "add",
        "-b",
        "feature-longer",
        other.to_str().unwrap(),
    ]);
    let text = success(repo.cli(&["get", "feature"]));
    assert_eq!(
        text.trim_end(),
        first.canonicalize().unwrap().to_str().unwrap()
    );
}

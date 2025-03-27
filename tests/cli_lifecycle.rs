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

#[test]
fn status_rejects_conflicting_machine_formats() {
    let repo = TestRepo::new();
    let out = repo.cli(&["status", "--json", "--csv", "--no-fetch"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot be used"));
}

#[test]
fn status_rejects_zero_watch_intervals_before_starting_a_timer() {
    let repo = TestRepo::new();
    let out = repo.cli(&["status", "--interval", "0", "--no-fetch"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid value"));
}

#[test]
fn status_handles_the_largest_stale_threshold_without_overflow() {
    let repo = TestRepo::new();
    let out = success(repo.cli(&[
        "status",
        "--json",
        "--no-fetch",
        "--stale-days",
        "18446744073709551615",
    ]));
    let value: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(value["summary"]["Total"], 1);
}

#[test]
fn status_summary_includes_staged_and_conflicted_states() {
    let repo = TestRepo::new();
    std::fs::write(repo.path.join("README.md"), "staged change\n").unwrap();
    repo.git(&["add", "README.md"]);
    let text = success(repo.cli(&["status", "--json", "--no-fetch"]));
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["summary"]["Staged"], 1);
    assert_eq!(value["summary"]["Conflict"], 0);
    assert_eq!(value["summary"]["Unknown"], 0);
    assert_eq!(value["summary"]["Total"], 1);
}

#[test]
fn status_csv_exposes_untracked_staged_and_conflict_counters() {
    let repo = TestRepo::new();
    std::fs::write(repo.path.join("new.txt"), "new").unwrap();
    let text = success(repo.cli(&["status", "--csv", "--no-fetch"]));
    let mut reader = csv::Reader::from_reader(text.as_bytes());
    let headers = reader.headers().unwrap().clone();
    for name in ["untracked", "staged", "conflicts"] {
        assert!(headers.iter().any(|h| h == name), "missing {name}");
    }
    let record = reader.records().next().unwrap().unwrap();
    let column = headers.iter().position(|h| h == "untracked").unwrap();
    assert_eq!(&record[column], "1");
}

#[test]
fn repo_option_targets_a_repository_without_a_shell_cd() {
    let first = TestRepo::new();
    let second = TestRepo::new();
    let text = success(first.cli(&["-C", second.path.to_str().unwrap(), "list", "--paths"]));
    assert_eq!(
        text.trim_end(),
        second.path.canonicalize().unwrap().to_str().unwrap()
    );
}

#[test]
fn config_path_prints_the_resolved_file_without_creating_it() {
    let repo = TestRepo::new();
    let text = success(repo.cli(&["config", "path"]));
    let path = std::path::Path::new(text.trim_end());
    assert!(path.ends_with("home/.config/gwtui/config.toml"));
    assert!(!path.exists());
}

#[test]
fn config_check_validates_without_writing_defaults() {
    let repo = TestRepo::new();
    assert!(success(repo.cli(&["config", "check"])).contains("valid"));
    assert!(
        !repo
            .temp
            .path()
            .join("home/.config/gwtui/config.toml")
            .exists()
    );
    let path = repo.temp.path().join("home/.config/gwtui/config.toml");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "[status]\nconcurrency = 0\n").unwrap();
    assert!(!repo.cli(&["config", "check"]).status.success());
}

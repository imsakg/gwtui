mod common;
use common::TestRepo;

#[test]
fn doctor_produces_a_useful_snapshot_outside_git() {
    let repo = TestRepo::new();
    std::fs::remove_dir_all(repo.path.join(".git")).unwrap();
    let out = repo.cli(&["doctor", "--json"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(value["repository"].is_null());
    for key in ["worktrees", "missing_worktrees", "locked_worktrees"] {
        assert_eq!(value[key], 0);
    }
}

#[test]
fn help_and_version_remain_available_when_configuration_is_invalid() {
    let repo = TestRepo::new();
    let path = repo.temp.path().join("home/.config/gwtui/config.toml");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "[status]\nconcurrency = 0\n").unwrap();
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["version"],
        vec!["doctor", "--help"],
    ] {
        assert!(repo.cli(&args).status.success(), "{args:?}");
    }
    assert!(!repo.cli(&["doctor"]).status.success());
}

#[test]
fn list_rejects_machine_output_flag_combinations_that_would_be_ignored() {
    let repo = TestRepo::new();
    for args in [vec!["list", "--null"], vec!["list", "--paths", "--json"]] {
        assert!(!repo.cli(&args).status.success(), "{args:?}");
    }
}

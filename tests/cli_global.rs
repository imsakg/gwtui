mod common;
use common::TestRepo;

fn success(out: std::process::Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn global_listing_and_branch_removal_work_without_origin_remotes() {
    let repo = TestRepo::new();
    let path = repo.child("feature");
    repo.git(&["worktree", "add", "-b", "feature", path.to_str().unwrap()]);
    success(repo.cli(&[
        "config",
        "set",
        "worktree.base_dir",
        repo.temp.path().to_str().unwrap(),
    ]));
    let rows: Vec<serde_json::Value> = serde_json::from_str(&success(
        repo.cli(&["list", "-g", "--json", "--sort", "branch"]),
    ))
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().filter(|r| r["is_main"] == true).count(), 1);
    let linked: Vec<serde_json::Value> =
        serde_json::from_str(&success(repo.cli(&["list", "-g", "--json", "--linked"]))).unwrap();
    assert_eq!(linked.len(), 1);
    success(repo.cli(&["remove", "-g", "feature", "--delete-branch"]));
    assert!(!path.exists());
    assert!(repo.git(&["branch", "--list", "feature"]).is_empty());
}

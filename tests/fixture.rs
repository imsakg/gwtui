mod common;

#[test]
fn fixture_starts_with_a_clean_main_branch() {
    let repo = common::TestRepo::new();
    assert_eq!(repo.git(&["branch", "--show-current"]), "main");
    assert_eq!(repo.git(&["status", "--porcelain"]), "");
    let out = repo.cli(&["list", "--json"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value.as_array().unwrap().len(), 1);
}

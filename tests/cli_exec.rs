mod common;
use common::TestRepo;

#[test]
fn exec_preserves_the_child_exit_status() {
    let repo = TestRepo::new();
    let out = repo.cli(&["exec", "main", "--", "sh", "-c", "exit 37"]);
    assert_eq!(out.status.code(), Some(37));
}

#[test]
fn exec_preserves_empty_arguments_and_shell_metacharacters_literally() {
    let repo = TestRepo::new();
    let literal = "space ; $(touch should-not-exist) & quote'\"";
    let out = repo.cli(&[
        "exec",
        "main",
        "--",
        "sh",
        "-c",
        "printf '<%s><%s>' \"$1\" \"$2\"",
        "sh",
        "",
        literal,
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        format!("<><{literal}>")
    );
    assert!(!repo.path.join("should-not-exist").exists());
}

#[test]
fn exec_requires_a_separator_and_a_program() {
    let repo = TestRepo::new();
    for args in [vec!["exec", "main", "echo"], vec!["exec", "main", "--"]] {
        assert!(!repo.cli(&args).status.success());
    }
}

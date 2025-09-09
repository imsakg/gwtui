mod common;
use common::TestRepo;

#[test]
fn every_public_command_has_noninteractive_help() {
    let repo = TestRepo::new();
    for command in [
        "add",
        "list",
        "get",
        "exec",
        "remove",
        "status",
        "prune",
        "lock",
        "unlock",
        "move",
        "repair",
        "doctor",
        "branches",
        "config",
        "task",
        "zellij",
        "completion",
    ] {
        let out = repo.cli(&[command, "--help"]);
        assert!(
            out.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage:"));
    }
}

#[test]
fn shell_completions_include_the_new_lifecycle_commands() {
    let repo = TestRepo::new();
    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        let out = repo.cli(&["completion", shell]);
        assert!(
            out.status.success(),
            "{shell}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.contains("doctor"), "{shell}");
        assert!(text.contains("repair"), "{shell}");
    }
}

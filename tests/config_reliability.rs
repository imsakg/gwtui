#![forbid(unsafe_code)]

#[test]
fn executable_template_and_sanitizer_constraints_are_explicit() {
    for invalid in ["", "   ", "zellij\0arg", "zellij\narg", " zellij"] {
        let mut cfg = Config::default();
        cfg.mux.zellij_command = invalid.into();
        assert!(cfg.validate().is_err(), "{invalid:?}");
        let mut cfg = Config::default();
        cfg.tasks.codex_executable = invalid.into();
        assert!(cfg.validate().is_err(), "{invalid:?}");
        let mut cfg = Config::default();
        cfg.tasks.claude_executable = invalid.into();
        assert!(cfg.validate().is_err(), "{invalid:?}");
    }
    let mut cfg = Config::default();
    cfg.mux.zellij_command = "/Applications/My Tools/zellij".into();
    assert!(cfg.validate().is_ok());
    cfg.worktree.naming_template = " ".into();
    assert!(cfg.validate().is_err());
    cfg.worktree.naming_template = "{{branch}}".into();
    cfg.worktree.sanitize.insert("".into(), "oops".into());
    assert!(cfg.validate().is_err());
    cfg.worktree.sanitize.remove("");
    cfg.worktree.sanitize.insert("/".into(), "".into());
    assert!(cfg.validate().is_ok());
}

#[test]
fn refresh_and_concurrency_limits_are_validated_before_use() {
    let mut cfg = Config::default();
    cfg.status.refresh_interval_ms = 0;
    assert!(cfg.validate().is_err());
    cfg.status.refresh_interval_ms = 1;
    assert!(cfg.validate().is_ok());
    for concurrency in [0, 1025, usize::MAX] {
        cfg.status.concurrency = concurrency;
        assert!(cfg.validate().is_err());
    }
    for concurrency in [1, 1024] {
        cfg.status.concurrency = concurrency;
        assert!(cfg.validate().is_ok());
    }
}

use gwtui::config::{self, Config};

#[test]
fn tilde_expansion_is_exact_and_cannot_reset_the_home_prefix() {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap();
    assert_eq!(config::expand_tilde("~"), home);
    assert_eq!(
        config::expand_path("~").unwrap(),
        std::path::PathBuf::from(&home)
    );
    assert_eq!(
        config::expand_tilde("~//nested"),
        std::path::Path::new(&home).join("nested").to_string_lossy()
    );
    assert_eq!(config::expand_tilde("~other/nested"), "~other/nested");
    assert_eq!(config::expand_tilde("some/~/path"), "some/~/path");
    assert_eq!(
        config::tilde_path(&format!("{home}-other/file")),
        format!("{home}-other/file")
    );
}

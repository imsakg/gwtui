#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// An isolated, locally configured repository for real-git integration tests.
pub struct TestRepo {
    pub temp: tempfile::TempDir,
    pub path: PathBuf,
}

impl Default for TestRepo {
    fn default() -> Self {
        Self::new()
    }
}

impl TestRepo {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().expect("test directory");
        let path = temp.path().join("repo");
        std::fs::create_dir(&path).unwrap();
        git(&path, &["init", "-b", "main"]);
        git(&path, &["config", "user.name", "Test Fixture"]);
        git(&path, &["config", "user.email", "fixture@example.invalid"]);
        git(&path, &["config", "commit.gpgsign", "false"]);
        git(&path, &["config", "core.hooksPath", "/dev/null"]);
        std::fs::write(path.join("README.md"), "initial\n").unwrap();
        git(&path, &["add", "README.md"]);
        git(&path, &["commit", "-m", "initial"]);
        Self { temp, path }
    }

    pub fn git(&self, args: &[&str]) -> String {
        git(&self.path, args)
    }

    pub fn child(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    /// Run the real CLI without inheriting the developer's personal config.
    pub fn cli(&self, args: &[&str]) -> Output {
        let home = self.temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Command::new(env!("CARGO_BIN_EXE_gwtui"))
            .args(args)
            .current_dir(&self.path)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env_remove("GWTUI_CONFIG")
            .output()
            .expect("run gwtui")
    }
}

pub fn git(path: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .current_dir(path)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

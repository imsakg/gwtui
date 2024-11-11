#![forbid(unsafe_code)]
use std::path::Path;
use std::process::{Command, Output};
fn cli(root: &Path, xdg: &Path, override_path: Option<&str>, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gwtui"));
    cmd.args(args)
        .current_dir(root)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("XDG_CONFIG_HOME", xdg)
        .env_remove("GWTUI_CONFIG");
    if let Some(path) = override_path {
        cmd.env("GWTUI_CONFIG", path);
    }
    cmd.output().unwrap()
}
fn text(out: Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}
#[test]
fn explicit_config_override_wins_and_is_shared_by_set_get_and_list() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let xdg = root.join("xdg");
    std::fs::create_dir_all(xdg.join("gwtui")).unwrap();
    std::fs::write(xdg.join("gwtui/config.toml"), "[ui]\nicons = true\n").unwrap();
    let custom = "settings/custom.toml";
    text(cli(
        root,
        &xdg,
        Some(custom),
        &["config", "set", "ui.icons", "false"],
    ));
    assert!(root.join(custom).exists());
    assert_eq!(
        text(cli(
            root,
            &xdg,
            Some(custom),
            &["config", "get", "ui.icons"]
        )),
        "false"
    );
    assert!(text(cli(root, &xdg, Some(custom), &["config", "list"])).contains("icons = false"));
    assert!(
        std::fs::read_to_string(xdg.join("gwtui/config.toml"))
            .unwrap()
            .contains("icons = true")
    );
    assert!(
        !cli(root, &xdg, Some(""), &["config", "list"])
            .status
            .success()
    );
}
#[test]
fn xdg_config_home_is_used_when_no_explicit_override_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let xdg = root.join("xdg");
    text(cli(
        root,
        &xdg,
        None,
        &["config", "set", "ui.icons", "false"],
    ));
    assert!(xdg.join("gwtui/config.toml").exists());
    assert_eq!(
        text(cli(root, &xdg, None, &["config", "get", "ui.icons"])),
        "false"
    );
    assert!(!root.join(".config/gwtui/config.toml").exists());
}

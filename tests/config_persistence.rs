#![forbid(unsafe_code)]
use gwtui::config::{get_value_string_at_path, set_value_string_at_path};

#[cfg(unix)]
#[test]
fn atomic_updates_keep_existing_readers_on_their_original_snapshot() {
    use std::io::Read;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("config.toml");
    let original = "# keep this comment\n[ui]\nicons = true\n";
    std::fs::write(&path, original).unwrap();
    let mut reader = std::fs::File::open(&path).unwrap();
    set_value_string_at_path(&path, "ui.icons", "false").unwrap();
    let mut snapshot = String::new();
    reader.read_to_string(&mut snapshot).unwrap();
    assert_eq!(snapshot, original);
    let updated = std::fs::read_to_string(&path).unwrap();
    assert!(updated.contains("# keep this comment"));
    assert!(updated.contains("icons = false"));
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn new_config_is_private_and_existing_mode_is_preserved() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("nested/config.toml");
    set_value_string_at_path(&path, "ui.icons", "false").unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    set_value_string_at_path(&path, "ui.icons", "true").unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

#[cfg(unix)]
#[test]
fn updates_follow_existing_config_symlinks_without_replacing_them() {
    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("actual.toml");
    let link = tmp.path().join("config.toml");
    std::fs::write(&target, "[ui]\nicons = true\n").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    set_value_string_at_path(&link, "ui.icons", "false").unwrap();
    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        get_value_string_at_path(&target, "ui.icons")
            .unwrap()
            .as_deref(),
        Some("false")
    );
}

#[test]
fn parallel_writers_leave_complete_toml_and_no_temporary_files() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("config.toml");
    set_value_string_at_path(&path, "ui.icons", "false").unwrap();
    let threads: Vec<_> = (0..12)
        .map(|n| {
            let path = path.clone();
            std::thread::spawn(move || {
                set_value_string_at_path(
                    &path,
                    "ui.icons",
                    if n % 2 == 0 { "true" } else { "false" },
                )
                .unwrap();
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let raw = std::fs::read_to_string(&path).unwrap();
    let _: gwtui::config::Config = toml::from_str(&raw).unwrap();
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 1);
}

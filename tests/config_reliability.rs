#![forbid(unsafe_code)]
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

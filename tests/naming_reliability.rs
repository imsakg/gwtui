#![forbid(unsafe_code)]
use gwtui::core::naming::{
    RepoId, parse_origin_url, render_template, sanitize_all, sanitize_for_filesystem,
};

#[test]
fn nested_groups_preserve_namespace_and_final_repository() {
    for url in [
        "https://gitlab.example/team/subgroup/service.git",
        "ssh://git@gitlab.example/team/subgroup/service.git/",
        "git@gitlab.example:team/subgroup/service.git",
    ] {
        let id = parse_origin_url(url).unwrap();
        assert_eq!(id.host, "gitlab.example");
        assert_eq!(id.owner, "team/subgroup");
        assert_eq!(id.repo, "service");
    }
}

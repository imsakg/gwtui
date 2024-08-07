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

#[test]
fn repository_hosts_exclude_credentials_and_transport_ports() {
    for url in [
        "https://alice:secret@git.example:443/team/repo.git",
        "ssh://deploy@git.example:2222/team/repo.git",
        "http://git.example:8080/team/repo",
    ] {
        let id = parse_origin_url(url).unwrap();
        assert_eq!(id.host, "git.example", "{url}");
        assert_eq!(id.repo, "repo");
        assert!(!format!("{id:?}").contains("secret"));
    }
}

#[test]
fn malformed_and_traversing_origins_do_not_produce_path_identities() {
    for url in [
        "https://host/../repo.git",
        "https://host/owner/../repo",
        "https://host//repo",
        "https://host/owner//repo",
        "https://host/owner/.git",
        "https://host/owner/.",
        "https://host/owner/..",
        "https://host/owner/repo?token=secret",
        "https://host/owner/repo#frag",
        "https://host/owner/repo\\escape",
        "https://host/owner/%2e%2e/repo",
        "https://host:bad/owner/repo",
        "https://host:99999/owner/repo",
        "https://@/owner/repo",
        "https://../owner/repo",
        "https://host/owner/white space",
        "git@host:/owner/repo",
    ] {
        assert!(parse_origin_url(url).is_none(), "{url}");
    }
}

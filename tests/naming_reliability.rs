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

#[test]
fn supports_git_transport_and_scp_style_deployment_users() {
    for url in [
        "git://git.example/team/repo.git",
        "deploy@git.example:team/repo.git",
        "git.example:team/repo.git",
        "builder@git.example:team/sub/repo",
    ] {
        let id = parse_origin_url(url).unwrap();
        assert_eq!(id.host, "git.example");
        assert_eq!(id.repo, "repo");
    }
    for url in [
        "/local/owner/repo",
        "../relative/repo",
        "C:\\work\\repo",
        "file:///owner/repo",
    ] {
        assert!(parse_origin_url(url).is_none(), "{url}");
    }
}

#[test]
fn empty_or_dot_only_branch_names_have_a_safe_fallback() {
    for name in ["", ".", "..", "...", "---", "/\\", "\0\n"] {
        assert_eq!(sanitize_for_filesystem(name), "branch", "{name:?}");
    }
    assert_eq!(sanitize_for_filesystem(".feature"), ".feature");
}

#[test]
fn filesystem_names_do_not_end_with_windows_ignored_dots_or_spaces() {
    for (input, expected) in [
        ("feature...", "feature"),
        ("feature.  ", "feature"),
        ("   ", "branch"),
        (" . ", "branch"),
        (".feature", ".feature"),
        ("a.b", "a.b"),
    ] {
        assert_eq!(sanitize_for_filesystem(input), expected, "{input:?}");
    }
}

#[test]
fn windows_device_names_are_escaped_even_with_extensions() {
    for input in [
        "CON", "con.txt", "PRN", "Aux", "nul.log", "COM1", "com9.txt", "LPT1", "lpt9.log", "COM¹",
        "LPT²",
    ] {
        assert_eq!(
            sanitize_for_filesystem(input),
            format!("_{input}"),
            "{input}"
        );
    }
    for ordinary in ["console", "COM0", "COM10", "LPT10", "auxiliary", ".CON"] {
        assert_eq!(sanitize_for_filesystem(ordinary), ordinary);
    }
}

#[test]
fn template_values_are_literal_instead_of_recursively_expanded() {
    let id = RepoId {
        host: "{{owner}}".into(),
        owner: "{{repo}}".into(),
        repo: "{{branch}}".into(),
    };
    assert_eq!(
        render_template(
            "{{host}}/{{owner}}/{{repo}}/{{branch}}/{{unknown}}",
            &id,
            "{{hash}}"
        ),
        "{{owner}}/{{repo}}/{{branch}}/{{hash}}/{{unknown}}"
    );
    assert_eq!(
        render_template(
            "{{.Host}}/{{.Owner}}/{{.Repository}}/{{.Branch}}",
            &id,
            "{{.Hash}}"
        ),
        "{{owner}}/{{repo}}/{{branch}}/{{.Hash}}"
    );
}

#[test]
fn empty_sanitizer_keys_do_not_inject_between_every_character() {
    let mappings =
        std::collections::BTreeMap::from([("".into(), "oops".into()), ("/".into(), "_".into())]);
    assert_eq!(sanitize_all("feat/path".into(), &mappings), "feat_path");
    assert_eq!(sanitize_all(String::new(), &mappings), "branch");
}

#[test]
fn sanitizer_is_idempotent_for_unicode_and_mixed_trailing_punctuation() {
    for value in [
        "foo-.",
        "foo .-.-",
        "feat/東京",
        "café\\branch",
        "CON...",
        "..",
        "💡/draft",
        "a::b?c",
        "a\t\nb",
        ".hidden",
    ] {
        let clean = sanitize_for_filesystem(value);
        assert_eq!(sanitize_for_filesystem(&clean), clean, "{value:?}");
        assert!(!clean.is_empty());
        assert!(!clean.ends_with(['.', ' ', '-']));
        assert!(
            !clean.chars().any(|c| c.is_control()
                || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        );
    }
}

#[test]
fn naming_aliases_and_hashes_are_stable_and_branch_sensitive() {
    let id = RepoId {
        host: "example.com".into(),
        owner: "team".into(),
        repo: "repo".into(),
    };
    let gwtui = render_template(
        "{{host}}/{{owner}}/{{repo}}/{{branch}}-{{hash}}",
        &id,
        "feature/x",
    );
    let gwq = render_template(
        "{{.Host}}/{{.Owner}}/{{.Repository}}/{{.Branch}}-{{.Hash}}",
        &id,
        "feature/x",
    );
    assert_eq!(gwtui, gwq);
    assert_eq!(render_template("{{hash}}", &id, "feature/x"), "8ab5c1e9");
    assert_ne!(
        render_template("{{hash}}", &id, "feature/x"),
        render_template("{{hash}}", &id, "feature/y")
    );
    assert_eq!(render_template("", &id, "main"), "");
}

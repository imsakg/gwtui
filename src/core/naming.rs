#![forbid(unsafe_code)]

use regex::Regex;
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoId {
    pub host: String,
    pub owner: String,
    pub repo: String,
}

#[must_use]
pub fn parse_origin_url(url: &str) -> Option<RepoId> {
    // Accept:
    // - https://host/owner/repo(.git)
    // - ssh://git@host/owner/repo(.git)
    // - git@host:owner/repo(.git)
    let url = url.trim();

    if let Some(rest) = url.strip_prefix("git@") {
        // git@github.com:owner/repo.git
        let (host, path) = rest.split_once(':')?;
        return parse_host_path(host, path);
    }

    let re = Regex::new(r"^(?:(?:https?)|ssh)://(?:git@)?([^/]+)/(.+)$").ok()?;
    let caps = re.captures(url)?;
    let host = caps.get(1)?.as_str();
    let path = caps.get(2)?.as_str();
    parse_host_path(host, path)
}

fn parse_host_path(host: &str, path: &str) -> Option<RepoId> {
    let mut parts = path.trim_matches('/').split('/');
    let owner = parts.next()?.to_owned();
    let repo_raw = parts.next()?.to_owned();
    let repo = repo_raw
        .strip_suffix(".git")
        .unwrap_or(&repo_raw)
        .to_owned();
    Some(RepoId {
        host: host.to_owned(),
        owner,
        repo,
    })
}

#[must_use]
pub fn render_template(template: &str, id: &RepoId, branch: &str) -> String {
    let hash = short_hash(&format!("{}/{}/{}", id.host, id.owner, id.repo), branch);

    // Support both gwtui-style and gwq-style tokens.
    template
        .replace("{{host}}", &id.host)
        .replace("{{owner}}", &id.owner)
        .replace("{{repo}}", &id.repo)
        .replace("{{branch}}", branch)
        .replace("{{hash}}", &hash)
        .replace("{{.Host}}", &id.host)
        .replace("{{.Owner}}", &id.owner)
        .replace("{{.Repository}}", &id.repo)
        .replace("{{.Branch}}", branch)
        .replace("{{.Hash}}", &hash)
}

#[must_use]
pub fn sanitize_all(
    mut s: String,
    sanitize: &std::collections::BTreeMap<String, String>,
) -> String {
    for (from, to) in sanitize {
        s = s.replace(from, to);
    }
    sanitize_for_filesystem(&s)
}

#[must_use]
pub fn sanitize_for_filesystem(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        // Replace path separators and NUL and other control chars.
        if c == '/' || c == '\\' || c == '\0' || c.is_control() {
            out.push('-');
            continue;
        }
        // Windows reserved characters.
        if matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|') {
            out.push('-');
            continue;
        }
        out.push(c);
    }
    // Collapse consecutive '-' for nicer paths.
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_owned()
}

fn short_hash(repo: &str, branch: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(repo.as_bytes());
    hasher.update(b"/");
    hasher.update(branch.as_bytes());
    let digest = hasher.finalize();
    let bytes = &digest[..4];
    let mut s = String::with_capacity(8);
    for b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn parses_origin_urls() {
        let id = parse_origin_url("https://github.com/imsakg/rusty-boilerplate.git").unwrap();
        assert_eq!(
            id,
            RepoId {
                host: "github.com".to_owned(),
                owner: "imsakg".to_owned(),
                repo: "rusty-boilerplate".to_owned()
            }
        );

        let id = parse_origin_url("ssh://git@github.com/imsakg/rusty-boilerplate.git").unwrap();
        assert_eq!(id.owner, "imsakg");

        let id = parse_origin_url("git@github.com:imsakg/rusty-boilerplate.git").unwrap();
        assert_eq!(id.repo, "rusty-boilerplate");

        assert!(parse_origin_url("not a url").is_none());
    }

    #[test]
    fn renders_templates_and_sanitizes() {
        let id = RepoId {
            host: "example.com".to_owned(),
            owner: "me".to_owned(),
            repo: "repo".to_owned(),
        };
        let rendered = render_template(
            "{{host}}/{{owner}}/{{repo}}/{{branch}}-{{hash}}",
            &id,
            "feat/x",
        );
        assert!(rendered.starts_with("example.com/me/repo/feat/x-"));
        assert_eq!(rendered.split('-').next_back().unwrap().len(), 8);

        let rendered = render_template(
            "{{.Host}}/{{.Owner}}/{{.Repository}}/{{.Branch}}-{{.Hash}}",
            &id,
            "b",
        );
        assert!(rendered.starts_with("example.com/me/repo/b-"));

        let mut sanitize = BTreeMap::new();
        sanitize.insert("/".to_owned(), "-".to_owned());
        sanitize.insert(":".to_owned(), "_".to_owned());
        let s = sanitize_all("feat/foo:bar".to_owned(), &sanitize);
        assert_eq!(s, "feat-foo_bar");
    }
}

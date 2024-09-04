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

    if !url.contains("://") {
        // SCP style supports SSH aliases and arbitrary deployment usernames.
        let (authority, path) = url.split_once(':')?;
        // A one-letter host followed by a backslash is a Windows drive path.
        if authority.len() == 1 && path.starts_with('\\') {
            return None;
        }
        return parse_host_path(authority, path);
    }

    let re = Regex::new(r"^(?:(?:https?)|ssh|git)://([^/]+)/(.+)$").ok()?;
    let caps = re.captures(url)?;
    let host = caps.get(1)?.as_str();
    let path = caps.get(2)?.as_str();
    parse_host_path(host, path)
}

fn parse_host_path(host: &str, path: &str) -> Option<RepoId> {
    // Credentials and transport ports must never become filesystem components.
    let host = host.rsplit('@').next()?;
    let host = if let Some((host, port)) = host.split_once(':') {
        if port.parse::<u16>().ok()? == 0 {
            return None;
        }
        host
    } else {
        host
    };
    if host.is_empty()
        || matches!(host, "." | "..")
        || !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return None;
    }
    let parts: Vec<_> = path.trim_end_matches('/').split('/').collect();
    if parts.iter().any(|part| !safe_remote_segment(part)) {
        return None;
    }
    if parts.len() < 2 {
        return None;
    }
    let owner = parts[..parts.len() - 1].join("/");
    let repo_raw = parts.last()?.to_string();
    let repo = repo_raw
        .strip_suffix(".git")
        .unwrap_or(&repo_raw)
        .to_owned();
    if repo.is_empty() || matches!(repo.as_str(), "." | "..") {
        return None;
    }
    Some(RepoId {
        host: host.to_owned(),
        owner,
        repo,
    })
}

fn safe_remote_segment(segment: &str) -> bool {
    let lower = segment.to_ascii_lowercase();
    !segment.is_empty()
        && !matches!(segment, "." | "..")
        && !segment.chars().any(|c| {
            c.is_control()
                || c.is_whitespace()
                || matches!(c, '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '#')
        })
        && !["%2e", "%2f", "%5c", "%00"]
            .iter()
            .any(|escape| lower.contains(escape))
}

#[must_use]
pub fn render_template(template: &str, id: &RepoId, branch: &str) -> String {
    let hash = short_hash(&format!("{}/{}/{}", id.host, id.owner, id.repo), branch);

    // Match original template tokens once; values are never parsed as templates.
    let re = Regex::new(
        r"\{\{(?:host|owner|repo|branch|hash|\.Host|\.Owner|\.Repository|\.Branch|\.Hash)\}\}",
    )
    .expect("static template token pattern");
    re.replace_all(template, |caps: &regex::Captures<'_>| {
        match &caps[0] {
            "{{host}}" | "{{.Host}}" => id.host.as_str(),
            "{{owner}}" | "{{.Owner}}" => id.owner.as_str(),
            "{{repo}}" | "{{.Repository}}" => id.repo.as_str(),
            "{{branch}}" | "{{.Branch}}" => branch,
            "{{hash}}" | "{{.Hash}}" => hash.as_str(),
            _ => unreachable!("static token pattern"),
        }
        .to_owned()
    })
    .into_owned()
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
    let out = out.trim_matches('-').trim_end_matches(['.', ' ']);
    if out.is_empty() || out.chars().all(|c| c == '.') {
        "branch".to_owned()
    } else if is_windows_device_name(out) {
        format!("_{out}")
    } else {
        out.to_owned()
    }
}

fn is_windows_device_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end_matches(' ')
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ["COM", "LPT"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                matches!(
                    suffix,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
        })
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

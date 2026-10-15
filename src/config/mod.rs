#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

use crate::error::GwtuiError;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Config {
    pub worktree: WorktreeConfig,
    pub discovery: DiscoveryConfig,
    pub ui: UiConfig,
    pub mux: MuxConfig,
    pub status: StatusConfig,
    pub tasks: TasksConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WorktreeConfig {
    #[serde(alias = "basedir")]
    pub base_dir: String,
    pub auto_mkdir: bool,
    #[serde(alias = "template")]
    pub naming_template: String,
    #[serde(alias = "sanitize_chars")]
    pub sanitize: BTreeMap<String, String>,
}

impl Default for WorktreeConfig {
    fn default() -> Self {
        let mut sanitize = BTreeMap::new();
        sanitize.insert("/".to_owned(), "-".to_owned());
        sanitize.insert(":".to_owned(), "-".to_owned());
        sanitize.insert(" ".to_owned(), "-".to_owned());
        Self {
            base_dir: "~/worktrees".to_owned(),
            auto_mkdir: true,
            naming_template: "{{host}}/{{owner}}/{{repo}}/{{branch}}".to_owned(),
            sanitize,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DiscoveryConfig {
    pub mode: DiscoveryMode,
    pub global_scan_depth: usize,
    pub cache_ttl_seconds: u64,
    pub dedupe_by_main_repo: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            mode: DiscoveryMode::Auto,
            global_scan_depth: 6,
            cache_ttl_seconds: 3,
            dedupe_by_main_repo: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryMode {
    Auto,
    Local,
    Global,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct UiConfig {
    pub icons: bool,
    pub tilde_home: bool,
    pub picker_preview: bool,
    pub picker_preview_lines: u16,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            icons: true,
            tilde_home: true,
            picker_preview: true,
            picker_preview_lines: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MuxConfig {
    pub backend: MuxBackend,
    pub zellij_command: String,
    pub require_session_for_run: bool,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            backend: MuxBackend::Zellij,
            zellij_command: "zellij".to_owned(),
            require_session_for_run: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MuxBackend {
    Zellij,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StatusConfig {
    pub refresh_interval_ms: u64,
    pub concurrency: usize,
    pub default_sort: String,
    pub default_filter: String,
}

impl Default for StatusConfig {
    fn default() -> Self {
        Self {
            refresh_interval_ms: 2000,
            concurrency: 8,
            default_sort: "activity".to_owned(),
            default_filter: "all".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct TasksConfig {
    pub enabled: bool,
    pub queue_dir: String,
    pub log_retention_days: u64,
    pub max_log_size_mb: u64,
    pub auto_cleanup: bool,
    pub runner: String,
    pub codex_executable: String,
    pub codex_timeout: String,
    pub claude_executable: String,
    pub claude_timeout: String,
    pub max_parallel: usize,
}

impl Default for TasksConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_dir: "~/.config/gwtui/tasks".to_owned(),
            log_retention_days: 30,
            max_log_size_mb: 100,
            auto_cleanup: true,
            runner: "codex".to_owned(),
            codex_executable: "codex".to_owned(),
            codex_timeout: "30m".to_owned(),
            claude_executable: "claude".to_owned(),
            claude_timeout: "30m".to_owned(),
            max_parallel: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConfigPaths {
    pub config_file: PathBuf,
}

pub fn default_paths() -> anyhow::Result<ConfigPaths> {
    let unix = home_config_path_unix();
    if !cfg!(windows) {
        return Ok(ConfigPaths { config_file: unix });
    }

    // Windows: prefer the Unix-style path if present for portability.
    if unix.exists() {
        return Ok(ConfigPaths { config_file: unix });
    }

    let proj = ProjectDirs::from("com", "gwtui", "gwtui")
        .context("failed to determine platform config directory")?;
    Ok(ConfigPaths {
        config_file: proj.config_dir().join("config.toml"),
    })
}

fn home_config_path_unix() -> PathBuf {
    let home = home_dir().unwrap_or_else(|| PathBuf::from("~"));
    home.join(".config").join("gwtui").join("config.toml")
}

fn home_dir() -> Option<PathBuf> {
    if let Some(v) = std::env::var_os("HOME") {
        return Some(PathBuf::from(v));
    }
    if let Some(v) = std::env::var_os("USERPROFILE") {
        return Some(PathBuf::from(v));
    }
    let drive = std::env::var_os("HOMEDRIVE");
    let path = std::env::var_os("HOMEPATH");
    match (drive, path) {
        (Some(d), Some(p)) => Some(PathBuf::from(d).join(PathBuf::from(p))),
        _ => None,
    }
}

#[must_use]
pub fn expand_tilde(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = home_dir()
    {
        return home.join(rest).to_string_lossy().to_string();
    }
    input.to_owned()
}

#[must_use]
pub fn tilde_path(input: &str) -> String {
    let Some(home) = home_dir() else {
        return input.to_owned();
    };
    let home_str = home.to_string_lossy();
    if let Some(rest) = input.strip_prefix(home_str.as_ref()) {
        if rest.is_empty() {
            return "~".to_owned();
        }
        if rest.starts_with(std::path::MAIN_SEPARATOR) {
            return format!("~{rest}");
        }
    }
    input.to_owned()
}

pub fn expand_path(input: &str) -> anyhow::Result<PathBuf> {
    let expanded = expand_env_vars(&expand_tilde(input));
    let p = PathBuf::from(expanded);
    if p.is_absolute() {
        return Ok(p);
    }
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    Ok(cwd.join(p))
}

fn expand_env_vars(input: &str) -> String {
    // Expand $VAR and ${VAR}. Leave unknown vars untouched.
    let re = regex::Regex::new(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?")
        .unwrap_or_else(|_| regex::Regex::new("$^").unwrap());
    re.replace_all(input, |caps: &regex::Captures<'_>| {
        let key = &caps[1];
        std::env::var(key).unwrap_or_else(|_| caps[0].to_owned())
    })
    .to_string()
}

pub fn load() -> anyhow::Result<(Config, toml_edit::DocumentMut, ConfigPaths)> {
    let paths = default_paths()?;
    let (doc, cfg) = load_from_file(&paths.config_file)?;
    cfg.validate()?;
    Ok((cfg, doc, paths))
}

pub fn list_resolved_toml() -> anyhow::Result<String> {
    let (cfg, _doc, _paths) = load()?;
    Ok(toml::to_string_pretty(&cfg)?)
}

pub fn get_value_string(key: &str) -> anyhow::Result<Option<String>> {
    let paths = default_paths()?;
    get_value_string_at_path(&paths.config_file, key)
}

pub fn set_value_string(key: &str, value: &str) -> anyhow::Result<()> {
    let paths = default_paths()?;
    set_value_string_at_path(&paths.config_file, key, value)
}

fn load_from_file(path: &Path) -> anyhow::Result<(toml_edit::DocumentMut, Config)> {
    if !path.exists() {
        return Ok((toml_edit::DocumentMut::new(), Config::default()));
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;

    let doc = raw
        .parse::<toml_edit::DocumentMut>()
        .with_context(|| format!("failed to parse TOML in {}", path.display()))?;

    let cfg: Config = toml::from_str(&raw)
        .with_context(|| format!("failed to deserialize TOML in {}", path.display()))?;
    Ok((doc, cfg))
}

pub fn get_value_string_at_path(path: &Path, key: &str) -> anyhow::Result<Option<String>> {
    let (_doc, cfg) = load_from_file(path)?;
    cfg.validate()?;

    if key == "tmux.enabled" {
        let enabled = cfg.mux.backend != MuxBackend::None;
        return Ok(Some(enabled.to_string()));
    }

    let norm = normalize_key(key);
    let value = lookup_value(&cfg, &norm);
    Ok(value.map(format_value_for_stdout))
}

pub fn set_value_string_at_path(path: &Path, key: &str, value: &str) -> anyhow::Result<()> {
    let (mut doc, cfg) = load_from_file(path)?;
    cfg.validate()?;

    let (norm_key, value_item) = normalize_key_and_parse_value(key, value, &cfg)?;
    apply_set(&mut doc, &norm_key, value_item)?;

    // Validate by re-parsing the updated doc into a Config.
    let new_raw = doc.to_string();
    let new_cfg: Config = toml::from_str(&new_raw)
        .with_context(|| format!("config update produced invalid TOML for {}", path.display()))?;
    new_cfg.validate()?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, new_raw.as_bytes())
        .with_context(|| format!("failed to write {}", path.display()))?;

    Ok(())
}

impl Config {
    pub fn validate(&self) -> Result<(), GwtuiError> {
        if self.worktree.base_dir.trim().is_empty() {
            return Err(GwtuiError::Config(
                "worktree.base_dir must not be empty".to_owned(),
            ));
        }
        if self.discovery.global_scan_depth == 0 {
            return Err(GwtuiError::Config(
                "discovery.global_scan_depth must be >= 1".to_owned(),
            ));
        }
        if self.status.concurrency == 0 {
            return Err(GwtuiError::Config(
                "status.concurrency must be >= 1".to_owned(),
            ));
        }
        if self.ui.picker_preview_lines == 0 {
            return Err(GwtuiError::Config(
                "ui.picker_preview_lines must be >= 1".to_owned(),
            ));
        }
        if self.tasks.max_parallel == 0 {
            return Err(GwtuiError::Config(
                "tasks.max_parallel must be >= 1".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyType {
    Bool,
    Int,
    String,
    Enum(&'static [&'static str]),
}

fn normalize_key(key: &str) -> String {
    // gwq compatibility aliases
    match key {
        "worktree.basedir" => "worktree.base_dir",
        "naming.template" => "worktree.naming_template",
        "naming.sanitize_chars" => "worktree.sanitize",
        "finder.preview" => "ui.picker_preview",
        "finder.preview_size" => "ui.picker_preview_lines",
        "tmux.tmux_command" => "mux.zellij_command",
        "codex.executable" => "tasks.codex_executable",
        "codex.timeout" => "tasks.codex_timeout",
        "claude.executable" => "tasks.claude_executable",
        "claude.timeout" => "tasks.claude_timeout",
        "claude.max_parallel" => "tasks.max_parallel",
        "claude.task.queue_dir" => "tasks.queue_dir",
        "claude.task.log_retention_days" => "tasks.log_retention_days",
        "claude.task.max_log_size_mb" => "tasks.max_log_size_mb",
        "claude.task.auto_cleanup" => "tasks.auto_cleanup",
        _ => key,
    }
    .to_owned()
}

fn normalize_key_and_parse_value(
    key: &str,
    value: &str,
    cfg: &Config,
) -> anyhow::Result<(String, toml_edit::Item)> {
    // Special-case gwq tmux.enabled -> gwtui mux.backend
    if key == "tmux.enabled" {
        let enabled = parse_bool(value).map_err(|msg| GwtuiError::InvalidConfigValue {
            key: key.to_owned(),
            msg,
        })?;
        let backend = if enabled { "zellij" } else { "none" };
        return Ok(("mux.backend".to_owned(), toml_edit::value(backend)));
    }

    let norm = normalize_key(key);
    if norm == "worktree.sanitize" {
        return Err(GwtuiError::InvalidConfigValue {
            key: key.to_owned(),
            msg: "set individual mappings via worktree.sanitize.<from> (example: gwtui config set worktree.sanitize./ -)".to_owned(),
        }
        .into());
    }
    let key_type =
        key_type(&norm, cfg).ok_or_else(|| GwtuiError::InvalidConfigKey(key.to_owned()))?;
    let item =
        match key_type {
            KeyType::Bool => toml_edit::value(parse_bool(value).map_err(|msg| {
                GwtuiError::InvalidConfigValue {
                    key: key.to_owned(),
                    msg,
                }
            })?),
            KeyType::Int => toml_edit::value(parse_int(value).map_err(|msg| {
                GwtuiError::InvalidConfigValue {
                    key: key.to_owned(),
                    msg,
                }
            })?),
            KeyType::String => toml_edit::value(value),
            KeyType::Enum(allowed) => {
                let v = value.trim();
                if !allowed.contains(&v) {
                    return Err(GwtuiError::InvalidConfigValue {
                        key: key.to_owned(),
                        msg: format!("must be one of: {}", allowed.join(", ")),
                    }
                    .into());
                }
                toml_edit::value(v)
            }
        };

    Ok((norm, item))
}

fn key_type(key: &str, _cfg: &Config) -> Option<KeyType> {
    // Dynamic keys (maps)
    if key == "worktree.sanitize" || key.starts_with("worktree.sanitize.") {
        return Some(KeyType::String);
    }

    Some(match key {
        "worktree.base_dir"
        | "worktree.naming_template"
        | "mux.zellij_command"
        | "status.default_sort"
        | "status.default_filter"
        | "tasks.queue_dir"
        | "tasks.runner"
        | "tasks.codex_executable"
        | "tasks.codex_timeout"
        | "tasks.claude_executable"
        | "tasks.claude_timeout" => KeyType::String,

        "worktree.auto_mkdir"
        | "discovery.dedupe_by_main_repo"
        | "ui.icons"
        | "ui.tilde_home"
        | "ui.picker_preview"
        | "mux.require_session_for_run"
        | "tasks.enabled"
        | "tasks.auto_cleanup" => KeyType::Bool,

        "discovery.global_scan_depth"
        | "discovery.cache_ttl_seconds"
        | "ui.picker_preview_lines"
        | "status.refresh_interval_ms"
        | "status.concurrency"
        | "tasks.log_retention_days"
        | "tasks.max_log_size_mb"
        | "tasks.max_parallel" => KeyType::Int,

        "discovery.mode" => KeyType::Enum(&["auto", "local", "global"]),
        "mux.backend" => KeyType::Enum(&["zellij", "none"]),

        _ => return None,
    })
}

fn parse_bool(s: &str) -> Result<bool, String> {
    match s.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("expected true|false, got '{other}'")),
    }
}

fn parse_int(s: &str) -> Result<i64, String> {
    s.trim()
        .parse::<i64>()
        .map_err(|e| format!("expected integer, got '{s}': {e}"))
}

fn apply_set(
    doc: &mut toml_edit::DocumentMut,
    key: &str,
    value: toml_edit::Item,
) -> anyhow::Result<()> {
    let parts: Vec<&str> = key.split('.').filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        return Err(GwtuiError::InvalidConfigKey(key.to_owned()).into());
    }

    let mut cur = doc.as_table_mut();
    for seg in &parts[..parts.len().saturating_sub(1)] {
        if !cur.contains_key(seg) {
            let mut t = toml_edit::Table::new();
            t.set_implicit(true);
            cur.insert(seg, toml_edit::Item::Table(t));
        }
        cur = cur[seg].as_table_mut().ok_or_else(|| {
            GwtuiError::Config(format!("cannot set {key}: '{seg}' is not a table"))
        })?;
    }

    let leaf = parts[parts.len() - 1];
    cur.insert(leaf, value);
    Ok(())
}

fn lookup_value(cfg: &Config, key: &str) -> Option<serde_json::Value> {
    let mut v = serde_json::to_value(cfg).ok()?;
    for seg in key.split('.').filter(|s| !s.is_empty()) {
        match v {
            serde_json::Value::Object(mut map) => {
                v = map.remove(seg)?;
            }
            _ => return None,
        }
    }
    Some(v)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn config_validation_catches_invalid_values() {
        let mut cfg = Config::default();
        cfg.tasks.max_parallel = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_set_and_get_dot_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");

        set_value_string_at_path(&path, "ui.icons", "false").unwrap();
        assert_eq!(
            get_value_string_at_path(&path, "ui.icons")
                .unwrap()
                .as_deref(),
            Some("false")
        );

        set_value_string_at_path(&path, "worktree.basedir", "~/wt").unwrap();
        assert_eq!(
            get_value_string_at_path(&path, "worktree.base_dir")
                .unwrap()
                .as_deref(),
            Some("~/wt")
        );

        set_value_string_at_path(&path, "worktree.sanitize./", "-").unwrap();
        assert_eq!(
            get_value_string_at_path(&path, "worktree.sanitize./")
                .unwrap()
                .as_deref(),
            Some("-")
        );

        set_value_string_at_path(&path, "tmux.enabled", "false").unwrap();
        assert_eq!(
            get_value_string_at_path(&path, "tmux.enabled")
                .unwrap()
                .as_deref(),
            Some("false")
        );

        let (doc, cfg) = load_from_file(&path).unwrap();
        let _ = doc;
        cfg.validate().unwrap();
        assert!(!cfg.ui.icons);
        assert_eq!(cfg.worktree.base_dir, "~/wt");
        assert_eq!(
            cfg.worktree.sanitize.get("/").map(String::as_str),
            Some("-")
        );
        assert_eq!(cfg.mux.backend, MuxBackend::None);
    }
}

fn format_value_for_stdout(v: serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s,
        other => serde_json::to_string_pretty(&other).unwrap_or_else(|_| other.to_string()),
    }
}

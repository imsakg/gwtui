#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    pub execution_id: String,
    pub task_id: String,
    pub task_name: String,
    #[serde(default)]
    pub prompt: String,
    pub worktree: String,
    pub repository: String,
    pub working_directory: String,
    pub status: ExecutionStatus,
    pub start_time: String,
    pub end_time: Option<String>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecutionManager {
    base: PathBuf,
}

impl ExecutionManager {
    #[must_use]
    pub fn new(queue_dir: PathBuf) -> Self {
        Self { base: queue_dir }
    }

    #[must_use]
    pub fn log_dir(&self) -> PathBuf {
        self.base.join("logs")
    }

    #[must_use]
    pub fn metadata_dir(&self) -> PathBuf {
        self.log_dir().join("metadata")
    }

    #[must_use]
    pub fn log_path(&self, execution_id: &str) -> PathBuf {
        self.log_dir().join(format!("{execution_id}.jsonl"))
    }

    #[must_use]
    pub fn metadata_path(&self, execution_id: &str) -> PathBuf {
        self.metadata_dir().join(format!("{execution_id}.json"))
    }

    #[must_use]
    pub fn new_execution_id() -> String {
        let id = Uuid::new_v4().simple().to_string();
        let short: String = id.chars().take(6).collect();
        format!("exec-{short}")
    }

    pub fn ensure_dirs(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(self.metadata_dir())
            .with_context(|| format!("failed to create {}", self.metadata_dir().display()))?;
        std::fs::create_dir_all(self.log_dir())
            .with_context(|| format!("failed to create {}", self.log_dir().display()))?;
        Ok(())
    }

    pub fn save_metadata(&self, meta: &ExecutionMetadata) -> anyhow::Result<()> {
        self.ensure_dirs()?;
        let path = self.metadata_path(&meta.execution_id);
        let tmp = path.with_extension("json.tmp");
        let data = serde_json::to_vec_pretty(meta)?;
        std::fs::write(&tmp, &data)
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("failed to rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    pub fn load_metadata(&self, execution_id: &str) -> anyhow::Result<ExecutionMetadata> {
        let path = self.metadata_path(execution_id);
        let data =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_slice(&data).with_context(|| format!("failed to parse {}", path.display()))
    }

    pub fn list_metadata(&self) -> anyhow::Result<Vec<ExecutionMetadata>> {
        let dir = self.metadata_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut metas: Vec<ExecutionMetadata> = Vec::new();
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Ok(data) = std::fs::read(&path) else {
                continue;
            };
            let Ok(meta) = serde_json::from_slice(&data) else {
                continue;
            };
            metas.push(meta);
        }

        metas.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        Ok(metas)
    }

    pub async fn open_log(&self, execution_id: &str) -> anyhow::Result<tokio::fs::File> {
        let path = self.log_path(execution_id);
        self.ensure_dirs()?;
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))
    }

    pub fn read_log_string(&self, execution_id: &str) -> anyhow::Result<String> {
        let path = self.log_path(execution_id);
        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Ok(data)
    }

    #[must_use]
    pub fn log_file_exists(&self, execution_id: &str) -> bool {
        self.log_path(execution_id).exists()
    }

    pub fn delete_execution(&self, execution_id: &str) -> anyhow::Result<()> {
        let log = self.log_path(execution_id);
        let meta = self.metadata_path(execution_id);
        let _ = std::fs::remove_file(log);
        let _ = std::fs::remove_file(meta);
        Ok(())
    }
}

#[must_use]
pub fn path_basename(path: &Path) -> String {
    match path.file_name().and_then(|s| s.to_str()) {
        Some(name) => name.to_owned(),
        None => path.to_string_lossy().into_owned(),
    }
}

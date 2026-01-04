#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use anyhow::Context as _;

use crate::task::model::Task;

#[derive(Debug, Clone)]
pub struct TaskStorage {
    dir: PathBuf,
}

impl TaskStorage {
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn ensure_dir(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("failed to create task queue dir {}", self.dir.display()))
    }

    pub fn save(&self, task: &Task) -> anyhow::Result<()> {
        self.ensure_dir()?;
        let path = self.task_path(&task.id)?;
        let tmp = path.with_extension("json.tmp");
        let data = serde_json::to_vec_pretty(task)?;
        std::fs::write(&tmp, &data)
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("failed to rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    pub fn load(&self, id: &str) -> anyhow::Result<Task> {
        let path = self.task_path(id)?;
        let legacy = self.dir.join(format!("{id}.json"));
        let path = if path.exists() { path } else { legacy };
        let data =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let task: Task = serde_json::from_slice(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(task)
    }

    pub fn list(&self) -> anyhow::Result<Vec<Task>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }
        let mut tasks: Vec<Task> = Vec::new();
        for entry in std::fs::read_dir(&self.dir)
            .with_context(|| format!("failed to read {}", self.dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Ok(data) = std::fs::read(&path) else {
                continue;
            };
            let Ok(task) = serde_json::from_slice(&data) else {
                continue;
            };
            tasks.push(task);
        }
        tasks.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));
        Ok(tasks)
    }

    pub fn delete(&self, id: &str) -> anyhow::Result<()> {
        if !self.dir.exists() {
            return Ok(());
        }
        validate_task_id(id)?;

        let path = self.dir.join(format!("task-{id}.json"));
        let legacy = self.dir.join(format!("{id}.json"));
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(legacy);
        Ok(())
    }

    fn task_path(&self, id: &str) -> anyhow::Result<PathBuf> {
        validate_task_id(id)?;
        Ok(self.dir.join(format!("task-{id}.json")))
    }
}

fn validate_task_id(id: &str) -> anyhow::Result<()> {
    if id.trim().is_empty() {
        anyhow::bail!("task ID is required");
    }
    if id.contains('/') || id.contains('\\') {
        anyhow::bail!("invalid task ID '{id}': must not contain path separators");
    }
    if id.contains("..") {
        anyhow::bail!("invalid task ID '{id}': must not contain '..'");
    }
    Ok(())
}

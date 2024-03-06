#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Waiting,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Task {
    pub id: String,
    pub runner: String,
    pub name: String,
    #[serde(default)]
    pub repository: Option<String>,
    pub worktree: String,
    pub base_branch: Option<String>,
    pub priority: u8,
    pub depends_on: Vec<String>,
    pub prompt: String,
    pub files: Vec<String>,
    pub verify: Vec<String>,
    pub auto_commit: bool,

    pub status: TaskStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub session_id: Option<String>,
    pub last_error: Option<String>,
}

impl Task {
    #[must_use]
    pub fn new_id() -> String {
        let id = Uuid::new_v4().simple().to_string();
        id.chars().take(6).collect()
    }
}

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

use crate::core::git::Git;
use crate::core::worktree::WorktreeManager;
use crate::task::execution::{ExecutionManager, ExecutionMetadata, ExecutionStatus};
use crate::task::model::{Task, TaskStatus};
use crate::task::storage::TaskStorage;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub queue_dir: PathBuf,
    pub parallel: usize,
    pub poll_interval: Duration,
    pub wait: bool,
    pub codex_executable: String,
    pub codex_timeout: Duration,
    pub claude_executable: String,
    pub claude_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLock {
    pub pid: u32,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStatusReport {
    pub running: bool,
    pub pid: Option<u32>,
    pub started_at: Option<String>,
    pub stop_requested: bool,
    pub counts: BTreeMap<String, usize>,
}

#[must_use]
pub fn lock_path(queue_dir: &Path) -> PathBuf {
    queue_dir.join("worker.lock")
}

#[must_use]
pub fn stop_path(queue_dir: &Path) -> PathBuf {
    queue_dir.join("worker.stop")
}

pub fn load_worker_lock(queue_dir: &Path) -> anyhow::Result<Option<WorkerLock>> {
    let path = lock_path(queue_dir);
    if !path.exists() {
        return Ok(None);
    }
    let data =
        std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let lock: WorkerLock = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(lock))
}

pub fn worker_status(queue_dir: &Path, tasks: &[Task]) -> anyhow::Result<WorkerStatusReport> {
    let lock = load_worker_lock(queue_dir)?;
    let stop_requested = stop_path(queue_dir).exists();

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for t in tasks {
        *counts
            .entry(format!("{:?}", t.status).to_lowercase())
            .or_insert(0) += 1;
    }

    Ok(WorkerStatusReport {
        running: lock.is_some(),
        pid: lock.as_ref().map(|l| l.pid),
        started_at: lock.as_ref().map(|l| l.started_at.clone()),
        stop_requested,
        counts,
    })
}

pub async fn request_stop(queue_dir: &Path, timeout: Duration) -> anyhow::Result<bool> {
    let lock = load_worker_lock(queue_dir)?;
    if lock.is_none() {
        return Ok(false);
    }
    let stop = stop_path(queue_dir);
    std::fs::write(&stop, b"stop\n")
        .with_context(|| format!("failed to write {}", stop.display()))?;

    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if !lock_path(queue_dir).exists() {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(false)
}

pub async fn run_worker(app_cfg: &crate::config::Config, cfg: WorkerConfig) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.queue_dir)
        .with_context(|| format!("failed to create {}", cfg.queue_dir.display()))?;

    let storage = TaskStorage::new(cfg.queue_dir.clone());
    let exec_mgr = ExecutionManager::new(cfg.queue_dir.clone());

    if app_cfg.tasks.auto_cleanup
        && app_cfg.tasks.log_retention_days > 0
        && let Err(e) = cleanup_old_logs(&exec_mgr, app_cfg.tasks.log_retention_days)
    {
        eprintln!("task log cleanup warning: {e}");
    }

    let lock_file = lock_path(&cfg.queue_dir);
    if lock_file.exists() {
        anyhow::bail!("worker already running ({} exists)", lock_file.display());
    }

    let lock = WorkerLock {
        pid: std::process::id(),
        started_at: now_rfc3339(),
    };
    std::fs::write(&lock_file, serde_json::to_vec_pretty(&lock)?)
        .with_context(|| format!("failed to write {}", lock_file.display()))?;

    let stop_file = stop_path(&cfg.queue_dir);
    let _guard = WorkerGuard {
        lock_file: lock_file.clone(),
        stop_file: stop_file.clone(),
    };

    // Reset any "running" tasks (previous worker may have crashed).
    reset_stale_running_tasks(&storage)?;

    let mut ticker = tokio::time::interval(cfg.poll_interval);
    let mut joinset: tokio::task::JoinSet<anyhow::Result<()>> = tokio::task::JoinSet::new();

    let mut empty_polls = 0u32;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            _ = ticker.tick() => {}
            Some(res) = joinset.join_next() => {
                if let Err(e) = res {
                    eprintln!("task join error: {e}");
                }
            }
        }

        if stop_file.exists() {
            break;
        }

        // Opportunistically reap completed tasks.
        while let Some(res) = joinset.try_join_next() {
            if let Err(e) = res {
                eprintln!("task join error: {e}");
            }
        }

        // Start new work when there is capacity.
        let active = joinset.len();
        let capacity = cfg.parallel.saturating_sub(active);
        if capacity == 0 {
            continue;
        }

        let tasks = storage.list()?;
        let (ready, has_pending) = ready_tasks(&tasks);

        if ready.is_empty() && !has_pending && active == 0 {
            empty_polls += 1;
            if !cfg.wait && empty_polls >= 2 {
                break;
            }
            continue;
        }
        empty_polls = 0;

        for task_id in ready.into_iter().take(capacity) {
            let app_cfg = app_cfg.clone();
            let storage = storage.clone();
            let exec_mgr = exec_mgr.clone();
            let worker_cfg = cfg.clone();
            joinset.spawn(async move {
                execute_task(&app_cfg, &worker_cfg, &storage, &exec_mgr, &task_id).await
            });
        }
    }

    // Graceful shutdown: wait for in-flight tasks.
    while let Some(_res) = joinset.join_next().await {}

    Ok(())
}

fn cleanup_old_logs(exec_mgr: &ExecutionManager, retention_days: u64) -> anyhow::Result<()> {
    const MAX_DAYS: i64 = i64::MAX / 86_400;
    let retention_days = i64::try_from(retention_days)
        .unwrap_or(MAX_DAYS)
        .min(MAX_DAYS);
    let cutoff = time::OffsetDateTime::now_utc() - time::Duration::days(retention_days);
    for meta in exec_mgr.list_metadata()? {
        if meta.status == ExecutionStatus::Running {
            continue;
        }
        let start = time::OffsetDateTime::parse(
            &meta.start_time,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
        if start < cutoff {
            let _ = exec_mgr.delete_execution(&meta.execution_id);
        }
    }
    Ok(())
}

fn reset_stale_running_tasks(storage: &TaskStorage) -> anyhow::Result<()> {
    let tasks = storage.list()?;
    for mut t in tasks {
        if t.status == TaskStatus::Running {
            t.status = TaskStatus::Pending;
            t.last_error =
                Some("previous worker stopped unexpectedly; task reset to pending".to_owned());
            let _ = storage.save(&t);
        }
    }
    Ok(())
}

fn ready_tasks(tasks: &[Task]) -> (Vec<String>, bool) {
    let mut by_id: BTreeMap<&str, &Task> = BTreeMap::new();
    for t in tasks {
        by_id.insert(&t.id, t);
    }

    let mut ready = Vec::new();
    let mut has_pending = false;

    for t in tasks {
        if !matches!(t.status, TaskStatus::Pending | TaskStatus::Waiting) {
            continue;
        }
        has_pending = true;

        let mut all_done = true;
        let mut any_failed = None;
        for dep in &t.depends_on {
            if let Some(dep_task) = by_id.get(dep.as_str()) {
                match dep_task.status {
                    TaskStatus::Completed => {}
                    TaskStatus::Failed => {
                        any_failed = Some(dep.clone());
                        all_done = false;
                        break;
                    }
                    _ => {
                        all_done = false;
                        break;
                    }
                }
            } else {
                all_done = false;
                break;
            }
        }

        // If a dependency failed, schedule this task so it can be marked failed quickly.
        if any_failed.is_some() || all_done {
            ready.push(t.id.clone());
        }
    }

    ready.sort_by(|a, b| {
        let pa = by_id.get(a.as_str()).map_or(0, |t| t.priority);
        let pb = by_id.get(b.as_str()).map_or(0, |t| t.priority);
        pb.cmp(&pa).then_with(|| a.cmp(b))
    });
    (ready, has_pending)
}

#[allow(clippy::too_many_lines)]
async fn execute_task(
    app_cfg: &crate::config::Config,
    cfg: &WorkerConfig,
    storage: &TaskStorage,
    exec_mgr: &ExecutionManager,
    task_id: &str,
) -> anyhow::Result<()> {
    let mut task = storage.load(task_id)?;

    // Dependency failures: fail fast.
    if let Some(dep) = first_failed_dependency(storage, &task) {
        task.status = TaskStatus::Failed;
        task.last_error = Some(format!("dependency failed: {dep}"));
        task.completed_at = Some(now_rfc3339());
        storage.save(&task)?;
        return Ok(());
    }

    task.status = TaskStatus::Running;
    task.started_at = Some(now_rfc3339());
    task.last_error = None;
    storage.save(&task)?;

    let execution_id = ExecutionManager::new_execution_id();
    task.session_id = Some(execution_id.clone());
    storage.save(&task)?;

    let prompt = task_prompt(&task);

    let mut meta = ExecutionMetadata {
        execution_id: execution_id.clone(),
        task_id: task.id.clone(),
        task_name: task.name.clone(),
        prompt: prompt.clone(),
        worktree: task.worktree.clone(),
        repository: task.repository.clone().unwrap_or_default(),
        working_directory: String::new(),
        status: ExecutionStatus::Running,
        start_time: now_rfc3339(),
        end_time: None,
        exit_code: None,
        error: None,
    };
    exec_mgr.save_metadata(&meta)?;

    let (repo_root, worktree_dir) = match resolve_repo_and_worktree(app_cfg, &task) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("{e}");
            meta.status = ExecutionStatus::Failed;
            meta.end_time = Some(now_rfc3339());
            meta.exit_code = Some(1);
            meta.error = Some(msg.clone());
            exec_mgr.save_metadata(&meta)?;

            task.status = TaskStatus::Failed;
            task.completed_at = Some(now_rfc3339());
            task.last_error = Some(msg);
            storage.save(&task)?;
            return Ok(());
        }
    };

    meta.repository = repo_root.to_string_lossy().to_string();
    meta.working_directory = worktree_dir.to_string_lossy().to_string();
    exec_mgr.save_metadata(&meta)?;

    let runner = task.runner.trim().to_lowercase();
    let result = match runner.as_str() {
        "codex" => {
            run_codex(
                exec_mgr,
                &meta,
                &cfg.codex_executable,
                cfg.codex_timeout,
                &prompt,
            )
            .await
        }
        "claude" => {
            run_claude(
                exec_mgr,
                &meta,
                &cfg.claude_executable,
                cfg.claude_timeout,
                &prompt,
            )
            .await
        }
        other => Err(anyhow::anyhow!("unsupported runner: {other}")),
    };

    let mut success = true;
    let mut exit_code = None;
    let mut err_str = None;

    match result {
        Ok(code) => {
            exit_code = Some(code);
            if code != 0 {
                success = false;
                err_str = Some(format!("runner exited with code {code}"));
            }
        }
        Err(e) => {
            success = false;
            err_str = Some(e.to_string());
        }
    }

    if success && let Err(e) = run_verify_commands(&worktree_dir, &task.verify).await {
        success = false;
        err_str = Some(format!("verification failed: {e}"));
    }

    if success
        && task.auto_commit
        && let Err(e) = auto_commit(&repo_root, &worktree_dir, &task)
    {
        success = false;
        err_str = Some(format!("auto-commit failed: {e}"));
    }

    meta.status = if success {
        ExecutionStatus::Completed
    } else {
        ExecutionStatus::Failed
    };
    meta.end_time = Some(now_rfc3339());
    meta.exit_code = exit_code;
    meta.error.clone_from(&err_str);
    exec_mgr.save_metadata(&meta)?;

    task.status = if success {
        TaskStatus::Completed
    } else {
        TaskStatus::Failed
    };
    task.completed_at = Some(now_rfc3339());
    task.last_error = err_str;
    storage.save(&task)?;

    Ok(())
}

fn task_prompt(task: &Task) -> String {
    let p = task.prompt.trim();
    if p.is_empty() {
        task.name.clone()
    } else {
        task.prompt.clone()
    }
}

fn resolve_repo_and_worktree(
    app_cfg: &crate::config::Config,
    task: &Task,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let repo = task.repository.clone().unwrap_or_default();
    let git = if repo.trim().is_empty() {
        Git::from_cwd()?
    } else {
        Git::from_dir(Path::new(&repo))?
    };
    let repo_root = git.repo_root().to_path_buf();
    let wm = WorktreeManager::new(git, app_cfg.clone());
    let worktree_dir = ensure_worktree(&wm, task)?;
    Ok((repo_root, worktree_dir))
}

fn first_failed_dependency(storage: &TaskStorage, task: &Task) -> Option<String> {
    for dep in &task.depends_on {
        if let Ok(t) = storage.load(dep)
            && t.status == TaskStatus::Failed
        {
            return Some(dep.clone());
        }
    }
    None
}

fn ensure_worktree(wm: &WorktreeManager, task: &Task) -> anyhow::Result<PathBuf> {
    if let Ok(worktrees) = wm.list() {
        for wt in &worktrees {
            if wt.branch == task.worktree {
                return Ok(PathBuf::from(&wt.path));
            }
        }
    }

    // Create it.
    let base = task.base_branch.as_deref();
    wm.add_from_base(&task.worktree, base, None)?;

    let worktrees = wm.list()?;
    for wt in &worktrees {
        if wt.branch == task.worktree {
            return Ok(PathBuf::from(&wt.path));
        }
    }

    anyhow::bail!(
        "failed to resolve worktree path after creation: {}",
        task.worktree
    )
}

async fn run_claude(
    exec_mgr: &ExecutionManager,
    meta: &ExecutionMetadata,
    claude_exe: &str,
    timeout: Duration,
    prompt: &str,
) -> anyhow::Result<i32> {
    exec_mgr.ensure_dirs()?;
    let mut log = exec_mgr.open_log(&meta.execution_id).await?;

    let mut cmd = tokio::process::Command::new(claude_exe);
    cmd.current_dir(&meta.working_directory);
    cmd.arg("--dangerously-skip-permissions");
    cmd.args(["--output-format", "stream-json"]);
    cmd.args(["-p", prompt]);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {claude_exe}"))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_task = None;
    let mut stderr_task = None;

    if let Some(out) = stdout {
        let mut reader = tokio::io::BufReader::new(out).lines();
        let mut log_out = log.try_clone().await?;
        let exec_id = meta.execution_id.clone();
        let task_id = meta.task_id.clone();
        stdout_task = Some(tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let entry = build_log_entry(&exec_id, &task_id, "stdout", &line);
                let mut s = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_owned());
                s.push('\n');
                let _ = log_out.write_all(s.as_bytes()).await;
            }
        }));
    }

    if let Some(err) = stderr {
        let mut reader = tokio::io::BufReader::new(err).lines();
        let mut log_err = log.try_clone().await?;
        let exec_id = meta.execution_id.clone();
        let task_id = meta.task_id.clone();
        stderr_task = Some(tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let entry = build_log_entry(&exec_id, &task_id, "stderr", &line);
                let mut s = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_owned());
                s.push('\n');
                let _ = log_err.write_all(s.as_bytes()).await;
            }
        }));
    }

    let status = if let Ok(res) = tokio::time::timeout(timeout, child.wait()).await {
        res?
    } else {
        let _ = child.kill().await;
        anyhow::bail!("runner timed out after {timeout:?}");
    };

    if let Some(t) = stdout_task {
        let _ = t.await;
    }
    if let Some(t) = stderr_task {
        let _ = t.await;
    }

    log.flush().await?;
    Ok(status.code().unwrap_or(1))
}

async fn run_codex(
    exec_mgr: &ExecutionManager,
    meta: &ExecutionMetadata,
    codex_exe: &str,
    timeout: Duration,
    prompt: &str,
) -> anyhow::Result<i32> {
    exec_mgr.ensure_dirs()?;
    let mut log = exec_mgr.open_log(&meta.execution_id).await?;

    let mut cmd = tokio::process::Command::new(codex_exe);
    cmd.current_dir(&meta.working_directory);
    cmd.arg("exec");
    cmd.arg("--dangerously-bypass-approvals-and-sandbox");
    cmd.args(["--color", "never"]);
    cmd.arg("--json");
    cmd.args(["-C", &meta.working_directory]);
    cmd.arg("-");
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to start {codex_exe}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        let mut buf = prompt.as_bytes().to_vec();
        buf.push(b'\n');
        let _ = stdin.write_all(&buf).await;
    }

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let mut stdout_task = None;
    let mut stderr_task = None;

    if let Some(out) = stdout {
        let mut reader = tokio::io::BufReader::new(out).lines();
        let mut log_out = log.try_clone().await?;
        let exec_id = meta.execution_id.clone();
        let task_id = meta.task_id.clone();
        stdout_task = Some(tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let entry = build_log_entry(&exec_id, &task_id, "stdout", &line);
                let mut s = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_owned());
                s.push('\n');
                let _ = log_out.write_all(s.as_bytes()).await;
            }
        }));
    }

    if let Some(err) = stderr {
        let mut reader = tokio::io::BufReader::new(err).lines();
        let mut log_err = log.try_clone().await?;
        let exec_id = meta.execution_id.clone();
        let task_id = meta.task_id.clone();
        stderr_task = Some(tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let entry = build_log_entry(&exec_id, &task_id, "stderr", &line);
                let mut s = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_owned());
                s.push('\n');
                let _ = log_err.write_all(s.as_bytes()).await;
            }
        }));
    }

    let status = if let Ok(res) = tokio::time::timeout(timeout, child.wait()).await {
        res?
    } else {
        let _ = child.kill().await;
        anyhow::bail!("runner timed out after {timeout:?}");
    };

    if let Some(t) = stdout_task {
        let _ = t.await;
    }
    if let Some(t) = stderr_task {
        let _ = t.await;
    }

    log.flush().await?;
    Ok(status.code().unwrap_or(1))
}

#[derive(Debug, Serialize)]
struct LogEntry {
    timestamp: String,
    execution_id: String,
    task_id: String,
    stream: String,
    #[serde(flatten)]
    payload: serde_json::Value,
}

fn build_log_entry(execution_id: &str, task_id: &str, stream: &str, line: &str) -> LogEntry {
    let payload: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|_| {
        serde_json::json!({
            "type": "text",
            "text": line,
        })
    });

    LogEntry {
        timestamp: now_rfc3339(),
        execution_id: execution_id.to_owned(),
        task_id: task_id.to_owned(),
        stream: stream.to_owned(),
        payload,
    }
}

async fn run_verify_commands(worktree_dir: &Path, commands: &[String]) -> anyhow::Result<()> {
    for cmd in commands {
        if cmd.trim().is_empty() {
            continue;
        }
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned());
        let status = tokio::process::Command::new(&shell)
            .args(["-lc", cmd])
            .current_dir(worktree_dir)
            .status()
            .await
            .with_context(|| format!("failed to run verify command: {cmd}"))?;
        if !status.success() {
            anyhow::bail!(
                "verify command failed with exit code {:?}: {cmd}",
                status.code()
            );
        }
    }
    Ok(())
}

fn auto_commit(repo_root: &Path, worktree_dir: &Path, task: &Task) -> anyhow::Result<()> {
    let git = Git::new(repo_root.to_path_buf());
    let status = git.run_in_dir(worktree_dir, &["status", "--porcelain"])?;
    if status.trim().is_empty() {
        return Ok(());
    }
    let _ = git.run_in_dir(worktree_dir, &["add", "-A"])?;
    let msg = format!("gwtui task {}: {}", task.id, task.name);
    let _ = git.run_in_dir(worktree_dir, &["commit", "-m", &msg])?;
    Ok(())
}

pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty duration");
    }

    let (num, unit) = s
        .chars()
        .position(|c| !c.is_ascii_digit())
        .map_or((s, ""), |i| s.split_at(i));
    let n: u64 = num
        .parse()
        .with_context(|| format!("invalid duration: {s}"))?;

    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 60 * 60),
        "w" => Duration::from_secs(n * 7 * 24 * 60 * 60),
        "d" => Duration::from_secs(n * 24 * 60 * 60),
        _ => anyhow::bail!("unsupported duration unit in '{s}' (use ms|s|m|h|d|w)"),
    })
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned())
}

struct WorkerGuard {
    lock_file: PathBuf,
    stop_file: PathBuf,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.lock_file);
        let _ = std::fs::remove_file(&self.stop_file);
    }
}

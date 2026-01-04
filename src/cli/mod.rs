#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context as _;
use clap::{CommandFactory as _, Parser, Subcommand};

use crate::config;
use crate::core::discovery;
use crate::core::git::Git;
use crate::core::status::{self, StatusCollector, StatusCollectorOptions, WorktreeStatus};
use crate::core::worktree::{Worktree, WorktreeManager};
use crate::mux::Mux as _;
use crate::mux::zellij::ZellijMux;
use crate::output::table::Table;
use crate::task::execution::{ExecutionManager, ExecutionMetadata, ExecutionStatus};
use crate::task::model::{Task, TaskStatus};
use crate::task::storage::TaskStorage;
use crate::task::worker::{self, WorkerConfig};
use crate::tui;
use crate::tui::picker::{self, PickerItem};
use crate::tui::status_dashboard::StatusDashboardOptions;

#[derive(Debug, Parser)]
#[command(
    name = "gwtui",
    version,
    about = "Git worktree manager (gwq-compatible)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Add(AddArgs),
    List(ListArgs),
    Get(GetArgs),
    Exec(ExecArgs),
    #[command(alias = "rm")]
    Remove(RemoveArgs),
    Status(StatusArgs),
    Prune,
    Config(ConfigArgs),
    Completion(CompletionArgs),
    Tmux(MuxArgs),
    Zellij(MuxArgs),
    Task(TaskArgs),
    Version,
}

#[derive(Debug, Parser)]
pub struct AddArgs {
    /// Create new branch
    #[arg(short = 'b', long = "branch")]
    pub branch: bool,
    /// Select branch using TUI
    #[arg(short = 'i', long = "interactive")]
    pub interactive: bool,
    /// Overwrite existing directory
    #[arg(short = 'f', long = "force")]
    pub force: bool,
    /// Branch name
    pub branch_name: Option<String>,
    /// Optional path
    pub path: Option<String>,
}

#[derive(Debug, Parser)]
pub struct ListArgs {
    /// Show detailed information
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
    /// Output in JSON format
    #[arg(long = "json")]
    pub json: bool,
    /// Show all worktrees from the configured base directory
    #[arg(short = 'g', long = "global")]
    pub global: bool,
}

#[derive(Debug, Parser)]
pub struct GetArgs {
    /// Get from all repositories
    #[arg(short = 'g', long = "global")]
    pub global: bool,
    /// Output null-terminated path
    #[arg(short = '0', long = "null")]
    pub null_terminate: bool,
    /// Pattern
    pub pattern: Option<String>,
}

#[derive(Debug, Parser)]
#[command(trailing_var_arg = true, allow_hyphen_values = true)]
pub struct ExecArgs {
    /// Raw args; we parse gwq-compatible flags manually.
    #[arg(required = true)]
    pub args: Vec<OsString>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Parser)]
pub struct RemoveArgs {
    /// Force delete even if dirty
    #[arg(short = 'f', long = "force")]
    pub force: bool,
    /// Show deletion targets only
    #[arg(short = 'd', long = "dry-run")]
    pub dry_run: bool,
    /// Remove from any worktree in the configured base directory
    #[arg(short = 'g', long = "global")]
    pub global: bool,
    /// Also delete the branch after removing worktree
    #[arg(short = 'b', long = "delete-branch")]
    pub delete_branch: bool,
    /// Force delete the branch even if not merged
    #[arg(long = "force-delete-branch")]
    pub force_delete_branch: bool,
    /// Pattern
    pub pattern: Option<String>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Parser)]
pub struct StatusArgs {
    /// Auto-refresh mode
    #[arg(short = 'w', long = "watch")]
    pub watch: bool,
    /// Refresh interval in seconds for watch mode
    #[arg(short = 'i', long = "interval", default_value_t = 5)]
    pub interval_seconds: u64,
    /// Filter by status (changed, up to date, inactive)
    #[arg(short = 'f', long = "filter", default_value = "")]
    pub filter: String,
    /// Sort by field (branch, modified, activity)
    #[arg(short = 's', long = "sort", default_value = "")]
    pub sort: String,
    /// Output as JSON
    #[arg(long = "json")]
    pub json: bool,
    /// Output as CSV
    #[arg(long = "csv")]
    pub csv: bool,
    /// Show additional information
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
    /// Show all worktrees from base directory
    #[arg(short = 'g', long = "global")]
    pub global: bool,
    /// Include running processes (slower)
    #[arg(long = "show-processes")]
    pub show_processes: bool,
    /// Skip remote status check (faster)
    #[arg(long = "no-fetch")]
    pub no_fetch: bool,
    /// Days of inactivity before marking as stale
    #[arg(long = "stale-days", default_value_t = 14)]
    pub stale_days: u64,
}

#[derive(Debug, Parser)]
pub struct CompletionArgs {
    pub shell: clap_complete::Shell,
}

#[derive(Debug, Parser)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub cmd: ConfigCmd,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCmd {
    List,
    Set(ConfigSetArgs),
    Get(ConfigGetArgs),
}

#[derive(Debug, Parser)]
pub struct ConfigSetArgs {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Parser)]
pub struct ConfigGetArgs {
    pub key: String,
}

#[derive(Debug, Parser)]
pub struct MuxArgs {
    #[command(subcommand)]
    pub cmd: TmuxCmd,
}

#[derive(Debug, Subcommand)]
pub enum TmuxCmd {
    List(TmuxListArgs),
    Run(TmuxRunArgs),
    Attach(TmuxAttachArgs),
    Kill(TmuxKillArgs),
}

#[derive(Debug, Parser)]
pub struct TmuxListArgs {
    #[arg(long = "json")]
    pub json: bool,
    #[arg(long = "csv")]
    pub csv: bool,
    #[arg(short = 'w', long = "watch")]
    pub watch: bool,
    #[arg(short = 's', long = "sort", default_value = "")]
    pub sort: String,
}

#[derive(Debug, Parser)]
pub struct TmuxRunArgs {
    #[arg(short = 'w', long = "worktree")]
    pub worktree: Option<String>,
    #[arg(long = "id")]
    pub id: Option<String>,
    #[arg(long = "context")]
    pub context: Option<String>,
    #[arg(long = "no-detach")]
    pub no_detach: bool,
    #[arg(long = "auto-cleanup")]
    pub auto_cleanup: bool,
    /// Command string (gwq joins args)
    #[arg(required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Parser)]
pub struct TmuxAttachArgs {
    #[arg(short = 'i', long = "interactive")]
    pub interactive: bool,
    pub pattern: Option<String>,
}

#[derive(Debug, Parser)]
pub struct TmuxKillArgs {
    #[arg(short = 'i', long = "interactive")]
    pub interactive: bool,
    #[arg(long = "all")]
    pub all: bool,
    #[arg(long = "force")]
    pub force: bool,
    pub pattern: Option<String>,
}

#[derive(Debug, Parser)]
pub struct TaskArgs {
    #[command(subcommand)]
    pub cmd: TaskCmd,
}

#[derive(Debug, Subcommand)]
pub enum TaskCmd {
    Add(TaskAddArgs),
    List(TaskListArgs),
    Show(TaskShowArgs),
    Logs(TaskLogsArgs),
    Worker(TaskWorkerArgs),
}

#[derive(Debug, Parser)]
pub struct TaskAddArgs {
    #[command(subcommand)]
    pub cmd: TaskAddCmd,
}

#[derive(Debug, Subcommand)]
pub enum TaskAddCmd {
    Claude(TaskAddRunnerArgs),
    Codex(TaskAddRunnerArgs),
}

#[derive(Debug, Parser)]
pub struct TaskAddRunnerArgs {
    /// Task NAME
    pub name: Option<String>,

    #[arg(short = 'w', long = "worktree")]
    pub worktree: Option<String>,
    #[arg(long = "base")]
    pub base: Option<String>,
    #[arg(short = 'p', long = "priority", default_value_t = 50)]
    pub priority: u8,
    #[arg(long = "depends-on")]
    pub depends_on: Vec<String>,
    #[arg(long = "prompt", default_value = "")]
    pub prompt: String,
    #[arg(long = "files")]
    pub files: Vec<String>,
    #[arg(long = "verify")]
    pub verify: Vec<String>,
    #[arg(long = "auto-commit")]
    pub auto_commit: bool,
    #[arg(short = 'f', long = "file")]
    pub file: Option<String>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Parser)]
pub struct TaskListArgs {
    #[arg(long = "filter", default_value = "")]
    pub filter: String,
    #[arg(long = "priority-min", default_value_t = 0)]
    pub priority_min: u8,
    #[arg(long = "watch")]
    pub watch: bool,
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
    #[arg(long = "json")]
    pub json: bool,
    #[arg(long = "csv")]
    pub csv: bool,
}

#[derive(Debug, Parser)]
pub struct TaskShowArgs {
    pub pattern: Option<String>,
}

#[derive(Debug, Parser)]
pub struct TaskLogsArgs {
    pub execution_id: Option<String>,
    #[arg(long = "status", default_value = "")]
    pub status: String,
    #[arg(long = "date", default_value = "")]
    pub date: String,
    #[arg(long = "contains", default_value = "")]
    pub contains: String,
    #[arg(long = "limit", default_value_t = 20)]
    pub limit: usize,
    #[arg(long = "json")]
    pub json: bool,
    #[arg(long = "plain")]
    pub plain: bool,

    #[command(subcommand)]
    pub cmd: Option<TaskLogsSubcommand>,
}

#[derive(Debug, Subcommand)]
pub enum TaskLogsSubcommand {
    Clean(TaskLogsCleanArgs),
}

#[derive(Debug, Parser)]
pub struct TaskLogsCleanArgs {
    #[arg(long = "older-than", default_value = "30d")]
    pub older_than: String,
}

#[derive(Debug, Subcommand)]
pub enum TaskWorkerCmd {
    Start(TaskWorkerStartArgs),
    Stop(TaskWorkerStopArgs),
    Status(TaskWorkerStatusArgs),
}

#[derive(Debug, Parser)]
pub struct TaskWorkerArgs {
    #[command(subcommand)]
    pub cmd: TaskWorkerCmd,
}

#[derive(Debug, Parser)]
pub struct TaskWorkerStartArgs {
    #[arg(long = "parallel", default_value_t = 0)]
    pub parallel: usize,
    #[arg(long = "daemon")]
    pub daemon: bool,
    #[arg(long = "wait")]
    pub wait: bool,
}

#[derive(Debug, Parser)]
pub struct TaskWorkerStopArgs {
    #[arg(long = "timeout", default_value = "5m")]
    pub timeout: String,
}

#[derive(Debug, Parser)]
pub struct TaskWorkerStatusArgs {
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
    #[arg(long = "json")]
    pub json: bool,
}

pub async fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = run(cli, std::env::args_os().collect()).await;
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli, raw_args: Vec<OsString>) -> anyhow::Result<ExitCode> {
    match cli.cmd {
        None => cmd_default().await,
        Some(Commands::Completion(args)) => {
            let mut cmd = Cli::command();
            clap_complete::generate(args.shell, &mut cmd, "gwtui", &mut std::io::stdout());
            Ok(ExitCode::SUCCESS)
        }
        Some(Commands::Config(args)) => match args.cmd {
            ConfigCmd::List => {
                print!("{}", config::list_resolved_toml()?);
                Ok(ExitCode::SUCCESS)
            }
            ConfigCmd::Set(set) => {
                config::set_value_string(&set.key, &set.value)?;
                println!("Set {} = {}", set.key, set.value);
                Ok(ExitCode::SUCCESS)
            }
            ConfigCmd::Get(get) => {
                let val = config::get_value_string(&get.key)?;
                match val {
                    Some(v) => {
                        println!("{v}");
                        Ok(ExitCode::SUCCESS)
                    }
                    None => anyhow::bail!(
                        "configuration key '{}' not found - use 'gwtui config list' to see available keys",
                        get.key
                    ),
                }
            }
        },
        Some(Commands::Add(args)) => cmd_add(args).await,
        Some(Commands::List(args)) => cmd_list(args).await,
        Some(Commands::Get(args)) => cmd_get(args).await,
        Some(Commands::Exec(args)) => cmd_exec(args, &raw_args).await,
        Some(Commands::Remove(args)) => cmd_remove(args).await,
        Some(Commands::Prune) => cmd_prune().await,
        Some(Commands::Status(args)) => cmd_status(args).await,
        Some(Commands::Tmux(args) | Commands::Zellij(args)) => cmd_tmux(args).await,
        Some(Commands::Task(args)) => cmd_task(args).await,
        Some(Commands::Version) => Ok(cmd_version()),
    }
}

async fn load_cfg() -> anyhow::Result<crate::config::Config> {
    let cfg = tokio::task::spawn_blocking(|| -> anyhow::Result<crate::config::Config> {
        let (cfg, _doc, _paths) = config::load()?;
        Ok(cfg)
    })
    .await??;
    Ok(cfg)
}

async fn cmd_default() -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;
    let start_global = Git::from_cwd().is_err();

    if tui::is_tty() {
        crate::tui::app::run(cfg.clone()).await?;
        return Ok(ExitCode::SUCCESS);
    }

    // Non-TTY fallback: run a single `status` refresh with config defaults.
    let args = StatusArgs {
        watch: false,
        interval_seconds: (cfg.status.refresh_interval_ms / 1000).max(1),
        filter: cfg.status.default_filter.clone(),
        sort: cfg.status.default_sort.clone(),
        json: false,
        csv: false,
        verbose: false,
        global: start_global,
        show_processes: false,
        no_fetch: false,
        stale_days: 14,
    };

    let statuses = collect_statuses_once(&cfg, &args).await?;
    output_statuses(&cfg, &args, &statuses)?;
    Ok(ExitCode::SUCCESS)
}

async fn cmd_add(args: AddArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;
    let git = Git::from_cwd()?;
    let wm = WorktreeManager::new(git.clone(), cfg.clone());

    let mut create_branch = args.branch;
    let (branch, custom_path) = if args.interactive {
        if args.branch_name.is_some() || args.path.is_some() {
            anyhow::bail!("cannot specify branch name or path with -i flag");
        }
        if !tui::is_tty() {
            anyhow::bail!("interactive selection requires a TTY");
        }

        let branches = git.list_branches(true)?;
        if branches.is_empty() {
            anyhow::bail!("no branches found");
        }
        let items: Vec<PickerItem> = branches
            .iter()
            .map(|b| {
                let marker = if b.is_current {
                    "* "
                } else if b.is_remote {
                    "→ "
                } else {
                    "  "
                };
                let preview = format!(
                    "Branch: {}\nType: {}\nLast commit: {}\nAuthor: {}\nDate: {}\nHash: {}",
                    b.name,
                    if b.is_current {
                        "Current"
                    } else if b.is_remote {
                        "Remote"
                    } else {
                        "Local"
                    },
                    truncate(&b.last_commit.message, 80),
                    b.last_commit.author,
                    b.last_commit.date_iso,
                    truncate_hash(&b.last_commit.hash),
                );
                PickerItem {
                    title: format!("{marker}{}", b.name),
                    preview,
                }
            })
            .collect();

        let idx = picker::pick_one("Select branch", &items)?;
        let selected = &branches[idx];

        let mut branch = selected.name.clone();
        if selected.is_remote
            && let Some(rest) = branch.strip_prefix("origin/")
        {
            branch = rest.to_owned();
            create_branch = true;
        }
        (branch, None)
    } else {
        let Some(branch) = args.branch_name else {
            anyhow::bail!("branch name is required");
        };
        (branch, args.path.map(std::path::PathBuf::from))
    };

    if let Some(path) = custom_path.as_deref()
        && !args.force
    {
        wm.validate_worktree_path(path)?;
    }

    wm.add(&branch, custom_path.as_deref(), create_branch)?;
    println!("Created worktree for branch '{branch}'");
    Ok(ExitCode::SUCCESS)
}

async fn cmd_list(args: ListArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;
    let worktrees: Vec<Worktree> = if args.global {
        list_global_worktrees(&cfg)?
    } else {
        match Git::from_cwd() {
            Ok(git) => {
                let wm = WorktreeManager::new(git, cfg.clone());
                wm.list()?
            }
            Err(_) => list_global_worktrees(&cfg)?,
        }
    };

    if args.json {
        let mut out = serde_json::to_string_pretty(&worktrees)?;
        out.push('\n');
        print!("{out}");
        return Ok(ExitCode::SUCCESS);
    }

    print_worktree_table(&cfg, &worktrees, args.verbose)?;
    Ok(ExitCode::SUCCESS)
}

fn list_global_worktrees(cfg: &crate::config::Config) -> anyhow::Result<Vec<Worktree>> {
    let entries = discovery::discover_global_worktrees(
        &cfg.worktree.base_dir,
        cfg.discovery.global_scan_depth,
    )?;
    Ok(entries
        .into_iter()
        .map(|e| {
            let branch = match &e.repository {
                Some(repo) => format!("{repo}:{}", e.branch),
                None => e.branch,
            };
            Worktree {
                path: e.path,
                branch,
                commit_hash: e.commit_hash,
                is_main: false,
                created_at: "0001-01-01T00:00:00Z".to_owned(),
            }
        })
        .collect())
}

async fn cmd_get(args: GetArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;

    let out_path = if args.global {
        get_global_worktree_path(&cfg, args.pattern.as_deref())?
    } else {
        match Git::from_cwd() {
            Ok(git) => get_local_worktree_path(&cfg, &git, args.pattern.as_deref())?,
            Err(_) => get_global_worktree_path(&cfg, args.pattern.as_deref())?,
        }
    };

    if args.null_terminate {
        use std::io::Write as _;
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(out_path.as_bytes())?;
        stdout.write_all(b"\0")?;
    } else {
        println!("{out_path}");
    }

    Ok(ExitCode::SUCCESS)
}

async fn cmd_exec(_args: ExecArgs, raw_args: &[OsString]) -> anyhow::Result<ExitCode> {
    let exec_args = extract_subcommand_args(raw_args, "exec").unwrap_or_default();
    let parsed = parse_exec_args(&exec_args)?;
    let cfg = load_cfg().await?;

    let worktree_path = if parsed.global {
        get_global_worktree_path(&cfg, parsed.pattern.as_deref())?
    } else {
        match Git::from_cwd() {
            Ok(git) => get_local_worktree_path(&cfg, &git, parsed.pattern.as_deref())?,
            Err(_) => get_global_worktree_path(&cfg, parsed.pattern.as_deref())?,
        }
    };

    let exit = execute_in_dir(&worktree_path, &parsed.command_args, parsed.stay)?;
    Ok(exit)
}

fn extract_subcommand_args(raw_args: &[OsString], subcommand: &str) -> Option<Vec<OsString>> {
    let mut found = false;
    let mut out = Vec::new();
    for arg in raw_args {
        if !found {
            if arg == subcommand {
                found = true;
            }
            continue;
        }
        out.push(arg.clone());
    }
    found.then_some(out)
}

async fn cmd_remove(args: RemoveArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;

    if args.global {
        remove_global(&cfg, &args)?;
        return Ok(ExitCode::SUCCESS);
    }

    // Local if possible; otherwise global fallback.
    match Git::from_cwd() {
        Ok(git) => {
            let wm = WorktreeManager::new(git, cfg.clone());
            remove_local(&cfg, &wm, &args)?;
        }
        Err(_) => remove_global(&cfg, &args)?,
    }

    Ok(ExitCode::SUCCESS)
}

async fn cmd_prune() -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;
    let git = Git::from_cwd()?;
    let wm = WorktreeManager::new(git, cfg);
    wm.prune()?;
    println!("Pruned stale worktree information");
    Ok(ExitCode::SUCCESS)
}

async fn cmd_status(args: StatusArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;

    if args.watch && tui::is_tty() {
        let opts = StatusDashboardOptions {
            cfg: cfg.clone(),
            start_global: args.global,
            status_filter: args.filter.clone(),
            sort: args.sort.clone(),
            verbose: args.verbose,
            show_processes: args.show_processes,
            fetch_remote: !args.no_fetch,
            stale_days: args.stale_days,
            refresh_interval: Duration::from_secs(args.interval_seconds),
        };
        crate::tui::status_dashboard::run(opts).await?;
        return Ok(ExitCode::SUCCESS);
    }

    if args.watch {
        return cmd_status_watch_nontty(&cfg, &args).await;
    }

    let statuses = collect_statuses_once(&cfg, &args).await?;
    output_statuses(&cfg, &args, &statuses)?;
    Ok(ExitCode::SUCCESS)
}

async fn collect_statuses_once(
    cfg: &crate::config::Config,
    args: &StatusArgs,
) -> anyhow::Result<Vec<WorktreeStatus>> {
    let worktrees: Vec<Worktree> = match Git::from_cwd() {
        Ok(git) if !args.global => {
            let wm = WorktreeManager::new(git, cfg.clone());
            wm.list()?
        }
        _ => {
            let entries = discovery::discover_global_worktrees(
                &cfg.worktree.base_dir,
                cfg.discovery.global_scan_depth,
            )?;
            entries
                .into_iter()
                .map(|e| Worktree {
                    path: e.path,
                    branch: e.branch,
                    commit_hash: e.commit_hash,
                    is_main: false,
                    created_at: "0001-01-01T00:00:00Z".to_owned(),
                })
                .collect()
        }
    };

    let collector = StatusCollector::new(StatusCollectorOptions {
        include_process: args.show_processes,
        fetch_remote: !args.no_fetch,
        stale_threshold: Duration::from_secs(args.stale_days * 24 * 60 * 60),
        base_dir: cfg.worktree.base_dir.clone(),
        concurrency: cfg.status.concurrency,
    });

    let mut statuses = collector.collect_all(&worktrees).await?;

    if !args.filter.trim().is_empty() {
        statuses = status::filter_statuses(statuses, &args.filter);
    }
    if !args.sort.trim().is_empty() {
        status::sort_statuses(&mut statuses, &args.sort);
    }

    Ok(statuses)
}

fn output_statuses(
    cfg: &crate::config::Config,
    args: &StatusArgs,
    statuses: &[WorktreeStatus],
) -> anyhow::Result<()> {
    if args.json {
        #[derive(serde::Serialize)]
        struct Summary {
            #[serde(rename = "Total")]
            total: usize,
            #[serde(rename = "Modified")]
            modified: usize,
            #[serde(rename = "Clean")]
            clean: usize,
            #[serde(rename = "Stale")]
            stale: usize,
        }

        #[derive(serde::Serialize)]
        struct Out<'a> {
            summary: Summary,
            worktrees: &'a [WorktreeStatus],
        }

        let mut modified = 0;
        let mut clean = 0;
        let mut stale = 0;
        for s in statuses {
            match s.status {
                status::WorktreeState::Modified => modified += 1,
                status::WorktreeState::Clean => clean += 1,
                status::WorktreeState::Stale => stale += 1,
                _ => {}
            }
        }

        let out = Out {
            summary: Summary {
                total: statuses.len(),
                modified,
                clean,
                stale,
            },
            worktrees: statuses,
        };
        let mut s = serde_json::to_string_pretty(&out)?;
        s.push('\n');
        print!("{s}");
        return Ok(());
    }

    if args.csv {
        let mut t = Table::new([
            "branch",
            "status",
            "modified",
            "added",
            "deleted",
            "ahead",
            "behind",
            "last_activity",
            "process",
        ]);
        for s in statuses {
            t.row([
                s.branch.clone(),
                status_to_raw_string(s.status).to_owned(),
                s.git_status.modified.to_string(),
                s.git_status.added.to_string(),
                s.git_status.deleted.to_string(),
                s.git_status.ahead.to_string(),
                s.git_status.behind.to_string(),
                s.last_activity.clone(),
                String::new(),
            ]);
        }
        t.write_csv()?;
        return Ok(());
    }

    output_status_table(cfg, args.verbose, statuses)
}

fn format_changes(gs: status::GitStatus) -> String {
    if gs.modified == 0 && gs.added == 0 && gs.deleted == 0 && gs.untracked == 0 {
        return "-".to_owned();
    }
    let mut parts = Vec::new();
    if gs.added > 0 {
        parts.push(format!("{} added", gs.added));
    }
    if gs.modified > 0 {
        parts.push(format!("{} modified", gs.modified));
    }
    if gs.deleted > 0 {
        parts.push(format!("{} deleted", gs.deleted));
    }
    if gs.untracked > 0 {
        parts.push(format!("{} untracked", gs.untracked));
    }
    parts.join(", ")
}

fn format_activity(last_activity: &str) -> String {
    if last_activity == "unknown" || last_activity.is_empty() {
        return "unknown".to_owned();
    }

    let Ok(t) = time::OffsetDateTime::parse(
        last_activity,
        &time::format_description::well_known::Rfc3339,
    ) else {
        return last_activity.to_owned();
    };

    let now = time::OffsetDateTime::now_utc();
    let diff = now - t;
    if diff < time::Duration::minutes(1) {
        "just now".to_owned()
    } else if diff < time::Duration::hours(1) {
        let mins = diff.whole_minutes();
        if mins == 1 {
            "1 min ago".to_owned()
        } else {
            format!("{mins} mins ago")
        }
    } else if diff < time::Duration::days(1) {
        let hours = diff.whole_hours();
        if hours == 1 {
            "1 hour ago".to_owned()
        } else {
            format!("{hours} hours ago")
        }
    } else if diff < time::Duration::days(7) {
        let days = diff.whole_days();
        if days == 1 {
            "1 day ago".to_owned()
        } else {
            format!("{days} days ago")
        }
    } else {
        t.date().to_string()
    }
}

fn status_to_raw_string(status: status::WorktreeState) -> &'static str {
    match status {
        status::WorktreeState::Clean => "clean",
        status::WorktreeState::Modified => "modified",
        status::WorktreeState::Staged => "staged",
        status::WorktreeState::Conflict => "conflict",
        status::WorktreeState::Stale => "stale",
        status::WorktreeState::Unknown => "unknown",
    }
}

fn output_status_table(
    cfg: &crate::config::Config,
    verbose: bool,
    statuses: &[WorktreeStatus],
) -> anyhow::Result<()> {
    if statuses.is_empty() {
        println!("No worktrees found");
        return Ok(());
    }

    if verbose {
        let mut t = Table::new([
            "BRANCH",
            "STATUS",
            "CHANGES",
            "AHEAD/BEHIND",
            "ACTIVITY",
            "PROCESS",
        ]);
        for s in statuses {
            let marker = if s.is_current && cfg.ui.icons {
                "● "
            } else {
                "  "
            };
            t.row([
                format!("{marker}{}", s.branch),
                status::format_status_for_table(s.status).to_owned(),
                format_changes(s.git_status),
                format!("↑{} ↓{}", s.git_status.ahead, s.git_status.behind),
                format_activity(&s.last_activity),
                "-".to_owned(),
            ]);
        }
        t.print()?;
    } else {
        let mut t = Table::new(["BRANCH", "STATUS", "CHANGES", "ACTIVITY"]);
        for s in statuses {
            let marker = if s.is_current && cfg.ui.icons {
                "● "
            } else {
                "  "
            };
            t.row([
                format!("{marker}{}", s.branch),
                status::format_status_for_table(s.status).to_owned(),
                format_changes(s.git_status),
                format_activity(&s.last_activity),
            ]);
        }
        t.print()?;
    }

    Ok(())
}

async fn cmd_status_watch_nontty(
    cfg: &crate::config::Config,
    args: &StatusArgs,
) -> anyhow::Result<ExitCode> {
    let mut ticker = tokio::time::interval(Duration::from_secs(args.interval_seconds));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok(ExitCode::SUCCESS);
            }
            _ = ticker.tick() => {
                print!("\x1b[H\x1b[2J");
                let statuses = collect_statuses_once(cfg, args).await?;

                let (total, changed, up_to_date, inactive) = summarize_statuses(&statuses);
                println!("Worktrees Status - Updated: {}", time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "unknown".to_owned()));
                println!("Total: {total} | Changed: {changed} | Up to date: {up_to_date} | Inactive: {inactive}\n");

                output_status_table(cfg, args.verbose, &statuses)?;
                println!("\n[Press Ctrl+C to exit]");
            }
        }
    }
}

fn summarize_statuses(statuses: &[WorktreeStatus]) -> (usize, usize, usize, usize) {
    let mut changed = 0usize;
    let mut clean = 0usize;
    let mut stale = 0usize;
    for s in statuses {
        match s.status {
            status::WorktreeState::Modified => changed += 1,
            status::WorktreeState::Clean => clean += 1,
            status::WorktreeState::Stale => stale += 1,
            _ => {}
        }
    }
    (statuses.len(), changed, clean, stale)
}

async fn cmd_tmux(args: MuxArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;
    if cfg.mux.backend == crate::config::MuxBackend::None {
        anyhow::bail!("mux backend is disabled (set mux.backend = \"zellij\")");
    }

    let mux = ZellijMux::new(
        cfg.mux.zellij_command.clone(),
        cfg.mux.require_session_for_run,
    );

    match args.cmd {
        TmuxCmd::List(a) => tmux_list(&cfg, &mux, &a).await?,
        TmuxCmd::Attach(a) => tmux_attach(&cfg, &mux, &a)?,
        TmuxCmd::Kill(a) => tmux_kill(&cfg, &mux, &a)?,
        TmuxCmd::Run(a) => tmux_run(&cfg, &mux, &a)?,
    }

    Ok(ExitCode::SUCCESS)
}

async fn tmux_list(
    cfg: &crate::config::Config,
    mux: &ZellijMux,
    args: &TmuxListArgs,
) -> anyhow::Result<()> {
    if args.watch {
        return tmux_list_watch(cfg, mux, args).await;
    }

    let sessions = mux.list_sessions()?;

    if args.json {
        #[derive(serde::Serialize)]
        struct OutSession<'a> {
            session_name: &'a str,
        }
        let out: Vec<OutSession<'_>> = sessions
            .iter()
            .map(|s| OutSession {
                session_name: &s.name,
            })
            .collect();
        let mut s = serde_json::to_string_pretty(&out)?;
        s.push('\n');
        print!("{s}");
        return Ok(());
    }

    if args.csv {
        let mut t = Table::new([
            "context",
            "identifier",
            "duration",
            "command",
            "working_dir",
            "session_name",
        ]);
        for s in &sessions {
            t.row([
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                s.name.clone(),
            ]);
        }
        t.write_csv()?;
        return Ok(());
    }

    if sessions.is_empty() {
        println!("No tmux sessions found");
        return Ok(());
    }

    let mut t = Table::new(["SESSION", "DURATION", "WORKING_DIR"]);
    for s in sessions {
        t.row([s.name, "-".to_owned(), "-".to_owned()]);
    }
    t.print()?;
    Ok(())
}

async fn tmux_list_watch(
    _cfg: &crate::config::Config,
    mux: &ZellijMux,
    _args: &TmuxListArgs,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = ticker.tick() => {
                print!("\x1b[H\x1b[2J");
                let sessions = mux.list_sessions().unwrap_or_default();
                println!("tmux Sessions - Updated: {}", time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| "unknown".to_owned()));
                println!("Total: {} sessions\n", sessions.len());

                if sessions.is_empty() {
                    println!("No tmux sessions found");
                } else {
                    let mut t = Table::new(["SESSION", "DURATION", "WORKING_DIR"]);
                    for s in sessions {
                        t.row([s.name, "-".to_owned(), "-".to_owned()]);
                    }
                    let _ = t.print();
                }
                println!("\n[Press Ctrl+C to exit]");
            }
        }
    }
}

fn tmux_attach(
    _cfg: &crate::config::Config,
    mux: &ZellijMux,
    args: &TmuxAttachArgs,
) -> anyhow::Result<()> {
    let sessions = mux.list_sessions()?;
    if sessions.is_empty() {
        anyhow::bail!("no tmux sessions found");
    }

    let session_name = if args.pattern.is_none() || args.interactive {
        if !tui::is_tty() {
            anyhow::bail!("interactive selection requires a TTY");
        }
        pick_one_session("Select session", &sessions)?
    } else {
        let pat = args.pattern.as_deref().unwrap_or_default().to_lowercase();
        let matches: Vec<_> = sessions
            .iter()
            .filter(|s| s.name.to_lowercase().contains(&pat))
            .cloned()
            .collect();
        if matches.is_empty() {
            anyhow::bail!(
                "no session found matching pattern: {}",
                args.pattern.as_deref().unwrap_or_default()
            );
        }
        if matches.len() == 1 {
            matches[0].name.clone()
        } else {
            if !tui::is_tty() {
                anyhow::bail!("multiple sessions match pattern; please be more specific");
            }
            pick_one_session("Select session", &matches)?
        }
    };

    mux.attach(&session_name)?;
    Ok(())
}

fn tmux_kill(
    _cfg: &crate::config::Config,
    mux: &ZellijMux,
    args: &TmuxKillArgs,
) -> anyhow::Result<()> {
    let sessions = mux.list_sessions()?;
    if sessions.is_empty() {
        println!("No tmux sessions found");
        return Ok(());
    }

    let mut to_kill: Vec<String> = Vec::new();

    if args.all {
        to_kill = sessions.iter().map(|s| s.name.clone()).collect();
    } else if args.pattern.is_none() || args.interactive {
        if !tui::is_tty() {
            anyhow::bail!("interactive selection requires a TTY");
        }
        to_kill = pick_many_sessions("Select sessions", &sessions)?;
    } else if let Some(pat) = args.pattern.as_deref() {
        let p = pat.to_lowercase();
        let matches: Vec<_> = sessions
            .iter()
            .filter(|s| s.name.to_lowercase().contains(&p))
            .map(|s| s.name.clone())
            .collect();
        if matches.is_empty() {
            anyhow::bail!("no session found matching pattern: {pat}");
        }
        if matches.len() == 1 {
            to_kill = matches;
        } else {
            if !tui::is_tty() {
                anyhow::bail!("multiple sessions match pattern; please be more specific");
            }
            let matched_sessions: Vec<_> = sessions
                .iter()
                .filter(|s| s.name.to_lowercase().contains(&p))
                .cloned()
                .collect();
            to_kill = pick_many_sessions("Select sessions", &matched_sessions)?;
        }
    }

    if to_kill.is_empty() {
        println!("No sessions selected for termination");
        return Ok(());
    }

    if !args.force && !confirm_kill_sessions(&to_kill)? {
        println!("Operation cancelled");
        return Ok(());
    }

    let mut failed = 0usize;
    for name in &to_kill {
        print!("Terminating session {name}...");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        match mux.kill(name) {
            Ok(()) => println!(" OK"),
            Err(e) => {
                println!(" FAILED: {e}");
                failed += 1;
            }
        }
    }

    let success = to_kill.len().saturating_sub(failed);
    print!("\nTerminated {success} session(s)");
    if failed > 0 {
        println!(" ({failed} failed)");
        anyhow::bail!(
            "{failed} out of {} sessions failed to terminate",
            to_kill.len()
        );
    }
    println!();

    Ok(())
}

fn tmux_run(
    cfg: &crate::config::Config,
    mux: &ZellijMux,
    args: &TmuxRunArgs,
) -> anyhow::Result<()> {
    let session = current_zellij_session(mux)?;
    let command = args.command.join(" ");

    let working_dir = if let Some(wt) = args.worktree.as_deref() {
        resolve_worktree_path_str(wt, cfg)?
    } else {
        std::env::current_dir()?.to_string_lossy().to_string()
    };

    let context = args.context.as_deref().unwrap_or("run");
    let identifier = args
        .id
        .clone()
        .unwrap_or_else(|| generate_identifier_from_command(&command, &working_dir));
    let pane_name = format!("{context}/{identifier}");

    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned());

    mux.run(
        &session,
        &PathBuf::from(&working_dir),
        Some(&pane_name),
        args.auto_cleanup,
        &shell,
        &command,
    )?;

    println!("Started pane: {pane_name} (session: {session})");
    println!("Command: {command}");
    println!("Working Directory: {working_dir}");
    if args.auto_cleanup {
        println!("Auto-cleanup: pane will close on exit");
    }

    // gwq's --no-detach behavior is inconsistent; we keep it as a no-op for zellij.
    if args.no_detach {
        println!("Note: --no-detach has no effect with zellij backend");
    }

    Ok(())
}

fn current_zellij_session(mux: &ZellijMux) -> anyhow::Result<String> {
    if let Ok(v) = std::env::var("ZELLIJ_SESSION_NAME")
        && !v.trim().is_empty()
    {
        return Ok(v);
    }
    if let Ok(v) = std::env::var("ZELLIJ_SESSION")
        && !v.trim().is_empty()
    {
        return Ok(v);
    }

    let sessions = mux.list_sessions()?;
    if sessions.len() == 1 {
        return Ok(sessions[0].name.clone());
    }

    anyhow::bail!(
        "unable to determine current zellij session (set ZELLIJ_SESSION_NAME or specify a single active session)"
    )
}

fn pick_one_session(title: &str, sessions: &[crate::mux::SessionInfo]) -> anyhow::Result<String> {
    let items: Vec<PickerItem> = sessions
        .iter()
        .map(|s| PickerItem {
            title: s.name.clone(),
            preview: format!("Session: {}", s.name),
        })
        .collect();
    let idx = picker::pick_one(title, &items)?;
    Ok(sessions[idx].name.clone())
}

fn pick_many_sessions(
    title: &str,
    sessions: &[crate::mux::SessionInfo],
) -> anyhow::Result<Vec<String>> {
    let items: Vec<PickerItem> = sessions
        .iter()
        .map(|s| PickerItem {
            title: s.name.clone(),
            preview: format!("Session: {}", s.name),
        })
        .collect();
    let idxs = picker::pick_many(title, &items)?;
    Ok(idxs.into_iter().map(|i| sessions[i].name.clone()).collect())
}

fn confirm_kill_sessions(sessions: &[String]) -> anyhow::Result<bool> {
    println!("\nThis will terminate {} session(s):", sessions.len());
    for s in sessions {
        println!("  ● {s}");
    }
    print!("\nAre you sure? (y/N): ");
    std::io::Write::flush(&mut std::io::stdout())?;
    let mut input = String::new();
    let _ = std::io::stdin().read_line(&mut input)?;
    let resp = input.trim().to_lowercase();
    Ok(resp == "y" || resp == "yes")
}

fn resolve_worktree_path_str(pattern: &str, cfg: &crate::config::Config) -> anyhow::Result<String> {
    let p = PathBuf::from(pattern);
    if p.is_absolute() && p.exists() {
        return Ok(p.to_string_lossy().to_string());
    }

    if let Ok(git) = Git::from_cwd() {
        let wm = WorktreeManager::new(git, cfg.clone());
        let matches = wm.get_matching_worktrees(pattern)?;
        if matches.len() == 1 {
            return Ok(matches[0].path.clone());
        }
        if matches.len() > 1 {
            anyhow::bail!("multiple worktrees match pattern '{pattern}', please be more specific");
        }
    }

    let entries = discovery::discover_global_worktrees(
        &cfg.worktree.base_dir,
        cfg.discovery.global_scan_depth,
    )?;
    let matches = discovery::filter_global_worktrees(&entries, pattern);
    if matches.is_empty() {
        anyhow::bail!("no worktree found matching pattern: {pattern}");
    }
    if matches.len() > 1 {
        anyhow::bail!("multiple worktrees match pattern '{pattern}', please be more specific");
    }
    Ok(matches[0].path.clone())
}

fn generate_identifier_from_command(command: &str, working_dir: &str) -> String {
    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.is_empty() {
        return "session".to_owned();
    }
    let base_cmd = std::path::Path::new(parts[0])
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("cmd");
    let dir_name = std::path::Path::new(working_dir)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if !dir_name.is_empty() && dir_name != "." && dir_name != "/" {
        format!("{base_cmd}-{dir_name}")
    } else {
        base_cmd.to_owned()
    }
}

async fn cmd_task(args: TaskArgs) -> anyhow::Result<ExitCode> {
    let cfg = load_cfg().await?;

    if !cfg.tasks.enabled {
        anyhow::bail!("task system is disabled (set tasks.enabled = true)");
    }

    let queue_dir = config::expand_path(&cfg.tasks.queue_dir)?;
    let storage = TaskStorage::new(queue_dir.clone());
    let exec_mgr = ExecutionManager::new(queue_dir.clone());

    match args.cmd {
        TaskCmd::Add(add) => match add.cmd {
            TaskAddCmd::Claude(a) => task_add_runner(&cfg, &storage, "claude", a)?,
            TaskAddCmd::Codex(a) => task_add_runner(&cfg, &storage, "codex", a)?,
        },
        TaskCmd::List(a) => task_list(&cfg, &storage, a).await?,
        TaskCmd::Show(a) => task_show(&cfg, &storage, &a)?,
        TaskCmd::Logs(a) => task_logs(&cfg, &exec_mgr, a)?,
        TaskCmd::Worker(w) => task_worker(&cfg, queue_dir, &storage, w).await?,
    }

    Ok(ExitCode::SUCCESS)
}

fn task_add_runner(
    cfg: &crate::config::Config,
    storage: &TaskStorage,
    runner: &str,
    args: TaskAddRunnerArgs,
) -> anyhow::Result<()> {
    if let Some(file) = args.file.as_deref() {
        let created = task_add_from_file(cfg, storage, runner, file)?;
        for task in &created {
            println!(
                "Task '{}' ({}) added successfully",
                display_task_name(task),
                task.id
            );
            if let Some(repo) = task.repository.as_deref() {
                println!("  Repository: {repo}");
            }
            println!("  Worktree: {}, Priority: {}", task.worktree, task.priority);
            if !task.depends_on.is_empty() {
                println!("  Dependencies: {}", task.depends_on.join(", "));
            }
            println!();
        }
        println!("Successfully added {} tasks from {}", created.len(), file);
        return Ok(());
    }

    let name = args.name.unwrap_or_default();
    if name.trim().is_empty() {
        anyhow::bail!("task name is required when not using --file flag");
    }
    let worktree = args.worktree.unwrap_or_default();
    if worktree.trim().is_empty() {
        anyhow::bail!("--worktree must be specified");
    }
    if !(1..=100).contains(&args.priority) {
        anyhow::bail!("priority must be between 1 and 100");
    }

    let repo_root = resolve_repository_root("")?;

    let task = Task {
        id: Task::new_id(),
        runner: runner.to_owned(),
        name: name.clone(),
        repository: Some(repo_root),
        worktree: worktree.clone(),
        base_branch: args.base.filter(|s| !s.trim().is_empty()),
        priority: args.priority,
        depends_on: args.depends_on,
        prompt: args.prompt,
        files: args.files,
        verify: args.verify,
        auto_commit: args.auto_commit,
        status: TaskStatus::Pending,
        created_at: now_rfc3339(),
        started_at: None,
        completed_at: None,
        session_id: None,
        last_error: None,
    };

    storage.save(&task)?;

    println!(
        "Task '{}' added successfully (ID: {})",
        display_task_name(&task),
        task.id
    );
    println!("Worktree: {}, Priority: {}", task.worktree, task.priority);
    if !task.depends_on.is_empty() {
        println!("Dependencies: {}", task.depends_on.join(", "));
    }

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct TaskFile {
    version: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    default_config: Option<TaskFileConfig>,
    tasks: Vec<TaskFileEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct TaskFileConfig {
    #[serde(default)]
    auto_commit: bool,
}

#[derive(Debug, serde::Deserialize)]
struct TaskFileEntry {
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    repository: String,
    worktree: String,
    #[serde(default)]
    base_branch: String,
    #[serde(default)]
    priority: u8,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    files_to_focus: Vec<String>,
    #[serde(default)]
    verification_commands: Vec<String>,
    #[serde(default)]
    config: Option<TaskFileConfig>,
}

fn task_add_from_file(
    _cfg: &crate::config::Config,
    storage: &TaskStorage,
    runner: &str,
    file: &str,
) -> anyhow::Result<Vec<Task>> {
    let data = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read task file: {file}"))?;
    let def: TaskFile =
        serde_yaml::from_str(&data).with_context(|| format!("failed to parse YAML: {file}"))?;

    if def.version.trim() != "1.0" {
        anyhow::bail!(
            "unsupported task file version: {} (expected 1.0)",
            def.version
        );
    }

    let default_repo = resolve_repository_root(def.repository.trim())?;
    let default_auto_commit = def.default_config.as_ref().is_some_and(|c| c.auto_commit);

    let mut created = Vec::new();
    for entry in def.tasks {
        if entry.id.trim().is_empty() {
            anyhow::bail!("task ID is required");
        }
        if entry.worktree.trim().is_empty() {
            anyhow::bail!("worktree must be specified");
        }
        let prio = if entry.priority == 0 {
            50
        } else {
            entry.priority
        };
        if !(1..=100).contains(&prio) {
            anyhow::bail!("priority must be between 1 and 100");
        }

        let repo_root = if entry.repository.trim().is_empty() {
            default_repo.clone()
        } else {
            resolve_repository_root(entry.repository.trim())?
        };
        let auto_commit = entry
            .config
            .as_ref()
            .map_or(default_auto_commit, |c| c.auto_commit);

        let task = Task {
            id: entry.id.clone(),
            runner: runner.to_owned(),
            name: entry.name.clone(),
            repository: Some(repo_root),
            worktree: entry.worktree.clone(),
            base_branch: if entry.base_branch.trim().is_empty() {
                None
            } else {
                Some(entry.base_branch.clone())
            },
            priority: prio,
            depends_on: entry.depends_on,
            prompt: entry.prompt,
            files: entry.files_to_focus,
            verify: entry.verification_commands,
            auto_commit,
            status: TaskStatus::Pending,
            created_at: now_rfc3339(),
            started_at: None,
            completed_at: None,
            session_id: None,
            last_error: None,
        };

        storage.save(&task)?;
        created.push(task);
    }

    Ok(created)
}

async fn task_list(
    cfg: &crate::config::Config,
    storage: &TaskStorage,
    args: TaskListArgs,
) -> anyhow::Result<()> {
    if args.watch && !args.json && !args.csv {
        return task_list_watch(cfg, storage, &args).await;
    }

    let mut tasks = storage.list()?;
    tasks = apply_task_list_filters(&tasks, &args);

    if args.json {
        let mut s = serde_json::to_string_pretty(&tasks)?;
        s.push('\n');
        print!("{s}");
        return Ok(());
    }

    if args.csv {
        let mut t = Table::new([
            "task_id",
            "worktree",
            "status",
            "priority",
            "dependencies",
            "duration",
        ]);
        for task in &tasks {
            t.row([
                task.id.clone(),
                task.worktree.clone(),
                task_status_str(task.status).to_owned(),
                task.priority.to_string(),
                if task.depends_on.is_empty() {
                    "0".to_owned()
                } else {
                    task.depends_on.len().to_string()
                },
                task_duration_string(task),
            ]);
        }
        t.write_csv()?;
        return Ok(());
    }

    if tasks.is_empty() {
        println!("No tasks found.");
        return Ok(());
    }

    let mut t = if args.verbose {
        Table::new([
            "TASK", "WORKTREE", "STATUS", "PRIORITY", "DEPS", "DURATION", "PROMPT",
        ])
    } else {
        Table::new(["TASK", "WORKTREE", "STATUS", "PRIORITY", "DEPS", "DURATION"])
    };
    for task in &tasks {
        let mut id = task.id.clone();
        if cfg.ui.icons {
            id = format!("{} {}", task_status_icon(task.status), task.id);
        }
        if args.verbose {
            t.row([
                id,
                task.worktree.clone(),
                task_status_str(task.status).to_owned(),
                task.priority.to_string(),
                if task.depends_on.is_empty() {
                    "-".to_owned()
                } else {
                    task.depends_on.len().to_string()
                },
                task_duration_string(task),
                if task.prompt.trim().is_empty() {
                    "-".to_owned()
                } else {
                    truncate(&task.prompt, 60)
                },
            ]);
        } else {
            t.row([
                id,
                task.worktree.clone(),
                task_status_str(task.status).to_owned(),
                task.priority.to_string(),
                if task.depends_on.is_empty() {
                    "-".to_owned()
                } else {
                    task.depends_on.len().to_string()
                },
                task_duration_string(task),
            ]);
        }
    }
    t.print()?;

    Ok(())
}

async fn task_list_watch(
    cfg: &crate::config::Config,
    storage: &TaskStorage,
    args: &TaskListArgs,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok(());
            }
            _ = ticker.tick() => {
                print!("\x1b[H\x1b[2J");
                let mut tasks = storage.list()?;
                tasks = apply_task_list_filters(&tasks, args);

                println!(
                    "Tasks - Updated: {}",
                    time::OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| "unknown".to_owned())
                );
                println!();

                let mut t = Table::new(["TASK", "WORKTREE", "STATUS", "PRIORITY", "DEPS", "DURATION"]);
                for task in &tasks {
                    let mut id = task.id.clone();
                    if cfg.ui.icons {
                        id = format!("{} {}", task_status_icon(task.status), task.id);
                    }
                    t.row([
                        id,
                        task.worktree.clone(),
                        task_status_str(task.status).to_owned(),
                        task.priority.to_string(),
                        if task.depends_on.is_empty() {
                            "-".to_owned()
                        } else {
                            task.depends_on.len().to_string()
                        },
                        task_duration_string(task),
                    ]);
                }
                t.print()?;
                println!("\n[Press Ctrl+C to exit]");
            }
        }
    }
}

fn apply_task_list_filters(tasks: &[Task], args: &TaskListArgs) -> Vec<Task> {
    let mut out: Vec<Task> = tasks.to_vec();

    if !args.filter.trim().is_empty() {
        let want = args.filter.trim();
        out.retain(|t| task_status_str(t.status) == want);
    }

    if args.priority_min > 0 {
        out.retain(|t| t.priority >= args.priority_min);
    }

    out
}

fn task_show(
    cfg: &crate::config::Config,
    storage: &TaskStorage,
    args: &TaskShowArgs,
) -> anyhow::Result<()> {
    let task = if let Some(pattern) = args.pattern.as_deref() {
        Some(find_task_by_pattern(storage, pattern)?)
    } else {
        select_task_interactively(storage)?
    };

    let Some(task) = task else {
        return Ok(());
    };

    print_task_details(cfg, &task);
    Ok(())
}

fn task_logs(
    cfg: &crate::config::Config,
    exec_mgr: &ExecutionManager,
    args: TaskLogsArgs,
) -> anyhow::Result<()> {
    if let Some(TaskLogsSubcommand::Clean(clean)) = args.cmd {
        task_logs_clean(cfg, exec_mgr, &clean)?;
        return Ok(());
    }

    if let Some(execution_id) = args.execution_id.as_deref() {
        // Show specific execution (filters apply only to list mode in gwq).
        return show_execution(cfg, exec_mgr, execution_id, args.plain);
    }

    if !tui::is_tty() {
        anyhow::bail!("execution ID is required when stdout is not a TTY");
    }

    let mut executions = exec_mgr.list_metadata()?;
    executions = filter_executions(cfg, exec_mgr, &executions, &args);

    if executions.len() > args.limit {
        executions.truncate(args.limit);
    }

    if args.json {
        let mut s = serde_json::to_string_pretty(&executions)?;
        s.push('\n');
        print!("{s}");
        return Ok(());
    }

    if executions.is_empty() {
        println!("No executions found.");
        return Ok(());
    }

    let selected = if executions.len() == 1 {
        executions[0].execution_id.clone()
    } else {
        let items: Vec<PickerItem> = executions
            .iter()
            .map(|m| PickerItem {
                title: format_execution_title(m),
                preview: format_execution_preview(cfg, m),
            })
            .collect();
        let idx = picker::pick_one("Select execution", &items)?;
        executions[idx].execution_id.clone()
    };

    show_execution(cfg, exec_mgr, &selected, args.plain)
}

fn task_logs_clean(
    _cfg: &crate::config::Config,
    exec_mgr: &ExecutionManager,
    args: &TaskLogsCleanArgs,
) -> anyhow::Result<()> {
    if !tui::is_tty() {
        anyhow::bail!("log cleanup requires a TTY for confirmation");
    }

    let dur = worker::parse_duration(&args.older_than)?;
    let secs = i64::try_from(dur.as_secs()).map_err(|_| anyhow::anyhow!("duration too large"))?;
    let cutoff = time::OffsetDateTime::now_utc() - time::Duration::seconds(secs);

    let executions = exec_mgr.list_metadata()?;
    let mut to_delete = Vec::new();
    for e in executions {
        if e.status == ExecutionStatus::Running {
            continue;
        }
        let start = parse_rfc3339(&e.start_time).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
        if start < cutoff {
            to_delete.push(e.execution_id.clone());
        }
    }

    if to_delete.is_empty() {
        println!("No old logs found to clean.");
        return Ok(());
    }

    println!(
        "Cleaning logs older than {:?} (before {})",
        dur,
        cutoff
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown".to_owned())
    );
    print!(
        "Found {} old executions to clean. Continue? [y/N]: ",
        to_delete.len()
    );
    std::io::Write::flush(&mut std::io::stdout())?;

    let mut resp = String::new();
    let _ = std::io::stdin().read_line(&mut resp)?;
    if resp.trim().to_lowercase() != "y" {
        println!("Cancelled.");
        return Ok(());
    }

    let mut deleted = 0usize;
    for id in &to_delete {
        exec_mgr.delete_execution(id)?;
        deleted += 1;
    }

    println!("Cleaned {deleted} log files.");

    Ok(())
}

async fn task_worker(
    cfg: &crate::config::Config,
    queue_dir: PathBuf,
    storage: &TaskStorage,
    args: TaskWorkerArgs,
) -> anyhow::Result<()> {
    match args.cmd {
        TaskWorkerCmd::Start(start) => task_worker_start(cfg, queue_dir, start).await,
        TaskWorkerCmd::Stop(stop) => task_worker_stop(cfg, queue_dir, stop).await,
        TaskWorkerCmd::Status(status) => task_worker_status(&queue_dir, storage, &status),
    }
}

async fn task_worker_start(
    cfg: &crate::config::Config,
    queue_dir: PathBuf,
    args: TaskWorkerStartArgs,
) -> anyhow::Result<()> {
    let parallel = if args.parallel == 0 {
        cfg.tasks.max_parallel
    } else {
        args.parallel
    };
    let poll_interval = Duration::from_secs(5);
    let codex_timeout = worker::parse_duration(&cfg.tasks.codex_timeout)?;
    let claude_timeout = worker::parse_duration(&cfg.tasks.claude_timeout)?;

    if args.daemon {
        // Best-effort daemon mode: spawn a detached worker process.
        let exe = std::env::current_exe().context("failed to resolve current executable")?;
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("task").arg("worker").arg("start");
        if args.parallel > 0 {
            cmd.args(["--parallel", &args.parallel.to_string()]);
        }
        if args.wait {
            cmd.arg("--wait");
        }
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        let child = cmd.spawn().context("failed to spawn worker process")?;
        println!("Started worker (pid={})", child.id());
        return Ok(());
    }

    println!("Starting worker (max parallel: {parallel})");

    let wcfg = WorkerConfig {
        queue_dir,
        parallel,
        poll_interval,
        wait: args.wait,
        codex_executable: cfg.tasks.codex_executable.clone(),
        codex_timeout,
        claude_executable: cfg.tasks.claude_executable.clone(),
        claude_timeout,
    };

    worker::run_worker(cfg, wcfg).await?;
    println!("Worker stopped.");
    Ok(())
}

async fn task_worker_stop(
    _cfg: &crate::config::Config,
    queue_dir: PathBuf,
    args: TaskWorkerStopArgs,
) -> anyhow::Result<()> {
    let timeout = worker::parse_duration(&args.timeout)?;

    let running = worker::load_worker_lock(&queue_dir)?.is_some();
    if !running {
        println!("No worker running.");
        return Ok(());
    }

    let stopped = worker::request_stop(&queue_dir, timeout).await?;
    if stopped {
        println!("Worker stopped.");
        return Ok(());
    }

    anyhow::bail!("timed out waiting for worker to stop (timeout: {timeout:?})")
}

fn task_worker_status(
    queue_dir: &Path,
    storage: &TaskStorage,
    args: &TaskWorkerStatusArgs,
) -> anyhow::Result<()> {
    let tasks = storage.list()?;
    let report = worker::worker_status(queue_dir, &tasks)?;

    if args.json {
        let mut s = serde_json::to_string_pretty(&report)?;
        s.push('\n');
        print!("{s}");
        return Ok(());
    }

    println!("Worker running: {}", report.running);
    if let Some(pid) = report.pid {
        println!("PID: {pid}");
    }
    if let Some(started) = report.started_at.as_deref() {
        println!("Started: {started}");
    }
    println!("Stop requested: {}", report.stop_requested);

    if args.verbose {
        println!("\nTask counts:");
        for (k, v) in &report.counts {
            println!("  {k}: {v}");
        }
    }

    Ok(())
}

fn find_task_by_pattern(storage: &TaskStorage, pattern: &str) -> anyhow::Result<Task> {
    if let Ok(task) = storage.load(pattern) {
        return Ok(task);
    }

    let tasks = storage.list()?;
    let mut matches = Vec::new();
    let p = pattern.to_lowercase();
    for t in tasks {
        if t.id.contains(pattern)
            || t.name.to_lowercase().contains(&p)
            || t.worktree.to_lowercase().contains(&p)
        {
            matches.push(t);
        }
    }

    if matches.is_empty() {
        anyhow::bail!("no task found matching pattern: {pattern}");
    }
    if matches.len() > 1 {
        anyhow::bail!(
            "multiple tasks match pattern '{pattern}': {} matches",
            matches.len()
        );
    }

    Ok(matches.remove(0))
}

fn select_task_interactively(storage: &TaskStorage) -> anyhow::Result<Option<Task>> {
    let tasks = storage.list()?;
    if tasks.is_empty() {
        println!("No tasks found.");
        return Ok(None);
    }

    if tasks.len() == 1 {
        return Ok(Some(tasks[0].clone()));
    }

    if !tui::is_tty() {
        anyhow::bail!("interactive task selection requires a TTY");
    }

    let items: Vec<PickerItem> = tasks
        .iter()
        .map(|t| PickerItem {
            title: format!(
                "{} [{}] {} ({}) - {}",
                task_status_icon(t.status),
                task_status_str(t.status),
                t.id,
                t.worktree,
                display_task_name(t)
            ),
            preview: format!(
                "Task: {}\nID: {}\nStatus: {}\nPriority: {}\nCreated: {}\nWorktree: {}\n\nPrompt: {}\n\nDependencies: {}",
                t.name,
                t.id,
                task_status_str(t.status),
                t.priority,
                t.created_at,
                t.worktree,
                truncate(&t.prompt, 200),
                if t.depends_on.is_empty() { "-".to_owned() } else { t.depends_on.join(", ") },
            ),
        })
        .collect();

    let idx = picker::pick_one("Select task", &items)?;
    Ok(Some(tasks[idx].clone()))
}

fn print_task_details(_cfg: &crate::config::Config, task: &Task) {
    println!("Task: {} (ID: {})", task.name, task.id);
    println!("Status: {}", task_status_str(task.status));
    println!("Priority: {}", task.priority);
    if let Some(repo) = task.repository.as_deref()
        && !repo.trim().is_empty()
    {
        println!("Repository: {repo}");
    }
    if !task.worktree.trim().is_empty() {
        println!("Worktree: {}", task.worktree);
    }
    println!("Created: {}", task.created_at);
    if let Some(started) = task.started_at.as_deref() {
        println!("Started: {started}");
    }
    if let Some(done) = task.completed_at.as_deref() {
        println!("Completed: {done}");
    }
    if !task.depends_on.is_empty() {
        println!("Dependencies: {}", task.depends_on.join(", "));
    }

    if !task.prompt.trim().is_empty() {
        println!("\nPrompt:\n{}", task.prompt);
    }

    if !task.verify.is_empty() {
        println!("\nVerification Commands:");
        for cmd in &task.verify {
            println!("- {cmd}");
        }
    }

    if let Some(err) = task.last_error.as_deref()
        && !err.trim().is_empty()
    {
        println!("\nLast Error:\n{err}");
    }
}

fn resolve_repository_root(repo: &str) -> anyhow::Result<String> {
    if repo.trim().is_empty() {
        return Ok(Git::from_cwd()?.repo_root().to_string_lossy().to_string());
    }
    let p = config::expand_path(repo)?;
    let git = Git::from_dir(&p).map_err(|_| anyhow::anyhow!("not a git repository: {repo}"))?;
    let root = git.repo_root().to_string_lossy().to_string();
    Ok(root)
}

fn display_task_name(task: &Task) -> String {
    if !task.name.trim().is_empty() {
        return task.name.clone();
    }
    if !task.prompt.trim().is_empty() {
        return truncate(&task.prompt, 60);
    }
    task.id.clone()
}

fn task_status_icon(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "○",
        TaskStatus::Waiting => "⏳",
        TaskStatus::Running => "●",
        TaskStatus::Completed => "✓",
        TaskStatus::Failed => "✗",
    }
}

fn task_status_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

fn task_duration_string(task: &Task) -> String {
    let Some(started) = task.started_at.as_deref() else {
        return "-".to_owned();
    };
    let start = parse_rfc3339(started).ok();
    let end = task
        .completed_at
        .as_deref()
        .and_then(|s| parse_rfc3339(s).ok())
        .or_else(|| Some(time::OffsetDateTime::now_utc()));

    match (start, end) {
        (Some(s), Some(e)) if e >= s => format_duration(e - s),
        _ => "-".to_owned(),
    }
}

fn format_duration(d: time::Duration) -> String {
    if d < time::Duration::minutes(1) {
        return format!("{}s", d.whole_seconds());
    }
    if d < time::Duration::hours(1) {
        return format!("{}m", d.whole_minutes());
    }
    let hours = d.whole_hours();
    let minutes = (d - time::Duration::hours(hours)).whole_minutes();
    format!("{hours}h {minutes}m")
}

fn parse_rfc3339(s: &str) -> anyhow::Result<time::OffsetDateTime> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| anyhow::anyhow!("invalid time: {e}"))
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn filter_executions(
    _cfg: &crate::config::Config,
    exec_mgr: &ExecutionManager,
    executions: &[ExecutionMetadata],
    args: &TaskLogsArgs,
) -> Vec<ExecutionMetadata> {
    let mut out: Vec<ExecutionMetadata> = executions.to_vec();

    if !args.status.trim().is_empty() {
        let want = args.status.trim();
        out.retain(|e| execution_status_str(e.status) == want);
    }

    if !args.date.trim().is_empty() {
        let want = args.date.trim();
        out.retain(|e| {
            if let Ok(t) = parse_rfc3339(&e.start_time) {
                t.date().to_string() == want
            } else {
                false
            }
        });
    }

    if !args.contains.trim().is_empty() {
        let needle = args.contains.to_lowercase();
        out.retain(|e| execution_contains(exec_mgr, e, &needle));
    }

    out
}

fn execution_contains(exec_mgr: &ExecutionManager, e: &ExecutionMetadata, needle: &str) -> bool {
    let hay = format!(
        "{}\n{}\n{}\n{}\n{}",
        e.execution_id, e.task_name, e.worktree, e.repository, e.prompt
    )
    .to_lowercase();
    if hay.contains(needle) {
        return true;
    }
    if exec_mgr.log_file_exists(&e.execution_id)
        && let Ok(s) = exec_mgr.read_log_string(&e.execution_id)
    {
        return s.to_lowercase().contains(needle);
    }
    false
}

fn format_execution_title(e: &ExecutionMetadata) -> String {
    let status = execution_status_str(e.status);
    let rel = execution_relative_time(&e.start_time);
    format!(
        "[{status}] {} ({}) - {rel}",
        e.execution_id, e.working_directory
    )
}

fn format_execution_preview(cfg: &crate::config::Config, e: &ExecutionMetadata) -> String {
    let mut wd = e.working_directory.clone();
    if cfg.ui.tilde_home {
        wd = config::tilde_path(&wd);
    }
    format!(
        "Execution: {}\nStatus: {}\nStarted: {}\nRepository: {}\nWorking dir: {}\n\nPrompt:\n{}",
        e.execution_id,
        execution_status_str(e.status),
        e.start_time,
        e.repository,
        wd,
        if e.prompt.trim().is_empty() {
            "-"
        } else {
            e.prompt.as_str()
        }
    )
}

fn execution_relative_time(start_time: &str) -> String {
    let Ok(t) = parse_rfc3339(start_time) else {
        return "unknown".to_owned();
    };
    let diff = time::OffsetDateTime::now_utc() - t;
    if diff < time::Duration::minutes(1) {
        "just now".to_owned()
    } else if diff < time::Duration::hours(1) {
        format!("{}m ago", diff.whole_minutes())
    } else if diff < time::Duration::days(1) {
        format!("{}h ago", diff.whole_hours())
    } else {
        format!("{}d ago", diff.whole_days())
    }
}

fn execution_status_str(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Running => "running",
        ExecutionStatus::Completed => "completed",
        ExecutionStatus::Failed => "failed",
        ExecutionStatus::Aborted => "aborted",
    }
}

fn show_execution(
    cfg: &crate::config::Config,
    exec_mgr: &ExecutionManager,
    execution_id: &str,
    plain: bool,
) -> anyhow::Result<()> {
    let meta = exec_mgr
        .load_metadata(execution_id)
        .with_context(|| format!("failed to load metadata for {execution_id}"))?;

    let mut out = String::new();
    let wd = if cfg.ui.tilde_home {
        config::tilde_path(&meta.working_directory)
    } else {
        meta.working_directory.clone()
    };
    let prompt = if meta.prompt.trim().is_empty() {
        "-"
    } else {
        meta.prompt.as_str()
    };
    let _ = write!(
        &mut out,
        "Execution: {}\nStatus: {} • Started: {}\nRepository: {}\nWorking directory: {}\n\nPrompt:\n{}\n\n",
        meta.execution_id,
        execution_status_str(meta.status),
        meta.start_time,
        meta.repository,
        wd,
        prompt,
    );

    if exec_mgr.log_file_exists(execution_id) {
        let log = exec_mgr.read_log_string(execution_id)?;
        out.push_str(&log);
    } else {
        out.push_str("⊘ Aborted (log file missing)\n");
    }

    if !plain && tui::is_tty() {
        crate::tui::log_viewer::run(&format!("Execution {}", meta.execution_id), &out)?;
        return Ok(());
    }

    print!("{out}");
    Ok(())
}

fn cmd_version() -> ExitCode {
    println!("gwtui version {}", env!("CARGO_PKG_VERSION"));
    if let Some(commit) = option_env!("GWTUI_GIT_COMMIT") {
        println!("  commit: {commit}");
    }
    if let Some(date) = option_env!("GWTUI_BUILD_DATE") {
        println!("  built: {date}");
    }
    println!("  rust: {}", rustc_version_runtime::version());
    println!(
        "  os/arch: {}/{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    ExitCode::SUCCESS
}

fn truncate_hash(hash: &str) -> String {
    hash.chars().take(8).collect()
}

fn truncate(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push_str("...");
    }
    out
}

fn print_worktree_table(
    cfg: &crate::config::Config,
    worktrees: &[Worktree],
    verbose: bool,
) -> anyhow::Result<()> {
    if worktrees.is_empty() {
        println!("No worktrees found");
        return Ok(());
    }

    if verbose {
        let mut t = Table::new(["BRANCH", "PATH", "COMMIT", "CREATED", "TYPE"]);
        for wt in worktrees {
            let marker = if wt.is_main && cfg.ui.icons {
                "● "
            } else {
                "  "
            };
            let mut path = wt.path.clone();
            if cfg.ui.tilde_home {
                path = config::tilde_path(&path);
            }
            t.row([
                format!("{marker}{}", wt.branch),
                path,
                truncate_hash(&wt.commit_hash),
                format_list_created(&wt.created_at),
                if wt.is_main {
                    "main".to_owned()
                } else {
                    "worktree".to_owned()
                },
            ]);
        }
        t.print()?;
    } else {
        let mut t = Table::new(["BRANCH", "PATH"]);
        for wt in worktrees {
            let marker = if wt.is_main && cfg.ui.icons {
                "● "
            } else {
                "  "
            };
            let mut path = wt.path.clone();
            if cfg.ui.tilde_home {
                path = config::tilde_path(&path);
            }
            t.row([format!("{marker}{}", wt.branch), path]);
        }
        t.print()?;
    }

    Ok(())
}

fn format_list_created(created_at: &str) -> String {
    if created_at.is_empty() {
        return "unknown".to_owned();
    }
    let Ok(t) =
        time::OffsetDateTime::parse(created_at, &time::format_description::well_known::Rfc3339)
    else {
        return "unknown".to_owned();
    };
    if t.year() <= 1970 {
        return "unknown".to_owned();
    }
    let now = time::OffsetDateTime::now_utc();
    let diff = now - t;
    if diff < time::Duration::hours(1) {
        format!("{} minutes ago", diff.whole_minutes())
    } else if diff < time::Duration::days(1) {
        format!("{} hours ago", diff.whole_hours())
    } else if diff < time::Duration::days(7) {
        format!("{} days ago", diff.whole_days())
    } else {
        t.date().to_string()
    }
}

fn get_local_worktree_path(
    cfg: &crate::config::Config,
    git: &Git,
    pattern: Option<&str>,
) -> anyhow::Result<String> {
    let wm = WorktreeManager::new(git.clone(), cfg.clone());
    let path = if let Some(pat) = pattern {
        let matches = wm.get_matching_worktrees(pat)?;
        resolve_one_worktree(matches, "worktree selection")?
    } else {
        let worktrees = wm.list()?;
        resolve_one_worktree(worktrees, "worktree selection")?
    };
    Ok(path)
}

fn get_global_worktree_path(
    cfg: &crate::config::Config,
    pattern: Option<&str>,
) -> anyhow::Result<String> {
    let entries = discovery::discover_global_worktrees(
        &cfg.worktree.base_dir,
        cfg.discovery.global_scan_depth,
    )?;
    if entries.is_empty() {
        anyhow::bail!("no worktrees found across all repositories");
    }

    let matches: Vec<&discovery::GlobalWorktreeEntry> = if let Some(pat) = pattern {
        let m = discovery::filter_global_worktrees(&entries, pat);
        if m.is_empty() {
            anyhow::bail!("no worktree matches pattern: {pat}");
        }
        m
    } else {
        entries.iter().collect()
    };

    if matches.len() == 1 {
        return Ok(matches[0].path.clone());
    }

    if !tui::is_tty() {
        anyhow::bail!("multiple worktrees match; refine your pattern");
    }

    let items: Vec<PickerItem> = matches
        .iter()
        .map(|e| {
            let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
            PickerItem {
                title: format!("{repo}:{} ({})", e.branch, e.path),
                preview: format!(
                    "Repository: {}\nBranch: {}\nPath: {}\nCommit: {}",
                    repo,
                    e.branch,
                    e.path,
                    truncate_hash(&e.commit_hash)
                ),
            }
        })
        .collect();

    let idx = picker::pick_one("Select worktree", &items)?;
    Ok(matches[idx].path.clone())
}

fn resolve_one_worktree(mut worktrees: Vec<Worktree>, title: &str) -> anyhow::Result<String> {
    if worktrees.is_empty() {
        anyhow::bail!("no worktrees found");
    }
    if worktrees.len() == 1 {
        return Ok(worktrees.remove(0).path);
    }
    if !tui::is_tty() {
        anyhow::bail!("multiple worktrees match; refine your pattern");
    }

    let items: Vec<PickerItem> = worktrees
        .iter()
        .map(|wt| PickerItem {
            title: format!("{} ({})", wt.branch, wt.path),
            preview: format!(
                "Branch: {}\nPath: {}\nCommit: {}\nCreated: {}",
                wt.branch,
                wt.path,
                truncate_hash(&wt.commit_hash),
                wt.created_at
            ),
        })
        .collect();
    let idx = picker::pick_one(title, &items)?;
    Ok(worktrees[idx].path.clone())
}

struct ParsedExecArgs {
    pattern: Option<String>,
    command_args: Vec<OsString>,
    global: bool,
    stay: bool,
}

fn parse_exec_args(args: &[OsString]) -> anyhow::Result<ParsedExecArgs> {
    let mut global = false;
    let mut stay = false;
    let mut pattern: Option<String> = None;
    let mut dashdash = None;

    for (i, arg) in args.iter().enumerate() {
        if arg == "--" {
            dashdash = Some(i);
            break;
        }
        let s = arg.to_string_lossy();
        match s.as_ref() {
            "-g" | "--global" => global = true,
            "-s" | "--stay" => stay = true,
            "-h" | "--help" => anyhow::bail!("use `gwtui exec --help`"),
            _ => {
                if s.starts_with('-') {
                    anyhow::bail!("unknown flag: {s}");
                }
                if pattern.is_none() {
                    pattern = Some(s.to_string());
                }
            }
        }
    }

    let Some(dashdash) = dashdash else {
        anyhow::bail!("missing -- separator. Use: gwtui exec [pattern] -- command [args...]");
    };

    let command_args = args.iter().skip(dashdash + 1).cloned().collect::<Vec<_>>();
    if command_args.is_empty() {
        anyhow::bail!("no command specified after --");
    }

    Ok(ParsedExecArgs {
        pattern,
        command_args,
        global,
        stay,
    })
}

fn execute_in_dir(dir: &str, command_args: &[OsString], stay: bool) -> anyhow::Result<ExitCode> {
    use std::process::Command;

    let mut cmd = Command::new(&command_args[0]);
    if command_args.len() > 1 {
        cmd.args(&command_args[1..]);
    }
    cmd.current_dir(dir);
    cmd.envs(std::env::vars());
    cmd.stdin(std::process::Stdio::inherit());
    cmd.stdout(std::process::Stdio::inherit());
    cmd.stderr(std::process::Stdio::inherit());

    let status = cmd.status()?;

    if stay {
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned());
        println!("Launching shell in: {dir}");
        println!("Type 'exit' to return to the original directory");

        let _ = Command::new(&shell)
            .current_dir(dir)
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status();
    }

    let code = status.code().unwrap_or(1);
    let code = u8::try_from(code).unwrap_or(1);
    Ok(ExitCode::from(code))
}

fn remove_local(
    _cfg: &crate::config::Config,
    wm: &WorktreeManager,
    args: &RemoveArgs,
) -> anyhow::Result<()> {
    let worktrees = wm.list()?;
    let candidates: Vec<Worktree> = worktrees.into_iter().filter(|w| !w.is_main).collect();
    if candidates.is_empty() {
        anyhow::bail!("no removable worktrees found");
    }

    let selected = if let Some(pat) = args.pattern.as_deref() {
        let pat_raw = pat;
        let pat = pat_raw.to_lowercase();
        let matches: Vec<Worktree> = candidates
            .iter()
            .filter(|w| {
                w.branch.to_lowercase().contains(&pat) || w.path.to_lowercase().contains(&pat)
            })
            .cloned()
            .collect();
        if matches.is_empty() {
            anyhow::bail!("no worktree found matching pattern: {pat_raw}");
        }
        if matches.len() == 1 {
            matches
        } else {
            pick_many_worktrees("Select worktrees", &matches)?
        }
    } else {
        pick_many_worktrees("Select worktrees", &candidates)?
    };

    if args.dry_run {
        println!("Would remove the following worktrees:");
        for wt in &selected {
            println!("  {} ({})", wt.branch, wt.path);
            if args.delete_branch {
                println!("    - Would delete branch: {}", wt.branch);
            }
        }
        return Ok(());
    }

    for wt in selected {
        let path = std::path::PathBuf::from(&wt.path);
        if args.delete_branch {
            if let Err(e) = wm.remove_with_branch(
                &path,
                &wt.branch,
                args.force,
                args.delete_branch,
                args.force_delete_branch,
            ) {
                eprintln!("Error: failed to remove {}: {e}", wt.branch);
                continue;
            }
            println!("Removed worktree: {}", wt.branch);
            println!("Deleted branch: {}", wt.branch);
        } else {
            if let Err(e) = wm.remove(&path, args.force) {
                eprintln!("Error: failed to remove {}: {e}", wt.branch);
                continue;
            }
            println!("Removed worktree: {}", wt.branch);
        }
    }

    Ok(())
}

fn remove_global(cfg: &crate::config::Config, args: &RemoveArgs) -> anyhow::Result<()> {
    let entries = discovery::discover_global_worktrees(
        &cfg.worktree.base_dir,
        cfg.discovery.global_scan_depth,
    )?;
    if entries.is_empty() {
        anyhow::bail!("no worktrees found in {}", cfg.worktree.base_dir);
    }
    let candidates: Vec<&discovery::GlobalWorktreeEntry> =
        entries.iter().filter(|e| !e.is_main).collect();
    if candidates.is_empty() {
        anyhow::bail!("no removable worktrees found");
    }

    let selected: Vec<&discovery::GlobalWorktreeEntry> = if let Some(pat) = args.pattern.as_deref()
    {
        let matches = discovery::filter_global_worktrees(&entries, pat);
        if matches.is_empty() {
            anyhow::bail!("no worktree matches pattern: {pat}");
        }
        if matches.len() == 1 {
            vec![matches[0]]
        } else {
            pick_many_global_worktrees("Select worktrees", &matches)?
        }
    } else {
        pick_many_global_worktrees("Select worktrees", &candidates)?
    };

    if args.dry_run {
        println!("Would remove the following worktrees:");
        for e in &selected {
            let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
            println!("  {repo}:{} ({})", e.branch, e.path);
            if args.delete_branch {
                println!("    - Would delete branch: {}", e.branch);
            }
        }
        return Ok(());
    }

    for e in selected {
        let git = Git::new(PathBuf::from(&e.path));
        let wm = WorktreeManager::new(git, cfg.clone());
        let path = std::path::PathBuf::from(&e.path);
        if args.delete_branch {
            if let Err(err) = wm.remove_with_branch(
                &path,
                &e.branch,
                args.force,
                args.delete_branch,
                args.force_delete_branch,
            ) {
                let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
                eprintln!("Error: failed to remove {repo}:{}: {err}", e.branch);
                continue;
            }
            let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
            println!("Removed worktree: {repo}:{}", e.branch);
            println!("Deleted branch: {}", e.branch);
        } else {
            if let Err(err) = wm.remove(&path, args.force) {
                let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
                eprintln!("Error: failed to remove {repo}:{}: {err}", e.branch);
                continue;
            }
            let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
            println!("Removed worktree: {repo}:{}", e.branch);
        }
    }

    Ok(())
}

fn pick_many_worktrees(title: &str, worktrees: &[Worktree]) -> anyhow::Result<Vec<Worktree>> {
    if !tui::is_tty() {
        anyhow::bail!("interactive selection requires a TTY");
    }
    let items: Vec<PickerItem> = worktrees
        .iter()
        .map(|wt| PickerItem {
            title: format!("{} ({})", wt.branch, wt.path),
            preview: format!(
                "Branch: {}\nPath: {}\nCommit: {}\nCreated: {}",
                wt.branch,
                wt.path,
                truncate_hash(&wt.commit_hash),
                wt.created_at
            ),
        })
        .collect();
    let indices = picker::pick_many(title, &items)?;
    Ok(indices.into_iter().map(|i| worktrees[i].clone()).collect())
}

fn pick_many_global_worktrees<'a>(
    title: &str,
    entries: &[&'a discovery::GlobalWorktreeEntry],
) -> anyhow::Result<Vec<&'a discovery::GlobalWorktreeEntry>> {
    if !tui::is_tty() {
        anyhow::bail!("interactive selection requires a TTY");
    }
    let items: Vec<PickerItem> = entries
        .iter()
        .map(|e| {
            let repo = e.repository.clone().unwrap_or_else(|| "unknown".to_owned());
            PickerItem {
                title: format!("{repo}:{} ({})", e.branch, e.path),
                preview: format!(
                    "Repository: {}\nBranch: {}\nPath: {}",
                    repo, e.branch, e.path
                ),
            }
        })
        .collect();
    let indices = picker::pick_many(title, &items)?;
    Ok(indices.into_iter().map(|i| entries[i]).collect())
}

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Tabs, Wrap,
};

use crate::config;
use crate::core::discovery;
use crate::core::git::Git;
use crate::core::status::{self, StatusCollector, StatusCollectorOptions, WorktreeStatus};
use crate::core::worktree::{Worktree, WorktreeManager};
use crate::mux::Mux as _;
use crate::mux::zellij::ZellijMux;
use crate::task::execution::{ExecutionManager, ExecutionMetadata};
use crate::task::model::Task;
use crate::task::storage::TaskStorage;
use crate::task::worker;
use crate::tui;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TabId {
    Status,
    Tasks,
    Mux,
    Config,
    Help,
}

impl TabId {
    const ALL: [TabId; 5] = [
        TabId::Status,
        TabId::Tasks,
        TabId::Mux,
        TabId::Config,
        TabId::Help,
    ];

    fn title(self) -> &'static str {
        match self {
            TabId::Status => "Status",
            TabId::Tasks => "Tasks",
            TabId::Mux => "Mux",
            TabId::Config => "Config",
            TabId::Help => "Help",
        }
    }

    fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn prev(self) -> Self {
        let idx = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTarget {
    Status,
    Tasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    Filtering,
    Command,
    NewTask,
    Confirm,
    Output,
    AddWorktree,
    ConfigEdit,
    Prompt,
}

#[derive(Debug, Clone)]
struct TextInput {
    text: String,
    cursor: usize,
}

impl TextInput {
    fn new(initial: impl Into<String>) -> Self {
        let text = initial.into();
        let cursor = text.chars().count();
        Self { text, cursor }
    }

    fn as_str(&self) -> &str {
        &self.text
    }

    fn insert_char(&mut self, c: char) {
        let mut chars: Vec<char> = self.text.chars().collect();
        let cur = self.cursor.min(chars.len());
        chars.insert(cur, c);
        self.text = chars.into_iter().collect();
        self.cursor = cur + 1;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut chars: Vec<char> = self.text.chars().collect();
        let cur = self.cursor.min(chars.len());
        if cur == 0 {
            return;
        }
        chars.remove(cur - 1);
        self.text = chars.into_iter().collect();
        self.cursor = cur - 1;
    }

    fn delete(&mut self) {
        let mut chars: Vec<char> = self.text.chars().collect();
        let cur = self.cursor.min(chars.len());
        if cur >= chars.len() {
            return;
        }
        chars.remove(cur);
        self.text = chars.into_iter().collect();
    }

    fn move_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    fn move_right(&mut self) {
        let len = self.text.chars().count();
        self.cursor = (self.cursor + 1).min(len);
    }

    fn move_home(&mut self) {
        self.cursor = 0;
    }

    fn move_end(&mut self) {
        self.cursor = self.text.chars().count();
    }
}

#[derive(Debug, Clone)]
struct ConfirmDialog {
    title: String,
    message: String,
    yes_label: String,
    no_label: String,
    action: ConfirmAction,
}

#[derive(Debug, Clone)]
enum ConfirmAction {
    RemoveSelectedWorktree(RemoveWorktreeOptions),
    KillSelectedSession,
    DeleteTask { id: String },
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy)]
struct RemoveWorktreeOptions {
    force: bool,
    dry_run: bool,
    delete_branch: bool,
    force_delete_branch: bool,
}

#[derive(Debug, Clone)]
enum OutputSource {
    Static,
    Execution {
        queue_dir: PathBuf,
        execution_id: String,
        pretty: bool,
        follow: bool,
    },
}

#[derive(Debug, Clone)]
struct OutputViewer {
    title: String,
    content: String,
    scroll: usize,
    source: OutputSource,
    last_update: Instant,
}

impl OutputViewer {
    fn new(title: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            content: content.into(),
            scroll: 0,
            source: OutputSource::Static,
            last_update: Instant::now(),
        }
    }

    fn new_execution(
        cfg: &crate::config::Config,
        queue_dir: PathBuf,
        execution_id: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let execution_id: String = execution_id.into();
        let content = render_execution_output(cfg, &queue_dir, &execution_id, true)?;
        Ok(Self {
            title: format!("Execution {execution_id}"),
            content,
            scroll: usize::MAX / 2, // start at bottom
            source: OutputSource::Execution {
                queue_dir,
                execution_id,
                pretty: true,
                follow: true,
            },
            last_update: Instant::now(),
        })
    }

    fn refresh_if_needed(&mut self, cfg: &crate::config::Config) {
        self.refresh(cfg, false);
    }

    fn refresh_now(&mut self, cfg: &crate::config::Config) {
        self.refresh(cfg, true);
    }

    fn refresh(&mut self, cfg: &crate::config::Config, force: bool) {
        let OutputSource::Execution {
            queue_dir,
            execution_id,
            pretty,
            follow,
        } = &self.source
        else {
            return;
        };

        if !force && self.last_update.elapsed() < Duration::from_millis(500) {
            return;
        }

        if let Ok(next) = render_execution_output(cfg, queue_dir, execution_id, *pretty) {
            self.content = next;
            if *follow {
                self.scroll = usize::MAX / 2;
            }
        }
        self.last_update = Instant::now();
    }

    fn total_lines(&self) -> usize {
        self.content.lines().count().max(1)
    }
}

#[derive(Debug, Clone)]
struct Toast {
    message: String,
    until: Instant,
}

impl Toast {
    fn info(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            until: Instant::now() + Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddWorktreeField {
    Branch,
    Path,
}

#[derive(Debug, Clone)]
struct AddWorktreeDialog {
    repo_dir: PathBuf,
    branch: TextInput,
    path: TextInput,
    create_branch: bool,
    force: bool,
    field: AddWorktreeField,
    error: Option<String>,
}

impl AddWorktreeDialog {
    fn new(repo_dir: PathBuf) -> Self {
        Self {
            repo_dir,
            branch: TextInput::new(""),
            path: TextInput::new(""),
            create_branch: false,
            force: false,
            field: AddWorktreeField::Branch,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigEditField {
    Key,
    Value,
}

#[derive(Debug, Clone)]
struct ConfigEditDialog {
    key: TextInput,
    value: TextInput,
    field: ConfigEditField,
    error: Option<String>,
}

impl ConfigEditDialog {
    fn new() -> Self {
        Self {
            key: TextInput::new(""),
            value: TextInput::new(""),
            field: ConfigEditField::Key,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptKind {
    StatusRefreshInterval,
    StatusStaleDays,
}

#[derive(Debug, Clone)]
struct PromptDialog {
    title: String,
    label: String,
    input: TextInput,
    kind: PromptKind,
    error: Option<String>,
}

impl PromptDialog {
    fn status_refresh_interval(app: &AppState) -> Self {
        let ms = app
            .status_refresh_interval_override_ms
            .unwrap_or(app.cfg.status.refresh_interval_ms)
            .max(100);
        Self {
            title: "Refresh interval".to_owned(),
            label: "Interval (e.g., 2s, 500ms): ".to_owned(),
            input: TextInput::new(format_interval_ms(ms)),
            kind: PromptKind::StatusRefreshInterval,
            error: None,
        }
    }

    fn status_stale_days(app: &AppState) -> Self {
        Self {
            title: "Stale days".to_owned(),
            label: "Days (empty = 14): ".to_owned(),
            input: TextInput::new(app.status_stale_days.to_string()),
            kind: PromptKind::StatusStaleDays,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NewTaskField {
    Worktree,
    Name,
    Prompt,
    Priority,
}

#[derive(Debug, Clone)]
struct NewTaskDialog {
    worktree: TextInput,
    name: TextInput,
    prompt: TextInput,
    priority: TextInput,
    field: NewTaskField,
    error: Option<String>,
}

impl NewTaskDialog {
    fn new(worktree: impl Into<String>, priority: u8) -> Self {
        Self {
            worktree: TextInput::new(worktree),
            name: TextInput::new(""),
            prompt: TextInput::new(""),
            priority: TextInput::new(priority.to_string()),
            field: NewTaskField::Prompt,
            error: None,
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct AppState {
    cfg: crate::config::Config,

    tab: TabId,
    mode: Mode,

    global: bool,
    filter: String,
    filter_input: TextInput,
    filter_target: FilterTarget,
    sort: String,
    status_verbose: bool,
    status_show_processes: bool,
    status_fetch_remote: bool,
    status_stale_days: u64,
    status_auto_refresh: bool,
    status_refresh_interval_override_ms: Option<u64>,

    statuses: Vec<WorktreeStatus>,
    status_state: TableState,
    needs_status_refresh: bool,
    last_status_refresh: Instant,

    tasks: Vec<Task>,
    task_state: TableState,
    needs_tasks_refresh: bool,
    task_filter: String,
    task_filter_input: TextInput,
    worker_status: Option<worker::WorkerStatusReport>,

    sessions: Vec<crate::mux::SessionInfo>,
    session_state: TableState,
    needs_sessions_refresh: bool,

    config_text: String,
    config_scroll: usize,
    needs_config_refresh: bool,

    command_input: TextInput,

    add_worktree: Option<AddWorktreeDialog>,
    config_edit: Option<ConfigEditDialog>,
    prompt: Option<PromptDialog>,
    new_task: Option<NewTaskDialog>,

    confirm: Option<ConfirmDialog>,
    output: Option<OutputViewer>,

    toast: Option<Toast>,
    last_error: Option<String>,
    should_quit: bool,
}

impl AppState {
    fn new(cfg: crate::config::Config) -> Self {
        let start_global = Git::from_cwd().is_err();
        let default_filter = cfg.status.default_filter.clone();
        let default_sort = cfg.status.default_sort.clone();

        let mut status_state = TableState::default();
        status_state.select(Some(0));
        let mut task_state = TableState::default();
        task_state.select(Some(0));
        let mut session_state = TableState::default();
        session_state.select(Some(0));

        Self {
            cfg,
            tab: TabId::Status,
            mode: Mode::Normal,
            global: start_global,
            filter: default_filter,
            filter_input: TextInput::new(""),
            filter_target: FilterTarget::Status,
            sort: default_sort,
            status_verbose: false,
            status_show_processes: false,
            status_fetch_remote: true,
            status_stale_days: 14,
            status_auto_refresh: true,
            status_refresh_interval_override_ms: None,
            statuses: Vec::new(),
            status_state,
            needs_status_refresh: true,
            last_status_refresh: Instant::now(),
            tasks: Vec::new(),
            task_state,
            needs_tasks_refresh: true,
            task_filter: String::new(),
            task_filter_input: TextInput::new(""),
            worker_status: None,
            sessions: Vec::new(),
            session_state,
            needs_sessions_refresh: true,
            config_text: String::new(),
            config_scroll: 0,
            needs_config_refresh: true,
            command_input: TextInput::new(""),
            add_worktree: None,
            config_edit: None,
            prompt: None,
            new_task: None,
            confirm: None,
            output: None,
            toast: None,
            last_error: None,
            should_quit: false,
        }
    }

    fn selected_status_index(&self) -> usize {
        self.status_state.selected().unwrap_or(0)
    }

    fn clamp_status_selection(&mut self) {
        if self.statuses.is_empty() {
            self.status_state.select(Some(0));
            return;
        }
        let idx = self.selected_status_index().min(self.statuses.len() - 1);
        self.status_state.select(Some(idx));
    }

    fn move_status_selection(&mut self, delta: i64) {
        if self.statuses.is_empty() {
            return;
        }
        let cur = i64::try_from(self.selected_status_index()).unwrap_or(0);
        let max = i64::try_from(self.statuses.len().saturating_sub(1)).unwrap_or(0);
        let next = (cur + delta).clamp(0, max);
        let next = usize::try_from(next).unwrap_or(0);
        self.status_state.select(Some(next));
    }

    fn selected_task_index(&self) -> usize {
        self.task_state.selected().unwrap_or(0)
    }

    fn clamp_task_selection(&mut self) {
        if self.tasks.is_empty() {
            self.task_state.select(Some(0));
            return;
        }
        let idx = self.selected_task_index().min(self.tasks.len() - 1);
        self.task_state.select(Some(idx));
    }

    fn move_task_selection(&mut self, delta: i64) {
        if self.tasks.is_empty() {
            return;
        }
        let cur = i64::try_from(self.selected_task_index()).unwrap_or(0);
        let max = i64::try_from(self.tasks.len().saturating_sub(1)).unwrap_or(0);
        let next = (cur + delta).clamp(0, max);
        let next = usize::try_from(next).unwrap_or(0);
        self.task_state.select(Some(next));
    }

    fn selected_session_index(&self) -> usize {
        self.session_state.selected().unwrap_or(0)
    }

    fn clamp_session_selection(&mut self) {
        if self.sessions.is_empty() {
            self.session_state.select(Some(0));
            return;
        }
        let idx = self.selected_session_index().min(self.sessions.len() - 1);
        self.session_state.select(Some(idx));
    }

    fn move_session_selection(&mut self, delta: i64) {
        if self.sessions.is_empty() {
            return;
        }
        let cur = i64::try_from(self.selected_session_index()).unwrap_or(0);
        let max = i64::try_from(self.sessions.len().saturating_sub(1)).unwrap_or(0);
        let next = (cur + delta).clamp(0, max);
        let next = usize::try_from(next).unwrap_or(0);
        self.session_state.select(Some(next));
    }
}

pub async fn run(cfg: crate::config::Config) -> anyhow::Result<()> {
    let terminal = tui::init_terminal()?;
    let mut guard = TerminalGuard::new(terminal);

    let mut app = AppState::new(cfg);
    refresh_all(&mut app).await;

    // Vibe-coding default: if there is active/queued task work, start on Tasks.
    if app.cfg.tasks.enabled {
        let worker_running = app.worker_status.as_ref().is_some_and(|r| r.running);
        if worker_running || !app.tasks.is_empty() {
            app.tab = TabId::Tasks;
        }
    }

    loop {
        if let Some(toast) = &app.toast
            && Instant::now() >= toast.until
        {
            app.toast = None;
        }

        if let Some(output) = app.output.as_mut() {
            output.refresh_if_needed(&app.cfg);
        }

        {
            let Some(terminal) = guard.terminal.as_mut() else {
                anyhow::bail!("terminal unavailable");
            };
            terminal.draw(|f| draw(f, &mut app))?;
        }

        if app.should_quit {
            break;
        }

        let refresh_interval_ms = app
            .status_refresh_interval_override_ms
            .unwrap_or(app.cfg.status.refresh_interval_ms)
            .max(100);
        let refresh_interval = Duration::from_millis(refresh_interval_ms);
        if app.status_auto_refresh && app.last_status_refresh.elapsed() >= refresh_interval {
            app.needs_status_refresh = true;
        }

        if app.needs_status_refresh {
            if let Err(e) = refresh_statuses(&mut app).await {
                app.last_error = Some(e.to_string());
            } else {
                app.last_error = None;
            }
            app.needs_status_refresh = false;
            app.last_status_refresh = Instant::now();
        }

        if app.needs_tasks_refresh {
            if let Err(e) = refresh_tasks(&mut app) {
                app.last_error = Some(e.to_string());
            }
            app.needs_tasks_refresh = false;
        }

        if app.needs_sessions_refresh {
            if let Err(e) = refresh_sessions(&mut app) {
                app.last_error = Some(e.to_string());
            }
            app.needs_sessions_refresh = false;
        }

        if app.needs_config_refresh {
            app.config_text = config::list_resolved_toml().unwrap_or_else(|e| e.to_string());
            app.needs_config_refresh = false;
        }

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            handle_key(key, &mut app, &mut guard)?;
        }
    }

    Ok(())
}

async fn refresh_all(app: &mut AppState) {
    let _ = refresh_statuses(app).await;
    let _ = refresh_tasks(app);
    let _ = refresh_sessions(app);
    app.config_text = config::list_resolved_toml().unwrap_or_else(|e| e.to_string());
    app.needs_config_refresh = false;
}

async fn refresh_statuses(app: &mut AppState) -> anyhow::Result<()> {
    let worktrees = load_worktrees(&app.cfg, app.global)?;

    let collector = StatusCollector::new(StatusCollectorOptions {
        include_process: app.status_show_processes,
        fetch_remote: app.status_fetch_remote,
        stale_threshold: Duration::from_secs(app.status_stale_days.saturating_mul(24 * 60 * 60)),
        base_dir: app.cfg.worktree.base_dir.clone(),
        concurrency: app.cfg.status.concurrency,
    });

    let mut statuses = collector.collect_all(&worktrees).await?;

    if !app.filter.trim().is_empty() && app.filter.trim() != "all" {
        statuses = status::filter_statuses(statuses, &app.filter);
    }
    if !app.sort.trim().is_empty() {
        status::sort_statuses(&mut statuses, &app.sort);
    }

    app.statuses = statuses;
    app.clamp_status_selection();
    Ok(())
}

fn refresh_tasks(app: &mut AppState) -> anyhow::Result<()> {
    if !app.cfg.tasks.enabled {
        app.tasks.clear();
        app.task_state.select(Some(0));
        app.worker_status = None;
        return Ok(());
    }
    let queue_dir = config::expand_path(&app.cfg.tasks.queue_dir)?;
    let storage = TaskStorage::new(queue_dir.clone());
    let all_tasks = storage.list().unwrap_or_default();
    app.worker_status = worker::worker_status(&queue_dir, &all_tasks).ok();

    let mut tasks = all_tasks;

    let needle = app.task_filter.trim().to_lowercase();
    if !needle.is_empty() {
        tasks.retain(|t| task_matches_query(t, &needle));
    }

    app.tasks = tasks;
    app.clamp_task_selection();
    Ok(())
}

fn refresh_sessions(app: &mut AppState) -> anyhow::Result<()> {
    if app.cfg.mux.backend == crate::config::MuxBackend::None {
        app.sessions.clear();
        app.session_state.select(Some(0));
        return Ok(());
    }
    let mux = ZellijMux::new(
        app.cfg.mux.zellij_command.clone(),
        app.cfg.mux.require_session_for_run,
    );
    app.sessions = mux.list_sessions()?;
    app.clamp_session_selection();
    Ok(())
}

fn load_worktrees(cfg: &crate::config::Config, global: bool) -> anyhow::Result<Vec<Worktree>> {
    if !global && let Ok(git) = Git::from_cwd() {
        let wm = WorktreeManager::new(git, cfg.clone());
        return Ok(wm.list()?);
    }

    let entries = discovery::discover_global_worktrees(
        &cfg.worktree.base_dir,
        cfg.discovery.global_scan_depth,
    )?;
    Ok(entries
        .into_iter()
        .map(|e| Worktree {
            path: e.path,
            branch: e.branch,
            commit_hash: e.commit_hash,
            is_main: false,
            created_at: "0001-01-01T00:00:00Z".to_owned(),
        })
        .collect())
}

#[allow(clippy::too_many_lines)]
fn draw(f: &mut Frame<'_>, app: &mut AppState) {
    let area = f.area();

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);

    draw_tabs(f, root[0], app);
    draw_body(f, root[1], app);
    draw_footer(f, root[2], app);

    if let Some(confirm) = &app.confirm {
        draw_confirm(f, confirm);
    }
    if let Some(output) = &app.output {
        draw_output(f, output);
    }

    match app.mode {
        Mode::Filtering => {
            draw_filter_popup(f, app);
            let popup = filter_popup_rect(area);
            let (title, prefix, input) = match app.filter_target {
                FilterTarget::Status => ("Worktree filter", "Filter: ", &app.filter_input),
                FilterTarget::Tasks => ("Task search", "Search: ", &app.task_filter_input),
            };
            let inner = Block::default()
                .borders(Borders::ALL)
                .title(title)
                .inner(popup);
            let prefix_len = prefix.chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(input.as_str(), input.cursor);
            let y = inner.y;
            f.set_cursor_position((x, y));
        }
        Mode::Command => {
            draw_command_popup(f, app);
            let popup = command_popup_rect(area);
            let inner = Block::default()
                .borders(Borders::ALL)
                .title("Command (gwtui …)")
                .inner(popup);
            let prefix_len = "> ".chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(app.command_input.as_str(), app.command_input.cursor);
            let y = inner.y;
            f.set_cursor_position((x, y));
        }
        Mode::NewTask => {
            let Some(dialog) = &app.new_task else {
                return;
            };
            draw_new_task_popup(f, app, dialog);

            let popup = centered_rect(80, 45, area);
            let inner = Block::default()
                .borders(Borders::ALL)
                .title("New task")
                .inner(popup);

            let (line_idx, prefix, input) = match dialog.field {
                NewTaskField::Worktree => (0u16, "Worktree: ", &dialog.worktree),
                NewTaskField::Name => (1u16, "Name:     ", &dialog.name),
                NewTaskField::Prompt => (2u16, "Prompt:   ", &dialog.prompt),
                NewTaskField::Priority => (3u16, "Priority: ", &dialog.priority),
            };

            let prefix_len = prefix.chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(input.as_str(), input.cursor);
            let y = inner.y + line_idx;
            f.set_cursor_position((x, y));
        }
        Mode::AddWorktree => {
            let Some(dialog) = &app.add_worktree else {
                return;
            };
            draw_add_worktree_popup(f, app, dialog);

            let popup = centered_rect(80, 40, area);
            let inner = Block::default()
                .borders(Borders::ALL)
                .title("Add worktree")
                .inner(popup);

            let (line_idx, prefix, input) = match dialog.field {
                AddWorktreeField::Branch => (2u16, "Branch: ", &dialog.branch),
                AddWorktreeField::Path => (3u16, "Path:   ", &dialog.path),
            };

            let prefix_len = prefix.chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(input.as_str(), input.cursor);
            let y = inner.y + line_idx;
            f.set_cursor_position((x, y));
        }
        Mode::ConfigEdit => {
            let Some(dialog) = &app.config_edit else {
                return;
            };
            draw_config_edit_popup(f, dialog);

            let popup = centered_rect(80, 35, area);
            let inner = Block::default()
                .borders(Borders::ALL)
                .title("Config set (key/value)")
                .inner(popup);

            let (line_idx, prefix, input) = match dialog.field {
                ConfigEditField::Key => (0u16, "Key:   ", &dialog.key),
                ConfigEditField::Value => (1u16, "Value: ", &dialog.value),
            };

            let prefix_len = prefix.chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(input.as_str(), input.cursor);
            let y = inner.y + line_idx;
            f.set_cursor_position((x, y));
        }
        Mode::Prompt => {
            let Some(dialog) = &app.prompt else {
                return;
            };
            draw_prompt_popup(f, dialog);

            let popup = centered_rect(80, 25, area);
            let inner = Block::default()
                .borders(Borders::ALL)
                .title(dialog.title.as_str())
                .inner(popup);

            let prefix_len = dialog.label.chars().count();
            let x = inner.x
                + u16::try_from(prefix_len).unwrap_or(0)
                + cursor_x_for_text(dialog.input.as_str(), dialog.input.cursor);
            let y = inner.y;
            f.set_cursor_position((x, y));
        }
        _ => {}
    }
}

fn draw_tabs(f: &mut Frame<'_>, area: Rect, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(area);

    let titles: Vec<Line> = TabId::ALL
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let idx = i + 1;
            let mut title = format!("{} [{idx}]", t.title());
            match t {
                TabId::Status => title = format!("{title} ({})", app.statuses.len()),
                TabId::Tasks => title = format!("{title} ({})", app.tasks.len()),
                TabId::Mux => title = format!("{title} ({})", app.sessions.len()),
                _ => {}
            }
            Line::from(title)
        })
        .collect();

    let selected = TabId::ALL.iter().position(|t| *t == app.tab).unwrap_or(0);

    let tabs = Tabs::new(titles)
        .select(selected)
        .style(Style::default().fg(Color::Gray))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightBlue)
                .add_modifier(Modifier::BOLD),
        )
        .divider(" | ");

    f.render_widget(tabs, chunks[0]);

    let cwd = std::env::current_dir()
        .ok()
        .map_or_else(|| "-".to_owned(), |p| p.to_string_lossy().to_string());
    let cwd = if app.cfg.ui.tilde_home {
        config::tilde_path(&cwd)
    } else {
        cwd
    };
    let path = Paragraph::new(Line::from(cwd))
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Right);
    f.render_widget(path, chunks[1]);
}

fn draw_body(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    match app.tab {
        TabId::Status => draw_status_tab(f, area, app),
        TabId::Tasks => draw_tasks_tab(f, area, app),
        TabId::Mux => draw_mux_tab(f, area, app),
        TabId::Config => draw_config_tab(f, area, app),
        TabId::Help => draw_help_tab(f, area, app),
    }
}

fn draw_footer(f: &mut Frame<'_>, area: Rect, app: &AppState) {
    let effective_mode = if app.confirm.is_some() {
        Mode::Confirm
    } else if app.output.is_some() {
        Mode::Output
    } else if app.add_worktree.is_some() {
        Mode::AddWorktree
    } else if app.config_edit.is_some() {
        Mode::ConfigEdit
    } else if app.prompt.is_some() {
        Mode::Prompt
    } else if app.new_task.is_some() {
        Mode::NewTask
    } else {
        app.mode
    };

    let mut left = match effective_mode {
        Mode::Normal => match app.tab {
            TabId::Status => "q quit • 1-5 tabs • j/k move • a add • / filter/search • s sort • v verbose • g local/global • w watch • i interval • n fetch • t stale • T task • r refresh • p prune • d remove • e exec • o exec-stay • : command".to_owned(),
            TabId::Tasks => "q quit • 1-5 tabs • j/k move • / search • n new • Enter logs • W start-worker • S stop-worker • R reset • D delete • l execs • w worker • : command".to_owned(),
            TabId::Mux => "q quit • 1-5 tabs • j/k move • a attach • x kill • r refresh • : command".to_owned(),
            TabId::Config => "q quit • 1-5 tabs • j/k scroll • r reload • e set • : command".to_owned(),
            TabId::Help => "q quit • 1-5 tabs • : command".to_owned(),
        },
        Mode::Filtering | Mode::Prompt => "Enter apply • Esc cancel".to_owned(),
        Mode::Command => "Enter run • Esc cancel".to_owned(),
        Mode::NewTask | Mode::ConfigEdit => "Enter next/apply • Tab switch field • Esc cancel".to_owned(),
        Mode::Confirm => match app.confirm.as_ref().map(|c| &c.action) {
            Some(ConfirmAction::RemoveSelectedWorktree(_)) => {
                "y confirm • n cancel • f force • d dry-run • b delete-branch • B force-delete-branch"
                    .to_owned()
            }
            Some(ConfirmAction::KillSelectedSession) => "y kill • n cancel".to_owned(),
            Some(ConfirmAction::DeleteTask { .. }) => "y delete • n cancel".to_owned(),
            None => "y confirm • n cancel".to_owned(),
        },
        Mode::Output => "q/Esc close • j/k scroll • PgUp/PgDn • g/G top/bottom • r refresh • f follow • p pretty".to_owned(),
        Mode::AddWorktree => "Enter apply • Tab switch field • Esc cancel • b create-branch • f force".to_owned(),
    };

    if let Some(err) = &app.last_error {
        left = format!("Error: {err}");
    } else if let Some(toast) = &app.toast {
        left.clone_from(&toast.message);
    }

    let mut right = String::new();
    if app.tab == TabId::Status {
        let interval_ms = app
            .status_refresh_interval_override_ms
            .unwrap_or(app.cfg.status.refresh_interval_ms)
            .max(100);
        let interval = format_interval_ms(interval_ms);
        right = format!(
            "{} • Filter: {} • Sort: {} • Watch: {} ({interval}) • Fetch: {} • Proc: {} • Stale: {}d • Verbose: {}",
            if app.global { "Global" } else { "Local" },
            if effective_mode == Mode::Filtering {
                app.filter_input.as_str()
            } else {
                app.filter.as_str()
            },
            if app.sort.trim().is_empty() {
                "activity"
            } else {
                &app.sort
            },
            if app.status_auto_refresh { "on" } else { "off" },
            if app.status_fetch_remote { "on" } else { "off" },
            if app.status_show_processes {
                "on"
            } else {
                "off"
            },
            app.status_stale_days,
            if app.status_verbose { "on" } else { "off" },
        );
    }

    // Gitui-like keybar: blue background
    let spans = vec![
        Span::styled(left, Style::default().fg(Color::White).bg(Color::Blue)),
        Span::raw(" "),
        Span::styled(
            right,
            Style::default()
                .fg(Color::White)
                .bg(Color::Blue)
                .add_modifier(Modifier::DIM),
        ),
    ];

    let p = Paragraph::new(Line::from(spans)).style(Style::default().bg(Color::Blue));
    f.render_widget(p, area);
}

fn draw_status_tab(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    draw_status_table(f, layout[0], app);
    draw_status_detail(f, layout[1], app);
}

fn draw_status_table(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let headers = if app.status_verbose {
        Row::new(vec![
            "BRANCH",
            "STATUS",
            "CHANGES",
            "AHEAD/BEHIND",
            "ACTIVITY",
        ])
    } else {
        Row::new(vec!["BRANCH", "STATUS", "CHANGES", "ACTIVITY"])
    }
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = app.statuses.iter().map(|s| {
        let marker = if s.is_current && app.cfg.ui.icons {
            "● "
        } else {
            "  "
        };
        let branch = format!("{marker}{}", s.branch);
        let changes = format_changes(s.git_status);
        let activity = format_activity(&s.last_activity);
        let status_cell = Cell::from(status::format_status_for_table(s.status))
            .style(worktree_state_style(s.status));
        if app.status_verbose {
            Row::new(vec![
                Cell::from(branch),
                status_cell,
                Cell::from(changes),
                Cell::from(format!("↑{} ↓{}", s.git_status.ahead, s.git_status.behind)),
                Cell::from(activity),
            ])
        } else {
            Row::new(vec![
                Cell::from(branch),
                status_cell,
                Cell::from(changes),
                Cell::from(activity),
            ])
        }
    });

    let widths = if app.status_verbose {
        vec![
            Constraint::Percentage(30),
            Constraint::Length(12),
            Constraint::Percentage(30),
            Constraint::Length(14),
            Constraint::Length(14),
        ]
    } else {
        vec![
            Constraint::Percentage(35),
            Constraint::Length(12),
            Constraint::Percentage(35),
            Constraint::Length(14),
        ]
    };

    let table = Table::new(rows, widths)
        .header(headers)
        .block(Block::default().borders(Borders::ALL).title("Worktrees"))
        .row_highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightBlue)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");

    f.render_stateful_widget(table, area, &mut app.status_state);
}

fn draw_status_detail(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let block = Block::default().borders(Borders::ALL).title("Details");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if app.statuses.is_empty() {
        let p = Paragraph::new("No worktrees.").wrap(Wrap { trim: true });
        f.render_widget(p, inner);
        return;
    }

    let idx = app.selected_status_index().min(app.statuses.len() - 1);
    let s = &app.statuses[idx];

    let mut path = s.path.clone();
    if app.cfg.ui.tilde_home {
        path = config::tilde_path(&path);
    }

    let lines = vec![
        Line::from(vec![
            Span::styled("Branch: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&s.branch),
        ]),
        Line::from(vec![
            Span::styled(
                "Repository: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(&s.repository),
        ]),
        Line::from(vec![
            Span::styled("Path: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(path),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(status::format_status_for_table(s.status)),
        ]),
        Line::from(format!(
            "Changes: {} added, {} modified, {} deleted, {} untracked",
            s.git_status.added, s.git_status.modified, s.git_status.deleted, s.git_status.untracked
        )),
        Line::from(format!(
            "Ahead/Behind: ↑{} ↓{}",
            s.git_status.ahead, s.git_status.behind
        )),
        Line::from(format!(
            "Last activity: {}",
            format_activity(&s.last_activity)
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("Tip: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(
                "Press 'T' to queue a task for this worktree, or ':' to run any CLI command.",
            ),
        ]),
    ];

    let p = Paragraph::new(lines).wrap(Wrap { trim: false });
    f.render_widget(p, inner);
}

fn draw_tasks_tab(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(1)])
        .split(area);

    draw_tasks_worker_status(f, outer[0], app);

    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(outer[1]);

    draw_tasks_table(f, layout[0], app);
    draw_task_detail(f, layout[1], app);
}

fn draw_tasks_worker_status(f: &mut Frame<'_>, area: Rect, app: &AppState) {
    let block = Block::default().borders(Borders::ALL).title("Worker");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if !app.cfg.tasks.enabled {
        f.render_widget(
            Paragraph::new("Task system disabled (tasks.enabled = false).")
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }

    let mut spans: Vec<Span> = Vec::new();
    spans.push(Span::styled(
        "Worker: ",
        Style::default().add_modifier(Modifier::BOLD),
    ));

    if let Some(report) = &app.worker_status {
        spans.push(Span::styled(
            if report.running { "running" } else { "stopped" },
            Style::default().fg(if report.running {
                Color::Green
            } else {
                Color::Red
            }),
        ));
        if let Some(pid) = report.pid {
            spans.push(Span::styled(
                format!(" (pid={pid})"),
                Style::default().add_modifier(Modifier::DIM),
            ));
        }
        if report.stop_requested {
            spans.push(Span::styled(
                " • stop requested",
                Style::default().fg(Color::Yellow),
            ));
        }

        let pending = *report.counts.get("pending").unwrap_or(&0);
        let waiting = *report.counts.get("waiting").unwrap_or(&0);
        let running = *report.counts.get("running").unwrap_or(&0);
        let completed = *report.counts.get("completed").unwrap_or(&0);
        let failed = *report.counts.get("failed").unwrap_or(&0);

        spans.push(Span::styled(" • ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!("pending {pending}"),
            task_status_style(crate::task::model::TaskStatus::Pending),
        ));
        spans.push(Span::styled("  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!("waiting {waiting}"),
            task_status_style(crate::task::model::TaskStatus::Waiting),
        ));
        spans.push(Span::styled("  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!("running {running}"),
            task_status_style(crate::task::model::TaskStatus::Running),
        ));
        spans.push(Span::styled("  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!("completed {completed}"),
            task_status_style(crate::task::model::TaskStatus::Completed),
        ));
        spans.push(Span::styled("  ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!("failed {failed}"),
            task_status_style(crate::task::model::TaskStatus::Failed),
        ));

        if !app.task_filter.trim().is_empty() {
            spans.push(Span::styled(" • ", Style::default().fg(Color::DarkGray)));
            spans.push(Span::styled(
                format!("search: {}", app.task_filter.trim()),
                Style::default().fg(Color::DarkGray),
            ));
        }
    } else {
        spans.push(Span::styled(
            "unknown",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ));
    }

    f.render_widget(
        Paragraph::new(Line::from(spans)).wrap(Wrap { trim: true }),
        inner,
    );
}

fn draw_tasks_table(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let headers = Row::new(vec!["TASK", "STATUS", "PRIO", "WORKTREE", "NAME"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = app.tasks.iter().map(|t| {
        let mut id = t.id.clone();
        if app.cfg.ui.icons {
            id = format!("{} {}", task_status_icon(t.status), t.id);
        }
        Row::new(vec![
            Cell::from(id),
            Cell::from(task_status_str(t.status)).style(task_status_style(t.status)),
            Cell::from(t.priority.to_string()),
            Cell::from(t.worktree.clone()),
            Cell::from(display_task_name(t)),
        ])
    });

    let table = Table::new(
        rows,
        vec![
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Percentage(30),
            Constraint::Min(10),
        ],
    )
    .header(headers)
    .block(Block::default().borders(Borders::ALL).title("Tasks"))
    .row_highlight_style(
        Style::default()
            .fg(Color::Black)
            .bg(Color::LightBlue)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("▸ ");

    f.render_stateful_widget(table, area, &mut app.task_state);
}

fn draw_task_detail(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let block = Block::default().borders(Borders::ALL).title("Details");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if !app.cfg.tasks.enabled {
        f.render_widget(
            Paragraph::new("Task system disabled (tasks.enabled = false).")
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }

    if app.tasks.is_empty() {
        f.render_widget(Paragraph::new("No tasks.").wrap(Wrap { trim: true }), inner);
        return;
    }

    let idx = app.selected_task_index().min(app.tasks.len() - 1);
    let t = &app.tasks[idx];

    let name = display_task_name(t);

    let lines = vec![
        Line::from(vec![
            Span::styled("Task: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&name),
        ]),
        Line::from(format!("ID: {}", t.id)),
        Line::from(format!("Runner: {}", t.runner)),
        Line::from(format!("Status: {}", task_status_str(t.status))),
        Line::from(format!("Priority: {}", t.priority)),
        Line::from(format!("Worktree: {}", t.worktree)),
        Line::from(format!("Created: {}", t.created_at)),
        Line::from(format!(
            "Started: {}",
            t.started_at.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "Completed: {}",
            t.completed_at.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "Execution: {}",
            t.session_id.as_deref().unwrap_or("-")
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("Prompt: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(if t.prompt.trim().is_empty() {
                "-"
            } else {
                &t.prompt
            }),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Tip: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(
                "Enter opens logs. Use 'f' to follow, 'p' to toggle pretty/raw, 'r' to refresh.",
            ),
        ]),
    ];

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_mux_tab(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    draw_mux_table(f, layout[0], app);
    draw_mux_detail(f, layout[1], app);
}

fn draw_mux_table(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let headers = Row::new(vec!["SESSION"]).style(Style::default().add_modifier(Modifier::BOLD));
    let rows = app.sessions.iter().map(|s| Row::new(vec![s.name.clone()]));
    let table = Table::new(rows, vec![Constraint::Percentage(100)])
        .header(headers)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Zellij sessions"),
        )
        .row_highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightBlue)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    f.render_stateful_widget(table, area, &mut app.session_state);
}

fn draw_mux_detail(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let block = Block::default().borders(Borders::ALL).title("Details");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if app.cfg.mux.backend == crate::config::MuxBackend::None {
        f.render_widget(
            Paragraph::new("Mux backend disabled (mux.backend = \"none\").")
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }

    if app.sessions.is_empty() {
        f.render_widget(
            Paragraph::new("No sessions.").wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }

    let idx = app.selected_session_index().min(app.sessions.len() - 1);
    let s = &app.sessions[idx];

    let lines = vec![
        Line::from(vec![
            Span::styled("Session: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&s.name),
        ]),
        Line::from(""),
        Line::from("Actions:"),
        Line::from("  a   attach (interactive)"),
        Line::from("  x   kill"),
        Line::from("  :   run any gwtui command"),
    ];
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_config_tab(f: &mut Frame<'_>, area: Rect, app: &mut AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Resolved config");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let total_lines = app.config_text.lines().count().max(1);
    let visible = inner.height as usize;
    let max_scroll = total_lines.saturating_sub(visible.max(1));
    if app.config_scroll > max_scroll {
        app.config_scroll = max_scroll;
    }

    let lines: Vec<Line> = app
        .config_text
        .lines()
        .map(|l| Line::from(l.to_owned()))
        .collect();
    let text = Text::from(lines);
    let para = Paragraph::new(text)
        .scroll((u16::try_from(app.config_scroll).unwrap_or(u16::MAX), 0))
        .wrap(Wrap { trim: false });
    f.render_widget(para, inner);
}

fn draw_help_tab(f: &mut Frame<'_>, area: Rect, _app: &mut AppState) {
    let block = Block::default().borders(Borders::ALL).title("Help");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines = vec![
        Line::from("gwtui TUI (GitUI-inspired)"),
        Line::from(""),
        Line::from("Global keys:"),
        Line::from("  1-5 / h/l   Switch tabs"),
        Line::from("  :           Command palette (run any CLI command)"),
        Line::from("  q / Ctrl-C  Quit"),
        Line::from(""),
        Line::from("Status tab:"),
        Line::from("  j/k, ↑/↓    Move selection"),
        Line::from("  a           Add worktree (interactive)"),
        Line::from("  A           Add worktree (manual)"),
        Line::from("  /           Filter/search (status or substring)"),
        Line::from("  s           Cycle sort"),
        Line::from("  v           Toggle verbose columns"),
        Line::from("  w           Toggle watch (auto-refresh)"),
        Line::from("  i           Set refresh interval"),
        Line::from("  n           Toggle remote fetch"),
        Line::from("  t           Set stale-days threshold"),
        Line::from("  T           New task for selected worktree"),
        Line::from("  P           Toggle process detection"),
        Line::from("  g           Toggle local/global"),
        Line::from("  r           Refresh"),
        Line::from("  p           Prune worktrees (current repo)"),
        Line::from("  d           Remove selected worktree"),
        Line::from("  e           Exec in selected worktree"),
        Line::from("  o           Exec+stay in selected worktree"),
        Line::from(""),
        Line::from("Tasks tab:"),
        Line::from("  /           Search"),
        Line::from("  n           New task"),
        Line::from("  Enter       Open logs for selected task"),
        Line::from("  W           Start worker (daemon)"),
        Line::from("  S           Stop worker"),
        Line::from("  R           Reset selected task → pending"),
        Line::from("  D           Delete selected task"),
        Line::from("  l           View executions list"),
        Line::from("  w           Worker status (verbose)"),
        Line::from(""),
        Line::from("Log viewer (Output):"),
        Line::from("  f           Toggle follow"),
        Line::from("  p           Toggle pretty/raw"),
        Line::from("  r           Refresh"),
        Line::from(""),
        Line::from("Mux tab:"),
        Line::from("  a           Attach to selected session"),
        Line::from("  x           Kill selected session"),
        Line::from(""),
        Line::from("Config tab:"),
        Line::from("  e           Set config key/value"),
    ];
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_confirm(f: &mut Frame<'_>, confirm: &ConfirmDialog) {
    let area = centered_rect(70, 30, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(confirm.title.as_str());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![Line::from(confirm.message.clone()), Line::from("")];

    if let ConfirmAction::RemoveSelectedWorktree(opts) = &confirm.action {
        lines.push(Line::from("Options:"));
        lines.push(Line::from(format!(
            "  [f] force: {}    [d] dry-run: {}",
            if opts.force { "on" } else { "off" },
            if opts.dry_run { "on" } else { "off" }
        )));
        lines.push(Line::from(format!(
            "  [b] delete-branch: {}    [B] force-delete-branch: {}",
            if opts.delete_branch { "on" } else { "off" },
            if opts.force_delete_branch {
                "on"
            } else {
                "off"
            }
        )));
        lines.push(Line::from(""));
    }

    lines.extend([Line::from(format!(
        "[y] {}    [n] {}",
        confirm.yes_label, confirm.no_label
    ))]);
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}

fn draw_output(f: &mut Frame<'_>, output: &OutputViewer) {
    let area = centered_rect(90, 90, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(output.title.as_str());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines: Vec<Line> = output
        .content
        .lines()
        .map(|l| Line::from(l.to_owned()))
        .collect();
    let text = Text::from(lines);
    let total_lines = output.total_lines();
    let visible = inner.height as usize;
    let max_scroll = total_lines.saturating_sub(visible.max(1));
    let scroll = output.scroll.min(max_scroll);
    let para = Paragraph::new(text)
        .scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0))
        .wrap(Wrap { trim: false });
    f.render_widget(para, inner);
}

fn draw_filter_popup(f: &mut Frame<'_>, app: &AppState) {
    let area = filter_popup_rect(f.area());
    f.render_widget(Clear, area);
    let (title, prefix, input) = match app.filter_target {
        FilterTarget::Status => ("Worktree filter", "Filter: ", &app.filter_input),
        FilterTarget::Tasks => ("Task search", "Search: ", &app.task_filter_input),
    };

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let line = Line::from(vec![
        Span::styled(prefix, Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(input.as_str()),
    ]);
    f.render_widget(Paragraph::new(line).wrap(Wrap { trim: true }), inner);
}

fn draw_command_popup(f: &mut Frame<'_>, app: &AppState) {
    let area = command_popup_rect(f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Command (gwtui …)");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let line = Line::from(vec![
        Span::styled("> ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(app.command_input.as_str()),
    ]);
    f.render_widget(Paragraph::new(line).wrap(Wrap { trim: true }), inner);
}

fn draw_new_task_popup(f: &mut Frame<'_>, app: &AppState, dialog: &NewTaskDialog) {
    let area = centered_rect(80, 45, f.area());
    f.render_widget(Clear, area);
    let block = Block::default().borders(Borders::ALL).title("New task");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let active_style = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);

    let worktree_style = if dialog.field == NewTaskField::Worktree {
        active_style
    } else {
        Style::default()
    };
    let name_style = if dialog.field == NewTaskField::Name {
        active_style
    } else {
        Style::default()
    };
    let prompt_style = if dialog.field == NewTaskField::Prompt {
        active_style
    } else {
        Style::default()
    };
    let prio_style = if dialog.field == NewTaskField::Priority {
        active_style
    } else {
        Style::default()
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Worktree: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(dialog.worktree.as_str(), worktree_style),
        ]),
        Line::from(vec![
            Span::styled("Name:     ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(dialog.name.as_str(), name_style),
        ]),
        Line::from(vec![
            Span::styled("Prompt:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(dialog.prompt.as_str(), prompt_style),
        ]),
        Line::from(vec![
            Span::styled("Priority: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(dialog.priority.as_str(), prio_style),
            Span::styled("  (1-100)", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Runner: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(app.cfg.tasks.runner.as_str()),
            Span::styled("  •  Start worker: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "W",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Stop: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "S",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    if let Some(err) = dialog.error.as_deref() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(
                "Error: ",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::styled(err, Style::default().fg(Color::Red)),
        ]));
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_add_worktree_popup(f: &mut Frame<'_>, app: &AppState, dialog: &AddWorktreeDialog) {
    let area = centered_rect(80, 40, f.area());
    f.render_widget(Clear, area);
    let block = Block::default().borders(Borders::ALL).title("Add worktree");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let repo = dialog.repo_dir.to_string_lossy().to_string();
    let repo = if app.cfg.ui.tilde_home {
        config::tilde_path(&repo)
    } else {
        repo
    };

    let branch_style = if dialog.field == AddWorktreeField::Branch {
        Style::default().fg(Color::Black).bg(Color::LightBlue)
    } else {
        Style::default()
    };
    let path_style = if dialog.field == AddWorktreeField::Path {
        Style::default().fg(Color::Black).bg(Color::LightBlue)
    } else {
        Style::default()
    };

    let branch_text = if dialog.branch.text.is_empty() {
        Span::styled("<branch>", branch_style.add_modifier(Modifier::DIM))
    } else {
        Span::styled(dialog.branch.as_str(), branch_style)
    };
    let path_text = if dialog.path.text.is_empty() {
        Span::styled("(auto)", path_style.add_modifier(Modifier::DIM))
    } else {
        Span::styled(dialog.path.as_str(), path_style)
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Repo: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(repo),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Branch: ", Style::default().add_modifier(Modifier::BOLD)),
            branch_text,
        ]),
        Line::from(vec![
            Span::styled("Path:   ", Style::default().add_modifier(Modifier::BOLD)),
            path_text,
        ]),
        Line::from(""),
        Line::from(format!(
            "[Tab] switch field   [b] create-branch: {}   [f] force: {}",
            if dialog.create_branch { "on" } else { "off" },
            if dialog.force { "on" } else { "off" }
        )),
        Line::from("Enter apply • Esc cancel"),
    ];

    if let Some(err) = &dialog.error {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!("Error: {err}"),
            Style::default().fg(Color::Red),
        )));
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_config_edit_popup(f: &mut Frame<'_>, dialog: &ConfigEditDialog) {
    let area = centered_rect(80, 35, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Config set (key/value)");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let key_style = if dialog.field == ConfigEditField::Key {
        Style::default().fg(Color::Black).bg(Color::LightBlue)
    } else {
        Style::default()
    };
    let val_style = if dialog.field == ConfigEditField::Value {
        Style::default().fg(Color::Black).bg(Color::LightBlue)
    } else {
        Style::default()
    };

    let key_text = if dialog.key.text.is_empty() {
        Span::styled("<key>", key_style.add_modifier(Modifier::DIM))
    } else {
        Span::styled(dialog.key.as_str(), key_style)
    };
    let val_text = if dialog.value.text.is_empty() {
        Span::styled("<value>", val_style.add_modifier(Modifier::DIM))
    } else {
        Span::styled(dialog.value.as_str(), val_style)
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Key:   ", Style::default().add_modifier(Modifier::BOLD)),
            key_text,
        ]),
        Line::from(vec![
            Span::styled("Value: ", Style::default().add_modifier(Modifier::BOLD)),
            val_text,
        ]),
        Line::from(""),
        Line::from("[Tab] switch field • Enter next/apply • Esc cancel"),
    ];

    if let Some(err) = &dialog.error {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!("Error: {err}"),
            Style::default().fg(Color::Red),
        )));
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn draw_prompt_popup(f: &mut Frame<'_>, dialog: &PromptDialog) {
    let area = centered_rect(80, 25, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(dialog.title.as_str());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines = vec![
        Line::from(vec![
            Span::styled(&dialog.label, Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(dialog.input.as_str()),
        ]),
        Line::from(""),
        Line::from("Enter apply • Esc cancel"),
    ];

    if let Some(err) = &dialog.error {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!("Error: {err}"),
            Style::default().fg(Color::Red),
        )));
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

#[allow(clippy::too_many_lines)]
fn handle_key(key: KeyEvent, app: &mut AppState, guard: &mut TerminalGuard) -> anyhow::Result<()> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        app.should_quit = true;
        return Ok(());
    }

    // Modals take precedence
    if app.confirm.is_some() {
        app.mode = Mode::Confirm;
        handle_confirm_key(key, app, guard);
        return Ok(());
    }
    if app.output.is_some() {
        app.mode = Mode::Output;
        handle_output_key(key, app);
        return Ok(());
    }
    if app.add_worktree.is_some() {
        app.mode = Mode::AddWorktree;
        handle_add_worktree_key(key, app);
        return Ok(());
    }
    if app.config_edit.is_some() {
        app.mode = Mode::ConfigEdit;
        handle_config_edit_key(key, app);
        return Ok(());
    }
    if app.prompt.is_some() {
        app.mode = Mode::Prompt;
        handle_prompt_key(key, app);
        return Ok(());
    }
    if app.new_task.is_some() {
        app.mode = Mode::NewTask;
        handle_new_task_key(key, app);
        return Ok(());
    }

    match app.mode {
        Mode::Filtering => {
            if matches!(key.code, KeyCode::Esc) {
                app.mode = Mode::Normal;
                return Ok(());
            }
            if matches!(key.code, KeyCode::Enter) {
                match app.filter_target {
                    FilterTarget::Status => {
                        let next = app.filter_input.text.trim();
                        app.filter = if next.is_empty() {
                            "all".to_owned()
                        } else {
                            next.to_owned()
                        };
                        app.needs_status_refresh = true;
                    }
                    FilterTarget::Tasks => {
                        let next = app.task_filter_input.text.trim();
                        app.task_filter = next.to_owned();
                        app.needs_tasks_refresh = true;
                    }
                }
                app.mode = Mode::Normal;
                return Ok(());
            }
            match app.filter_target {
                FilterTarget::Status => handle_text_input_key(key, &mut app.filter_input),
                FilterTarget::Tasks => handle_text_input_key(key, &mut app.task_filter_input),
            }
            return Ok(());
        }
        Mode::Command => {
            if matches!(key.code, KeyCode::Esc) {
                app.mode = Mode::Normal;
                return Ok(());
            }
            if matches!(key.code, KeyCode::Enter) {
                let cmd = app.command_input.text.trim().to_owned();
                app.mode = Mode::Normal;
                if !cmd.is_empty()
                    && let Err(e) = run_command_palette(cmd.as_str(), app, guard)
                {
                    app.last_error = Some(e.to_string());
                }
                return Ok(());
            }
            handle_text_input_key(key, &mut app.command_input);
            return Ok(());
        }
        _ => {}
    }

    // Global keys (normal mode)
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => {
            app.should_quit = true;
            return Ok(());
        }
        KeyCode::Char('?' | '5') => app.tab = TabId::Help,
        KeyCode::Char(':') => {
            app.mode = Mode::Command;
            if app.command_input.text.is_empty() {
                app.command_input = TextInput::new("");
            }
            return Ok(());
        }
        KeyCode::Char('1') => app.tab = TabId::Status,
        KeyCode::Char('2') => app.tab = TabId::Tasks,
        KeyCode::Char('3') => app.tab = TabId::Mux,
        KeyCode::Char('4') => app.tab = TabId::Config,
        KeyCode::Char('h') => app.tab = app.tab.prev(),
        KeyCode::Char('l') if app.tab != TabId::Tasks => app.tab = app.tab.next(),
        _ => {}
    }

    match app.tab {
        TabId::Status => {
            handle_status_tab_key(key, app, guard);
            Ok(())
        }
        TabId::Tasks => {
            handle_tasks_tab_key(key, app, guard);
            Ok(())
        }
        TabId::Mux => handle_mux_tab_key(key, app, guard),
        TabId::Config => {
            handle_config_tab_key(key, app);
            Ok(())
        }
        TabId::Help => Ok(()),
    }
}

#[allow(clippy::too_many_lines)]
fn handle_status_tab_key(key: KeyEvent, app: &mut AppState, guard: &mut TerminalGuard) {
    match key.code {
        KeyCode::Char('/') => {
            app.filter_target = FilterTarget::Status;
            app.mode = Mode::Filtering;
            let current = app.filter.trim();
            let initial = if current.is_empty() || current.eq_ignore_ascii_case("all") {
                ""
            } else {
                current
            };
            app.filter_input = TextInput::new(initial);
        }
        KeyCode::Char('a') => {
            let repo_dir = match selected_worktree_dir_for_command(app) {
                Ok(d) => Some(d),
                Err(e) => {
                    app.last_error = Some(e.to_string());
                    return;
                }
            };

            match run_interactive_command_in_dir(
                vec!["add".to_owned(), "-i".to_owned()],
                repo_dir,
                app,
                guard,
            ) {
                Ok(true) => app.needs_status_refresh = true,
                Ok(false) => {}
                Err(e) => app.last_error = Some(e.to_string()),
            }
        }
        KeyCode::Char('A') => {
            if let Ok(repo_dir) = selected_worktree_dir_for_command(app) {
                app.add_worktree = Some(AddWorktreeDialog::new(repo_dir));
                app.mode = Mode::AddWorktree;
            } else {
                app.last_error = Some("add requires being inside a git repo".to_owned());
            }
        }
        KeyCode::Char('r') => app.needs_status_refresh = true,
        KeyCode::Char('v') => app.status_verbose = !app.status_verbose,
        KeyCode::Char('w') => app.status_auto_refresh = !app.status_auto_refresh,
        KeyCode::Char('n') => {
            app.status_fetch_remote = !app.status_fetch_remote;
            app.needs_status_refresh = true;
        }
        KeyCode::Char('P') => {
            app.status_show_processes = !app.status_show_processes;
            app.needs_status_refresh = true;
            if app.status_show_processes {
                app.last_error = Some("process detection not implemented yet".to_owned());
            }
        }
        KeyCode::Char('i') => {
            app.prompt = Some(PromptDialog::status_refresh_interval(app));
            app.mode = Mode::Prompt;
        }
        KeyCode::Char('t') => {
            app.prompt = Some(PromptDialog::status_stale_days(app));
            app.mode = Mode::Prompt;
        }
        KeyCode::Char('T') => {
            if app.statuses.is_empty() {
                return;
            }
            let idx = app.selected_status_index().min(app.statuses.len() - 1);
            let branch = app.statuses[idx].branch.clone();
            open_new_task(app, &branch);
        }
        KeyCode::Char('g') => {
            app.global = !app.global;
            app.needs_status_refresh = true;
        }
        KeyCode::Char('s') => {
            app.sort = next_sort(&app.sort).to_owned();
            app.needs_status_refresh = true;
        }
        KeyCode::Up | KeyCode::Char('k') => app.move_status_selection(-1),
        KeyCode::Down | KeyCode::Char('j') => app.move_status_selection(1),
        KeyCode::PageUp => app.move_status_selection(-10),
        KeyCode::PageDown => app.move_status_selection(10),
        KeyCode::Char('p') => {
            // prune current repo if possible
            if let Ok(git) = Git::from_cwd() {
                let wm = WorktreeManager::new(git, app.cfg.clone());
                if let Err(e) = wm.prune() {
                    app.last_error = Some(e.to_string());
                } else {
                    app.last_error = None;
                    app.needs_status_refresh = true;
                }
            } else {
                app.last_error = Some("prune requires being inside a git repo".to_owned());
            }
        }
        KeyCode::Char('d') => {
            if app.statuses.is_empty() {
                return;
            }
            let idx = app.selected_status_index().min(app.statuses.len() - 1);
            let s = &app.statuses[idx];
            app.confirm = Some(ConfirmDialog {
                title: "Remove worktree".to_owned(),
                message: format!(
                    "Remove worktree at:\n{}\n\nThis runs: git worktree remove",
                    s.path
                ),
                yes_label: "remove".to_owned(),
                no_label: "cancel".to_owned(),
                action: ConfirmAction::RemoveSelectedWorktree(RemoveWorktreeOptions {
                    force: false,
                    dry_run: false,
                    delete_branch: false,
                    force_delete_branch: false,
                }),
            });
        }
        KeyCode::Char('e') => {
            if app.statuses.is_empty() {
                return;
            }
            let idx = app.selected_status_index().min(app.statuses.len() - 1);
            let s = &app.statuses[idx];
            let p = quote_arg(&s.path);
            app.command_input = TextInput::new(format!("exec {p} -- "));
            app.mode = Mode::Command;
        }
        KeyCode::Char('o') => {
            if app.statuses.is_empty() {
                return;
            }
            let idx = app.selected_status_index().min(app.statuses.len() - 1);
            let s = &app.statuses[idx];
            let p = quote_arg(&s.path);
            app.command_input = TextInput::new(format!("exec -s {p} -- "));
            app.mode = Mode::Command;
        }
        _ => {}
    }
}

fn handle_tasks_tab_key(key: KeyEvent, app: &mut AppState, _guard: &mut TerminalGuard) {
    match key.code {
        KeyCode::Char('/') => {
            app.filter_target = FilterTarget::Tasks;
            app.mode = Mode::Filtering;
            app.task_filter_input = TextInput::new(app.task_filter.as_str());
        }
        KeyCode::Char('n') => {
            let worktree = app
                .tasks
                .get(
                    app.selected_task_index()
                        .min(app.tasks.len().saturating_sub(1)),
                )
                .map(|t| t.worktree.clone())
                .unwrap_or_default();
            open_new_task(app, &worktree);
        }
        KeyCode::Char('r') => app.needs_tasks_refresh = true,
        KeyCode::Up | KeyCode::Char('k') => app.move_task_selection(-1),
        KeyCode::Down | KeyCode::Char('j') => app.move_task_selection(1),
        KeyCode::PageUp => app.move_task_selection(-10),
        KeyCode::PageDown => app.move_task_selection(10),
        KeyCode::Enter => {
            if let Err(e) = open_selected_task_logs(app) {
                app.last_error = Some(e.to_string());
            }
        }
        KeyCode::Char('W') => match run_captured_command(vec![
            "task".to_owned(),
            "worker".to_owned(),
            "start".to_owned(),
            "--daemon".to_owned(),
        ]) {
            Ok(out) => {
                let msg = out.lines().next().unwrap_or("").trim();
                if !msg.is_empty() {
                    app.toast = Some(Toast::info(msg.to_owned()));
                }
                app.needs_tasks_refresh = true;
            }
            Err(e) => app.last_error = Some(e.to_string()),
        },
        KeyCode::Char('S') => match run_captured_command(vec![
            "task".to_owned(),
            "worker".to_owned(),
            "stop".to_owned(),
        ]) {
            Ok(out) => {
                let msg = out.lines().next().unwrap_or("").trim();
                if !msg.is_empty() {
                    app.toast = Some(Toast::info(msg.to_owned()));
                }
                app.needs_tasks_refresh = true;
            }
            Err(e) => app.last_error = Some(e.to_string()),
        },
        KeyCode::Char('R') => match reset_selected_task(app) {
            Ok(Some(id)) => {
                app.toast = Some(Toast::info(format!("Reset task {id} → pending")));
                app.needs_tasks_refresh = true;
            }
            Ok(None) => {}
            Err(e) => app.last_error = Some(e.to_string()),
        },
        KeyCode::Char('D') => {
            if let Err(e) = confirm_delete_selected_task(app) {
                app.last_error = Some(e.to_string());
            }
        }
        KeyCode::Char('l') => {
            // Task logs list (exec metadata) as output view.
            if !app.cfg.tasks.enabled {
                return;
            }
            match config::expand_path(&app.cfg.tasks.queue_dir) {
                Ok(queue_dir) => {
                    let exec_mgr = ExecutionManager::new(queue_dir);
                    let mut execs = exec_mgr.list_metadata().unwrap_or_default();
                    execs.sort_by(|a, b| b.start_time.cmp(&a.start_time));
                    let out = format_execution_list(&execs);
                    app.output = Some(OutputViewer::new("Executions", out));
                }
                Err(e) => app.last_error = Some(e.to_string()),
            }
        }
        KeyCode::Char('w') => {
            // Worker status in output view.
            if !app.cfg.tasks.enabled {
                return;
            }
            match run_captured_command(vec![
                "task".to_owned(),
                "worker".to_owned(),
                "status".to_owned(),
                "--verbose".to_owned(),
            ]) {
                Ok(out) => app.output = Some(OutputViewer::new("Worker status", out)),
                Err(e) => app.last_error = Some(e.to_string()),
            }
        }
        _ => {}
    }
}

fn handle_mux_tab_key(
    key: KeyEvent,
    app: &mut AppState,
    guard: &mut TerminalGuard,
) -> anyhow::Result<()> {
    match key.code {
        KeyCode::Char('r') => app.needs_sessions_refresh = true,
        KeyCode::Up | KeyCode::Char('k') => app.move_session_selection(-1),
        KeyCode::Down | KeyCode::Char('j') => app.move_session_selection(1),
        KeyCode::PageUp => app.move_session_selection(-10),
        KeyCode::PageDown => app.move_session_selection(10),
        KeyCode::Char('a') => {
            if app.sessions.is_empty() {
                return Ok(());
            }
            let idx = app.selected_session_index().min(app.sessions.len() - 1);
            let name = app.sessions[idx].name.clone();
            run_interactive_attach(&name, app, guard)?;
            app.needs_sessions_refresh = true;
        }
        KeyCode::Char('x') => {
            if app.sessions.is_empty() {
                return Ok(());
            }
            let idx = app.selected_session_index().min(app.sessions.len() - 1);
            let name = app.sessions[idx].name.clone();
            app.confirm = Some(ConfirmDialog {
                title: "Kill session".to_owned(),
                message: format!("Kill zellij session '{name}'?"),
                yes_label: "kill".to_owned(),
                no_label: "cancel".to_owned(),
                action: ConfirmAction::KillSelectedSession,
            });
        }
        _ => {}
    }
    Ok(())
}

fn handle_config_tab_key(key: KeyEvent, app: &mut AppState) {
    match key.code {
        KeyCode::Char('r') => app.needs_config_refresh = true,
        KeyCode::Char('e') => {
            app.config_edit = Some(ConfigEditDialog::new());
            app.mode = Mode::ConfigEdit;
        }
        KeyCode::Up | KeyCode::Char('k') => app.config_scroll = app.config_scroll.saturating_sub(1),
        KeyCode::Down | KeyCode::Char('j') => {
            app.config_scroll = app.config_scroll.saturating_add(1);
        }
        KeyCode::PageUp => app.config_scroll = app.config_scroll.saturating_sub(10),
        KeyCode::PageDown => app.config_scroll = app.config_scroll.saturating_add(10),
        KeyCode::Char('g') => app.config_scroll = 0,
        KeyCode::Char('G') => app.config_scroll = usize::MAX / 2,
        _ => {}
    }
}

fn handle_confirm_key(key: KeyEvent, app: &mut AppState, guard: &mut TerminalGuard) {
    match key.code {
        KeyCode::Char('n') | KeyCode::Esc => {
            app.confirm = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Char('y') => {
            let Some(confirm) = app.confirm.take() else {
                app.mode = Mode::Normal;
                return;
            };
            app.mode = Mode::Normal;
            match confirm.action {
                ConfirmAction::RemoveSelectedWorktree(opts) => {
                    if let Err(e) = remove_selected_worktree(app, opts) {
                        app.last_error = Some(e.to_string());
                    } else {
                        app.last_error = None;
                        app.needs_status_refresh = true;
                    }
                }
                ConfirmAction::KillSelectedSession => {
                    if let Err(e) = kill_selected_session(app, guard) {
                        app.last_error = Some(e.to_string());
                    } else {
                        app.last_error = None;
                        app.needs_sessions_refresh = true;
                    }
                }
                ConfirmAction::DeleteTask { id } => {
                    if let Err(e) = delete_task_by_id(app, &id) {
                        app.last_error = Some(e.to_string());
                    } else {
                        app.last_error = None;
                        app.toast = Some(Toast::info(format!("Deleted task {id}")));
                        app.needs_tasks_refresh = true;
                    }
                }
            }
        }
        KeyCode::Char('f') => {
            if let Some(confirm) = app.confirm.as_mut()
                && let ConfirmAction::RemoveSelectedWorktree(opts) = &mut confirm.action
            {
                opts.force = !opts.force;
            }
        }
        KeyCode::Char('d') => {
            if let Some(confirm) = app.confirm.as_mut()
                && let ConfirmAction::RemoveSelectedWorktree(opts) = &mut confirm.action
            {
                opts.dry_run = !opts.dry_run;
            }
        }
        KeyCode::Char('b') => {
            if let Some(confirm) = app.confirm.as_mut()
                && let ConfirmAction::RemoveSelectedWorktree(opts) = &mut confirm.action
            {
                opts.delete_branch = !opts.delete_branch;
                if !opts.delete_branch {
                    opts.force_delete_branch = false;
                }
            }
        }
        KeyCode::Char('B') => {
            if let Some(confirm) = app.confirm.as_mut()
                && let ConfirmAction::RemoveSelectedWorktree(opts) = &mut confirm.action
            {
                opts.force_delete_branch = !opts.force_delete_branch;
                if opts.force_delete_branch {
                    opts.delete_branch = true;
                }
            }
        }
        _ => {}
    }
}

fn open_new_task(app: &mut AppState, worktree: &str) {
    let priority = 50u8;
    let mut dialog = NewTaskDialog::new(worktree.to_owned(), priority);
    dialog.field = if worktree.trim().is_empty() {
        NewTaskField::Worktree
    } else {
        NewTaskField::Prompt
    };
    app.new_task = Some(dialog);
    app.mode = Mode::NewTask;
}

#[allow(clippy::too_many_lines)]
fn handle_new_task_key(key: KeyEvent, app: &mut AppState) {
    let Some(dialog) = app.new_task.as_mut() else {
        app.mode = Mode::Normal;
        return;
    };

    let next_field = |f: NewTaskField| match f {
        NewTaskField::Worktree => NewTaskField::Name,
        NewTaskField::Name => NewTaskField::Prompt,
        NewTaskField::Prompt => NewTaskField::Priority,
        NewTaskField::Priority => NewTaskField::Worktree,
    };
    let prev_field = |f: NewTaskField| match f {
        NewTaskField::Worktree => NewTaskField::Priority,
        NewTaskField::Name => NewTaskField::Worktree,
        NewTaskField::Prompt => NewTaskField::Name,
        NewTaskField::Priority => NewTaskField::Prompt,
    };

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.new_task = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Tab => {
            dialog.error = None;
            dialog.field = next_field(dialog.field);
        }
        KeyCode::BackTab => {
            dialog.error = None;
            dialog.field = prev_field(dialog.field);
        }
        KeyCode::Char('W') => match run_captured_command(vec![
            "task".to_owned(),
            "worker".to_owned(),
            "start".to_owned(),
            "--daemon".to_owned(),
        ]) {
            Ok(out) => {
                let msg = out.lines().next().unwrap_or("").trim();
                if !msg.is_empty() {
                    app.toast = Some(Toast::info(msg.to_owned()));
                }
                app.needs_tasks_refresh = true;
            }
            Err(e) => dialog.error = Some(e.to_string()),
        },
        KeyCode::Char('S') => match run_captured_command(vec![
            "task".to_owned(),
            "worker".to_owned(),
            "stop".to_owned(),
        ]) {
            Ok(out) => {
                let msg = out.lines().next().unwrap_or("").trim();
                if !msg.is_empty() {
                    app.toast = Some(Toast::info(msg.to_owned()));
                }
                app.needs_tasks_refresh = true;
            }
            Err(e) => dialog.error = Some(e.to_string()),
        },
        KeyCode::Enter => {
            dialog.error = None;
            if dialog.field != NewTaskField::Priority {
                dialog.field = next_field(dialog.field);
                return;
            }

            if !app.cfg.tasks.enabled {
                dialog.error = Some("task system disabled (tasks.enabled = false)".to_owned());
                return;
            }

            let worktree = dialog.worktree.text.trim();
            if worktree.is_empty() {
                dialog.error = Some("worktree is required".to_owned());
                dialog.field = NewTaskField::Worktree;
                return;
            }

            let name_raw = dialog.name.text.trim();
            let prompt_raw = dialog.prompt.text.trim();
            if name_raw.is_empty() && prompt_raw.is_empty() {
                dialog.error = Some("name or prompt is required".to_owned());
                dialog.field = NewTaskField::Prompt;
                return;
            }

            let priority = match dialog.priority.text.trim() {
                "" => 50u8,
                raw => match raw.parse::<u8>() {
                    Ok(v) if (1..=100).contains(&v) => v,
                    _ => {
                        dialog.error = Some("priority must be 1-100".to_owned());
                        dialog.field = NewTaskField::Priority;
                        return;
                    }
                },
            };

            let runner = app.cfg.tasks.runner.trim();
            let runner = if runner.is_empty() { "codex" } else { runner };
            let runner = runner.to_lowercase();
            let repository = Git::from_cwd()
                .ok()
                .map(|g| g.repo_root().to_string_lossy().to_string());

            let name = if name_raw.is_empty() {
                prompt_raw.to_owned()
            } else {
                name_raw.to_owned()
            };

            let task = Task {
                id: Task::new_id(),
                runner,
                name: name.clone(),
                repository,
                worktree: worktree.to_owned(),
                base_branch: None,
                priority,
                depends_on: Vec::new(),
                prompt: prompt_raw.to_owned(),
                files: Vec::new(),
                verify: Vec::new(),
                auto_commit: false,
                status: crate::task::model::TaskStatus::Pending,
                created_at: now_rfc3339(),
                started_at: None,
                completed_at: None,
                session_id: None,
                last_error: None,
            };

            match config::expand_path(&app.cfg.tasks.queue_dir) {
                Ok(queue_dir) => {
                    let storage = TaskStorage::new(queue_dir);
                    match storage.save(&task) {
                        Ok(()) => {
                            app.toast = Some(Toast::info(format!(
                                "Queued task {} ({})",
                                display_task_name(&task),
                                task.id
                            )));
                            app.new_task = None;
                            app.mode = Mode::Normal;
                            app.needs_tasks_refresh = true;
                        }
                        Err(e) => dialog.error = Some(e.to_string()),
                    }
                }
                Err(e) => dialog.error = Some(e.to_string()),
            }
        }
        _ => {
            dialog.error = None;
            match dialog.field {
                NewTaskField::Worktree => handle_text_input_key(key, &mut dialog.worktree),
                NewTaskField::Name => handle_text_input_key(key, &mut dialog.name),
                NewTaskField::Prompt => handle_text_input_key(key, &mut dialog.prompt),
                NewTaskField::Priority => handle_text_input_key(key, &mut dialog.priority),
            }
        }
    }
}

fn handle_add_worktree_key(key: KeyEvent, app: &mut AppState) {
    let Some(dialog) = app.add_worktree.as_mut() else {
        app.mode = Mode::Normal;
        return;
    };

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.add_worktree = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Tab => {
            dialog.field = match dialog.field {
                AddWorktreeField::Branch => AddWorktreeField::Path,
                AddWorktreeField::Path => AddWorktreeField::Branch,
            };
        }
        KeyCode::Char('b') => dialog.create_branch = !dialog.create_branch,
        KeyCode::Char('f') => dialog.force = !dialog.force,
        KeyCode::Enter => {
            let repo_dir = dialog.repo_dir.clone();
            let branch = dialog.branch.text.trim().to_owned();
            let path_raw = dialog.path.text.trim().to_owned();
            let create_branch = dialog.create_branch;
            let force = dialog.force;

            if branch.is_empty() {
                dialog.error = Some("branch is required".to_owned());
                return;
            }

            let custom_path = (!path_raw.is_empty()).then(|| PathBuf::from(path_raw));

            match Git::from_dir(&repo_dir) {
                Ok(git) => {
                    let wm = WorktreeManager::new(git, app.cfg.clone());
                    if let Some(p) = custom_path.as_deref()
                        && !force
                        && let Err(e) = wm.validate_worktree_path(p)
                    {
                        dialog.error = Some(e.to_string());
                        return;
                    }

                    match wm.add(&branch, custom_path.as_deref(), create_branch) {
                        Ok(()) => {
                            app.add_worktree = None;
                            app.mode = Mode::Normal;
                            app.last_error = None;
                            app.needs_status_refresh = true;
                        }
                        Err(e) => dialog.error = Some(e.to_string()),
                    }
                }
                Err(e) => dialog.error = Some(e.to_string()),
            }
        }
        _ => match dialog.field {
            AddWorktreeField::Branch => handle_text_input_key(key, &mut dialog.branch),
            AddWorktreeField::Path => handle_text_input_key(key, &mut dialog.path),
        },
    }
}

fn handle_config_edit_key(key: KeyEvent, app: &mut AppState) {
    let Some(dialog) = app.config_edit.as_mut() else {
        app.mode = Mode::Normal;
        return;
    };

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.config_edit = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Tab => {
            dialog.field = match dialog.field {
                ConfigEditField::Key => ConfigEditField::Value,
                ConfigEditField::Value => ConfigEditField::Key,
            };
        }
        KeyCode::Enter => match dialog.field {
            ConfigEditField::Key => dialog.field = ConfigEditField::Value,
            ConfigEditField::Value => {
                let key = dialog.key.text.trim().to_owned();
                let value = dialog.value.text.trim().to_owned();
                if key.is_empty() {
                    dialog.error = Some("key is required".to_owned());
                    return;
                }

                match config::set_value_string(&key, &value) {
                    Ok(()) => match config::load() {
                        Ok((cfg, _doc, _paths)) => {
                            app.cfg = cfg;
                            app.needs_status_refresh = true;
                            app.needs_tasks_refresh = true;
                            app.needs_sessions_refresh = true;
                            app.needs_config_refresh = true;
                            app.config_edit = None;
                            app.mode = Mode::Normal;
                            app.last_error = None;
                        }
                        Err(e) => dialog.error = Some(e.to_string()),
                    },
                    Err(e) => dialog.error = Some(e.to_string()),
                }
            }
        },
        _ => match dialog.field {
            ConfigEditField::Key => handle_text_input_key(key, &mut dialog.key),
            ConfigEditField::Value => handle_text_input_key(key, &mut dialog.value),
        },
    }
}

fn handle_prompt_key(key: KeyEvent, app: &mut AppState) {
    let Some(dialog) = app.prompt.as_mut() else {
        app.mode = Mode::Normal;
        return;
    };

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.prompt = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Enter => {
            let raw = dialog.input.text.trim();
            match dialog.kind {
                PromptKind::StatusRefreshInterval => match parse_interval_to_ms(raw) {
                    Ok(Some(ms)) => {
                        app.status_refresh_interval_override_ms = Some(ms.max(100));
                        app.prompt = None;
                        app.mode = Mode::Normal;
                    }
                    Ok(None) => {
                        app.status_refresh_interval_override_ms = None;
                        app.prompt = None;
                        app.mode = Mode::Normal;
                    }
                    Err(e) => dialog.error = Some(e.to_string()),
                },
                PromptKind::StatusStaleDays => {
                    if raw.is_empty() {
                        app.status_stale_days = 14;
                        app.needs_status_refresh = true;
                        app.prompt = None;
                        app.mode = Mode::Normal;
                        return;
                    }
                    match raw.parse::<u64>() {
                        Ok(days) if days > 0 => {
                            app.status_stale_days = days;
                            app.needs_status_refresh = true;
                            app.prompt = None;
                            app.mode = Mode::Normal;
                        }
                        _ => dialog.error = Some("expected a positive integer".to_owned()),
                    }
                }
            }
        }
        _ => handle_text_input_key(key, &mut dialog.input),
    }
}

fn handle_output_key(key: KeyEvent, app: &mut AppState) {
    let Some(output) = app.output.as_mut() else {
        app.mode = Mode::Normal;
        return;
    };
    let total = output.total_lines();
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.output = None;
            app.mode = Mode::Normal;
        }
        KeyCode::Char('r') => {
            output.refresh_now(&app.cfg);
        }
        KeyCode::Char('f') => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = !*follow;
                if *follow {
                    output.scroll = usize::MAX / 2;
                    output.refresh_now(&app.cfg);
                }
            }
        }
        KeyCode::Char('p') => {
            if let OutputSource::Execution { pretty, .. } = &mut output.source {
                *pretty = !*pretty;
                output.refresh_now(&app.cfg);
            }
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = (output.scroll + 1).min(total.saturating_sub(1));
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = output.scroll.saturating_sub(1);
        }
        KeyCode::PageDown => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = (output.scroll + 10).min(total.saturating_sub(1));
        }
        KeyCode::PageUp => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = output.scroll.saturating_sub(10);
        }
        KeyCode::Char('g') => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = 0;
        }
        KeyCode::Char('G') => {
            if let OutputSource::Execution { follow, .. } = &mut output.source {
                *follow = false;
            }
            output.scroll = total.saturating_sub(1);
        }
        _ => {}
    }
}

fn handle_text_input_key(key: KeyEvent, input: &mut TextInput) {
    match key.code {
        KeyCode::Backspace => input.backspace(),
        KeyCode::Delete => input.delete(),
        KeyCode::Left => input.move_left(),
        KeyCode::Right => input.move_right(),
        KeyCode::Home => input.move_home(),
        KeyCode::End => input.move_end(),
        KeyCode::Char(c) => {
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT)
            {
                input.insert_char(c);
            }
        }
        _ => {}
    }
}

fn run_command_palette(
    cmd: &str,
    app: &mut AppState,
    guard: &mut TerminalGuard,
) -> anyhow::Result<()> {
    let args = parse_command_line(cmd)?;
    if args.is_empty() {
        return Ok(());
    }

    // Special-case: `status --watch` should map to the Status tab refresh loop.
    if args.first().is_some_and(|s| s == "status")
        && args.iter().any(|a| a == "--watch" || a == "-w")
    {
        app.tab = TabId::Status;
        app.needs_status_refresh = true;
        app.output = Some(OutputViewer::new(
            "Status watch",
            "You're already in watch mode in the Status tab.\nUse 'r' to refresh or adjust refresh_interval_ms in config.",
        ));
        return Ok(());
    }

    // Decide whether to run interactively (suspend TUI) or capture output.
    let interactive = is_interactive_cli_command(&args);
    if interactive {
        let _ = run_interactive_command(args, app, guard)?;
        app.needs_status_refresh = true;
        app.needs_tasks_refresh = true;
        app.needs_sessions_refresh = true;
        app.needs_config_refresh = true;
        return Ok(());
    }

    let out = run_captured_command(args)?;
    app.output = Some(OutputViewer::new("Command output", out));
    app.needs_status_refresh = true;
    app.needs_tasks_refresh = true;
    app.needs_sessions_refresh = true;
    app.needs_config_refresh = true;
    Ok(())
}

fn is_interactive_cli_command(args: &[String]) -> bool {
    // Commands that should take over the terminal.
    // - exec: inherits stdio and can open shells/tools
    // - tmux/zellij attach: interactive
    // - task worker start: long-running; better in normal terminal
    // - anything explicitly using -i/--interactive in a way that would otherwise require a TTY
    if args.is_empty() {
        return false;
    }
    if args[0] == "exec" {
        return true;
    }
    if (args[0] == "tmux" || args[0] == "zellij") && args.get(1).is_some_and(|s| s == "attach") {
        return true;
    }
    if args[0] == "task"
        && args.get(1).is_some_and(|s| s == "worker")
        && args.get(2).is_some_and(|s| s == "start")
    {
        return true;
    }
    args.iter().any(|a| a == "-i" || a == "--interactive")
}

fn run_captured_command(args: Vec<String>) -> anyhow::Result<String> {
    use std::process::Command;

    let exe = std::env::current_exe()?;
    let out = Command::new(exe).args(args).output()?;

    let mut s = String::new();
    if !out.stdout.is_empty() {
        s.push_str(&String::from_utf8_lossy(&out.stdout));
    }
    if !out.stderr.is_empty() {
        if !s.ends_with('\n') && !s.is_empty() {
            s.push('\n');
        }
        s.push_str(&String::from_utf8_lossy(&out.stderr));
    }
    if s.trim().is_empty() {
        s = format!("(exit code: {})", out.status.code().unwrap_or(1));
    }
    Ok(s)
}

fn run_interactive_attach(
    name: &str,
    app: &mut AppState,
    guard: &mut TerminalGuard,
) -> anyhow::Result<()> {
    if app.cfg.mux.backend == crate::config::MuxBackend::None {
        anyhow::bail!("mux backend is disabled (mux.backend = \"none\")");
    }
    let args = vec!["zellij".to_owned(), "attach".to_owned(), name.to_owned()];
    // Run via the CLI so behavior is consistent with gwtui zellij attach.
    run_interactive_command(args, app, guard).map(|_| ())
}

fn run_interactive_command(
    args: Vec<String>,
    app: &mut AppState,
    guard: &mut TerminalGuard,
) -> anyhow::Result<bool> {
    run_interactive_command_in_dir(args, None, app, guard)
}

fn run_interactive_command_in_dir(
    args: Vec<String>,
    dir: Option<PathBuf>,
    app: &mut AppState,
    guard: &mut TerminalGuard,
) -> anyhow::Result<bool> {
    use std::process::Command;

    // Suspend TUI to give full control to the spawned command.
    if let Some(term) = guard.terminal.take() {
        tui::restore_terminal(term)?;
    }

    let exe = std::env::current_exe()?;
    let mut cmd = Command::new(exe);
    cmd.args(args);
    if let Some(dir) = dir {
        cmd.current_dir(dir);
    }
    let status = cmd
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()?;

    // Resume TUI
    guard.terminal = Some(tui::init_terminal()?);

    if status.success() {
        app.last_error = None;
    } else {
        app.last_error = Some(format!(
            "command failed (exit code: {})",
            status.code().unwrap_or(1)
        ));
    }
    Ok(status.success())
}

fn selected_worktree_dir_for_command(app: &AppState) -> anyhow::Result<PathBuf> {
    if Git::from_cwd().is_ok() {
        return Ok(std::env::current_dir()?);
    }
    if app.statuses.is_empty() {
        anyhow::bail!("no worktrees available");
    }
    let idx = app.selected_status_index().min(app.statuses.len() - 1);
    Ok(PathBuf::from(app.statuses[idx].path.clone()))
}

fn remove_selected_worktree(app: &mut AppState, opts: RemoveWorktreeOptions) -> anyhow::Result<()> {
    if app.statuses.is_empty() {
        return Ok(());
    }
    let idx = app.selected_status_index().min(app.statuses.len() - 1);
    let s = &app.statuses[idx];
    if s.is_current {
        anyhow::bail!(
            "refusing to remove the current worktree (cd to another directory and retry)"
        );
    }

    let wt_root = PathBuf::from(&s.path);
    let git = Git::from_dir(&wt_root)?;

    // Run the removal from the repo's common dir parent (main worktree) to avoid
    // edge-cases when invoked from within other worktrees.
    let common_dir_raw = git.run(&["rev-parse", "--git-common-dir"])?;
    let common_dir_raw = common_dir_raw.trim();
    let common_dir_path = PathBuf::from(common_dir_raw);
    let common_dir_abs = if common_dir_path.is_absolute() {
        common_dir_path
    } else {
        wt_root.join(common_dir_path)
    };
    let main_root = common_dir_abs
        .parent()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve main worktree dir"))?
        .to_path_buf();

    let main_git = Git::new(main_root);
    if opts.dry_run {
        use std::fmt::Write as _;

        let mut out = String::new();
        out.push_str("Dry run - no changes made.\n\n");
        let _ = writeln!(&mut out, "Worktree: {}", s.path);
        let _ = writeln!(&mut out, "Branch: {}", s.branch);
        out.push('\n');

        out.push_str("Would run:\n");
        let _ = writeln!(
            &mut out,
            "  git worktree remove{} {}",
            if opts.force { " --force" } else { "" },
            wt_root.display()
        );
        if opts.delete_branch && !s.branch.trim().is_empty() {
            let _ = writeln!(
                &mut out,
                "  git branch {} {}",
                if opts.force_delete_branch { "-D" } else { "-d" },
                s.branch
            );
        }
        app.output = Some(OutputViewer::new("Remove worktree (dry-run)", out));
        return Ok(());
    }

    main_git.remove_worktree(&wt_root, opts.force)?;
    if opts.delete_branch && !s.branch.trim().is_empty() {
        main_git.delete_branch(&s.branch, opts.force_delete_branch)?;
    }
    Ok(())
}

fn kill_selected_session(app: &mut AppState, _guard: &mut TerminalGuard) -> anyhow::Result<()> {
    if app.cfg.mux.backend == crate::config::MuxBackend::None {
        return Ok(());
    }
    if app.sessions.is_empty() {
        return Ok(());
    }
    let idx = app.selected_session_index().min(app.sessions.len() - 1);
    let name = app.sessions[idx].name.clone();
    let mux = ZellijMux::new(
        app.cfg.mux.zellij_command.clone(),
        app.cfg.mux.require_session_for_run,
    );
    mux.kill(&name)?;
    Ok(())
}

fn confirm_delete_selected_task(app: &mut AppState) -> anyhow::Result<()> {
    if !app.cfg.tasks.enabled {
        anyhow::bail!("task system disabled (tasks.enabled = false)");
    }
    if app.tasks.is_empty() {
        return Ok(());
    }
    let idx = app.selected_task_index().min(app.tasks.len() - 1);
    let t = &app.tasks[idx];

    let name = display_task_name(t);
    app.confirm = Some(ConfirmDialog {
        title: "Delete task".to_owned(),
        message: format!(
            "Delete task:\n{name}\n\nID: {}\n\nThis removes the task from the queue.\nExecution logs are not deleted.",
            t.id
        ),
        yes_label: "delete".to_owned(),
        no_label: "cancel".to_owned(),
        action: ConfirmAction::DeleteTask { id: t.id.clone() },
    });
    Ok(())
}

fn reset_selected_task(app: &mut AppState) -> anyhow::Result<Option<String>> {
    if !app.cfg.tasks.enabled {
        return Ok(None);
    }
    if app.tasks.is_empty() {
        return Ok(None);
    }

    let idx = app.selected_task_index().min(app.tasks.len() - 1);
    let id = app.tasks[idx].id.clone();

    let queue_dir = config::expand_path(&app.cfg.tasks.queue_dir)?;
    let storage = TaskStorage::new(queue_dir);
    let mut task = storage.load(&id)?;

    task.status = crate::task::model::TaskStatus::Pending;
    task.started_at = None;
    task.completed_at = None;
    task.session_id = None;
    task.last_error = None;
    storage.save(&task)?;

    Ok(Some(id))
}

fn delete_task_by_id(app: &mut AppState, id: &str) -> anyhow::Result<()> {
    let queue_dir = config::expand_path(&app.cfg.tasks.queue_dir)?;
    let storage = TaskStorage::new(queue_dir);
    storage.delete(id)?;
    Ok(())
}

fn open_selected_task_logs(app: &mut AppState) -> anyhow::Result<()> {
    if !app.cfg.tasks.enabled {
        anyhow::bail!("task system disabled (tasks.enabled = false)");
    }
    if app.tasks.is_empty() {
        return Ok(());
    }

    let idx = app.selected_task_index().min(app.tasks.len() - 1);
    let t = &app.tasks[idx];

    let queue_dir = config::expand_path(&app.cfg.tasks.queue_dir)?;

    let exec_id = t
        .session_id
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| latest_execution_id_for_task(&queue_dir, &t.id));

    let Some(exec_id) = exec_id else {
        app.toast = Some(Toast::info(
            "No execution yet for this task (start the worker with 'W')".to_owned(),
        ));
        return Ok(());
    };

    app.output = Some(OutputViewer::new_execution(&app.cfg, queue_dir, exec_id)?);
    Ok(())
}

fn latest_execution_id_for_task(queue_dir: &std::path::Path, task_id: &str) -> Option<String> {
    let exec_mgr = ExecutionManager::new(queue_dir.to_path_buf());
    let metas = exec_mgr.list_metadata().unwrap_or_default();
    metas
        .into_iter()
        .filter(|m| m.task_id == task_id)
        .max_by(|a, b| a.start_time.cmp(&b.start_time))
        .map(|m| m.execution_id)
}

fn task_matches_query(t: &Task, needle: &str) -> bool {
    if needle.trim().is_empty() {
        return true;
    }

    let status = task_status_str(t.status);
    let repo = t.repository.as_deref().unwrap_or("");

    t.id.to_lowercase().contains(needle)
        || t.name.to_lowercase().contains(needle)
        || t.worktree.to_lowercase().contains(needle)
        || t.prompt.to_lowercase().contains(needle)
        || repo.to_lowercase().contains(needle)
        || status.to_lowercase().contains(needle)
}

fn display_task_name(task: &Task) -> String {
    if !task.name.trim().is_empty() {
        return truncate_str(&task.name, 60);
    }
    if !task.prompt.trim().is_empty() {
        return truncate_str(&task.prompt, 60);
    }
    task.id.clone()
}

fn truncate_str(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('…');
    }
    out
}

fn task_status_icon(status: crate::task::model::TaskStatus) -> &'static str {
    match status {
        crate::task::model::TaskStatus::Pending => "○",
        crate::task::model::TaskStatus::Waiting => "⏳",
        crate::task::model::TaskStatus::Running => "●",
        crate::task::model::TaskStatus::Completed => "✓",
        crate::task::model::TaskStatus::Failed => "✗",
    }
}

fn task_status_style(status: crate::task::model::TaskStatus) -> Style {
    match status {
        crate::task::model::TaskStatus::Pending => Style::default().fg(Color::DarkGray),
        crate::task::model::TaskStatus::Waiting => Style::default().fg(Color::Yellow),
        crate::task::model::TaskStatus::Running => Style::default().fg(Color::Cyan),
        crate::task::model::TaskStatus::Completed => Style::default().fg(Color::Green),
        crate::task::model::TaskStatus::Failed => Style::default().fg(Color::Red),
    }
}

fn worktree_state_style(state: status::WorktreeState) -> Style {
    match state {
        status::WorktreeState::Clean => Style::default().fg(Color::Green),
        status::WorktreeState::Modified => Style::default().fg(Color::Yellow),
        status::WorktreeState::Staged => Style::default().fg(Color::Cyan),
        status::WorktreeState::Conflict => Style::default().fg(Color::Red),
        status::WorktreeState::Stale | status::WorktreeState::Unknown => {
            Style::default().fg(Color::DarkGray)
        }
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn render_execution_output(
    cfg: &crate::config::Config,
    queue_dir: &std::path::Path,
    execution_id: &str,
    pretty: bool,
) -> anyhow::Result<String> {
    use std::fmt::Write as _;

    let exec_mgr = ExecutionManager::new(queue_dir.to_path_buf());
    let meta = exec_mgr.load_metadata(execution_id)?;

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

    let log_path = exec_mgr.log_path(execution_id);
    if !log_path.exists() {
        out.push_str("⊘ Aborted (log file missing)\n");
        return Ok(out);
    }

    let (log, truncated) = read_tail_file(&log_path, 2 * 1024 * 1024)?;
    if truncated {
        out.push_str("… (log truncated; showing tail)\n\n");
    }

    if pretty {
        out.push_str(&format_log_pretty(&log));
    } else {
        out.push_str(&log);
    }

    Ok(out)
}

fn read_tail_file(path: &std::path::Path, max_bytes: usize) -> anyhow::Result<(String, bool)> {
    use std::io::{Read as _, Seek as _};

    let mut f = std::fs::File::open(path)?;
    let len = usize::try_from(f.metadata()?.len()).unwrap_or(usize::MAX);
    let start = len.saturating_sub(max_bytes);
    let truncated = start > 0;
    f.seek(std::io::SeekFrom::Start(start as u64))?;

    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let mut s = String::from_utf8_lossy(&buf).to_string();

    // If we didn't start at 0, drop the partial first line.
    if truncated {
        if let Some(pos) = s.find('\n') {
            s = s[(pos + 1)..].to_owned();
        } else {
            s.clear();
        }
    }

    Ok((s, truncated))
}

fn format_log_pretty(log: &str) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();

    for line in log.lines() {
        let line = line.trim_end();
        if line.trim().is_empty() {
            continue;
        }

        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            out.push_str(line);
            out.push('\n');
            continue;
        };

        let stream = v.get("stream").and_then(|v| v.as_str()).unwrap_or("");

        if let Some(text) = extract_text_field(&v) {
            let ty = v.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let is_delta = ty.contains("delta") || v.get("delta").is_some();

            if stream == "stderr" {
                if !out.ends_with('\n') && !out.is_empty() {
                    out.push('\n');
                }
                out.push_str("[stderr] ");
                out.push_str(text);
                out.push('\n');
            } else if is_delta {
                out.push_str(text);
            } else {
                if !out.ends_with('\n') && !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(text);
                if !text.ends_with('\n') {
                    out.push('\n');
                }
            }
            continue;
        }

        // Fall back to showing stderr lines and notable event types.
        if stream == "stderr" {
            if !out.ends_with('\n') && !out.is_empty() {
                out.push('\n');
            }
            out.push_str("[stderr] ");
            out.push_str(line);
            out.push('\n');
            continue;
        }

        if let Some(ty) = v.get("type").and_then(|v| v.as_str())
            && !ty.trim().is_empty()
            && ty != "text"
        {
            if !out.ends_with('\n') && !out.is_empty() {
                out.push('\n');
            }
            let _ = writeln!(&mut out, "[{ty}]");
        }
    }

    out
}

fn extract_text_field(v: &serde_json::Value) -> Option<&str> {
    match v {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(s)) = map.get("text") {
                return Some(s.as_str());
            }

            if let Some(serde_json::Value::Object(delta)) = map.get("delta")
                && let Some(serde_json::Value::String(s)) = delta.get("text")
            {
                return Some(s.as_str());
            }

            for v in map.values() {
                if let Some(s) = extract_text_field(v) {
                    return Some(s);
                }
            }
            None
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                if let Some(s) = extract_text_field(v) {
                    return Some(s);
                }
            }
            None
        }
        _ => None,
    }
}

fn format_execution_list(execs: &[ExecutionMetadata]) -> String {
    if execs.is_empty() {
        return "No executions found.".to_owned();
    }
    let mut out = String::new();
    for e in execs {
        let _ = std::fmt::Write::write_fmt(
            &mut out,
            format_args!(
                "[{}] {}  {}  {}\n",
                execution_status_str(e.status),
                e.execution_id,
                e.start_time,
                e.working_directory
            ),
        );
    }
    out
}

fn execution_status_str(status: crate::task::execution::ExecutionStatus) -> &'static str {
    match status {
        crate::task::execution::ExecutionStatus::Running => "running",
        crate::task::execution::ExecutionStatus::Completed => "completed",
        crate::task::execution::ExecutionStatus::Failed => "failed",
        crate::task::execution::ExecutionStatus::Aborted => "aborted",
    }
}

fn task_status_str(status: crate::task::model::TaskStatus) -> &'static str {
    match status {
        crate::task::model::TaskStatus::Pending => "pending",
        crate::task::model::TaskStatus::Waiting => "waiting",
        crate::task::model::TaskStatus::Running => "running",
        crate::task::model::TaskStatus::Completed => "completed",
        crate::task::model::TaskStatus::Failed => "failed",
    }
}

fn next_sort(current: &str) -> &'static str {
    match current.trim().to_lowercase().as_str() {
        "activity" => "modified",
        "modified" | "changes" => "branch",
        "branch" | "name" => "status",
        _ => "activity",
    }
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

fn format_interval_ms(ms: u64) -> String {
    if ms.is_multiple_of(1000) {
        format!("{}s", ms / 1000)
    } else {
        format!("{ms}ms")
    }
}

fn parse_interval_to_ms(input: &str) -> anyhow::Result<Option<u64>> {
    let s = input.trim();
    if s.is_empty() {
        return Ok(None);
    }

    let lower = s.to_ascii_lowercase();
    if let Some(rest) = lower.strip_suffix("ms") {
        let ms: u64 = rest.trim().parse()?;
        if ms == 0 {
            anyhow::bail!("interval must be > 0");
        }
        return Ok(Some(ms));
    }

    let rest = lower.strip_suffix('s').unwrap_or(lower.as_str());
    let secs: u64 = rest.trim().parse()?;
    if secs == 0 {
        anyhow::bail!("interval must be > 0");
    }
    Ok(Some(secs.saturating_mul(1000)))
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn filter_popup_rect(area: Rect) -> Rect {
    let w = area.width.min(80);
    let h = 3u16;
    Rect {
        x: area.x + (area.width.saturating_sub(w)) / 2,
        y: area.y + 1,
        width: w,
        height: h,
    }
}

fn command_popup_rect(area: Rect) -> Rect {
    let w = area.width.min(90);
    let h = 3u16;
    Rect {
        x: area.x + (area.width.saturating_sub(w)) / 2,
        y: area.y + 1,
        width: w,
        height: h,
    }
}

fn cursor_x_for_text(text: &str, cursor: usize) -> u16 {
    // `Paragraph` doesn't do cursor for us; approximate by counting graphemes as chars.
    let prefix: String = text.chars().take(cursor).collect();
    u16::try_from(prefix.chars().count()).unwrap_or(0)
}

fn quote_arg(s: &str) -> String {
    // Minimal shell-ish quoting for our `parse_command_line`:
    // we use double-quotes and escape backslashes and quotes.
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

fn parse_command_line(s: &str) -> anyhow::Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;

    for c in s.chars() {
        if escape {
            cur.push(c);
            escape = false;
            continue;
        }

        match c {
            '\\' if in_double || !in_single => {
                escape = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if !cur.is_empty() {
                    out.push(cur.clone());
                    cur.clear();
                }
            }
            _ => cur.push(c),
        }
    }

    if escape {
        anyhow::bail!("trailing escape in command");
    }
    if in_single || in_double {
        anyhow::bail!("unterminated quote in command");
    }
    if !cur.is_empty() {
        out.push(cur);
    }

    Ok(out)
}

struct TerminalGuard {
    terminal: Option<ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>>,
}

impl TerminalGuard {
    fn new(
        terminal: ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    ) -> Self {
        Self {
            terminal: Some(terminal),
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Some(terminal) = self.terminal.take() {
            let _ = tui::restore_terminal(terminal);
        }
    }
}

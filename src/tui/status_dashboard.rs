#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Row, Table, TableState, Wrap};

use crate::core::discovery;
use crate::core::git::Git;
use crate::core::status::{self, StatusCollector, StatusCollectorOptions, WorktreeStatus};
use crate::core::worktree::{Worktree, WorktreeManager};
use crate::tui;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct StatusDashboardOptions {
    pub cfg: crate::config::Config,
    pub start_global: bool,
    pub status_filter: String,
    pub sort: String,
    pub verbose: bool,
    pub show_processes: bool,
    pub fetch_remote: bool,
    pub stale_days: u64,
    pub refresh_interval: Duration,
}

pub async fn run(opts: StatusDashboardOptions) -> anyhow::Result<()> {
    let terminal = tui::init_terminal()?;
    let mut guard = TerminalGuard::new(terminal);

    let mut app = AppState::new(&opts);
    refresh(&mut app, &opts).await?;

    let mut last_refresh = Instant::now();

    loop {
        {
            let Some(terminal) = guard.terminal.as_mut() else {
                anyhow::bail!("terminal unavailable");
            };
            terminal.draw(|f| draw(f, &mut app, &opts))?;
        }

        let should_refresh = app.needs_refresh || last_refresh.elapsed() >= opts.refresh_interval;
        if should_refresh {
            app.needs_refresh = false;
            if let Err(e) = refresh(&mut app, &opts).await {
                app.last_error = Some(e.to_string());
            } else {
                app.last_error = None;
            }
            last_refresh = Instant::now();
        }

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
            && handle_key(key, &mut app)
        {
            break;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    Filtering,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct AppState {
    global: bool,
    filter: String,
    sort: String,
    verbose: bool,

    mode: Mode,
    filter_input: String,

    statuses: Vec<WorktreeStatus>,
    table_state: TableState,

    show_detail: bool,
    needs_refresh: bool,
    last_error: Option<String>,
}

impl AppState {
    fn new(opts: &StatusDashboardOptions) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        Self {
            global: opts.start_global,
            filter: opts.status_filter.clone(),
            sort: opts.sort.clone(),
            verbose: opts.verbose,
            mode: Mode::Normal,
            filter_input: String::new(),
            statuses: Vec::new(),
            table_state,
            show_detail: false,
            needs_refresh: true,
            last_error: None,
        }
    }

    fn selected_index(&self) -> usize {
        self.table_state.selected().unwrap_or(0)
    }

    fn clamp_selection(&mut self) {
        if self.statuses.is_empty() {
            self.table_state.select(Some(0));
            return;
        }
        let idx = self.selected_index().min(self.statuses.len() - 1);
        self.table_state.select(Some(idx));
    }

    fn move_selection(&mut self, delta: i64) {
        if self.statuses.is_empty() {
            return;
        }
        let cur = i64::try_from(self.selected_index()).unwrap_or(i64::MAX);
        let max = i64::try_from(self.statuses.len().saturating_sub(1)).unwrap_or(i64::MAX);
        let next = (cur + delta).clamp(0, max);
        let next = usize::try_from(next).unwrap_or(0);
        self.table_state.select(Some(next));
    }
}

async fn refresh(app: &mut AppState, opts: &StatusDashboardOptions) -> anyhow::Result<()> {
    let worktrees = load_worktrees(&opts.cfg, app.global)?;

    let collector = StatusCollector::new(StatusCollectorOptions {
        include_process: opts.show_processes,
        fetch_remote: opts.fetch_remote,
        stale_threshold: Duration::from_secs(opts.stale_days * 24 * 60 * 60),
        base_dir: opts.cfg.worktree.base_dir.clone(),
        concurrency: opts.cfg.status.concurrency,
    });

    let mut statuses = collector.collect_all(&worktrees).await?;

    if !app.filter.trim().is_empty() {
        statuses = status::filter_statuses(statuses, &app.filter);
    }
    if !app.sort.trim().is_empty() {
        status::sort_statuses(&mut statuses, &app.sort);
    }

    app.statuses = statuses;
    app.clamp_selection();
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

fn handle_key(key: KeyEvent, app: &mut AppState) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return true;
    }

    match app.mode {
        Mode::Normal => match key.code {
            KeyCode::Esc | KeyCode::Char('q') => return true,
            KeyCode::Char('/') => {
                app.mode = Mode::Filtering;
                let current = app.filter.trim();
                app.filter_input = if current.is_empty() || current.eq_ignore_ascii_case("all") {
                    String::new()
                } else {
                    current.to_owned()
                };
            }
            KeyCode::Char('r') => app.needs_refresh = true,
            KeyCode::Char('g') => {
                app.global = !app.global;
                app.needs_refresh = true;
            }
            KeyCode::Char('s') => {
                app.sort = next_sort(&app.sort).to_owned();
                app.needs_refresh = true;
            }
            KeyCode::Up | KeyCode::Char('k') => app.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => app.move_selection(1),
            KeyCode::PageUp => app.move_selection(-10),
            KeyCode::PageDown => app.move_selection(10),
            KeyCode::Enter => app.show_detail = !app.show_detail,
            _ => {}
        },
        Mode::Filtering => match key.code {
            KeyCode::Esc => app.mode = Mode::Normal,
            KeyCode::Enter => {
                let next = app.filter_input.trim();
                app.filter = if next.is_empty() {
                    "all".to_owned()
                } else {
                    next.to_owned()
                };
                app.mode = Mode::Normal;
                app.needs_refresh = true;
            }
            KeyCode::Backspace => {
                app.filter_input.pop();
            }
            KeyCode::Char(c) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL)
                    && !key.modifiers.contains(KeyModifiers::ALT)
                {
                    app.filter_input.push(c);
                }
            }
            _ => {}
        },
    }

    false
}

fn next_sort(current: &str) -> &'static str {
    match current.trim().to_lowercase().as_str() {
        "activity" => "modified",
        "modified" | "changes" => "branch",
        "branch" | "name" => "status",
        _ => "activity",
    }
}

fn draw(f: &mut Frame<'_>, app: &mut AppState, opts: &StatusDashboardOptions) {
    let area = f.area();
    let outer = Block::default().title("gwtui status").borders(Borders::ALL);
    let inner = outer.inner(area);
    f.render_widget(outer, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(inner);

    draw_table(f, chunks[0], app, opts);
    draw_footer(f, chunks[1], app);

    if app.show_detail {
        draw_detail_popup(f, app);
    }
}

fn draw_table(
    f: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &mut AppState,
    opts: &StatusDashboardOptions,
) {
    let headers = if app.verbose {
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
        let marker = if s.is_current && opts.cfg.ui.icons {
            "● "
        } else {
            "  "
        };
        let branch = format!("{marker}{}", s.branch);
        let status_txt = status::format_status_for_table(s.status);
        let changes = format_changes(s.git_status);
        let activity = format_activity(&s.last_activity);

        if app.verbose {
            Row::new(vec![
                branch,
                status_txt.to_owned(),
                changes,
                format!("↑{} ↓{}", s.git_status.ahead, s.git_status.behind),
                activity,
            ])
        } else {
            Row::new(vec![branch, status_txt.to_owned(), changes, activity])
        }
    });

    let widths = if app.verbose {
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
        .block(Block::default().borders(Borders::ALL).title(if app.global {
            "Global"
        } else {
            "Local"
        }))
        .row_highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">");

    f.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_footer(f: &mut Frame<'_>, area: ratatui::layout::Rect, app: &AppState) {
    let keys = match app.mode {
        Mode::Normal => {
            "q/Esc quit • / filter • s sort • g toggle global • r refresh • Enter details"
        }
        Mode::Filtering => "Enter apply • Esc cancel",
    };

    let filter = match app.mode {
        Mode::Filtering => app.filter_input.as_str(),
        Mode::Normal => app.filter.as_str(),
    };

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("Filter: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(filter),
        Span::raw("   "),
        Span::styled("Sort: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(if app.sort.trim().is_empty() {
            "activity"
        } else {
            &app.sort
        }),
    ]));
    if let Some(err) = &app.last_error {
        lines.push(Line::from(vec![Span::styled(
            format!("Last error: {err}"),
            Style::default().fg(Color::Red),
        )]));
    } else {
        lines.push(Line::from(vec![Span::styled(
            keys,
            Style::default().fg(Color::DarkGray),
        )]));
    }

    let p = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

fn draw_detail_popup(f: &mut Frame<'_>, app: &AppState) {
    if app.statuses.is_empty() {
        return;
    }
    let idx = app.selected_index().min(app.statuses.len() - 1);
    let s = &app.statuses[idx];

    let popup_area = centered_rect(80, 70, f.area());
    f.render_widget(Clear, popup_area);

    let lines = vec![
        Line::from(format!("Branch: {}", s.branch)),
        Line::from(format!("Repository: {}", s.repository)),
        Line::from(format!("Path: {}", s.path)),
        Line::from(""),
        Line::from(format!(
            "Status: {}",
            status::format_status_for_table(s.status)
        )),
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
        Line::from("Press Enter to close."),
    ];

    let p = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Details"))
        .wrap(Wrap { trim: false });
    f.render_widget(p, popup_area);
}

fn centered_rect(
    percent_x: u16,
    percent_y: u16,
    r: ratatui::layout::Rect,
) -> ratatui::layout::Rect {
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

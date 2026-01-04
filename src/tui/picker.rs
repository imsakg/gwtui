#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

use crate::error::GwtuiError;
use crate::tui;

#[derive(Debug, Clone)]
pub struct PickerItem {
    pub title: String,
    pub preview: String,
}

pub fn pick_one(title: &str, items: &[PickerItem]) -> Result<usize, GwtuiError> {
    let res = run_picker(title, items, false)?;
    res.into_iter()
        .next()
        .ok_or_else(|| GwtuiError::Other("no selection".to_owned()))
}

pub fn pick_many(title: &str, items: &[PickerItem]) -> Result<Vec<usize>, GwtuiError> {
    run_picker(title, items, true)
}

fn run_picker(title: &str, items: &[PickerItem], multi: bool) -> Result<Vec<usize>, GwtuiError> {
    if items.is_empty() {
        return Err(GwtuiError::Other(
            "no items available for selection".to_owned(),
        ));
    }
    if !tui::is_tty() {
        return Err(GwtuiError::Other(
            "interactive selection requires a TTY".to_owned(),
        ));
    }

    let terminal = tui::init_terminal()?;
    let mut guard = TerminalGuard::new(terminal);

    let lower_titles: Vec<String> = items.iter().map(|i| i.title.to_lowercase()).collect();

    let mut query = String::new();
    let mut filtered: Vec<usize> = (0..items.len()).collect();
    let mut selected = 0usize;
    let mut list_state = ListState::default();
    list_state.select(Some(0));
    let mut multi_selected: BTreeSet<usize> = BTreeSet::new();
    let mut show_help = false;

    loop {
        let terminal = guard
            .terminal
            .as_mut()
            .ok_or_else(|| GwtuiError::Other("terminal unavailable".to_owned()))?;
        terminal
            .draw(|f| {
                draw_ui(
                    f,
                    title,
                    items,
                    &query,
                    &filtered,
                    selected,
                    &multi_selected,
                    multi,
                    show_help,
                    &mut list_state,
                );
            })
            .map_err(|e| GwtuiError::Other(format!("failed to draw picker: {e}")))?;

        if event::poll(Duration::from_millis(50))
            .map_err(|e| GwtuiError::Other(format!("event poll failed: {e}")))?
            && let Event::Key(key) =
                event::read().map_err(|e| GwtuiError::Other(format!("event read failed: {e}")))?
            && handle_key(
                key,
                items,
                &lower_titles,
                &mut query,
                &mut filtered,
                &mut selected,
                &mut list_state,
                &mut multi_selected,
                multi,
                &mut show_help,
            )?
        {
            // accepted
            let selection = if multi {
                if multi_selected.is_empty() {
                    vec![filtered.get(selected).copied().unwrap_or(0)]
                } else {
                    multi_selected.iter().copied().collect()
                }
            } else {
                vec![filtered.get(selected).copied().unwrap_or(0)]
            };
            return Ok(selection);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_key(
    key: KeyEvent,
    _items: &[PickerItem],
    lower_titles: &[String],
    query: &mut String,
    filtered: &mut Vec<usize>,
    selected: &mut usize,
    list_state: &mut ListState,
    multi_selected: &mut BTreeSet<usize>,
    multi: bool,
    show_help: &mut bool,
) -> Result<bool, GwtuiError> {
    // returns Ok(true) when accepted
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return Err(GwtuiError::Cancelled);
    }

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => return Err(GwtuiError::Cancelled),
        KeyCode::Char('?') => {
            *show_help = !*show_help;
        }
        KeyCode::Enter => return Ok(true),
        KeyCode::Tab => {
            if multi
                && let Some(&idx) = filtered.get(*selected)
                && !multi_selected.insert(idx)
            {
                multi_selected.remove(&idx);
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if *selected > 0 {
                *selected -= 1;
                list_state.select(Some(*selected));
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if *selected + 1 < filtered.len() {
                *selected += 1;
                list_state.select(Some(*selected));
            }
        }
        KeyCode::PageUp => {
            let page = 10;
            *selected = selected.saturating_sub(page);
            list_state.select(Some(*selected));
        }
        KeyCode::PageDown => {
            let page = 10;
            *selected = (*selected + page).min(filtered.len().saturating_sub(1));
            list_state.select(Some(*selected));
        }
        KeyCode::Backspace => {
            query.pop();
            recompute_filter(query, lower_titles, filtered, selected, list_state);
        }
        KeyCode::Char(c) => {
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT)
            {
                query.push(c);
                recompute_filter(query, lower_titles, filtered, selected, list_state);
            }
        }
        _ => {}
    }

    Ok(false)
}

fn recompute_filter(
    query: &str,
    lower_titles: &[String],
    filtered: &mut Vec<usize>,
    selected: &mut usize,
    list_state: &mut ListState,
) {
    let q = query.to_lowercase();
    if q.is_empty() {
        *filtered = (0..lower_titles.len()).collect();
    } else {
        *filtered = lower_titles
            .iter()
            .enumerate()
            .filter_map(|(i, t)| t.contains(&q).then_some(i))
            .collect();
    }

    if filtered.is_empty() {
        *filtered = (0..lower_titles.len()).collect();
    }

    if *selected >= filtered.len() {
        *selected = 0;
    }
    list_state.select(Some(*selected));
}

#[allow(clippy::too_many_arguments)]
fn draw_ui(
    f: &mut Frame<'_>,
    title: &str,
    items: &[PickerItem],
    query: &str,
    filtered: &[usize],
    selected: usize,
    multi_selected: &BTreeSet<usize>,
    multi: bool,
    show_help: bool,
    list_state: &mut ListState,
) {
    let area = f.area();
    let outer = Block::default().title(title).borders(Borders::ALL);
    let inner = outer.inner(area);
    f.render_widget(outer, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(2)])
        .split(inner);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(chunks[0]);

    let list_items: Vec<ListItem> = filtered
        .iter()
        .map(|&idx| {
            let mut line = items[idx].title.clone();
            if multi {
                let marker = if multi_selected.contains(&idx) {
                    "[x] "
                } else {
                    "[ ] "
                };
                line = format!("{marker}{line}");
            }
            ListItem::new(Line::from(line))
        })
        .collect();

    let list = List::new(list_items)
        .block(Block::default().borders(Borders::ALL).title("Items"))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">");
    f.render_stateful_widget(list, body[0], list_state);

    let preview_idx = filtered.get(selected).copied().unwrap_or(0);
    let preview = Paragraph::new(items[preview_idx].preview.clone())
        .block(Block::default().borders(Borders::ALL).title("Preview"))
        .wrap(Wrap { trim: false });
    f.render_widget(preview, body[1]);

    let help = if show_help {
        if multi {
            "j/k, ↑/↓ move • Tab toggle • Enter accept • ? help • Esc/q cancel"
        } else {
            "j/k, ↑/↓ move • Enter accept • ? help • Esc/q cancel"
        }
    } else if multi {
        "Type to filter • Tab multi-select • ? help"
    } else {
        "Type to filter • ? help"
    };

    let bottom = Paragraph::new(Line::from(vec![
        Span::styled("Query: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(query),
        Span::raw("  "),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ]));
    f.render_widget(bottom, chunks[1]);

    if show_help {
        let popup_area = centered_rect(70, 60, area);
        f.render_widget(Clear, popup_area);
        let lines = vec![
            Line::from("Keys:"),
            Line::from("  j/k, ↑/↓    Move"),
            Line::from("  PgUp/PgDn   Page"),
            Line::from("  Enter       Accept"),
            Line::from("  Esc/q       Cancel"),
            Line::from("  Backspace   Delete query char"),
            Line::from("  ?           Toggle help"),
            Line::from(if multi {
                "  Tab         Toggle selection"
            } else {
                ""
            }),
        ];
        let popup = Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .wrap(Wrap { trim: false });
        f.render_widget(popup, popup_area);
    }
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

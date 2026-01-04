#![forbid(unsafe_code)]

use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::error::GwtuiError;
use crate::tui;

pub fn run(title: &str, content: &str) -> Result<(), GwtuiError> {
    if !tui::is_tty() {
        return Err(GwtuiError::Other("log viewer requires a TTY".to_owned()));
    }

    let lines: Vec<&str> = content.lines().collect();
    let mut scroll: usize = 0;

    let terminal = tui::init_terminal()?;
    let mut guard = TerminalGuard::new(terminal);

    loop {
        let terminal = guard
            .terminal
            .as_mut()
            .ok_or_else(|| GwtuiError::Other("terminal unavailable".to_owned()))?;

        terminal
            .draw(|f| {
                let area = f.area();
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Min(1), Constraint::Length(1)])
                    .split(area);

                let block = Block::default().title(title).borders(Borders::ALL);
                let inner = block.inner(chunks[0]);
                f.render_widget(block, chunks[0]);

                let visible = inner.height as usize;
                let max_scroll = lines.len().saturating_sub(visible);
                if scroll > max_scroll {
                    scroll = max_scroll;
                }

                let text = Text::from(
                    lines
                        .iter()
                        .map(|l| Line::from((*l).to_owned()))
                        .collect::<Vec<_>>(),
                );
                let para = Paragraph::new(text)
                    .scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0))
                    .wrap(Wrap { trim: false })
                    .style(Style::default().fg(Color::White));
                f.render_widget(para, inner);

                let help =
                    Paragraph::new("q/Esc: quit • j/k: scroll • PgUp/PgDn • g/G: top/bottom")
                        .style(
                            Style::default()
                                .fg(Color::DarkGray)
                                .add_modifier(Modifier::ITALIC),
                        );
                f.render_widget(help, chunks[1]);
            })
            .map_err(|e| GwtuiError::Other(format!("failed to draw log viewer: {e}")))?;

        if event::poll(Duration::from_millis(50))
            .map_err(|e| GwtuiError::Other(format!("event poll failed: {e}")))?
            && let Event::Key(key) =
                event::read().map_err(|e| GwtuiError::Other(format!("event read failed: {e}")))?
        {
            match handle_key(key, lines.len(), &mut scroll) {
                KeyAction::Continue => {}
                KeyAction::Quit => return Ok(()),
                KeyAction::Cancelled => return Err(GwtuiError::Cancelled),
            }
        }
    }
}

enum KeyAction {
    Continue,
    Quit,
    Cancelled,
}

fn handle_key(key: KeyEvent, total_lines: usize, scroll: &mut usize) -> KeyAction {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return KeyAction::Cancelled;
    }

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => KeyAction::Quit,
        KeyCode::Char('j') | KeyCode::Down => {
            *scroll = (*scroll + 1).min(total_lines.saturating_sub(1));
            KeyAction::Continue
        }
        KeyCode::Char('k') | KeyCode::Up => {
            *scroll = scroll.saturating_sub(1);
            KeyAction::Continue
        }
        KeyCode::PageDown => {
            *scroll = (*scroll + 10).min(total_lines.saturating_sub(1));
            KeyAction::Continue
        }
        KeyCode::PageUp => {
            *scroll = scroll.saturating_sub(10);
            KeyAction::Continue
        }
        KeyCode::Char('g') => {
            *scroll = 0;
            KeyAction::Continue
        }
        KeyCode::Char('G') => {
            *scroll = total_lines.saturating_sub(1);
            KeyAction::Continue
        }
        _ => KeyAction::Continue,
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
        if let Some(term) = self.terminal.take() {
            let _ = tui::restore_terminal(term);
        }
    }
}

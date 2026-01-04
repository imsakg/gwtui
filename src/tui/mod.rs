#![forbid(unsafe_code)]

pub mod app;
pub mod log_viewer;
pub mod picker;
pub mod status_dashboard;

use std::io;
use std::io::IsTerminal as _;

use crate::error::GwtuiError;

#[must_use]
pub fn is_tty() -> bool {
    std::io::stdout().is_terminal()
}

pub fn init_terminal()
-> Result<ratatui::Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>, GwtuiError> {
    use crossterm::terminal::enable_raw_mode;
    use ratatui::backend::CrosstermBackend;

    enable_raw_mode().map_err(|e| GwtuiError::Other(format!("failed to enable raw mode: {e}")))?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)
        .map_err(|e| GwtuiError::Other(format!("failed to enter alt screen: {e}")))?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = ratatui::Terminal::new(backend)
        .map_err(|e| GwtuiError::Other(format!("failed to create terminal: {e}")))?;
    Ok(terminal)
}

pub fn restore_terminal(
    mut terminal: ratatui::Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
) -> Result<(), GwtuiError> {
    use crossterm::terminal::disable_raw_mode;

    disable_raw_mode()
        .map_err(|e| GwtuiError::Other(format!("failed to disable raw mode: {e}")))?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen
    )
    .map_err(|e| GwtuiError::Other(format!("failed to leave alt screen: {e}")))?;
    terminal
        .show_cursor()
        .map_err(|e| GwtuiError::Other(format!("failed to show cursor: {e}")))?;
    Ok(())
}

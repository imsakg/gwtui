#![forbid(unsafe_code)]

use std::io;

#[derive(Debug, Default)]
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(headers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub fn row(&mut self, cols: impl IntoIterator<Item = impl Into<String>>) {
        self.rows.push(cols.into_iter().map(Into::into).collect());
    }

    pub fn print(&self) -> io::Result<()> {
        let mut out = io::stdout().lock();
        self.write_to(&mut out)
    }

    pub fn write_csv(&self) -> io::Result<()> {
        let mut wtr = csv::Writer::from_writer(io::stdout().lock());
        wtr.write_record(&self.headers)?;
        for row in &self.rows {
            wtr.write_record(row)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn write_to(&self, mut out: impl io::Write) -> io::Result<()> {
        let mut widths = vec![0usize; self.headers.len()];
        for (i, h) in self.headers.iter().enumerate() {
            widths[i] = widths[i].max(visible_width(h));
        }
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i >= widths.len() {
                    widths.push(0);
                }
                widths[i] = widths[i].max(visible_width(cell));
            }
        }

        writeln!(&mut out, "{}", format_row(&self.headers, &widths))?;
        for row in &self.rows {
            writeln!(&mut out, "{}", format_row(row, &widths))?;
        }
        Ok(())
    }
}

fn visible_width(s: &str) -> usize {
    // Best-effort: assume each char is width 1. Ratatui UI handles widths separately.
    s.chars().count()
}

fn format_row(row: &[String], widths: &[usize]) -> String {
    let mut out = String::new();
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            out.push_str("  ");
        }
        let w = widths
            .get(i)
            .copied()
            .unwrap_or_else(|| visible_width(cell));
        out.push_str(cell);
        let pad = w.saturating_sub(visible_width(cell));
        for _ in 0..pad {
            out.push(' ');
        }
    }
    out
}

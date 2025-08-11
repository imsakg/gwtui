use gwtui::output::table::Table;

#[test]
fn plain_tables_align_wide_and_combining_unicode_by_terminal_columns() {
    let mut table = Table::new(["NAME", "N"]);
    table.row(["猫", "1"]);
    table.row(["e\u{301}", "2"]);
    let mut bytes = Vec::new();
    table.write_to(&mut bytes).unwrap();
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "NAME  N\n猫    1\ne\u{301}     2\n"
    );
}

#[test]
fn human_tables_escape_control_characters_without_corrupting_rows() {
    let mut table = Table::new(["PATH"]);
    table.row(["one\ntwo\t\u{1b}[31m"]);
    let mut bytes = Vec::new();
    table.write_to(&mut bytes).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(text.contains("one\\ntwo\\t\\u{1b}[31m"));
    assert!(!text.contains('\u{1b}'));
}

#[test]
fn csv_sink_round_trips_quotes_commas_unicode_and_newlines() {
    let mut table = Table::new(["path", "branch"]);
    table.row(["one,\"two\"\n猫", "feature"]);
    let mut bytes = Vec::new();
    table.write_csv_to(&mut bytes).unwrap();
    let mut csv = csv::Reader::from_reader(bytes.as_slice());
    let row = csv.records().next().unwrap().unwrap();
    assert_eq!(&row[0], "one,\"two\"\n猫");
    assert_eq!(&row[1], "feature");
}

#[test]
fn table_writer_errors_are_not_swallowed() {
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let table = Table::new(["header"]);
    assert!(table.write_to(Broken).is_err());
    assert!(table.write_csv_to(Broken).is_err());
}

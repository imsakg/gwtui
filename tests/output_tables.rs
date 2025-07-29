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
